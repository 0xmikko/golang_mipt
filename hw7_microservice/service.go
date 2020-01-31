package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Auth struct {
	Host      string
	ACL       map[string][]string
	AdmServer AServerI
}

func getServAndName(str string) (string, string) {
	str = strings.TrimPrefix(str, "/")
	strArr := strings.Split(str, "/")
	if len(strArr) != 2 {
		log.Fatal("Error function name", str)
	}
	return strArr[0], strArr[1]

}

func isInArray(needle string, array []string) bool {

	nArea, _ := getServAndName(needle)

	for _, cmp := range array {
		if needle == cmp {
			return true
		}

		cArea, cFunc := getServAndName(cmp)
		if cFunc == "*" && cArea == nArea {
			return true
		}

	}
	return false
}

func (a *Auth) isAuthenticated(ctx context.Context, method string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	userName := md.Get("consumer")
	if len(userName) == 0 || userName[0] == "" {
		return "", status.Error(codes.Unauthenticated, "No userName")
	}

	foundACLs, ok := a.ACL[userName[0]]

	if !ok || !isInArray(method, foundACLs) {
		log.Print("Unauthorised", userName[0])
		return "", status.Error(codes.Unauthenticated, "No userName")
	}

	return userName[0], nil

}

func getHostFromContext(ctx context.Context) string {
	md, _ := peer.FromContext(ctx)
	return md.Addr.String()
}

func (a *Auth) AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	consumer, err := a.isAuthenticated(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	host := getHostFromContext(ctx)
	a.AdmServer.LogRequest(consumer, info.FullMethod, host)

	return handler(ctx, req)

}

func (a *Auth) AuthStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	ctx := ss.Context()
	consumer, err := a.isAuthenticated(ctx, info.FullMethod)
	if err != nil {
		return err
	}

	host := getHostFromContext(ctx)
	a.AdmServer.LogRequest(consumer, info.FullMethod, host)

	return handler(srv, ss)
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {

	auth := Auth{
		Host: listenAddr,
	}
	err := json.Unmarshal([]byte(ACLData), &auth.ACL)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		default:

			lis, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Fatal("Can't listen port ", err)
				return
			}

			admServer := NewAdminServer()
			auth.AdmServer = admServer

			server := grpc.NewServer(grpc.UnaryInterceptor(auth.AuthInterceptor), grpc.StreamInterceptor(auth.AuthStreamInterceptor))

			RegisterAdminServer(server, admServer)
			RegisterBizServer(server, NewBizServer())

			err = server.Serve(lis)
			if err != nil {
				log.Fatal("Can't start server ", err)
				return
			}
			return
		}
	}()
	return nil

}

type BServer struct {
}

func NewBizServer() BizServer {
	return &BServer{}

}

func (b *BServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

type AdmServer struct {
	loggers []chan Event
	stats   []*Stat

	mu *sync.Mutex
}

type AServerI interface {
	AdminServer
	LogRequest(consumer, method, host string)
}

func NewAdminServer() AServerI {
	return &AdmServer{mu: &sync.Mutex{}}
}

func (a *AdmServer) LogRequest(consumer, method, host string) {
	e := Event{
		Timestamp: 44,
		Consumer:  consumer,
		Method:    method,
		Host:      host,
	}

	a.mu.Lock()

	loggerCopy := make([]chan Event, len(a.loggers))
	copy(loggerCopy, a.loggers)

	a.mu.Unlock()

	go func() {
		for _, c := range loggerCopy {
			if c != nil {
				c <- e
			}
		}
	}()

	a.mu.Lock()
	// Update Statistic
	for _, s := range a.stats {
		s.ByConsumer[consumer]++
		s.ByMethod[method]++
	}

	a.mu.Unlock()

}

func (a *AdmServer) Logging(c *Nothing, b Admin_LoggingServer) error {

	inc := make(chan Event)

	a.mu.Lock()

	num := len(a.loggers)
	a.loggers = append(a.loggers, inc)

	a.mu.Unlock()

	for {
		select {
		case e := <-inc:
			log.Printf("Sending %+v", e)
			err := b.Send(&e)
			if err != nil {
				a.mu.Lock()
				a.loggers[num] = nil
				a.mu.Unlock()
				return err
			}

		}
	}

}

func (a *AdmServer) Statistics(in *StatInterval, out Admin_StatisticsServer) error {

	duration := time.Duration(in.IntervalSeconds) * time.Second
	ticker := time.NewTicker(duration)
	s := Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}

	a.mu.Lock()

	num := len(a.stats)
	a.stats = append(a.stats, &s)

	a.mu.Unlock()

	for tick := range ticker.C {
		a.mu.Lock()
		log.Printf("Sending stat %+v at %s", s, tick.String())
		err := out.Send(&s)

		s.ByMethod = make(map[string]uint64)
		s.ByConsumer = make(map[string]uint64)

		a.mu.Unlock()
		if err != nil {
			ticker.Stop()

			a.mu.Lock()
			a.stats[num] = nil
			a.mu.Unlock()

			return err
		}

	}
	return nil
}
