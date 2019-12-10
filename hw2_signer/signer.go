package hw3

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// сюда писать код
func SingleHash(in, out chan interface{}) {

	wg := &sync.WaitGroup{}
	quota := make(chan struct{}, 1)

	for val := range in {

		wg.Add(1)

		data := strconv.Itoa(val.(int))

		go func(data string, out chan interface{}) {
			defer wg.Done()

			crc32 := crc32worker(data)
			md5chan := md5worker(data, quota)
			crc32md5 := crc32worker(<-md5chan)

			result := <-crc32 + "~" + <-crc32md5

			out <- result

		}(data, out)

	}

	wg.Wait()
}

func md5worker(data string, quota chan struct{}) chan string {
	out := make(chan string, 1)
	go func(out chan<- string) {
		quota <- struct{}{}

		out <- DataSignerMd5(data)

		<-quota
	}(out)

	return out
}

func crc32worker(data string) chan string {
	out := make(chan string, 1)
	go func(out chan<- string) {
		out <- DataSignerCrc32(data)
	}(out)
	return out

}

type crc2th struct {
	th     int
	result string
}

func dcrc32(out chan crc2th, th int, data string) {

	out <- crc2th{th, DataSignerCrc32(data)}
}

func distributedCrc32(data []string) []string {

	crc2chan := make(chan crc2th, len(data))
	results := make([]string, len(data))

	for th, val := range data {
		go dcrc32(crc2chan, th, val)
	}

	for th := 0; th < len(data); th++ {
		res := <-crc2chan
		results[res.th] = res.result
	}

	return results
}

func MultiHash(in, out chan interface{}) {

	wg := &sync.WaitGroup{}

	for val := range in {

		fmt.Println("Multi Hash got ", val)

		data := val.(string)

		wg.Add(1)
		go func(data string, out chan interface{}) {
			defer wg.Done()
			inputData := make([]string, 6)
			for th := 0; th < 6; th++ {
				inputData[th] = strconv.Itoa(th) + data
			}

			resultsData := distributedCrc32(inputData)

			out <- strings.Join(resultsData, "")
		}(data, out)
	}
	wg.Wait()

}

func CombineResults(in, out chan interface{}) {

	var results []string

	for val := range in {
		results = append(results, val.(string))
	}

	sort.Strings(results)
	result := strings.Join(results, "_")

	// Join results into one string and send it to out channel
	out <- result
}

func ExecutePipeline(jobs ...job) {

	wg := &sync.WaitGroup{}
	var in chan interface{}

	for _, newJob := range jobs {

		wg.Add(1)

		out := make(chan interface{}, 100)

		go func(j job, in, out chan interface{}) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(newJob, in, out)

		// Swap channels
		in = out
	}

	wg.Wait()

}
