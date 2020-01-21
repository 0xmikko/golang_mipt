package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type FieldValidator struct {
	Paramname string   //- если указано - то брать из параметра с этим именем, иначе lowercase от имени
	Required  bool     //- поле не должно быть пустым (не должно иметь значение по-умолчанию)
	Enum      []string //- "одно из"
	Default   string   //- если указано и приходит пустое значение (значение по-умолчанию) - устанавливать то что написано указано в default
	Min       int      //- >= X для типа int, для строк len(str) >=
	Max       int      //- <= X для типа int

}

func isInArray(needle string, array []string) bool {
	for _, cmp := range array {
		if needle == cmp {
			return true
		}
	}
	return false
}

func GetQueryValues(r *http.Request) url.Values {
	values := r.URL.Query()

	if r.Method == "POST" {
		r.ParseForm()
		values = r.Form
	}
	return values

}

func middleWare(next http.HandlerFunc, auth bool, allowedMethods ...string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if auth && r.Header.Get("X-Auth") != "100500" {
			JSONError(w, ApiError{
				HTTPStatus: http.StatusForbidden,
				Err:        errors.New("unauthorized"),
			})
			return
		}

		if len(allowedMethods) > 0 && !isInArray(r.Method, allowedMethods) {
			JSONError(w, ApiError{
				HTTPStatus: http.StatusNotAcceptable,
				Err:        errors.New("bad method"),
			})
			return
		}

		next.ServeHTTP(w, r)

	})
}

func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	JSONError(w, ApiError{
		HTTPStatus: http.StatusNotFound,
		Err:        errors.New("unknown method"),
	})
}

func JSONError(w http.ResponseWriter, e error) {

	ae, ok := e.(ApiError)
	if !ok {
		ae = ApiError{
			HTTPStatus: http.StatusInternalServerError,
			Err:        e,
		}
	}

	w.WriteHeader(ae.HTTPStatus)
	w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, ae.Error())))

}

func JSONOK(w http.ResponseWriter, result interface{}) {

	type Response struct {
		Response interface{} `json:"response"`
		Error    string      `json:"error"`
	}

	resp := Response{
		Response: result,
	}

	json, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"cant unmarshal response"`))
	}
	w.WriteHeader(http.StatusOK)
	w.Write(json)

}

func badRequestError(str string) error {
	return ApiError{
		HTTPStatus: http.StatusBadRequest,
		Err:        errors.New(str),
	}
}

func GetAndValidateString(values url.Values, fv *FieldValidator) (string, error) {
	value := values.Get(fv.Paramname)

	if value == "" {
		if fv.Required {
			return "", badRequestError(fv.Paramname + " must me not empty")
		}
		value = fv.Default
	}

	if len(value) < fv.Min {
		return "", badRequestError(fv.Paramname + " len must be >= " + strconv.Itoa(fv.Min))
	}

	if len(fv.Enum) > 0 {
		if !isInArray(value, fv.Enum) {
			return "", badRequestError(fv.Paramname + " must be one of [" + strings.Join(fv.Enum, ", ") + "]")
		}
	}

	return value, nil
}

func GetAndValidateInt(values url.Values, fv *FieldValidator) (int, error) {
	value, err := strconv.Atoi(values.Get(fv.Paramname))
	if err != nil {
		return 0, badRequestError(fv.Paramname + " must be int")
	}

	if value < fv.Min {
		return 0, badRequestError(fv.Paramname + " must be >= " + strconv.Itoa(fv.Min))
	}

	if value > fv.Max {
		return 0, badRequestError(fv.Paramname + " must be <= " + strconv.Itoa(fv.Max))
	}

	return value, nil
}

func (srv *MyApi) CreateHandler(w http.ResponseWriter, r *http.Request) {

	query := GetQueryValues(r)

	var p CreateParams
	var err error

	p.Login, err = GetAndValidateString(query, &FieldValidator{
		Paramname: "login",
		Required:  true,
		Enum:      nil,
		Default:   "",
		Min:       10,
		Max:       0,
	})
	if err != nil {
		JSONError(w, err)
		return
	}

	p.Name, err = GetAndValidateString(query, &FieldValidator{
		Paramname: "full_name",
		Required:  false,
		Enum:      nil,
		Default:   "",
		Min:       0,
		Max:       0,
	})
	if err != nil {
		JSONError(w, err)
		return
	}

	p.Age, err = GetAndValidateInt(query, &FieldValidator{
		Paramname: "age",
		Required:  false,
		Enum:      nil,
		Default:   "",
		Min:       0,
		Max:       128,
	})
	if err != nil {
		JSONError(w, err)
		return
	}

	p.Status, err = GetAndValidateString(query, &FieldValidator{
		Paramname: "status",
		Required:  false,
		Enum:      []string{"user", "moderator", "admin"},
		Default:   "user",
		Min:       0,
		Max:       0,
	})
	if err != nil {
		JSONError(w, err)
		return
	}

	nu, err := srv.Create(context.Background(), p)
	if err != nil {
		JSONError(w, err)
		return
	}
	JSONOK(w, nu)
}

func (srv *MyApi) ProfileHandler(w http.ResponseWriter, r *http.Request) {

	query := GetQueryValues(r)

	var p ProfileParams
	var err error

	p.Login, err = GetAndValidateString(query, &FieldValidator{
		Paramname: "login",
		Required:  true,
		Enum:      nil,
		Default:   "",
		Min:       0,
		Max:       0,
	})
	if err != nil {
		JSONError(w, err)
		return
	}

	nu, err := srv.Profile(context.Background(), p)
	if err != nil {
		JSONError(w, err)
		return
	}

	JSONOK(w, nu)

}

func (srv *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := http.NewServeMux()
	s.HandleFunc("/user/profile", middleWare(srv.ProfileHandler, false))

	s.HandleFunc("/user/create", middleWare(srv.CreateHandler, true, "POST"))
	s.HandleFunc("/", NotFoundHandler)
	s.ServeHTTP(w, r)
}

func (srv *OtherApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	for _, param := range params {
		fmt.Println(param)
	}
}
