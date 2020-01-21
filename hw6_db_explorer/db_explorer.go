package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type ApiError struct {
	HTTPStatus int
	Err        error
}

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

type DB struct {
	db     *sql.DB
	tables []string
}

func NewDbExplorer(db *sql.DB) (*DB, error) {
	newDB := &DB{db: db}
	newDB.getTablesListService()
	return newDB, nil
}

func isInArray(needle string, array []string) bool {
	for _, cmp := range array {
		if needle == cmp {
			return true
		}
	}
	return false
}

func (d *DB) getTablesListService() {
	d.tables = make([]string, 0)
	rows, err := d.db.Query("SHOW TABLES;")
	if err != nil {
		log.Print("Her", err)
	}

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {

		}
		d.tables = append(d.tables, table)

	}
	if err = rows.Err(); err != nil {
		log.Print("Herrrr", err)
	}

	log.Printf("Found %d tables", len(d.tables))
}

func (d *DB) isTableExists(table string) error {
	if isInArray(table, d.tables) {
		return nil
	}
	return ApiError{
		HTTPStatus: http.StatusNotFound,
		Err:        errors.New("unknown table"),
	}
}

//
//
// SERVER PART
//
//

func JSONOK(w http.ResponseWriter, result interface{}) {

	type Response struct {
		Response interface{} `json:"response"`
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

func parseURL(url string, noIDneeded bool) (string, string, error) {
	url = strings.TrimPrefix(url, "/")
	if strings.Contains(url, "/") {
		if noIDneeded {
			return "", "", ApiError{
				HTTPStatus: http.StatusBadRequest,
				Err:        errors.New("Wrong parameters"),
			}
		}
		elms := strings.Split(url, "/")
		if len(elms) < 2 {
			return "", "", ApiError{
				HTTPStatus: http.StatusBadRequest,
				Err:        errors.New("Wrong parameters"),
			}
		}
		return elms[0], elms[1], nil
	}
	return url, "", nil
}

func getQueryParam(str, paramName string, defaultV int) (int, error) {

	queryParams, err := url.ParseQuery(str)
	if err != nil {
		return 0, ApiError{
			HTTPStatus: http.StatusInternalServerError,
			Err:        err,
		}
	}

	value := 0
	valueStr, ok := queryParams[paramName]
	if ok {
		value, err = strconv.Atoi(valueStr[0])
		if err != nil {
			return 0, ApiError{
				HTTPStatus: http.StatusInternalServerError,
				Err:        errors.New("Cant convert offset to int"),
			}
		}
	} else {
		return defaultV, nil
	}
	return value, nil
}

func (d *DB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if r.URL.Path == "/" {
			d.GetTablesListHandler(w, r)
			return
		}
		table, id, err := parseURL(r.URL.Path, false)
		if err != nil {
			JSONError(w, err)
		}
		if id == "" {

			offset, err := getQueryParam(r.URL.RawQuery, "offset", 0)
			if err != nil {
				JSONError(w, err)
				return
			}
			limit, err := getQueryParam(r.URL.RawQuery, "limit", 5)
			if err != nil {
				JSONError(w, err)
				return
			}

			d.GetListByOffsetAndLimitHandler(w, r, table, offset, limit)
			return

		}
		d.GetElementByIdHandler(w, r, table, id)

	case "PUT":
		table, _, err := parseURL(r.URL.Path, false)
		if err != nil {
			JSONError(w, err)
		}

		d.InsertNewRowHandler(w, r, table)

	case "POST":
		table, id, err := parseURL(r.URL.Path, false)
		if err != nil {
			JSONError(w, err)
		}

		d.UpdateRowHandler(w, r, table, id)

	case "DELETE":
		table, id, err := parseURL(r.URL.Path, false)
		if err != nil {
			JSONError(w, err)
		}

		d.DeleteRowHandler(w, r, table, id)
	}
}

func (d *DB) GetTablesListHandler(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]interface{})
	results["tables"] = d.tables
	JSONOK(w, results)
}

func (d *DB) GetListByOffsetAndLimitHandler(w http.ResponseWriter, r *http.Request, table string, offset, limit int) {

	err := d.isTableExists(table)
	if err != nil {
		JSONError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("List Request for " + table + "  " + strconv.Itoa(offset) + " " + strconv.Itoa(limit)))
}

func (d *DB) GetElementByIdHandler(w http.ResponseWriter, r *http.Request, table, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Got request for " + table + "  " + id))
}

// PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST-параметры)
func (d *DB) InsertNewRowHandler(w http.ResponseWriter, r *http.Request, table string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Create New Row " + table + "  "))
}

// POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST-параметры)
func (d *DB) UpdateRowHandler(w http.ResponseWriter, r *http.Request, table string, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Update New Row " + table + "  " + id))
}

// DELETE /$table/$id - удаляет запись
func (d *DB) DeleteRowHandler(w http.ResponseWriter, r *http.Request, table string, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Delete New Row " + table + "  " + id))
}
