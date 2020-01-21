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

// COMMON
func isInArray(needle string, array []string) bool {
	for _, cmp := range array {
		if needle == cmp {
			return true
		}
	}
	return false
}

// ERRORS
var (
	ErrorUnknownTable = errors.New("unknown table")
)

// TABLE
// This struct manages all table SQL operations
type Column struct {
	Field         string
	Type          string
	Null          bool
	Key           bool
	AutoIncrement bool
}

type Table struct {
	db      *sql.DB
	table   string
	columns []Column
}

type TableI interface {
	//FindByID(id string) ([]map[string]interface{}, error)
	GetListData(offset, limit int) ([]map[string]interface{}, error)
}

func NewTable(db *sql.DB, table string) TableI {

	newTable := &Table{
		db:      db,
		table:   table,
		columns: make([]Column, 0),
	}

	rows, err := db.Query(fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", table))
	if err != nil {
		log.Fatalf("Can get columns for table %s. \n%s ", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var column Column
		var Collation, Default sql.NullString
		var Null, Key, Extra, Privileges, Comment string

		err := rows.Scan(&column.Field, &column.Type, &Collation, &Null, &Key, &Default, &Extra, &Privileges, &Comment)
		if err != nil {
			log.Fatal("Cant get table signature", err)
		}

		if Null == "YES" {
			column.Null = true
		}

		if strings.Contains(Extra, "auto_increment") {
			column.AutoIncrement = true
		}

		newTable.columns = append(newTable.columns, column)

	}

	if err = rows.Err(); err != nil {
		log.Print("Error loading tables signature for", err)
	}

	return newTable
}

func (s *Table) GetListData(offset, limit int) ([]map[string]interface{}, error) {

	result := make([]map[string]interface{}, 0)

	basicQuery := fmt.Sprintf("SELECT * FROM `%s` LIMIT ? OFFSET ? ;", s.table)
	rows, err := s.db.Query(basicQuery, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		//var extra string
		cts, _ := rows.ColumnTypes()
		cls, _ := rows.Columns()
		log.Printf("%+v %+v", cls, cts)

		response := make([]interface{}, len(cts))

		for i, ct := range cts {
			colType := ct.DatabaseTypeName()

			colType = strings.Split(colType, "(")[0]

			switch colType {
			case "INT":
				fallthrough
			case "SMALLINT":
				fallthrough
			case "TINYINT":
				pro := sql.NullInt32{
					Int32: 0,
					Valid: false,
				}
				response[i] = &pro
			case "TEXT":
				fallthrough
			case "VARCHAR":
				pro := sql.NullString{
					String: "",
					Valid:  false,
				}
				response[i] = &pro

			}

		}

		err := rows.Scan(response...)
		if err != nil {
			log.Fatal("Cant get table signature", err)
		}

		mapResponse := make(map[string]interface{})

		for i, name := range cls {
			value := response[i]

			switch value.(type) {

			case *sql.NullString:
				mapResponse[name], err = value.(*sql.NullString).Value()
				continue
			case *sql.NullInt32:
				mapResponse[name], err = value.(*sql.NullInt32).Value()
				continue
			case *sql.NullInt64:
				mapResponse[name], err = value.(*sql.NullInt64).Value()
				continue
			case *sql.NullFloat64:
				mapResponse[name], err = value.(*sql.NullFloat64).Value()
				continue
			default:
				mapResponse[name] = value
			}

		}

		result = append(result, mapResponse)
	}
	if err = rows.Err(); err != nil {
		log.Print("Error loading tables signature for", err)
	}

	return result, nil
}

// STORE

type Store struct {
	db     *sql.DB
	tables map[string]TableI
}

type StoreI interface {
	GetTablesList() []string
	GetListData(table string, offset, limit int) ([]map[string]interface{}, error)
}

func NewStore(db *sql.DB) StoreI {
	newStore := &Store{db: db}
	newStore.loadTables()
	return newStore
}

func (s *Store) loadTables() {

	s.tables = make(map[string]TableI)

	tables, err := s.getTableNames()
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		s.tables[table] = NewTable(s.db, table)
	}

}

func (s *Store) getTableNames() ([]string, error) {
	result := make([]string, 0)
	rows, err := s.db.Query("SHOW TABLES;")

	if err != nil {
		log.Print("Her", err)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		result = append(result, table)

	}
	if err = rows.Err(); err != nil {
		log.Print("Herrrr", err)
	}
	return result, nil
}

func (s *Store) isTableExists(table string) error {

	if _, ok := s.tables[table]; ok {
		return nil
	}

	return ErrorUnknownTable
}

func (s *Store) GetTablesList() []string {
	result := make([]string, len(s.tables))
	i := 0
	for key, _ := range s.tables {
		result[i] = key
		i++
	}
	return result
}

func (s *Store) GetListData(table string, offset, limit int) ([]map[string]interface{}, error) {
	err := s.isTableExists(table)
	if err != nil {
		return nil, err
	}

	return s.tables[table].GetListData(offset, limit)
}

//
// HANDLERS
//

type ApiError struct {
	HTTPStatus int
	Err        error
}

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

type Explorer struct {
	s StoreI
}

func NewDbExplorer(db *sql.DB) (*Explorer, error) {
	newService := NewStore(db)
	return &Explorer{s: newService}, nil
}

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
		switch e {
		case ErrorUnknownTable:
			ae = ApiError{
				HTTPStatus: http.StatusNotFound,
				Err:        e,
			}

		default:

			ae = ApiError{
				HTTPStatus: http.StatusInternalServerError,
				Err:        e,
			}
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

func (d *Explorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (d *Explorer) GetTablesListHandler(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]interface{})
	results["tables"] = d.s.GetTablesList()
	JSONOK(w, results)
}

func (d *Explorer) GetListByOffsetAndLimitHandler(w http.ResponseWriter, r *http.Request, table string, offset, limit int) {

	var err error
	results := make(map[string]interface{})
	results["records"], err = d.s.GetListData(table, offset, limit)
	if err != nil {
		JSONError(w, err)
		return
	}

	JSONOK(w, results)
}

func (d *Explorer) GetElementByIdHandler(w http.ResponseWriter, r *http.Request, table, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Got request for " + table + "  " + id))
}

// PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST-параметры)
func (d *Explorer) InsertNewRowHandler(w http.ResponseWriter, r *http.Request, table string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Create New Row " + table + "  "))
}

// POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST-параметры)
func (d *Explorer) UpdateRowHandler(w http.ResponseWriter, r *http.Request, table string, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Update New Row " + table + "  " + id))
}

// DELETE /$table/$id - удаляет запись
func (d *Explorer) DeleteRowHandler(w http.ResponseWriter, r *http.Request, table string, id string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Delete New Row " + table + "  " + id))
}
