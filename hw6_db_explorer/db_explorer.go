package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
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
	ErrorUnknownTable    = errors.New("unknown table")
	ErrorNoItemFound     = errors.New("record not found")
	ErrorWrongParameters = errors.New("wrong parameters")
)

// TABLE

type Column struct {
	Field         string
	Type          string
	Null          bool
	Key           bool
	AutoIncrement bool
	Default       bool
}

// This struct manages all table SQL operations
type Table struct {
	db      *sql.DB
	table   string
	columns []Column
	key     string
}

type TableI interface {
	FindByID(id int64) (map[string]interface{}, error)
	ListData(offset, limit int) ([]map[string]interface{}, error)
	Insert(map[string]interface{}) (string, int64, error)
	Update(int64, map[string]interface{}) (int64, error)
	Delete(id int64) (int64, error)
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
		var Type, Null, Key, Extra, Privileges, Comment string

		err := rows.Scan(&column.Field, &Type, &Collation, &Null, &Key, &Default, &Extra, &Privileges, &Comment)
		if err != nil {
			log.Fatal("Cant get table signature", err)
		}

		column.Type = strings.ToUpper(strings.Split(Type, "(")[0])

		if Null == "YES" {
			column.Null = true
		}

		if Key != "" {
			column.Key = true
			newTable.key = column.Field
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

func (s *Table) ListData(offset, limit int) ([]map[string]interface{}, error) {

	basicQuery := fmt.Sprintf("SELECT * FROM `%s` LIMIT ? OFFSET ? ;", s.table)
	rows, err := s.db.Query(basicQuery, limit, offset)
	if err != nil {
		return nil, err
	}

	return s.getRows(rows)
}

func (s *Table) FindByID(id int64) (map[string]interface{}, error) {
	basicQuery := fmt.Sprintf("SELECT * FROM `%s` WHERE %s = ? ;", s.table, s.key)
	rows, err := s.db.Query(basicQuery, id)
	if err != nil {
		return nil, err
	}

	results, err := s.getRows(rows)
	if len(results) == 0 {
		return nil, ErrorNoItemFound
	}

	return results[0], nil

}

// Returns data by ready rows request and format them into map[string]interface{}
func (s *Table) getRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0)
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
	if err := rows.Err(); err != nil {
		log.Print("Error loading tables signature for", err)
	}

	return result, nil
}

func (s *Table) Insert(data map[string]interface{}) (string, int64, error) {

	query := fmt.Sprintf("INSERT INTO `%s` SET ", s.table)
	values := make([]interface{}, 0)

	firstVal := true
	for _, field := range s.columns {
		if field.AutoIncrement {
			continue
		}
		fname := field.Field
		value, ok := data[fname]
		if ok {
			values = append(values, value)

		} else {
			if field.Null {
				values = append(values, nil)
			} else {
				switch field.Type {
				case "INT":
					fallthrough
				case "SMALLINT":
					fallthrough
				case "TINYINT":
					var vInt int
					values = append(values, vInt)
				case "TEXT":
					fallthrough
				case "VARCHAR":
					var vString string
					values = append(values, vString)
				case "FLOAT":
					var vFloat float64
					values = append(values, vFloat)
				}
			}
		}
		if !firstVal {
			query += ", "
		}
		query += fname + "=? "
		firstVal = false
	}

	log.Printf("%+v\n", query)

	result, err := s.db.Exec(query, values...)
	if err != nil {
		return "", 0, err
	}

	newID, err := result.LastInsertId()
	if err != nil {
		return "", 0, err
	}

	log.Printf("%+d\n", newID)

	return s.key, newID, nil
}

func (s *Table) Update(id int64, data map[string]interface{}) (int64, error) {

	query := fmt.Sprintf("UPDATE `%s` SET ", s.table)
	values := make([]interface{}, 0)

	firstVal := true
	for _, field := range s.columns {

		fname := field.Field
		value, ok := data[fname]

		if !ok {
			continue
		}

		// IF POST HAS KEY UPDATE - THROW AN ERROR
		if field.Key {
			return 0, ApiError{
				HTTPStatus: http.StatusBadRequest,
				Err:        errors.New("field " + field.Field + " have invalid type"),
			}

		}

		// NULL CASE
		if value == nil && !field.Null {
			return 0, ApiError{
				HTTPStatus: http.StatusBadRequest,
				Err:        errors.New("field " + field.Field + " have invalid type"),
			}
		}

		if value != nil {
			valueType := reflect.TypeOf(value).String()
			log.Println(field.Type, valueType)
			switch field.Type {
			case "INT":
				fallthrough
			case "SMALLINT":
				fallthrough
			case "TINYINT":
				log.Println("QRE")
				value, ok = value.(int)
				if !ok {
					return 0, ApiError{
						HTTPStatus: http.StatusBadRequest,
						Err:        errors.New("field " + field.Field + " have invalid type"),
					}
				}
			case "FLOAT":
				value, ok = value.(float32)
				if !ok {
					return 0, ApiError{
						HTTPStatus: http.StatusBadRequest,
						Err:        errors.New("field " + field.Field + " have invalid type"),
					}
				}

			case "TEXT":
				fallthrough
			case "VARCHAR":
				if valueType != "string" {
					return 0, ApiError{
						HTTPStatus: http.StatusBadRequest,
						Err:        errors.New("field " + field.Field + " have invalid type"),
					}
				}
			}

		}

		if ok {
			values = append(values, value)
			if !firstVal {
				query += ", "
			}
			query += fname + "=?"
			firstVal = false
		}
	}

	query += fmt.Sprintf(" WHERE %s=?", s.key)
	values = append(values, id)
	log.Printf("%+v\n", query)

	result, err := s.db.Exec(query, values...)
	if err != nil {
		return 0, err
	}

	newID, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	log.Printf("UPDATED %+d\n", newID)

	return newID, nil
}

func (s *Table) Delete(id int64) (int64, error) {
	query := fmt.Sprintf("DELETE FROM `%s` WHERE %s=? ", s.table, s.key)
	result, err := s.db.Exec(query, id)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil

}

// ****************************************************
// STORE

type Store struct {
	db     *sql.DB
	tables map[string]TableI
}

type StoreI interface {
	GetTablesList() []string
	GetListData(table string, offset, limit int) ([]map[string]interface{}, error)
	FindByID(table string, id int64) (map[string]interface{}, error)
	InsertData(table string, data map[string]interface{}) (idField string, id int64, err error)
	UpdateData(table string, id int64, data map[string]interface{}) (int64, error)
	Delete(table string, id int64) (int64, error)
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
	for key := range s.tables {
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

	return s.tables[table].ListData(offset, limit)
}

func (s *Store) FindByID(table string, id int64) (map[string]interface{}, error) {
	err := s.isTableExists(table)
	if err != nil {
		return nil, err
	}

	return s.tables[table].FindByID(id)
}

func (s *Store) InsertData(table string, data map[string]interface{}) (idField string, id int64, err error) {
	err = s.isTableExists(table)
	if err != nil {
		return idField, 0, err
	}

	return s.tables[table].Insert(data)
}

func (s *Store) UpdateData(table string, id int64, data map[string]interface{}) (int64, error) {
	err := s.isTableExists(table)
	if err != nil {
		return 0, err
	}

	return s.tables[table].Update(id, data)
}

func (s *Store) Delete(table string, id int64) (int64, error) {
	err := s.isTableExists(table)
	if err != nil {
		return 0, err
	}

	return s.tables[table].Delete(id)
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

	jsonResponse, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"cant unmarshal response"`))
	}
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)

}

func JSONError(w http.ResponseWriter, e error) {

	ae, ok := e.(ApiError)
	if !ok {
		switch e {
		case ErrorUnknownTable:
			fallthrough
		case ErrorNoItemFound:
			ae = ApiError{
				HTTPStatus: http.StatusNotFound,
				Err:        e,
			}

		case ErrorWrongParameters:
			ae = ApiError{
				HTTPStatus: http.StatusBadRequest,
				Err:        errors.New("Wrong parameters"),
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

func parseURL(url string, noIDneeded bool) (string, int64, error) {
	url = strings.Trim(url, "/")
	if strings.Contains(url, "/") {
		if noIDneeded {
			return "", 0, ErrorWrongParameters
		}
		elms := strings.Split(url, "/")
		if len(elms) < 2 {
			return "", 0, ErrorWrongParameters
		}

		id, err := strconv.ParseInt(elms[1], 0, 64)
		if err != nil {
			return "", 0, ErrorWrongParameters
		}

		return elms[0], id, nil
	}
	return url, 0, nil
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
			return defaultV, nil
		}
	} else {
		return defaultV, nil
	}
	return value, nil
}

func getPostData(r *http.Request) (map[string]interface{}, error) {
	decoder := json.NewDecoder(r.Body)
	var data map[string]interface{}
	err := decoder.Decode(&data)
	return data, err
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
		if id == 0 {

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

func (d *Explorer) GetElementByIdHandler(w http.ResponseWriter, r *http.Request, table string, id int64) {
	var err error
	results := make(map[string]interface{})
	results["record"], err = d.s.FindByID(table, id)
	if err != nil {
		JSONError(w, err)
		return
	}

	JSONOK(w, results)
}

// PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST-параметры)
func (d *Explorer) InsertNewRowHandler(w http.ResponseWriter, r *http.Request, table string) {
	results := make(map[string]interface{})

	data, err := getPostData(r)
	if err != nil {
		JSONError(w, err)
		return
	}

	idField, id, err := d.s.InsertData(table, data)
	log.Println(table, id, err)

	if err != nil {
		JSONError(w, err)
		return
	}

	results[idField] = id
	JSONOK(w, results)

}

// POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST-параметры)
func (d *Explorer) UpdateRowHandler(w http.ResponseWriter, r *http.Request, table string, id int64) {
	results := make(map[string]interface{})

	data, err := getPostData(r)
	if err != nil {
		JSONError(w, err)
		return
	}

	updated, err := d.s.UpdateData(table, id, data)

	if err != nil {
		JSONError(w, err)
		return
	}

	results["updated"] = updated
	JSONOK(w, results)
}

// DELETE /$table/$id - удаляет запись
func (d *Explorer) DeleteRowHandler(w http.ResponseWriter, r *http.Request, table string, id int64) {

	results := make(map[string]interface{})
	deleted, err := d.s.Delete(table, id)

	if err != nil {
		JSONError(w, err)
		return
	}

	results["deleted"] = deleted
	JSONOK(w, results)
}
