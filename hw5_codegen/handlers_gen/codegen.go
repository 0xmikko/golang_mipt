package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"strconv"
	"strings"
	"text/template"
)

// код писать тут

type Func struct {
	FuncName  string
	ClassName string
	Inparams  string
	ApiParams ApiParameters
	Fields    []FieldValidator
}

type ApiParameters struct {
	URL    string
	Auth   bool
	Method string
}

type FieldValidator struct {
	FieldName string
	Type      string
	Required  bool     //- поле не должно быть пустым (не должно иметь значение по-умолчанию)
	Paramname string   //- если указано - то брать из параметра с этим именем, иначе lowercase от имени
	Enum      []string //- "одно из"
	Default   string   //- если указано и приходит пустое значение (значение по-умолчанию) - устанавливать то что написано указано в default
	Min       int      //- >= X для типа int, для строк len(str) >=
	Max       int      //- <= X для типа int

}

func parseInputFile(inFilename string) []Func {

	results := make([]Func, 0)

	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, inFilename, nil, parser.ParseComments)
	if err != nil {
		log.Fatal("Cant parse file, error at", err)
	}

	for _, f := range node.Decls {
		g, ok := f.(*ast.FuncDecl)
		if !ok {
			fmt.Printf("SKIP %T is not *ast.FuncDecl\n", f)
			continue
		}
		log.Println(g.Name, g.Doc)
		if g.Doc == nil {
			fmt.Printf("SKIP function %#v doesnt have comments\n", g.Name)
			continue
		}

		found := ""
		for _, comment := range g.Doc.List {
			if strings.HasPrefix(comment.Text, "// apigen:api") {
				found = comment.Text
			}
		}

		if found == "" {
			fmt.Printf("SKIP function %#v doesnt have apigen mark\n", g.Name)
			continue
		}

		// If we are here, it means that we found a function with apigen parameters

		fmt.Printf(")))))))))))))) API FUNCTION FOUND (((((((")

		funcApi := Func{
			FuncName:  g.Name.Name,
			ApiParams: parseApiParamsFromComments(found),
			Inparams:  getFunctionInparams(g),
		}

		// Trim prefix & Unmarshal json

		star, ok := g.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			log.Println("No star found")
			continue
		}
		ind, ok := star.X.(*ast.Ident)
		if !ok {
			log.Println("Ooops")
		}

		funcApi.ClassName = ind.Obj.Name

		funcApi.Fields = parseStruct(inFilename, funcApi.Inparams)

		fmt.Printf("\n\nPOPOPOPP====================\n%+v", funcApi)
		results = append(results, funcApi)

	}

	return results

}

func parseApiParamsFromComments(str string) ApiParameters {
	var apiParams ApiParameters

	str = strings.TrimPrefix(str, "// apigen:api")

	err := json.Unmarshal([]byte(str), &apiParams)
	if err != nil {
		log.Println("Cant parse json", str)
	}

	return apiParams
}

func getFunctionInparams(g *ast.FuncDecl) string {
	for _, param := range g.Type.Params.List {
		i, ok := param.Type.(*ast.Ident)
		if !ok {
			log.Println("Not ok in param teps")
		} else {
			return i.Name
		}
	}
	return ""
}

func parseStruct(inFilename, structName string) []FieldValidator {

	results := make([]FieldValidator, 0)

	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, inFilename, nil, parser.ParseComments)
	if err != nil {
		log.Fatal("Cant parse file, error at", err)
	}

	for _, f := range node.Decls {
		g, ok := f.(*ast.GenDecl)
		if !ok {
			fmt.Printf("SKIP %T is not *ast.FuncDecl\n", f)
			continue
		}

		for _, i := range g.Specs {
			k, ok := i.(*ast.TypeSpec)
			if !ok {
				continue
			}

			if k.Name.Name != structName {
				continue
			}

			fmt.Printf("KTYPE: %+v\n\n", k.Type)
			u, ok := k.Type.(*ast.StructType)
			if !ok {
				continue
			}

			for _, field := range u.Fields.List {
				tagValue := []string{}
				if field.Tag != nil {
					tagValue = getTags(field.Tag.Value, "apivalidator")
				}
				fmt.Printf("%+v %+v %+v\n", field.Names[0], field.Type, tagValue)

				typeName := field.Type.(*ast.Ident)

				vd, err := NewFieldValidator(field.Names[0].Name, typeName.Name, tagValue)
				fmt.Print("===+")
				if err != nil {
					log.Fatal("Incorrect parameters in apivalidator tag ")
				}
				results = append(results, *vd)
				fmt.Printf("%+v\n\n", vd)
			}

		}
	}

	return results
}

// Extract particular tag from tags string and return slice with tag values
func getTags(str, tag string) []string {
	if !strings.HasPrefix(str, "`"+tag) {
		return []string{}
	}
	str = strings.TrimPrefix(str, "`"+tag+`:"`)
	sp := strings.Split(str, `"`)
	if len(sp) < 2 {
		return []string{}
	}

	return strings.Split(sp[0], ",")
}

func NewFieldValidator(name, typeof string, params []string) (*FieldValidator, error) {
	var f FieldValidator

	// Setting default value
	f.FieldName = name
	f.Type = typeof
	f.Paramname = strings.ToLower(name)

	for _, param := range params {
		expr := strings.Split(param, "=")
		key := expr[0]
		value := ""
		if len(expr) > 1 {
			value = expr[1]
		}

		switch key {
		case "required":
			f.Required = true
		case "paramname":
			f.Paramname = value
		case "enum":
			f.Enum = strings.Split(value, "|")
		case "default":
			f.Default = value
		case "min":
			min, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			f.Min = min
		case "max":
			max, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			f.Max = max

		}
	}
	return &f, nil
}

func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		log.Fatal("Please, use codegen in.go out.go")
	}
	funcs := parseInputFile(args[0])
	generateFile(args[1], funcs)
	log.Printf("\n\n%+v", funcs)
}

var (
	funcHandlerStartTpl = template.Must(template.New("funcHandlerStartTpl").Parse(`
	// {{.FuncName}}
	func (srv *{{.ClassName}}) {{.FuncName}}Handler(w http.ResponseWriter, r *http.Request) {

	query := GetQueryValues(r)

	var p {{.Inparams}}
	var err error
`))

	validateStringTpl = template.Must(template.New("validateStringTpl").Parse(`
	// {{.FieldName}}
	p.{{.FieldName}}, err = GetAndValidateString(query, &FieldValidator{
		Paramname: "{{.Paramname}}",
		Required:  {{.Required}},
		Enum:      []string{ {{range $field := .Enum}}"{{$field}}", {{end}} },
		Default:   "{{.Default}}",
		Min:       {{.Min}},
		Max:       {{.Max}},
	})
	if err != nil {
		JSONError(w, err)
		return
	}
`))

	validateIntTpl = template.Must(template.New("validateIntTpl").Parse(`
	// {{.FieldName}}
	p.{{.FieldName}}, err = GetAndValidateInt(query, &FieldValidator{
		Paramname: "{{.Paramname}}",
		Required:  {{.Required}},
		Enum:      []string{ {{range $field := .Enum}}"$field", {{end}} },
		Default:   "{{.Default}}",
		Min:       {{.Min}},
		Max:       {{.Max}},
	})
	if err != nil {
		JSONError(w, err)
		return
	}
`))

	funcHandlerFinishTpl = template.Must(template.New("funcHandlerFinishTpl").Parse(`
	nu, err := srv.{{.FuncName}}(context.Background(), p)
	if err != nil {
	JSONError(w, err)
	return
	}
	JSONOK(w, nu)
}
`))

	ServeHTTPStartTpl = template.Must(template.New("ServeHTTPStartTpl").Parse(`
	func (srv *{{.ClassName}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := http.NewServeMux()
`))

	SimpleHandlerTpl = template.Must(template.New("SimpleHandlerTpl").Parse(`
	s.HandleFunc("{{.ApiParams.URL}}", middleWare(srv.{{.FuncName}}Handler, {{.ApiParams.Auth}}{{if .ApiParams.Method}}, "{{.ApiParams.Method}}" {{end}}))
`))

	ServeHTTPFinishTpl = template.Must(template.New("ServeHTTPFinishTpl").Parse(`

	s.HandleFunc("/", NotFoundHandler)
	s.ServeHTTP(w, r)
}
`))
)

func generateFile(outFilename string, funcs []Func) {
	out, err := os.Create(outFilename)
	if err != nil {
		log.Fatal("Cant create file", err)
	}
	defer out.Close()
	// Create FilePrefix
	out.WriteString(FilePrefix())

	classMap := make(map[string][]Func)

	for _, f := range funcs {
		funcHandlerStartTpl.Execute(out, f)
		for _, field := range f.Fields {
			switch field.Type {
			case "int":
				validateIntTpl.Execute(out, field)
			case "string":
				validateStringTpl.Execute(out, field)
			default:
				log.Fatal("Cant recognise type", field.Type)

			}

		}
		funcHandlerFinishTpl.Execute(out, f)

		classMap[f.ClassName] = append(classMap[f.ClassName], f)
	}

	for _, value := range classMap {
		ServeHTTPStartTpl.Execute(out, value[0])

		for _, v := range value {
			SimpleHandlerTpl.Execute(out, v)
		}

		ServeHTTPFinishTpl.Execute(out, value[0])
	}
}

// Generates beginning of file
func FilePrefix() string {
	return `
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

		if len(allowedMethods)>0 && !isInArray(r.Method, allowedMethods) {
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
	w.Write([]byte(fmt.Sprintf(` + "`" + `{"error": "%s"}` + "`" + `, ae.Error())))

}

func JSONOK(w http.ResponseWriter, result interface{}) {

	type Response struct {
		Response interface{} ` + "`" + `json:"response"` + "`" + `
		Error    string      ` + "`" + `json:"error"` + "`" + `
	}

	resp := Response{
		Response: result,
	}

	json, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(` + "`" + `{"error":"cant unmarshal response"` + "`" + `))
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

	if len(fv.Enum) >0 {
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
`
}
