// Copyright 2012 Kamil Kisiel. All rights reserved.
// Modified 2023 by Marius Schmalz
// Use of this source code is governed by the MIT
// license which can be found in the LICENSE file.

/*
Package sqlstruct provides some convenience functions for using structs with
the Go standard library's database/sql package.

The package matches struct field names to SQL query column names. A field can
also specify a matching column with "sql" tag, if it's different from field
name.  Unexported fields or fields marked with `sql:"-"` are ignored, just like
with "encoding/json" package.

For example:
ToDo (See Readme)
*/
package sqlstruct

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var (
	// NameMapper is the function used to convert struct fields which do not have sql tags
	// into database column names.
	//
	// The default mapper converts field names to lower case. If instead you would prefer
	// field names converted to snake case, simply assign sqlstruct.ToSnakeCase to the variable:
	//
	//	sqlstruct.NameMapper = sqlstruct.ToSnakeCase
	//
	// Alternatively for a custom mapping, any func(string) string can be used instead.
	NameMapper = strings.ToLower

	// A cache of fieldInfos to save reflecting every time. Inspired by encoding/xml
	fieldInfoCache     map[string]fieldInfo
	fieldInfoCacheLock sync.RWMutex

	// TagName is the name of the tag to use on struct fields
	TagName = "sql"

	// Global database handle to use for queries
	// Used for Insert, QueryBasic,
	db *sql.DB

	QueryReplace   = "*"
	InQueryReplace = "(*)"
)

type (
	// fieldInfo is a mapping of field tag values to their indices
	fieldInfo map[string][]int

	// Rows defines the interface of types that are scannable with the Scan function.
	// It is implemented by the sql.Rows type from the standard library
	Rows interface {
		Scan(...any) error
		Columns() ([]string, error)
	}

	// Scanner is an interface used by Scan.
	Scanner interface {
		Scan(src any) error
	}
)

func init() {
	fieldInfoCache = make(map[string]fieldInfo)
}

// SetDatabase sets the global database handle to be used by the Query function.
func SetDatabase(sqldb *sql.DB) {
	db = sqldb
}

// Scan scans the next row from rows in to a struct pointed to by dest. The struct type
// should have exported fields tagged with the "sql" tag. Columns from row which are not
// mapped to any struct fields are ignored. Struct fields which have no matching column
// in the result set are left unchanged.
func Scan[T any](dest *T, rows Rows) error {
	return doScan(dest, rows)
}

// Columns returns a string containing a sorted, comma-separated list of column names as
// defined by the type s. s must be a struct that has exported fields tagged with the "sql" tag.
func Columns[T any]() string {
	return strings.Join(cols[T](), ", ")
}

// Query executes the given query using the global database handle and returns the resulting objects in a slice.
// SetDatabase must be called before using this function.
// The query should use the QueryReplace (* by default) string to indicate where the columns from the struct type T should be inserted.
//
// For example for the following struct:
//
//	type User struct {
//		ID   int
//		Name string
//	}
//
// and the following query
//
//	SELECT * FROM users WHERE id = ?
//
// the query sent to the database will be
//
//	SELECT id, name FROM users WHERE id = ?
//
// and a list of User objects will be returned.
func Query[T any](query string, args ...any) (slice []T, err error) {
	rows, err := doQuery[T](query, args...)
	if err != nil {
		return
	}

	defer func() {
		err = joinOrErr(err, rows.Close())
	}()

	slice, err = SliceFromRows[T](rows)
	return
}

// QueryRow works similar to Query except it returns only the first row from the result set.
// SetDatabase must be called before using this function.
// The query should use the QueryReplace (* by default) string to indicate where the columns from the struct type T should be inserted.
func QueryRow[T any](query string, args ...any) (stru T, err error) {
	rows, err := doQuery[T](query, args...)
	if err != nil {
		return
	}

	defer func() {
		err = joinOrErr(err, rows.Close())
	}()

	if !rows.Next() {
		err = sql.ErrNoRows
		return
	}
	err = Scan[T](&stru, rows)
	return
}

// Deprecated: Use QueryBasic instead.
func QueryInts(query string, args ...any) (results []int, err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}

	var result []int
	for rows.Next() {
		var num int
		err = rows.Scan(&num)
		if err != nil {
			return nil, err
		}
		result = append(result, num)
	}
	return result, nil
}

// QueryBasic is Query, but for basic data types.
func QueryBasic[T string | int | int64 | float32 | float64](query string, args ...any) (results []T, err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}

	var result []T
	for rows.Next() {
		var data T
		err = rows.Scan(&data)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}

func In(query string, args ...any) error {
	if !strings.Contains(query, InQueryReplace) {
		panic("sqlstruct: in query not found")
	}

	doInQuery(query, args)
	_, err := db.Exec(query, args...)
	return err
}

func doInQuery(query string, args []any) (string, []any) {
	// for now, we expect that there is only one of these.
	if strings.Count(query, InQueryReplace) > 1 {
		panic("sqlstruct: only one in query is supported")
	}

	// if the IN is the only argument, we can just replace it
	if strings.Count(query, "?")+strings.Count(query, "(*)") == 1 {
		newQuery := strings.Replace(query, InQueryReplace, "("+inQuery(len(args))+")", 1)
		return newQuery, args
	}

	// otherwise, get the index of the list in the argument list
	// flatten it and put it at the correct index

	// get the index of the inQueryReplace
	index := strings.Index(query, InQueryReplace) + 1

	// get the index of the argument in the argument list of the list for the IN
	argIndex := strings.Count(query[:index], "?")

	// get and replace the argument by flattening it
	if len(args) <= argIndex {
		panic("sqlstruct: not enough arguments for in query")
	}
	argList := toAny(args[argIndex])
	newArgs := replaceWithFlatten(args, argList, argIndex)

	// edit the query
	newQuery := strings.Replace(query, InQueryReplace, "("+inQuery(len(argList))+")", 1)
	return newQuery, newArgs
}

func replaceWithFlatten(first []any, second []any, index int) []any {
	result := make([]any, 0, len(first)+len(second)-1)
	result = append(result, first[:index]...)
	result = append(result, second...)
	result = append(result, first[index+1:]...)
	return result
}

func inQuery(amountOfValues int) string {
	attrs := strings.Repeat(", ?", amountOfValues)
	attrs = strings.TrimPrefix(attrs, ", ")
	return attrs
}

func toAny(s any) []any {
	v := reflect.ValueOf(s)
	r := make([]any, v.Len())
	for i := 0; i < v.Len(); i++ {
		r[i] = v.Index(i).Interface()
	}
	return r
}

func doQuery[T any](query string, args ...any) (rows *sql.Rows, err error) {
	if db == nil {
		err = errors.New("sqlstruct: database not set")
		return
	}

	query = strings.Replace(query, QueryReplace, Columns[T](), 1)
	if strings.Contains(query, InQueryReplace) {
		doInQuery(query, args)
	}

	rows, err = db.Query(
		query,
		args...,
	)
	if err != nil {
		return
	}

	return
}

// SliceFromRows returns a slice of structs from the given rows by calling Scan on each row.
func SliceFromRows[T any](rows *sql.Rows) (slice []T, err error) {
	for rows.Next() {
		var stru T
		err = Scan[T](&stru, rows)
		if err != nil {
			return
		}

		slice = append(slice, stru)
	}

	return
}

// ToSnakeCase converts a string to snake case, words separated with underscores.
// It's intended to be used with NameMapper to map struct field names to snake case database fields.
func ToSnakeCase(src string) string {
	thisUpper := false
	prevUpper := false

	buf := bytes.NewBufferString("")
	for i, v := range src {
		if v >= 'A' && v <= 'Z' {
			thisUpper = true
		} else {
			thisUpper = false
		}
		if i > 0 && thisUpper && !prevUpper {
			buf.WriteRune('_')
		}
		prevUpper = thisUpper
		buf.WriteRune(v)
	}
	return strings.ToLower(buf.String())
}

// getFieldInfo creates a fieldInfo for the provided type. Fields that are not tagged
// with the "sql" tag and unexported fields are not included.
func getFieldInfo(typ reflect.Type) fieldInfo {
	fieldInfoCacheLock.RLock()
	finfo, ok := fieldInfoCache[typ.String()+TagName]
	fieldInfoCacheLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(TagName)

		// Skip unexported fields or fields marked with "-"
		if f.PkgPath != "" || tag == "-" {
			continue
		}

		// Handle embedded structs
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			// Check what is struct not sql Null type like sql.NullString sql.NullBool sql.Null...
			scannerType := reflect.TypeOf((*Scanner)(nil)).Elem()
			if !reflect.PointerTo(f.Type).Implements(scannerType) {
				for k, v := range getFieldInfo(f.Type) {
					finfo[k] = append([]int{i}, v...)
				}
				continue
			}
		}

		// Use field name for untagged fields
		if tag == "" {
			tag = f.Name
		}
		tag = NameMapper(tag)

		finfo[tag] = []int{i}
	}

	fieldInfoCacheLock.Lock()
	fieldInfoCache[typ.String()+TagName] = finfo
	fieldInfoCacheLock.Unlock()

	return finfo
}

func doScan[T any](dest *T, rows Rows) error {
	destv := reflect.ValueOf(dest)
	typ := destv.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must be pointer to struct; got %T", destv))
	}
	fInfo := getFieldInfo(typ.Elem())

	elem := destv.Elem()
	var values []interface{}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, name := range columns {
		idx, ok := fInfo[NameMapper(name)]
		var v interface{}
		if !ok {
			// There is no field mapped to this column, so we discard it
			v = &sql.RawBytes{}
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
		}
		values = append(values, v)
	}

	return rows.Scan(values...)
}

func cols[T any]() []string {
	// ToDo: use reflect.TypeFor here, starting with Go 1.22 (?)
	var v = reflect.TypeOf((*T)(nil))
	fields := getFieldInfo(v.Elem())

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}

	sort.Strings(names)
	return names
}

func joinOrErr(err, nErr error) error {
	if nErr != nil {
		if err == nil {
			err = nErr
		} else {
			err = errors.Join(err, nErr)
		}
	}
	return err
}
