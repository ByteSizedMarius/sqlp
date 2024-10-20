// Copyright 2012 Kamil Kisiel. All rights reserved.
// Modified 2023 by Marius Schmalz
// Use of this source code is governed by the MIT
// license which can be found in the LICENSE file.

/*
Package sqlp provides some convenience functions for using structs with
the Go standard library's database/sql package.

The package matches struct field names to SQL query column names. A field can
also specify a matching column with "sql" tag, if it's different from field
name.  Unexported fields or fields marked with `sql:"-"` are ignored, just like
with "encoding/json" package.

For example:
ToDo (See Readme)
*/
package sqlp

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
	// field names converted to snake case, simply assign sqlp.ToSnakeCase to the variable:
	//
	//	sqlp.NameMapper = sqlp.ToSnakeCase
	//
	// Alternatively for a custom mapping, any func(string) string can be used instead.
	NameMapper = strings.ToLower

	// A cache of fieldInfos to save reflecting every time. Inspired by encoding/xml
	fieldInfoCache     map[string]fieldInfo
	fieldInfoCacheLock sync.RWMutex

	// TagName is the name of the tag to use on struct fields
	TagName        = "sql"
	AutoGenTagName = "sql-auto"
	IgnoreTagName  = "sql-ign"

	// Global database handle to use for queries
	// Used for Insert, QueryBasic,
	db *sql.DB

	QueryReplace   = "SELECT *"
	InQueryReplace = "IN (*)"

	ErrNotSet = errors.New("sqlp: database not set")
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

// ——————————————————————————————————————————————————————————————————————————————
// Exports
// ——————————————————————————————————————————————————————————————————————————————

// SetDatabase sets the global database handle to be used by the Query function.
func SetDatabase(sqldb *sql.DB) {
	db = sqldb
}

// Scan scans the next row from rows in to a struct pointed to by dest. The struct type
// should have exported fields tagged with the "sql" tag. Columns from row which are not
// mapped to any struct fields are ignored. Struct fields which have no matching column
// in the result set are left unchanged.
// Deprecated: Use Query-functions.
func Scan[T any](dest *T, rows Rows) error {
	return doScan(dest, rows)
}

// Columns returns a string containing a sorted, comma-separated list of column names as
// defined by the type s. s must be a struct that has exported fields tagged with the "sql" tag.
// Deprecated: Use Query-functions.
func Columns[T any]() string {
	return strings.Join(cols[T](true, false), ", ")
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
//
// In addition, "IN"-queries are supported. If the query contains the InQueryReplace string,
// the function will automatically replace it with the correct amount of "?".
// For example, if you give the following query
//
//	SELECT * FROM users WHERE id IN (*)
//
// and the following arguments
//
//	Query("SELECT * FROM users WHERE id IN (*) AND name LIKE '%?'", []int{1, 2, 3}, "a")
func Query[T any](query string, args ...any) (results []T, err error) {
	rows, err := doQuery[T](query, args...)
	if err != nil {
		return
	}

	defer func() {
		err = joinOrErr(err, rows.Close())
	}()

	results, err = sliceFromRows[T](rows)
	return
}

// QueryRow works similar to Query except it returns only the first row from the result set.
// SetDatabase must be called before using this function.
// Check the Query function for more information.
func QueryRow[T any](query string, args ...any) (result T, err error) {
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
	err = Scan[T](&result, rows)
	return
}

// QueryBasic is Query, but for basic data types.
func QueryBasic[T string | int | int64 | float32 | float64](query string, args ...any) (results []T, err error) {
	if strings.Contains(query, InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args = InQuery(query, args)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return
	}

	defer func() {
		err = joinOrErr(err, rows.Close())
	}()

	for rows.Next() {
		var data T
		err = rows.Scan(&data)
		if err != nil {
			return
		}
		results = append(results, data)
	}
	return
}

// QueryBasicRow is QueryRow, but for basic data types.
func QueryBasicRow[T string | int | int64 | float32 | float64](query string, args ...any) (result T, err error) {
	if strings.Contains(query, InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args = InQuery(query, args)
	}

	rows, err := db.Query(query, args...)
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

	err = rows.Scan(&result)
	if err != nil {
		return
	}
	return result, nil
}

// Insert inserts the given object into the table and returns the last inserted id.
// Autogenerated fields can be tagged with `sql-auto:""` (AutoGenTagName) in order for them to be ignored during insert.
func Insert[T any](obj T, table string) (int, error) {
	if db == nil {
		panic(ErrNotSet)
	}

	columnString, values := prepareInsert[T](obj)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnString, inQuery(len(values)))

	res, err := db.Exec(query, values...)
	if err != nil {
		return 0, fmt.Errorf("sqlp: error inserting into %s: %w (query: %s)", table, err, query)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlp: error getting last inserted id: %w", err)
	}

	return int(id), nil
}

func Update[T any](obj T, table string) error {
	if db == nil {
		panic(ErrNotSet)
	}
	columnString, values, pkCol := prepareUpdate[T](obj)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s=?", table, columnString, pkCol)

	_, err := db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("sqlp: error updating %s: %w (query: %s)", table, err, query)
	}
	return nil
}

func Delete[T any](pk any, table string) error {
	if db == nil {
		panic(ErrNotSet)
	}
	v := reflect.TypeOf((*T)(nil)).Elem()
	if v.Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must a struct; got %T", v))
	}
	pkCol, _ := getPkFieldInfo(v)

	query := fmt.Sprintf("DELETE FROM %s WHERE %s=?", table, pkCol)
	_, err := db.Exec(query, pk)
	if err != nil {
		return fmt.Errorf("sqlp: error deleting from %s: %w (query: %s)", table, err, query)
	}
	return nil
}

func In(query string, args ...any) error {
	if !strings.Contains(query, InQueryReplace) {
		panic("sqlstruct: in query not found")
	}

	query, args = InQuery(query, args)
	_, err := db.Exec(query, args...)
	return err
}

func InQuery(query string, args []any) (string, []any) {
	// for now, we expect that there is only one of these.
	if strings.Count(query, InQueryReplace) > 1 {
		panic("sqlp: only one in query is supported")
	}

	// if the IN is the only argument, we can just replace it
	if (strings.Count(query, "?") + strings.Count(query, InQueryReplace)) == 1 {
		//if len(args) == 1 {
		//	args = ToAny(args[0])
		//} else {
		//	args = ToAny(args)
		//}
		newQuery := strings.Replace(query, InQueryReplace, "IN ("+inQuery(len(args))+")", 1)
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
		panic("sqlp: not enough arguments for in query")
	}
	argList := ToAny(args[argIndex])
	newArgs := replaceWithFlatten(args, argList, argIndex)

	// edit the query
	newQuery := strings.Replace(query, InQueryReplace, "("+inQuery(len(argList))+")", 1)
	return newQuery, newArgs
}

// ——————————————————————————————————————————————————————————————————————————————
// NameMapper
// ——————————————————————————————————————————————————————————————————————————————

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

// ——————————————————————————————————————————————————————————————————————————————
// General Helper
// ——————————————————————————————————————————————————————————————————————————————

func doQuery[T any](query string, args ...any) (rows *sql.Rows, err error) {
	if db == nil {
		panic(ErrNotSet)
	}

	query = strings.Replace(query, QueryReplace, "SELECT "+Columns[T](), 1)
	if strings.Contains(query, InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args = InQuery(query, args)
	}

	rows, err = db.Query(query, args...)
	if err != nil {
		return
	}

	return
}

// sliceFromRows returns a slice of structs from the given rows by calling Scan on each row.
func sliceFromRows[T any](rows *sql.Rows) (slice []T, err error) {
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

func getPkFieldInfo(typ reflect.Type) (string, []int) {
	fieldInfoCacheLock.RLock()
	finfo, ok := fieldInfoCache[typ.String()+AutoGenTagName]
	fieldInfoCacheLock.RUnlock()

	// if not cached, get the primary key field by reflection
	if !ok {
		finfo = make(fieldInfo)
		n := typ.NumField()
		for i := 0; i < n; i++ {
			f := typ.Field(i)
			_, isPk := f.Tag.Lookup(AutoGenTagName)
			if !isPk {
				continue
			}
			finfo[f.Name] = []int{i}
		}

		if len(finfo) != 1 {
			panic("sqlp: expected exactly one primary key")
		}
	}

	// ToDo: 1.23?
	// https://github.com/golang/go/issues/61900
	for col, idx := range finfo {
		return col, idx
	}

	return "", nil
}

// getFieldInfo creates a fieldInfo for the provided type. Fields that are not tagged
// with the "sql" tag and unexported fields are not included.
func getFieldInfo(typ reflect.Type, includePk bool, applyIgnore bool) fieldInfo {
	key := fmt.Sprintf("%s%s%t%t", typ.String(), TagName, includePk, applyIgnore)
	fieldInfoCacheLock.RLock()
	finfo, ok := fieldInfoCache[key]
	fieldInfoCacheLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(TagName)

		// check if the field has the primary key tag
		_, isPk := f.Tag.Lookup(AutoGenTagName)
		_, shouldIgnore := f.Tag.Lookup(IgnoreTagName)

		// Skip unexported fields or fields marked with "-"
		if f.PkgPath != "" || tag == "-" || (!includePk && isPk) || (applyIgnore && shouldIgnore) {
			continue
		}

		// Handle embedded structs
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			scannerType := reflect.TypeOf((*Scanner)(nil)).Elem()
			if !reflect.PointerTo(f.Type).Implements(scannerType) {
				for k, v := range getFieldInfo(f.Type, includePk, applyIgnore) {
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

	// Update cache
	fieldInfoCacheLock.Lock()
	fieldInfoCache[key] = finfo
	fieldInfoCacheLock.Unlock()

	return finfo
}

// doScan scans the next row from rows in to a struct pointed to by dest.
// The mapping of columns to struct fields is done by matching the column name to the
// struct field name or given tag.
func doScan[T any](dest *T, rows Rows) error {
	// reflect the value and check if dest is of the correct type
	destv := reflect.ValueOf(dest)
	typ := destv.Type()
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must be pointer to struct; got %T", destv))
	}

	// Get the dest's fieldInfo. FieldInfo maps the sql-tag to the fields index.
	fInfo := getFieldInfo(typ.Elem(), true, false)

	// Get the columns contained in the row
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Iterate the rows columns and map the column to the dest's field
	var ptrsToScanInto []any
	elem := destv.Elem()
	for _, cName := range columns {

		// Get the field index for the column
		idx, isMapped := fInfo[NameMapper(cName)]
		var v any

		// Check if the column is mapped to a field
		if isMapped {
			v = elem.FieldByIndex(idx).Addr().Interface()
		} else {
			// Discard the field. Needs to still be scanned because scanning is based on index.
			v = &sql.RawBytes{}
		}

		ptrsToScanInto = append(ptrsToScanInto, v)
	}

	return rows.Scan(ptrsToScanInto...)
}

func cols[T any](includePk bool, applyIgnore bool) []string {
	// ToDo: use reflect.TypeFor here, starting with Go 1.22 (?)
	var v = reflect.TypeOf((*T)(nil))
	fields := getFieldInfo(v.Elem(), includePk, applyIgnore)

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}

	// sort the names to ensure consistent ordering
	// required for the cache to work
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

func prepareInsert[T any](src T) (string, []any) {
	colNames, values, _ := prepareColumns(src, false, false)
	return strings.Join(colNames, ", "), values
}

func prepareUpdate[T any](src T) (string, []any, string) {
	colNames, values, pkCol := prepareColumns(src, false, true)
	return strings.Join(colNames, "=?,") + "=?", values, pkCol
}

func prepareColumns[T any](src T, includePk bool, pkLast bool) ([]string, []any, string) {
	// Get the dest's fieldInfo. FieldInfo maps the sql-tag to the fields index.
	destv, typ := rft(src)
	fInfo := getFieldInfo(typ, includePk, true)

	colNames := make([]string, 0, len(fInfo))
	values := make([]any, 0, len(fInfo))
	for col, idx := range fInfo {
		// add the column name to the column names slice
		colNames = append(colNames, col)

		// add the value to the values slice
		values = append(values, destv.FieldByIndex(idx).Interface())
	}

	// get the primary key column and value
	var pkCol string
	if pkLast {
		var pkIdx []int
		pkCol, pkIdx = getPkFieldInfo(typ)
		values = append(values, destv.FieldByIndex(pkIdx).Interface())
	}

	return colNames, values, pkCol
}

func rft[T any](src T) (reflect.Value, reflect.Type) {
	// reflect the value and check if dest is of the correct type
	destv := reflect.ValueOf(src)
	typ := destv.Type()
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must a struct; got %T", destv))
	}
	return destv, typ
}

// ——————————————————————————————————————————————————————————————————————————————
// In Query Helper
// ——————————————————————————————————————————————————————————————————————————————

func replaceWithFlatten(first []any, second []any, index int) []any {
	result := make([]any, 0, len(first)+len(second)-1)
	result = append(result, first[:index]...)
	result = append(result, second...)
	result = append(result, first[index+1:]...)
	return result
}

func ToAny(s any) []any {
	v := reflect.ValueOf(s)
	r := make([]any, v.Len())
	for i := 0; i < v.Len(); i++ {
		r[i] = v.Index(i).Interface()
	}
	return r
}

func inQuery(amountOfValues int) string {
	attrs := strings.Repeat(", ?", amountOfValues)
	attrs = strings.TrimPrefix(attrs, ", ")
	return attrs
}
