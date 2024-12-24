package sqlpdb

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ByteSizedMarius/sqlp/sqlpin"
	"github.com/ByteSizedMarius/sqlp/sqlputil"
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

	ErrNotSet = errors.New("sqlp: database not set")
)

const (
	// TagName is the name of the tag to use on struct fields
	TagName        = "sql"
	AutoGenTagName = "sql-auto"
	IgnoreTagName  = "sql-ign"

	QueryReplace = "SELECT *"
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

	Repo interface {
		TableName() string
	}
)

func init() {
	fieldInfoCache = make(map[string]fieldInfo)
}

func InsertRdb[T Repo](db *sql.DB, obj T) (int, error) {
	return InsertDb(db, obj, obj.TableName())
}

func UpdateRdb[T Repo](db *sql.DB, obj T) error {
	return UpdateDb(db, obj, obj.TableName())
}

func DeleteRdb[T Repo](db *sql.DB, obj T) error {
	// get the pk from the object based on the tag
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("sqlp: expected pointer to struct")
	}

	// get the name first
	pkCol, _, err := getPkFieldInfo(v.Type())
	if err != nil {
		err = errors.Join(err, fmt.Errorf("sqlp: error getting primary key for deletion"))
		return err
	}

	// get the value
	pk := v.FieldByName(pkCol).Interface()
	return DeleteDb[T](db, pk, obj.TableName())
}

// QueryDb executes the given query using the global database handle and returns the resulting objects in a slice.
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
func QueryDb[T any](db *sql.DB, query string, args ...any) (results []T, err error) {
	rows, err := doQueryDb[T](db, query, args...)
	if err != nil {
		return
	}

	defer func() {
		err = joinOrErr(err, rows.Close())
	}()

	results, err = sliceFromRows[T](rows)
	return
}

// QueryRowDb works similar to Query except it returns only the first row from the result set.
// SetDatabase must be called before using this function.
// Check the Query function for more information.
func QueryRowDb[T any](db *sql.DB, query string, args ...any) (result T, err error) {
	rows, err := doQueryDb[T](db, query, args...)
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
	err = doScan[T](&result, rows)
	return
}

// QueryBasicDb is Query, but for basic data types.
func QueryBasicDb[T string | int | int64 | float32 | float64](db *sql.DB, query string, args ...any) (results []T, err error) {
	if strings.Contains(query, sqlpin.InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args, err = sqlpin.InQuery(query, args)
		if err != nil {
			return
		}
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

// QueryBasicRowDb is QueryRow, but for basic data types.
func QueryBasicRowDb[T string | int | int64 | float32 | float64](db *sql.DB, query string, args ...any) (result T, err error) {
	if strings.Contains(query, sqlpin.InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args, err = sqlpin.InQuery(query, args)
		if err != nil {
			return
		}
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

func InDb(db *sql.DB, query string, args ...any) (err error) {
	if !strings.Contains(query, sqlpin.InQueryReplace) {
		panic("sqlstruct: in query not found")
	}

	query, args, err = sqlpin.InQuery(query, args)
	if err != nil {
		return
	}

	_, err = db.Exec(query, args...)
	return err
}

func InsertDb[T any](db *sql.DB, obj T, table string) (int, error) {
	if db == nil {
		return 0, ErrNotSet
	}

	columnString, values, err := prepareInsert[T](obj)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnString, sqlputil.BuildPlaceholders(len(values)))

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

func UpdateDb[T any](db *sql.DB, obj T, table string) error {
	if db == nil {
		panic(ErrNotSet)
	}
	columnString, values, pkCol, err := prepareUpdate[T](obj)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s=?", table, columnString, pkCol)

	_, err = db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("sqlp: error updating %s: %w (query: %s)", table, err, query)
	}
	return nil
}

func DeleteDb[T any](db *sql.DB, pk any, table string) error {
	if db == nil {
		return ErrNotSet
	}
	v := reflect.TypeOf((*T)(nil)).Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("dest must a struct; got %T", v)
	}
	pkCol, _, err := getPkFieldInfo(v)
	if err != nil {
		err = errors.Join(err, fmt.Errorf("sqlp: error getting primary key for deletion"))
		return err
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s=?", table, pkCol)
	_, err = db.Exec(query, pk)
	if err != nil {
		return fmt.Errorf("sqlp: error deleting from %s: %w (query: %s)", table, err, query)
	}
	return nil
}

// --------

func doQueryDb[T any](db *sql.DB, query string, args ...any) (rows *sql.Rows, err error) {
	if db == nil {
		return nil, ErrNotSet
	}

	query = strings.Replace(query, QueryReplace, "SELECT "+columns[T](), 1)
	if strings.Contains(query, sqlpin.InQueryReplace) {
		if len(args) == 0 {
			return
		}
		query, args, err = sqlpin.InQuery(query, args)
		if err != nil {
			return
		}
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
		err = doScan[T](&stru, rows)
		if err != nil {
			return
		}

		slice = append(slice, stru)
	}

	return
}

func getPkFieldInfo(typ reflect.Type) (string, []int, error) {
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
			return "", nil, fmt.Errorf("sqlp: expected exactly one primary key; got %d", len(finfo))
		}
	}

	// ToDo: 1.23?
	// https://github.com/golang/go/issues/61900
	for col, idx := range finfo {
		return col, idx, nil
	}

	return "", nil, nil
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
		return fmt.Errorf("dest must be pointer to struct; got %T", destv)
	}

	// Get the dest's fieldInfo. FieldInfo maps the sql-tag to the fields index.
	fInfo := getFieldInfo(typ.Elem(), true, false)

	// Get the columns contained in the row
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Iterate the rows columns and map the column to the dest's field
	var ptrsToScanInto []any
	elem := destv.Elem()
	for _, cName := range cols {

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

func getColumns[T any](includePk bool, applyIgnore bool) []string {
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

func prepareInsert[T any](src T) (string, []any, error) {
	colNames, values, _, err := prepareColumns(src, false, false)
	if err != nil {
		return "", nil, err
	}
	return strings.Join(colNames, ", "), values, nil
}

func prepareUpdate[T any](src T) (string, []any, string, error) {
	colNames, values, pkCol, err := prepareColumns(src, false, true)
	if err != nil {
		return "", nil, "", err
	}

	return strings.Join(colNames, "=?,") + "=?", values, pkCol, nil
}

func prepareColumns[T any](src T, includePk bool, pkLast bool) ([]string, []any, string, error) {
	// Get the dest's fieldInfo. FieldInfo maps the sql-tag to the fields index.
	destv, typ, err := rft(src)
	if err != nil {
		return nil, nil, "", err
	}
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
		pkCol, pkIdx, err = getPkFieldInfo(typ)
		if err != nil {
			err = errors.Join(err, fmt.Errorf("sqlp: error getting primary key for deletion"))
			return nil, nil, "", err
		}
		values = append(values, destv.FieldByIndex(pkIdx).Interface())
	}

	return colNames, values, pkCol, nil
}

func rft[T any](src T) (reflect.Value, reflect.Type, error) {
	// reflect the value and check if dest is of the correct type
	destv := reflect.ValueOf(src)
	typ := destv.Type()
	if typ.Kind() != reflect.Struct {
		return reflect.Value{}, nil, fmt.Errorf("dest must a struct; got %T", destv)
	}
	return destv, typ, nil
}

// columns returns a string containing a sorted, comma-separated list of column names as
// defined by the type s. s must be a struct that has exported fields tagged with the "sql" tag.
func columns[T any]() string {
	return strings.Join(getColumns[T](true, false), ", ")
}
