// Copyright 2012 Kamil Kisiel. All rights reserved.
// Modified 2023 by Marius Schmalz
// Use of this source code is governed by the MIT
// license which can be found in the LICENSE file.
package sqlstruct

import (
	"reflect"
	"testing"
)

type EmbeddedType struct {
	FieldE string `sql:"field_e"`
}
type NullStringTest struct {
	String string
	Valid  bool
}

// Scan is mock version of sql.Scan for NullString
func (ns *NullStringTest) Scan(value interface{}) error {
	if value == nil {
		ns.String, ns.Valid = "", false
		return nil
	}
	ns.Valid = true
	ns.String = value.(string)
	return nil
}

//goland:noinspection GoSnakeCaseUsage
type testType struct {
	FieldA  string `sql:"field_a"`
	FieldB  string `sql:"-"`       // Ignored
	FieldC  string `sql:"field_C"` // Different letter case
	Field_D string // Field name is used
	EmbeddedType
}

type testType2 struct {
	FieldA   string `sql:"field_a"`
	FieldSec string `sql:"field_sec"`
}

type testType3 struct {
	FieldA string `sql:"field_a"`
	FieldB string `sql:"-"`       // Ignored
	FieldC string `sql:"field_C"` // Different letter case
	FieldD string // Field name is used
	EmbeddedType
	NullStringTest `sql:"field_f"` //Like sql.NullString struct
}

// testRows is a mock version of sql.Rows which can only scan strings
type testRows struct {
	columns []string
	values  []interface{}
}

//goland:noinspection GoMixedReceiverTypes
func (r testRows) Scan(dest ...interface{}) error {
	for i := range r.values {
		v := reflect.ValueOf(dest[i])
		if v.Kind() != reflect.Ptr {
			panic("Not a pointer!")
		}
		if scanner, ok := dest[i].(Scanner); ok {
			return scanner.Scan(r.values[i])
		}
		switch dest[i].(type) {
		case *string:
			*(dest[i].(*string)) = r.values[i].(string)
		default:
			// Do nothing. We assume the tests only use strings here
		}
	}
	return nil
}

//goland:noinspection GoMixedReceiverTypes
func (r testRows) Columns() ([]string, error) {
	return r.columns, nil
}

//goland:noinspection GoMixedReceiverTypes
func (r *testRows) addValue(c string, v interface{}) {
	r.columns = append(r.columns, c)
	r.values = append(r.values, v)
}

func TestColumns(t *testing.T) {
	e := "field_a, field_c, field_d, field_e"
	c := Columns[testType]()

	if c != e {
		t.Errorf("expected %q got %q", e, c)
	}
}

func TestColumnsAliased(t *testing.T) {
	expected := "t1.field_a AS t1_field_a, t1.field_c AS t1_field_c, "
	expected += "t1.field_d AS t1_field_d, t1.field_e AS t1_field_e"
	actual := ColumnsAliased[testType]("t1")

	if expected != actual {
		t.Errorf("Expected %q got %q", expected, actual)
	}

	expected = "t2.field_a AS t2_field_a, t2.field_sec AS t2_field_sec"
	actual = ColumnsAliased[testType2]("t2")

	if expected != actual {
		t.Errorf("Expected %q got %q", expected, actual)
	}
}

func TestScan(t *testing.T) {
	rows := testRows{}
	rows.addValue("field_a", "a")
	rows.addValue("field_b", "b")
	rows.addValue("field_c", "c")
	rows.addValue("field_d", "d")
	rows.addValue("fieldd", "d")
	rows.addValue("field_e", "e")
	rows.addValue("field_f", "f")

	e := testType{"a", "", "c", "d", EmbeddedType{"e"}}
	e3 := testType3{"a", "", "c", "d", EmbeddedType{"e"}, NullStringTest{"f", true}}
	var r testType
	err := Scan(&r, rows)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	r3 := testType3{}
	if err := Scan(&r3, rows); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if r != e {
		t.Errorf("expected %q got %q", e, r)
	}
	if r3 != e3 {
		t.Errorf("expected %v got %v", e3, r3)
	}

}

func TestScanAliased(t *testing.T) {
	rows := testRows{}
	rows.addValue("t1_field_a", "a")
	rows.addValue("t1_field_b", "b")
	rows.addValue("t1_field_c", "c")
	rows.addValue("t1_field_d", "d")
	rows.addValue("t1_field_e", "e")
	rows.addValue("t2_field_a", "a2")
	rows.addValue("t2_field_sec", "sec")

	expected := testType{"a", "", "c", "d", EmbeddedType{"e"}}
	var actual testType
	err := ScanAliased(&actual, rows, "t1")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if expected != actual {
		t.Errorf("expected %q got %q", expected, actual)
	}

	expected2 := testType2{"a2", "sec"}
	var actual2 testType2

	err = ScanAliased(&actual2, rows, "t2")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if expected2 != actual2 {
		t.Errorf("expected %q got %q", expected2, actual2)
	}
}

func TestToSnakeCase(t *testing.T) {
	var s string
	s = ToSnakeCase("FirstName")
	if "first_name" != s {
		t.Errorf("expected first_name got %q", s)
	}

	s = ToSnakeCase("First")
	if "first" != s {
		t.Errorf("expected first got %q", s)
	}

	s = ToSnakeCase("firstName")
	if "first_name" != s {
		t.Errorf("expected first_name got %q", s)
	}
}

func TestReplaceWithFlatten(t *testing.T) {
	tests := []struct {
		name     string
		first    []any
		second   []any
		index    int
		expected []any
	}{
		{
			name:     "Standard case",
			first:    []any{"A", "B", "X", "D", "E"},
			second:   []any{"1", "2", "3"},
			index:    2,
			expected: []any{"A", "B", "1", "2", "3", "D", "E"},
		},
		{
			name:     "Index at start",
			first:    []any{"X", "B", "C", "D"},
			second:   []any{"1", "2"},
			index:    0,
			expected: []any{"1", "2", "B", "C", "D"},
		},
		{
			name:     "Index at end",
			first:    []any{"A", "B", "C", "X"},
			second:   []any{"1", "2", "3"},
			index:    3,
			expected: []any{"A", "B", "C", "1", "2", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceWithFlatten(tt.first, tt.second, tt.index)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("replaceWithFlatten(%v, %v, %d) = %v; want %v", tt.first, tt.second, tt.index, result, tt.expected)
			}
		})
	}
}

func TestDoInQuerySimple(t *testing.T) {
	query := "DELETE FROM table WHERE id IN (*)"
	values := []any{1, 2, 3}

	expectedQuery := "DELETE FROM table WHERE id IN (?, ?, ?)"
	expectedArgs := []any{1, 2, 3}

	actualQuery, actualArgs := doInQuery(query, values)

	if actualQuery != expectedQuery {
		t.Errorf("expected %q got %q", expectedQuery, actualQuery)
	}
	if !reflect.DeepEqual(actualArgs, expectedArgs) {
		t.Errorf("expected %v got %v", expectedArgs, actualArgs)
	}
}

func TestDoInQuery(t *testing.T) {
	query := "DELETE FROM table WHERE id=? AND name IN (*)"
	values := []any{0, []int{1, 2, 3}}

	expectedQuery := "DELETE FROM table WHERE id=? AND name IN (?, ?, ?)"
	expectedArgs := []any{0, 1, 2, 3}

	actualQuery, actualArgs := doInQuery(query, values)

	if actualQuery != expectedQuery {
		t.Errorf("expected %q got %q", expectedQuery, actualQuery)
	}
	if !reflect.DeepEqual(actualArgs, expectedArgs) {
		t.Errorf("expected %v got %v", expectedArgs, actualArgs)
	}
}
