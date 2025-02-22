// Copyright 2012 Kamil Kisiel. All rights reserved.
// Modified >2023 by Marius Schmalz
// Use of this source code is governed by the MIT
// license which can be found in the LICENSE file.

// Package sqlp is a custom orm for Go that uses reflection to map structs to database tables.
// Heavy work in progress. Breaking changes every week. No tests. No documentation. No optimization (20-30% slower than native). No guarantees.
// Do not use.
package sqlp

import (
	"database/sql"
	. "github.com/ByteSizedMarius/sqlp/sqlpdb"
)

var (
	// Global database handle to use for queries
	db *sql.DB
)

// SetDatabase sets the global database handle to be used by the Query function.
func SetDatabase(sqldb *sql.DB) {
	db = sqldb
}

// ——————————————————————————————————————————————————————————————————————————————
// Queries
// ——————————————————————————————————————————————————————————————————————————————

func Query[T any](query string, args ...any) (results []T, err error) {
	return QueryDb[T](db, query, args...)
}

func QueryRow[T any](query string, args ...any) (result T, err error) {
	return QueryRowDb[T](db, query, args...)
}

func QueryBasic[T string | int | int64 | float32 | float64](query string, args ...any) (results []T, err error) {
	return QueryBasicDb[T](db, query, args...)
}

func QueryBasicRow[T string | int | int64 | float32 | float64](query string, args ...any) (result T, err error) {
	return QueryBasicRowDb[T](db, query, args...)
}

// ——————————————————————————————————————————————————————————————————————————————
// Repo Functions
// ——————————————————————————————————————————————————————————————————————————————

// GetAll retrieves all rows from the table that the Repo type maps to.
func GetAll[T Repo]() ([]T, error) {
	return GetRdb[T](db)
}

// GetAllWhere retrieves all rows from the table that the Repo type maps to, where the where clause is true.
// The clause should start with "WHERE" or "ORDERBY".
func GetAllWhere[T Repo](where string, args ...any) ([]T, error) {
	return GetWhereRdb[T](db, where, args...)
}

// GetSingleWhere retrieves the first row from the table that the Repo type maps to that matches the where clause.
// The clause should start with "WHERE" or "ORDERBY".
func GetSingleWhere[T Repo](where string, args ...any) (res T, err error) {
	return GetSingleWhereRdb[T](db, where, args...)
}

// GetByPk retrieves a single row from the table that the Repo type maps to, where the primary key matches the given value.
func GetByPk[T Repo](pk any) (T, error) {
	return GetPkDb[T](db, pk)
}

// Insert inserts a new row into the table that the Repo type maps to.
func Insert[T Repo](obj T) (int, error) {
	return InsertDb[T](db, obj)
}

// Update updates the row in the table that the Repo type maps to.
func Update[T Repo](obj T) error {
	return UpdateDb[T](db, obj)
}

// DeleteObj deletes the row in the table that the Repo type maps to based on the primary key of the given object.
func DeleteObj[T Repo](obj T) error {
	return DeleteDb[T](db, obj)
}

// Delete deletes the row in the table that the Repo type maps to based on the given primary key.
func Delete[T Repo](pk any) error {
	return DeletePkDb[T](db, pk)
}
