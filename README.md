sqlp is a custom orm for Go that uses reflection to map structs to database tables.
Heavy work in progress. Breaking changes every week. No tests. No documentation. No optimization (20-30% slower than native). No guarantees.
Do not use.

# sql+

[![Go Reference](https://pkg.go.dev/badge/github.com/ByteSizedMarius/sqlp.svg)](https://pkg.go.dev/github.com/ByteSizedMarius/sqlp)

sql+ provides some convenience functions for using structs with go's database/sql package

it is fully based on [kisielk](https://github.com/kisielk)s package. I am using it for experimenting with orm-concepts and am using it for some
private projects. Simple stuff, no external dependencies.