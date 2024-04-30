# sql+

[![Go Reference](https://pkg.go.dev/badge/github.com/ByteSizedMarius/sqlp.svg)](https://pkg.go.dev/github.com/ByteSizedMarius/sqlp)

sql+ provides some convenience functions for using structs with go's database/sql package

it is fully based on [kisielk](https://github.com/kisielk)s package. I am using it for experimenting with orm-concepts and am using it for some
private projects. Simple stuff, no external dependencies.

Stuff changes and breaks fast atm. Testing is limited.

Documentation can be found at http://godoc.org/github.com/ByteSizedMarius/sqlp

## Changes made in this fork

1. Use generics instead of `interface{}`: In my opinion, using generics improves readability and allows for additional functionality.
2. Keep Language Injections intact: Intellij IDEs offer language injections that, in this case, provide support for sql-queries if literals match sql
   query patterns. This was previously not possible, because for injecting columns dynamically with sqlp, a pattern
   like `fmt.Sprintf("SELECT %s FROM ...", sqlstruct.Columns(mystruct{}))` had to be used.
3. Improved the package for my use-cases: While I would love for someone else to find use in this package, one of its main goals is to allow for the
   removal of boilerplate and redundant code in my private project by integrating patterns I often deploy. For example, some limited support
   for `IN(*)`-Select queries and `INSERT`s were added.

## Usage

This package allows linking a struct and its database-counterpart, which means that `SELECT`-queries automatically reflect changes made to the
datastructure by injecting the required columns into the query.

This works by extracting the exported fields of a struct, converting their names and inserting them into the given query. Just write the queries as
normal using the autocomplete language injections provide and let your struct-definitions and sqlp take care of the columns.
