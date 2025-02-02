package sqlpin

import (
	"fmt"
	"github.com/ByteSizedMarius/sqlp/sqlputil"
	"reflect"
	"strings"
)

const (
	InQueryReplace = "IN (*)"
)

func InQuery(query string, args []any) (string, []any, error) {
	// for now, we expect that there is only one of these.
	if strings.Count(query, InQueryReplace) > 1 {
		return "", nil, fmt.Errorf("sqlp: only one in query is supported")
	}

	// if the IN is the only argument, we can just replace it
	if (strings.Count(query, "?") + strings.Count(query, InQueryReplace)) == 1 {
		// Handle no args case
		if len(args) == 0 {
			newQuery := strings.Replace(query, InQueryReplace, "= FALSE", 1)
			return newQuery, nil, nil
		}

		// Check if the argument is a list
		v := reflect.ValueOf(args[0])
		if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
			// If it's an empty list, return FALSE
			if v.Len() == 0 {
				newQuery := strings.Replace(query, InQueryReplace, "= FALSE", 1)
				return newQuery, nil, nil
			}

			// It's a non-empty list, so flatten it to become our new args
			newQuery := strings.Replace(query, InQueryReplace, "IN ("+sqlputil.BuildPlaceholders(v.Len())+")", 1)
			return newQuery, sqlputil.ToAny(args[0]), nil
		}

		newQuery := strings.Replace(query, InQueryReplace, "IN ("+sqlputil.BuildPlaceholders(len(args))+")", 1)
		return newQuery, args, nil
	}

	// otherwise, get the index of the list in the argument list
	// flatten it and put it at the correct index

	// get the index of the inQueryReplace
	index := strings.Index(query, InQueryReplace) + 1

	// get the index of the argument in the argument list of the list for the IN
	argIndex := strings.Count(query[:index], "?")

	// get and replace the argument by flattening it
	if len(args) <= argIndex {
		return "", nil, fmt.Errorf("sqlp: not enough arguments for in query")
	}

	argList := sqlputil.ToAny(args[argIndex])
	if len(argList) == 0 {
		newQuery := strings.Replace(query, InQueryReplace, "= FALSE", 1)
		newArgs := append(args[:argIndex], args[argIndex+1:]...)
		return newQuery, newArgs, nil
	}
	newArgs := replaceWithFlatten(args, argList, argIndex)

	// edit the query
	newQuery := strings.Replace(query, InQueryReplace, "IN ("+sqlputil.BuildPlaceholders(len(argList))+")", 1)
	return newQuery, newArgs, nil
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
