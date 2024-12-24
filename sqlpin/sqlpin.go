package sqlpin

import (
	"fmt"
	"github.com/ByteSizedMarius/sqlp/sqlputil"
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
