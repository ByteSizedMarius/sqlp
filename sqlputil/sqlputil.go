package sqlputil

import (
	"reflect"
	"strings"
)

func ToAny(s any) []any {
	v := reflect.ValueOf(s)
	r := make([]any, v.Len())
	for i := 0; i < v.Len(); i++ {
		r[i] = v.Index(i).Interface()
	}
	return r
}

func BuildPlaceholders(amountOfValues int) string {
	attrs := strings.Repeat(", ?", amountOfValues)
	attrs = strings.TrimPrefix(attrs, ", ")
	return attrs
}
