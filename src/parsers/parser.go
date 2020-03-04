// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package parsers

import (
	"strings"

	"parsers/sqlparser"
)

func Parse(sql string) (sqlparser.Statement, error) {
	node, err := sqlparser.ParseStrictDDL(sql)
	if err != nil && strings.HasPrefix(strings.ToLower(sql), "insert") {
		if strings.HasSuffix(strings.ToLower(sql), "values") {
			sql += "('fill up')"
		} else {
			sql += " values('fill up')"
		}
		return sqlparser.ParseStrictDDL(sql)
	}
	return node, err
}
