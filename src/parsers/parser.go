// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package parsers

import (
	"parsers/sqlparser"
)

func Parse(sql string) (sqlparser.Statement, error) {
	return sqlparser.ParseStrictDDL(sql)
}
