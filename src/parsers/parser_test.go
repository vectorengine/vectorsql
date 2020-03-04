// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package parsers

import (
	"fmt"
	"testing"

	"parsers/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestParse1(t *testing.T) {
	querys := []string{
		"insert into t1(a,b)values(1,3)",
		"insert into t1 values",
		"insert into t1 FORMAT xx",
	}

	for _, query := range querys {
		_, err := Parse(query)
		assert.Nil(t, err)
	}
}

func TestParseDDL(t *testing.T) {
	querys := []string{"create table t1(a UInt32, b Int32,x String) Engine = Memory"}

	for _, query := range querys {
		ast, err := Parse(query)
		node := ast.(*sqlparser.DDL)
		fmt.Printf("%+v, %+v\n", node.TableSpec.Options, err)

		buf := sqlparser.NewTrackedBuffer(nil)
		node.Format(buf)
		fmt.Printf("%s\n", buf.String())
	}
}
