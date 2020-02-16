// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package parsers

import (
	"fmt"
	"testing"

	"parsers/sqlparser"

	"github.com/davecgh/go-spew/spew"
)

func TestParse1(t *testing.T) {
	querys := []string{
		//"create table t1(a int)",
		//"select a.id, t3.x from t1 as a join t2 as b on a.id=b.id",
		//"select * from a where sum(a) > 13 and b <1",
		//"select (a+5)*8/4 from t1",
		//"select * from t1 union select * from t2",
		//"SELECT * FROM range(range_start => 1, range_end => 5) r",
		//"select name, sum(id), (id+1) from system.tables where (name='db1' or name='db2') and (id+1)>3",
		//"SELECT * FROM range(range_start => 1, range_end => 5) r where i > 10",
		//"SELECT * FROM range(1, 5) l join range(7, 10) r on l.i=r.i",
		"insert into t1(a,b)values(1,3)",
	}

	for _, query := range querys {
		node, err := Parse(query)
		fmt.Printf("%+v, %+v\n", spew.Sdump(node), err)
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
