// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"fmt"
	"testing"

	"parsers"
	"parsers/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestSelectPlan(t *testing.T) {
	tests := []struct {
		err    error
		name   string
		query  string
		expect string
	}{
		{
			name:   "simple",
			query:  "select * from t1",
			expect: "\n->ScanNode\t--> (table=[, t1]), \n->ProjectNode\t--> (), \n->SinkNode\t-->",
		},
		{
			name:   "simple",
			query:  "select name, sum(id), (id+1) from system.tables where (name='db1' or name='db2') and (id+1)>3",
			expect: "\n->ScanNode\t--> (table=[system, tables]), \n->ProjectNode\t--> (VariableNode=[$name], FuncExpressionNode=(Func=[SUM], Args=[[VariableNode=[$id]]]), FuncExpressionNode=(Func=[+], Args=[[VariableNode=[$id] ConstantNode=<1>]])), \n->FilterNode\t--> (AndNode=(Func=[AND], Left=[OrNode=(Func=[OR], Left=[BooleanExpressionNode=(Func=[=], Args=[[VariableNode=[$name] ConstantNode=<db1>]])], Right=[BooleanExpressionNode=(Func=[=], Args=[[VariableNode=[$name] ConstantNode=<db2>]])])], Right=[BooleanExpressionNode=(Func=[>], Args=[[FuncExpressionNode=(Func=[+], Args=[[VariableNode=[$id] ConstantNode=<1>]]) ConstantNode=<3>]])])), \n->SinkNode\t-->",
		},
		{
			name:   "tvf",
			query:  "SELECT * FROM range(range_start -> 1, range_end -> 5) r",
			expect: "\n->TableValuedFunctionNode\t--> (Func=[range], Args=[TableValuedFunctionExpressionNode=(Func=[], Args=[FuncExpressionNode=(Func=[->], Args=[[VariableNode=[$range_start] ConstantNode=<1>]])]), TableValuedFunctionExpressionNode=(Func=[], Args=[FuncExpressionNode=(Func=[->], Args=[[VariableNode=[$range_end] ConstantNode=<5>]])])]), \n->ProjectNode\t--> (), \n->SinkNode\t-->",
		},
	}

	for _, test := range tests {
		fmt.Print(test.query)
		statement, err := parsers.Parse(test.query)
		assert.Nil(t, err)

		plan := NewSelectPlan(statement.(*sqlparser.Select))
		err = plan.Build()
		assert.Nil(t, err)
		fmt.Printf("%+v\n", plan)

		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
