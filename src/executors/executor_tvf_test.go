// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"mocks"
	"testing"

	"columns"
	"datablocks"
	"datatypes"
	"planners"

	"github.com/stretchr/testify/assert"
)

func TestTVFExecutor(t *testing.T) {
	tests := []struct {
		name    string
		plan    *planners.TableValuedFunctionPlan
		expect  *datablocks.DataBlock
		estring string
	}{
		{
			name: "TableValuedFunctionExecutor",
			plan: planners.NewTableValuedFunctionPlan("range",
				planners.NewMapPlan(
					planners.NewConstantPlan(1),
					planners.NewConstantPlan(5),
				),
			),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "i", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{1},
				[]interface{}{2},
				[]interface{}{3},
				[]interface{}{4},
			),
			estring: "\n->TableValuedFunctionExecutor\t--> \n->TableValuedFunctionNode\t--> (Func=[range], Args=[ConstantNode=<1>, ConstantNode=<5>])",
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()

		ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
		tree := NewExecutorTree(ctx)

		executor1 := NewTableValuedFunctionExecutor(ctx, test.plan)
		tree.Add(executor1)
		executor2 := NewSinkExecutor(ctx, nil)
		tree.Add(executor2)

		pipeline, err := tree.BuildPipeline()
		assert.Nil(t, err)
		pipeline.Run()

		assert.Equal(t, test.name, executor1.Name())
		assert.Equal(t, test.estring, executor1.String())

		for x := range pipeline.Last().In().Recv() {
			expect := test.expect
			actual := x.(*datablocks.DataBlock)
			assert.Equal(t, expect, actual)
		}
	}
}
