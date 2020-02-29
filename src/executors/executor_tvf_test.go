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
		name   string
		plan   *planners.TableValuedFunctionPlan
		expect *datablocks.DataBlock
	}{
		{
			name: "TableValuedFunctionExecutor-rangetable",
			plan: planners.NewTableValuedFunctionPlan("rangetable",
				planners.NewMapPlan(
					planners.NewTableValuedFunctionExpressionPlan(
						"",
						planners.NewFunctionExpressionPlan("->",
							planners.NewVariablePlan("row"),
							planners.NewConstantPlan(3),
						),
					),
					planners.NewTableValuedFunctionExpressionPlan(
						"",
						planners.NewFunctionExpressionPlan("->",
							planners.NewVariablePlan("c1"),
							planners.NewConstantPlan("UInt32"),
						),
					),
					planners.NewTableValuedFunctionExpressionPlan(
						"",
						planners.NewFunctionExpressionPlan("->",
							planners.NewVariablePlan("c2"),
							planners.NewConstantPlan("String"),
						),
					),
				),
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "c1", DataType: datatypes.NewUInt32DataType()},
					{Name: "c2", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{0, "string-0"},
				[]interface{}{1, "string-1"},
				[]interface{}{2, "string-2"},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

			for x := range pipeline.Last().In().Recv() {
				expect := test.expect
				actual := x.(*datablocks.DataBlock)
				assert.Equal(t, expect, actual)
			}
		})
	}
}
