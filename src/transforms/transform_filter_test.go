// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"context"
	"testing"

	"columns"
	"datablocks"
	"datatypes"
	"mocks"
	"planners"
	"processors"

	"github.com/stretchr/testify/assert"
)

func TestFilterTransform(t *testing.T) {
	tests := []struct {
		name   string
		plan   planners.IPlan
		source *datablocks.DataBlock
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			plan: planners.NewBinaryExpressionPlan(
				"=",
				planners.NewVariablePlan("name"),
				planners.NewConstantPlan("y"),
			),
			source: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 10},
				[]interface{}{"y", 10},
				[]interface{}{"z", 10},
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"y", 10},
			),
		},
		{
			name: "like",
			plan: planners.NewBinaryExpressionPlan(
				"like",
				planners.NewVariablePlan("name"),
				planners.NewConstantPlan("y%"),
			),
			source: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"xx", 10},
				[]interface{}{"yz", 11},
				[]interface{}{"yx", 12},
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"yz", 11},
				[]interface{}{"yx", 12},
			),
		},
		{
			name: "and",
			plan: planners.NewBinaryExpressionPlan(
				"AND",
				planners.NewBinaryExpressionPlan(
					"like",
					planners.NewVariablePlan("name"),
					planners.NewConstantPlan("y%"),
				),
				planners.NewBinaryExpressionPlan(
					">",
					planners.NewVariablePlan("age"),
					planners.NewConstantPlan(11),
				),
			),
			source: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"xx", 10},
				[]interface{}{"yz", 11},
				[]interface{}{"yx", 12},
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"yx", 12},
			),
		},
		{
			name: "and",
			plan: planners.NewBinaryExpressionPlan(
				"AND",
				planners.NewBinaryExpressionPlan(
					"OR",
					planners.NewBinaryExpressionPlan(
						"=",
						planners.NewVariablePlan("name"),
						planners.NewConstantPlan("x"),
					),
					planners.NewBinaryExpressionPlan(
						"=",
						planners.NewVariablePlan("name"),
						planners.NewConstantPlan("y"),
					),
				),
				planners.NewBinaryExpressionPlan(
					">",
					planners.NewVariablePlan("age"),
					planners.NewConstantPlan(10),
				),
			),
			source: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 10},
				[]interface{}{"y", 11},
				[]interface{}{"z", 12},
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"y", 11},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()
		ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

		stream := mocks.NewMockBlockInputStream(test.source)
		datasource := NewDataSourceTransform(ctx, stream)

		plan := planners.NewFilterPlan(test.plan)
		err := plan.Build()
		assert.Nil(t, err)

		filter := NewFilterTransform(ctx, plan)

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(filter)
		pipeline.Add(sink)
		pipeline.Run()

		err = pipeline.Wait(func(x interface{}) error {
			expect := test.expect
			actual := x.(*datablocks.DataBlock)
			assert.Equal(t, expect, actual)
			return nil
		})
		assert.Nil(t, err)
	}
}
