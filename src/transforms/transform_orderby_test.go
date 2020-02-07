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

func TestOrderByTransfrom(t *testing.T) {
	tests := []struct {
		name   string
		plan   planners.IPlan
		source *datablocks.DataBlock
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			plan: planners.NewOrderByPlan(
				planners.Order{
					Expression: planners.NewVariablePlan("name"),
					Direction:  "asc",
				},
				planners.Order{
					Expression: planners.NewVariablePlan("age"),
					Direction:  "desc",
				},
			),
			source: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"z", 13},
				[]interface{}{"y", 12},
				[]interface{}{"y", 13},
			),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"y", 13},
				[]interface{}{"y", 12},
				[]interface{}{"z", 13},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()
		ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

		stream := mocks.NewMockBlockInputStream(test.source)
		datasource := NewDataSourceTransform(ctx, stream)

		orderby := NewOrderByTransform(ctx, test.plan.(*planners.OrderByPlan))

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(orderby)
		pipeline.Add(sink)
		pipeline.Run()

		err := pipeline.Wait(func(x interface{}) error {
			actual := x.(*datablocks.DataBlock)
			expect := test.expect
			assert.True(t, mocks.DataBlockEqual(actual, expect))
			return nil
		})
		assert.Nil(t, err)
	}
}
