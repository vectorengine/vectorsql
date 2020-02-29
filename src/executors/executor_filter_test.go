// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"context"
	"testing"

	"columns"
	"datablocks"
	"datatypes"
	"mocks"
	"planners"
	"processors"
	"transforms"

	"github.com/stretchr/testify/assert"
)

func TestFilterExecutor(t *testing.T) {
	tests := []struct {
		name   string
		plan   planners.IPlan
		source []interface{}
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			plan: planners.NewFilterPlan(
				planners.NewBinaryExpressionPlan(
					"=",
					planners.NewVariablePlan("name"),
					planners.NewConstantPlan("y"),
				)),
			source: mocks.NewSourceFromSlice(
				mocks.NewBlockFromSlice(
					[]*columns.Column{
						{Name: "name", DataType: datatypes.NewStringDataType()},
						{Name: "age", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{"x", 10},
					[]interface{}{"y", 10},
					[]interface{}{"z", 10},
				)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"y", 10},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, cleanup := mocks.NewMock()
			defer cleanup()
			ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)

			stream := mocks.NewMockBlockInputStream(test.source)

			tctx := transforms.NewTransformContext(mock.Ctx, mock.Log, mock.Conf)
			datasource := transforms.NewDataSourceTransform(tctx, stream)

			filter := NewFilterExecutor(ctx, test.plan.(*planners.FilterPlan))
			transform, err := filter.Execute()
			assert.Nil(t, err)

			sink := processors.NewSink("sink")
			pipeline := processors.NewPipeline(context.Background())
			pipeline.Add(datasource)
			pipeline.Add(transform)
			pipeline.Add(sink)
			pipeline.Run()

			err = pipeline.Wait(func(x interface{}) error {
				actual := x.(*datablocks.DataBlock)
				expect := test.expect
				assert.True(t, mocks.DataBlockEqual(actual, expect))
				return nil
			})
			assert.Nil(t, err)
		})
	}
}
