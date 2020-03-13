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

func TestProjectionTransfrom(t *testing.T) {
	tests := []struct {
		name   string
		plan   planners.IPlan
		source []interface{}
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			plan: planners.NewProjectPlan(planners.NewMapPlan(
				planners.NewConstantPlan("age"),
			)),
			source: mocks.NewSourceFromSlice(
				mocks.NewBlockFromSlice(
					[]*columns.Column{
						{Name: "name", DataType: datatypes.NewStringDataType()},
						{Name: "age", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{"x", 11},
					[]interface{}{"z", 13},
					[]interface{}{"y", 12},
					[]interface{}{"y", 13},
				)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{11},
				[]interface{}{13},
				[]interface{}{12},
				[]interface{}{13},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, cleanup := mocks.NewMock()
			defer cleanup()

			mock.Conf.Server.DefaultBlockSize = 3
			ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

			stream := mocks.NewMockBlockInputStream(test.source)
			datasource := NewDataSourceTransform(ctx, stream)

			projection := NewProjectionTransform(ctx, test.plan.(*planners.ProjectionPlan))

			sink := processors.NewSink("sink")
			pipeline := processors.NewPipeline(context.Background())
			pipeline.Add(datasource)
			pipeline.Add(projection)
			pipeline.Add(sink)
			pipeline.Run()

			err := pipeline.Wait(func(x interface{}) error {
				actual := x.(*datablocks.DataBlock)
				expect := test.expect
				assert.True(t, mocks.DataBlockEqual(actual, expect))
				return nil
			})
			assert.Nil(t, err)
			stats := projection.(*ProjectionTransform).Stats()
			assert.True(t, stats.TotalRowsToRead.Get() > 0)
		})
	}
}
