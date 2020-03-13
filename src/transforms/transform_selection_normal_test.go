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

func TestSelectionNormalTransfrom(t *testing.T) {
	tests := []struct {
		name   string
		plan   planners.IPlan
		source []interface{}
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			plan: planners.NewSelectionPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("name"),
				),
				planners.NewMapPlan(),
			),
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
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"y", 12},
				[]interface{}{"y", 13},
				[]interface{}{"z", 13},
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
			orderby := NewOrderByTransform(ctx,
				planners.NewOrderByPlan(
					planners.Order{
						Expression: planners.NewVariablePlan("name"),
						Direction:  "asc",
					},
				))

			selection := NewNormalSelectionTransform(ctx, test.plan.(*planners.SelectionPlan))

			sink := processors.NewSink("sink")
			pipeline := processors.NewPipeline(context.Background())
			pipeline.Add(datasource)
			pipeline.Add(selection)
			pipeline.Add(orderby)
			pipeline.Add(sink)
			pipeline.Run()

			var actual *datablocks.DataBlock
			err := pipeline.Wait(func(x interface{}) error {
				y := x.(*datablocks.DataBlock)
				if actual == nil {
					actual = y
				} else {
					actual.Append(y)
				}
				return nil
			})
			assert.Nil(t, err)
			expect := test.expect
			assert.True(t, mocks.DataBlockEqual(actual, expect))

			stats := selection.(*NormalSelectionTransform).Stats()
			assert.Equal(t, stats.TotalRowsToRead.Get(), int64(4))
		})
	}
}
