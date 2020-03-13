// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"context"
	"fmt"
	"testing"

	"base/errors"
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
		name      string
		plan      planners.IPlan
		source    []interface{}
		expect    *datablocks.DataBlock
		errString string
	}{
		{
			name: "simple-pass",
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
			source: mocks.NewSourceFromSlice(
				mocks.NewBlockFromSlice(
					[]*columns.Column{
						{Name: "name", DataType: datatypes.NewStringDataType()},
						{Name: "age", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{"x", 11},
					[]interface{}{"z", 13},
					[]interface{}{"y", 12},
				),
				mocks.NewBlockFromSlice(
					[]*columns.Column{
						{Name: "name", DataType: datatypes.NewStringDataType()},
						{Name: "age", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{"x", 21},
					[]interface{}{"z", 23},
					[]interface{}{"y", 22},
				),
			),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 21},
				[]interface{}{"x", 11},
				[]interface{}{"y", 22},
				[]interface{}{"y", 12},
				[]interface{}{"z", 23},
				[]interface{}{"z", 13},
			),
		},
		{
			name: "simple-error-pass",
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
			source: mocks.NewSourceFromSlice(
				mocks.NewBlockFromSlice(
					[]*columns.Column{
						{Name: "name", DataType: datatypes.NewStringDataType()},
						{Name: "age", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{"x", 11},
				),
				errors.New("pass-by-error"),
			),
			errString: "pass-by-error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

			var actual *datablocks.DataBlock
			err := pipeline.Wait(func(x interface{}) error {
				switch x := x.(type) {
				case *datablocks.DataBlock:
					if actual == nil {
						actual = x
					} else {
						err := actual.Append(x)
						assert.Nil(t, err)
					}
				}
				return nil
			})

			expect := test.expect
			if test.errString != "" {
				assert.Equal(t, test.errString, fmt.Sprintf("%s", err))
			} else {
				assert.Nil(t, err)
				assert.True(t, mocks.DataBlockEqual(expect, actual))
				stats := orderby.(*OrderByTransform).Stats()
				assert.Equal(t, stats.TotalRowsToRead.Get(), int64(6))
			}
		})
	}
}
