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

func TestGroupByTransform(t *testing.T) {
	tests := []struct {
		name   string
		plan   *planners.GroupByPlan
		source []interface{}
		expect *datablocks.DataBlock
	}{
		{
			name: "simple-all)",
			plan: planners.NewGroupByPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("name"),
					planners.NewVariablePlan("age"),
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
					[]interface{}{"x", 15},
					[]interface{}{"y", 19},
					[]interface{}{"z", 20},
				)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"x", 15},
				[]interface{}{"y", 19},
				[]interface{}{"z", 20},
			),
		},
		{
			name: "simple-age",
			plan: planners.NewGroupByPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("age"),
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
					[]interface{}{"x", 15},
					[]interface{}{"y", 19},
					[]interface{}{"z", 20},
				)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{11},
				[]interface{}{15},
				[]interface{}{19},
				[]interface{}{20},
			),
		},
		{
			name: "simple-age-as-xage",
			plan: planners.NewGroupByPlan(
				planners.NewMapPlan(
					planners.NewAliasedExpressionPlan("xage",
						planners.NewVariablePlan("age"),
					),
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
					[]interface{}{"x", 15},
					[]interface{}{"y", 19},
					[]interface{}{"z", 20},
				)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "xage", DataType: datatypes.NewInt64DataType()},
				},
				[]interface{}{11},
				[]interface{}{15},
				[]interface{}{19},
				[]interface{}{20},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()
		ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

		stream := mocks.NewMockBlockInputStream(test.source)
		datasource := NewDataSourceTransform(ctx, stream)

		groupby := NewGroupByTransform(ctx, test.plan)
		projection := NewProjectionTransform(ctx, planners.NewProjectPlan(test.plan.Projects))

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(groupby)
		pipeline.Add(projection)
		pipeline.Add(sink)
		pipeline.Run()

		for x := range pipeline.Out() {
			switch x := x.(type) {
			case *datablocks.DataBlock:
				assert.True(t, mocks.DataBlockEqual(x, test.expect))
			}
		}
	}
}
