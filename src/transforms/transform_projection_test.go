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

func TestProjectionTransform(t *testing.T) {
	tests := []struct {
		name   string
		plan   *planners.ProjectionPlan
		source *datablocks.DataBlock
		expect *datablocks.DataBlock
	}{
		{
			name: "simple-all)",
			plan: planners.NewProjectPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("name"),
					planners.NewVariablePlan("age"),
				),
			),
			source: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"x", 15},
				[]interface{}{"y", 19},
				[]interface{}{"z", 20},
			),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
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
			plan: planners.NewProjectPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("age"),
				),
			),
			source: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"x", 15},
				[]interface{}{"y", 19},
				[]interface{}{"z", 20},
			),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
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
			plan: planners.NewProjectPlan(
				planners.NewMapPlan(
					planners.NewAliasedExpressionPlan("xage",
						planners.NewVariablePlan("age"),
					),
				),
			),
			source: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "age", DataType: datatypes.NewInt32DataType()},
				},
				[]interface{}{"x", 11},
				[]interface{}{"x", 15},
				[]interface{}{"y", 19},
				[]interface{}{"z", 20},
			),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "xage", DataType: datatypes.NewInt32DataType()},
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

		projection := NewProjectionTransform(ctx, test.plan)

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(projection)
		pipeline.Add(sink)
		pipeline.Run()

		var blocks []*datablocks.DataBlock
		for x := range pipeline.Out() {
			blocks = append(blocks, x.(*datablocks.DataBlock))
		}
		actual, err := datablocks.Append(blocks...)
		assert.Nil(t, err)
		expect := test.expect
		assert.Equal(t, expect, actual)
	}
}
