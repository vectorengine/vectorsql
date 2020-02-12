// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"context"
	"mocks"
	"testing"

	"columns"
	"datablocks"
	"datatypes"
	"planners"
	"processors"

	"github.com/stretchr/testify/assert"
)

func TestGroupByTransform(t *testing.T) {
	tests := []struct {
		name   string
		plan   *planners.GroupByPlan
		source *datablocks.DataBlock
		expect *datablocks.DataBlock
	}{
		{
			name: "simple-(max(age) group by name)",
			plan: planners.NewGroupByPlan(
				planners.NewMapPlan(
					planners.NewVariablePlan("name"),
					planners.NewVariablePlan("age"),
				),
				nil,
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
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()
		ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

		stream := mocks.NewMockBlockInputStream(test.source)
		datasource := NewDataSourceTransform(ctx, stream)

		groupby := NewGroupByTransform(ctx, test.plan)

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(groupby)
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
