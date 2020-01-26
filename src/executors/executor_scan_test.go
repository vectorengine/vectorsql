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

func TestScanExecutor(t *testing.T) {
	tests := []struct {
		name    string
		plan    *planners.ScanPlan
		expect  *datablocks.DataBlock
		estring string
	}{
		{
			name: "ScanExecutor",
			plan: planners.NewScanPlan("databases", "system"),
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "engine", DataType: datatypes.NewStringDataType()},
					{Name: "data_path", DataType: datatypes.NewStringDataType()},
					{Name: "metadata_path", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{"system", "SYSTEM", "data9000/data/system", "data9000/metadata/system"},
			),
			estring: "\n->ScanExecutor\t--> \n->ScanNode\t--> (table=[system, databases])",
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()

		ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
		tree := NewExecutorTree(ctx)

		executor1 := NewScanExecutor(ctx, test.plan)
		tree.Add(executor1)
		executor2 := NewSinkExecutor(ctx, nil)
		tree.Add(executor2)

		pipeline, err := tree.BuildPipeline()
		assert.Nil(t, err)
		pipeline.Run()

		assert.Equal(t, test.name, executor1.Name())
		assert.Equal(t, test.estring, executor1.String())

		for x := range pipeline.Last().In().Recv() {
			expect := test.expect
			actual := x.(*datablocks.DataBlock)
			assert.Equal(t, expect, actual)
		}
	}
}
