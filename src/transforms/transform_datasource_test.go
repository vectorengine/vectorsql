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
	"processors"

	"github.com/stretchr/testify/assert"
)

func TestDataSourceTransfrom(t *testing.T) {
	tests := []struct {
		name   string
		source []interface{}
		expect *datablocks.DataBlock
	}{
		{
			name: "simple",
			source: mocks.NewSourceFromSlice(mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{"x"},
				[]interface{}{"y"},
				[]interface{}{"z"},
			)),
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{"x"},
				[]interface{}{"y"},
				[]interface{}{"z"},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()
		ctx := NewTransformContext(mock.Ctx, mock.Log, mock.Conf)

		stream := mocks.NewMockBlockInputStream(test.source)
		datasource := NewDataSourceTransform(ctx, stream)

		sink := processors.NewSink("sink")
		pipeline := processors.NewPipeline(context.Background())
		pipeline.Add(datasource)
		pipeline.Add(sink)
		pipeline.Run()

		err := pipeline.Wait(func(x interface{}) error {
			expect := test.expect
			actual := x.(*datablocks.DataBlock)
			assert.Equal(t, expect, actual)
			return nil
		})
		assert.Nil(t, err)
	}
}
