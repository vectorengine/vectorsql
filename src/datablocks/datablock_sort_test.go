// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"testing"
)

func TestDataBlockSort(t *testing.T) {
	/*
		tests := []struct {
			name   string
			sorter []Sorter
			expect []interface{}
			expect *datablocks.DataBlock
		}{
			{
				name: "sort",
				sorter: []Sorter{
					{column: "i", direction: "desc"},
				},
				expect: mocks.NewBlockFromSlice(
					[]columns.Column{
						{Name: "i", DataType: datatypes.NewInt32DataType()},
					},
					[]interface{}{1},
					[]interface{}{2},
					[]interface{}{3},
					[]interface{}{4},
				),
			},
		}

		for _, test := range tests {

			ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
			executor := NewSelectExecutor(ctx, plan)
			transform, err := executor.Execute()
			assert.Nil(t, err)

			for x := range transform.In().Recv() {
				expect := test.expect
				actual := x.(*datablocks.DataBlock)
				assert.Equal(t, expect, actual)
			}
		}
	*/
}
