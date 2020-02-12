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

func TestShowTablessExecutor(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		err    string
		expect *datablocks.DataBlock
	}{

		{
			name:  "show tables",
			query: "show tables where `engine` like '%SYSTEM_%' and name like '%tab%' limit 2",
			expect: mocks.NewBlockFromSlice(
				[]columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "database", DataType: datatypes.NewStringDataType()},
					{Name: "engine", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{"databases", "system", "SYSTEM_DATABASES"},
				[]interface{}{"tables", "system", "SYSTEM_TABLES"},
			),
		},
	}

	for _, test := range tests {
		mock, cleanup := mocks.NewMock()
		defer cleanup()

		plan, err := planners.PlanFactory(test.query)
		assert.Nil(t, err)

		ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
		executor, err := ExecutorFactory(ctx, plan)
		assert.Nil(t, err)

		transform, err := executor.Execute()
		if test.err != "" {
			assert.Equal(t, test.err, err.Error())
		} else {
			assert.Nil(t, err)
			if transform != nil {
				for x := range transform.In().Recv() {
					expect := test.expect
					actual := x.(*datablocks.DataBlock)
					assert.True(t, mocks.DataBlockEqual(expect, actual))
				}
			}
		}
	}
}
