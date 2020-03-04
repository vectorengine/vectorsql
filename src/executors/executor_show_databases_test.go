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

func TestShowDatabasesExecutor(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		err    string
		expect *datablocks.DataBlock
	}{
		{
			name:  "create-db",
			query: "create database db1",
		},
		{
			name:  "show databases",
			query: "show databases",
			expect: mocks.NewBlockFromSlice(
				[]*columns.Column{
					{Name: "name", DataType: datatypes.NewStringDataType()},
					{Name: "engine", DataType: datatypes.NewStringDataType()},
					{Name: "data_path", DataType: datatypes.NewStringDataType()},
					{Name: "metadata_path", DataType: datatypes.NewStringDataType()},
				},
				[]interface{}{"db1", "", "data9000/data/db1", "data9000/metadata/db1"},
				[]interface{}{"system", "SYSTEM", "data9000/data/system", "data9000/metadata/system"},
			),
		},
		{
			name:  "drop-db",
			query: "drop database db1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, cleanup := mocks.NewMock()
			defer cleanup()

			plan, err := planners.PlanFactory(test.query)
			assert.Nil(t, err)

			ctx := NewExecutorContext(mock.Ctx, mock.Log, mock.Conf, mock.Session)
			executor, err := ExecutorFactory(ctx, plan)
			assert.Nil(t, err)

			result, err := executor.Execute()
			if test.err != "" {
				assert.Equal(t, test.err, err.Error())
			} else {
				assert.Nil(t, err)
				if result.In != nil {
					for x := range result.Read() {
						expect := test.expect
						actual := x.(*datablocks.DataBlock)
						assert.True(t, mocks.DataBlockEqual(expect, actual))
					}
				}
			}
		})
	}
}
