// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"mocks"
	"testing"

	"planners"

	"github.com/stretchr/testify/assert"
)

func TestCreateTableExecutor(t *testing.T) {
	tests := []struct {
		name  string
		query string
		err   string
	}{
		{
			name:  "create-db",
			query: "create database db1",
		},
		{
			name:  "create-table",
			query: "create table db1.t1(a UInt32) Engine=Memory",
		},
		{
			name:  "create-table-exists",
			query: "create table db1.t1(a UInt32) Engine=Memory",
			err:   "db1.t1 exists",
		},
		{
			name:  "drop",
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

			transform, err := executor.Execute()
			if test.err != "" {
				assert.Equal(t, test.err, err.Error())
			} else {
				executor.String()
				assert.Nil(t, err)
				assert.Nil(t, transform)
			}
		})
	}
}
