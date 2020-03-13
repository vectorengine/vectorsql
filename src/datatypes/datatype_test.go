// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestDataType(t *testing.T) {
	tests := []struct {
		name   string
		val    datavalues.IDataValue
		expect IDataType
	}{
		{
			name:   "Int32-passed",
			val:    datavalues.MakeInt32(32),
			expect: NewInt32DataType(),
		},
		{
			name:   "Int64-passed",
			val:    datavalues.MakeInt(64),
			expect: NewInt64DataType(),
		},
		{
			name:   "Float-passed",
			val:    datavalues.MakeFloat(64.1),
			expect: NewFloat64DataType(),
		},
		{
			name:   "String-passed",
			val:    datavalues.MakeString("string"),
			expect: NewStringDataType(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := GetDataTypeByValue(test.val)
			assert.Nil(t, err)
			assert.Equal(t, test.expect, actual)
		})
	}
}
