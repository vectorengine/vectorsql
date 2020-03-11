// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"bytes"
	"math"
	"testing"

	"base/binary"
	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestDataTypeInt32(t *testing.T) {
	tests := []struct {
		name   string
		expect datavalues.IDataValue
		errStr string
	}{
		{
			name:   "DataTypeInt32-passed",
			expect: datavalues.ToValue(32),
		},
		{
			name:   "DataTypeInt32-overflow-passed",
			expect: datavalues.ToValue(math.MaxInt32 + 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dt, err := DataTypeFactory(DataTypeInt32Name)
			assert.Nil(t, err)

			buf := &bytes.Buffer{}
			err = dt.Serialize(binary.NewWriter(buf), test.expect)
			assert.Nil(t, err)
			err = dt.SerializeText(binary.NewWriter(buf), test.expect)
			assert.Nil(t, err)

			actual, err := dt.Deserialize(binary.NewReader(buf))
			assert.Nil(t, err)
			assert.Equal(t, test.expect, actual)
		})
	}
}
