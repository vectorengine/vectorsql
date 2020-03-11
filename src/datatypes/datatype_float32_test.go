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

func TestDataTypeFloat32(t *testing.T) {
	tests := []struct {
		name   string
		expect datavalues.IDataValue
		errStr string
	}{
		{
			name:   "DataTypeFloat32-passed",
			expect: datavalues.ToValue(float32(32)),
		},
		{
			name:   "DataTypeFloat32-overflow-passed",
			expect: datavalues.ToValue(math.MaxFloat32 + 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dt, err := DataTypeFactory(DataTypeFloat32Name)
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
