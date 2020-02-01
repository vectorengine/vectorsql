// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestTableValuedFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []*datavalues.Value
		expect *datavalues.Value
		err    string
	}{
		{
			name: "tvf-range-ok",
			fn:   FuncTableValuedFunctionRange,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(3),
			},
			expect: datavalues.MakeTuple(
				datavalues.MakeTuple(datavalues.ToValue(1)),
				datavalues.MakeTuple(datavalues.ToValue(2)),
			),
		},
		{
			name: "tvf-range-type-error",
			fn:   FuncTableValuedFunctionRange,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeString("x"),
			},
			err: ("bad argument at index 1: expected type 3 but got 6"),
		},
		{
			name: "tvf-rangetable-ok",
			fn:   FuncTableValuedFunctionRangeTable,
			args: []*datavalues.Value{
				datavalues.MakeInt(3),
				datavalues.MakeString("UInt32"),
				datavalues.MakeString("String"),
			},
			expect: datavalues.MakeTuple(
				datavalues.MakeTuple(datavalues.ToValue(0), datavalues.ToValue("string-0")),
				datavalues.MakeTuple(datavalues.ToValue(1), datavalues.ToValue("string-1")),
				datavalues.MakeTuple(datavalues.ToValue(2), datavalues.ToValue("string-2")),
			),
		},
		{
			name: "tvf-rangetable-type-error",
			fn:   FuncTableValuedFunctionRangeTable,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
			},
			err: ("expected at least 2 arguments, but got 1"),
		},
		{
			name: "tvf-zip-ok",
			fn:   FuncTableValuedFunctionZip,
			args: []*datavalues.Value{
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue(2),
				),
				datavalues.MakeTuple(
					datavalues.ToValue("a"),
					datavalues.ToValue("b"),
				),
				datavalues.MakeTuple(
					datavalues.ToValue(11),
					datavalues.ToValue(22),
				),
			},
			expect: datavalues.MakeTuple(
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue("a"),
					datavalues.ToValue(11),
				),
				datavalues.MakeTuple(
					datavalues.ToValue(2),
					datavalues.ToValue("b"),
					datavalues.ToValue(22),
				),
			),
		},
		{
			name: "tvf-zip-type-error",
			fn:   FuncTableValuedFunctionZip,
			args: []*datavalues.Value{
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue(2),
				),
			},
			err: ("expected at least 2 arguments, but got 1"),
		},
	}

	for _, test := range tests {
		err := test.fn.Validator.Validate(test.args...)
		if test.err != "" {
			assert.Equal(t, test.err, err.Error())
			continue
		} else {
			assert.Nil(t, err)
		}

		actual, err := test.fn.Logic(test.args...)
		assert.Nil(t, err)
		assert.Equal(t, test.expect, actual)
	}
}

func TestTableValuedFunctionZipPerformance(t *testing.T) {
	fn := FuncTableValuedFunctionZip

	loop := 10000
	t1 := make([]*datavalues.Value, loop)
	t2 := make([]*datavalues.Value, loop)
	for i := 0; i < loop; i++ {
		t1[i] = datavalues.ToValue(i)
		t2[i] = datavalues.ToValue(i)
	}

	_, err := fn.Logic(datavalues.MakeTuple(t1...), datavalues.MakeTuple(t2...))
	assert.Nil(t, err)
}
