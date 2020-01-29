// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"errors"
	"testing"

	"datatypes"

	"github.com/stretchr/testify/assert"
)

func TestTableValuedFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []datatypes.Value
		expect datatypes.Value
		err    error
	}{
		{
			name: "tvf-range-ok",
			fn:   FuncTableValuedFunctionRange,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(3),
			},
			expect: datatypes.MakeTuple(
				datatypes.ToValue(1),
				datatypes.ToValue(2),
			),
		},
		{
			name: "tvf-range-type-error",
			fn:   FuncTableValuedFunctionRange,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeString("x"),
			},
			err: errors.New("type.error"),
		},
		{
			name: "tvf-zip-ok",
			fn:   FuncTableValuedFunctionZip,
			args: []datatypes.Value{
				datatypes.MakeTuple(
					datatypes.ToValue(1),
					datatypes.ToValue(2),
				),
				datatypes.MakeTuple(
					datatypes.ToValue("a"),
					datatypes.ToValue("b"),
				),
				datatypes.MakeTuple(
					datatypes.ToValue(11),
					datatypes.ToValue(22),
				),
			},
			expect: datatypes.MakeTuple(
				datatypes.MakeTuple(
					datatypes.ToValue(1),
					datatypes.ToValue("a"),
					datatypes.ToValue(11),
				),
				datatypes.MakeTuple(
					datatypes.ToValue(2),
					datatypes.ToValue("b"),
					datatypes.ToValue(22),
				),
			),
		},
		{
			name: "tvf-zip-type-error",
			fn:   FuncTableValuedFunctionZip,
			args: []datatypes.Value{
				datatypes.ToValue(1),
				datatypes.ToValue(2),
			},
			err: errors.New("type.error"),
		},
	}

	for _, test := range tests {
		err := test.fn.Validator.Validate(test.args...)
		if test.err != nil {
			assert.NotNil(t, err)
			continue
		} else {
			assert.Nil(t, err)
		}

		actual, err := test.fn.Logic(test.args...)
		assert.Nil(t, err)
		assert.Equal(t, test.expect, actual)
	}
}
