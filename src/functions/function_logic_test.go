// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"testing"

	"datatypes"

	"github.com/stretchr/testify/assert"
)

func TestLogicFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []datatypes.Value
		expect datatypes.Value
		err    error
	}{
		{
			name: "and-ok",
			fn:   FuncLogicAnd,
			args: []datatypes.Value{
				datatypes.MakeBool(true),
				datatypes.MakeBool(true),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "and-not-ok",
			fn:   FuncLogicAnd,
			args: []datatypes.Value{
				datatypes.MakeBool(true),
				datatypes.MakeBool(false),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "or-ok",
			fn:   FuncLogicOr,
			args: []datatypes.Value{
				datatypes.MakeBool(true),
				datatypes.MakeBool(false),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "and-not-ok",
			fn:   FuncLogicOr,
			args: []datatypes.Value{
				datatypes.MakeBool(false),
				datatypes.MakeBool(false),
			},
			expect: datatypes.MakeBool(false),
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
