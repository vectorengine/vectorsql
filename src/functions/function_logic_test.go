// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestLogicFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []*datavalues.Value
		expect *datavalues.Value
		err    error
	}{
		{
			name: "and-ok",
			fn:   FuncLogicAnd,
			args: []*datavalues.Value{
				datavalues.MakeBool(true),
				datavalues.MakeBool(true),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "and-not-ok",
			fn:   FuncLogicAnd,
			args: []*datavalues.Value{
				datavalues.MakeBool(true),
				datavalues.MakeBool(false),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "or-ok",
			fn:   FuncLogicOr,
			args: []*datavalues.Value{
				datavalues.MakeBool(true),
				datavalues.MakeBool(false),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "and-not-ok",
			fn:   FuncLogicOr,
			args: []*datavalues.Value{
				datavalues.MakeBool(false),
				datavalues.MakeBool(false),
			},
			expect: datavalues.MakeBool(false),
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
