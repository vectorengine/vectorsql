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

func TestCompareFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []datatypes.Value
		expect datatypes.Value
		err    error
	}{
		{
			name: "equal-ok",
			fn:   FuncCompareEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(1),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "equal-not-ok",
			fn:   FuncCompareEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "notequal-ok",
			fn:   FuncCompareNotEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(1),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "notequal-not-ok",
			fn:   FuncCompareNotEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "greaterthan-ok",
			fn:   FuncCompareGreaterThan,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(1),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "greaterthan-not-ok",
			fn:   FuncCompareGreaterThan,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "greaterequal-ok",
			fn:   FuncCompareGreaterEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "greaterequal-ok",
			fn:   FuncCompareGreaterEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(1),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "greaterequal-not-ok",
			fn:   FuncCompareGreaterEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "lessthan-ok",
			fn:   FuncCompareLessThan,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "lessthan-not-ok",
			fn:   FuncCompareLessThan,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "lessequal-ok",
			fn:   FuncCompareLessEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "lessequal-ok",
			fn:   FuncCompareLessEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeInt(2),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "lessequal-not-ok",
			fn:   FuncCompareLessEqual,
			args: []datatypes.Value{
				datatypes.MakeInt(2),
				datatypes.MakeInt(1),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "like-ok",
			fn:   FuncCompareLike,
			args: []datatypes.Value{
				datatypes.MakeString("xxx"),
				datatypes.MakeString(`x%`),
			},
			expect: datatypes.MakeBool(true),
		},
		{
			name: "like-not-ok",
			fn:   FuncCompareLike,
			args: []datatypes.Value{
				datatypes.MakeString("xxx"),
				datatypes.MakeString(`%y`),
			},
			expect: datatypes.MakeBool(false),
		},
		{
			name: "like-type-error",
			fn:   FuncCompareLike,
			args: []datatypes.Value{
				datatypes.MakeInt(1),
				datatypes.MakeString(`%y`),
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
