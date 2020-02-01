// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"errors"
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestCompareFunctions(t *testing.T) {
	tests := []struct {
		name   string
		fn     *Function
		args   []*datavalues.Value
		expect *datavalues.Value
		err    error
	}{
		{
			name: "equal-ok",
			fn:   FuncCompareEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(1),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "equal-not-ok",
			fn:   FuncCompareEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "notequal-ok",
			fn:   FuncCompareNotEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(1),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "notequal-not-ok",
			fn:   FuncCompareNotEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "greaterthan-ok",
			fn:   FuncCompareGreaterThan,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(1),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "greaterthan-not-ok",
			fn:   FuncCompareGreaterThan,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "greaterequal-ok",
			fn:   FuncCompareGreaterEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "greaterequal-ok",
			fn:   FuncCompareGreaterEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(1),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "greaterequal-not-ok",
			fn:   FuncCompareGreaterEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "lessthan-ok",
			fn:   FuncCompareLessThan,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "lessthan-not-ok",
			fn:   FuncCompareLessThan,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "lessequal-ok",
			fn:   FuncCompareLessEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "lessequal-ok",
			fn:   FuncCompareLessEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeInt(2),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "lessequal-not-ok",
			fn:   FuncCompareLessEqual,
			args: []*datavalues.Value{
				datavalues.MakeInt(2),
				datavalues.MakeInt(1),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "like-ok",
			fn:   FuncCompareLike,
			args: []*datavalues.Value{
				datavalues.MakeString("xxx"),
				datavalues.MakeString(`x%`),
			},
			expect: datavalues.MakeBool(true),
		},
		{
			name: "like-not-ok",
			fn:   FuncCompareLike,
			args: []*datavalues.Value{
				datavalues.MakeString("xxx"),
				datavalues.MakeString(`%y`),
			},
			expect: datavalues.MakeBool(false),
		},
		{
			name: "like-type-error",
			fn:   FuncCompareLike,
			args: []*datavalues.Value{
				datavalues.MakeInt(1),
				datavalues.MakeString(`%y`),
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
