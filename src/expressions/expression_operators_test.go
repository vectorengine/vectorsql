// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestOperatorsExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      IExpression
		expect    *datavalues.Value
		errstring string
	}{
		{
			name:   "(1+2)",
			expr:   ADD(1, 2),
			expect: datavalues.ToValue(3),
		},
		{
			name:   "(a+b)",
			expr:   ADD("a", "b"),
			expect: datavalues.ToValue(3),
		},
		{
			name:   "(a+3)",
			expr:   ADD("a", 3),
			expect: datavalues.ToValue(4),
		},
		{
			name:   "(a+3)",
			expr:   ADD("a", CONST(3)),
			expect: datavalues.ToValue(4),
		},
		{
			name:   "(1+3)",
			expr:   ADD(CONST(1), CONST(3)),
			expect: datavalues.ToValue(4),
		},
		{
			name:   "(1-3)",
			expr:   SUB(CONST(1), CONST(3)),
			expect: datavalues.ToValue(-2),
		},
		{
			name:   "a+(1-3)",
			expr:   ADD("a", SUB(CONST(1), CONST(3))),
			expect: datavalues.ToValue(-1),
		},
		{
			name:   "a+(b*3)",
			expr:   ADD("a", MUL("b", 3)),
			expect: datavalues.ToValue(7),
		},
		{
			name:   "a/b",
			expr:   DIV("a", "b"),
			expect: datavalues.ToValue(0),
		},
		{
			name:      "a+c",
			expr:      ADD("a", "c"),
			errstring: "not-ok",
		},
	}

	for _, test := range tests {
		params := Map{
			"a": datavalues.ToValue(1),
			"b": datavalues.ToValue(2),
			"c": datavalues.ToValue("c"),
		}
		actual, err := test.expr.Update(params)
		if test.errstring != "" {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, test.expect, actual)

			err = test.expr.Walk(func(e IExpression) (bool, error) {
				return true, nil
			})
			assert.Nil(t, err)
		}
	}
}

func TestOperatorsParamsExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      IExpression
		expect    *datavalues.Value
		errstring string
	}{
		{
			name:   "(1+2)",
			expr:   ADD(1, 2),
			expect: datavalues.ToValue(3),
		},
		{
			name:      "(a+b)",
			expr:      ADD("a", "b"),
			errstring: "params is nil",
		},
	}

	for _, test := range tests {
		actual, err := test.expr.Update(nil)
		if test.errstring != "" {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, test.expect, actual)
		}
	}
}
