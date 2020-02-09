// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestConditionsExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      IExpression
		expect    *datavalues.Value
		errstring string
	}{
		{
			name:   "a<b",
			expr:   LT("a", "b"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "b<a",
			expr:   LT("b", "a"),
			expect: datavalues.ToValue(false),
		},
		{
			name:   "a<(b+1)",
			expr:   LT("a", ADD("b", 1)),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "b<=b",
			expr:   LTE("b", "b"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "a=a",
			expr:   EQ("a", "a"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "a=b",
			expr:   EQ("a", "b"),
			expect: datavalues.ToValue(false),
		},
		{
			name:   "a<>a",
			expr:   NEQ("a", "a"),
			expect: datavalues.ToValue(false),
		},
		{
			name:   "a<>b",
			expr:   NEQ("a", "b"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "b>a",
			expr:   GT("b", "a"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "a>b",
			expr:   GT("a", "b"),
			expect: datavalues.ToValue(false),
		},
		{
			name:   "a>=a",
			expr:   GTE("a", "a"),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "(a<b) AND (c<d)",
			expr:   AND(LT("c", "d"), LT("c", "d")),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "(a<b) AND (c>d)",
			expr:   AND(LT("c", "d"), GT("c", "d")),
			expect: datavalues.ToValue(false),
		},
		{
			name:   "(a<b) OR (c<d)",
			expr:   OR(LT("c", "d"), LT("c", "d")),
			expect: datavalues.ToValue(true),
		},
		{
			name:   "(a>b) OR (c>d)",
			expr:   OR(GT("c", "d"), GT("c", "d")),
			expect: datavalues.ToValue(false),
		},
	}

	for _, test := range tests {
		params := Map{
			"a": datavalues.ToValue(1),
			"b": datavalues.ToValue(2),
			"c": datavalues.ToValue("c"),
			"d": datavalues.ToValue("d"),
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
