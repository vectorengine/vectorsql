// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestAggregatorsExpression(t *testing.T) {
	tests := []struct {
		name   string
		expr   IExpression
		expect *datavalues.Value
	}{
		{
			name:   "sum(1)",
			expr:   SUM(1),
			expect: datavalues.ToValue(2.0),
		},
		{
			name:   "sum(a)",
			expr:   SUM("a"),
			expect: datavalues.ToValue(4.0),
		},
		{
			name:   "sum(a+1)",
			expr:   SUM(ADD("a", 1)),
			expect: datavalues.ToValue(6.0),
		},
		{
			name:   "sum(b)+a",
			expr:   ADD(SUM("b"), "a"),
			expect: datavalues.ToValue(10.0),
		},
		{
			name:   "sum(a+(b+1))",
			expr:   SUM(ADD("a", ADD("b", 1))),
			expect: datavalues.ToValue(13.0),
		},
		{
			name:   "min(a)",
			expr:   MIN("a"),
			expect: datavalues.ToValue(1.0),
		},
		{
			name:   "min(a+1)",
			expr:   MIN(ADD("a", 1)),
			expect: datavalues.ToValue(2.0),
		},
		{
			name:   "max(a)",
			expr:   MAX("a"),
			expect: datavalues.ToValue(3.0),
		},
		{
			name:   "max(a+1)",
			expr:   MAX(ADD("a", 1)),
			expect: datavalues.ToValue(4.0),
		},
		{
			name:   "count(b)",
			expr:   COUNT("b"),
			expect: datavalues.ToValue(2),
		},
		{
			name:   "count(*)",
			expr:   COUNT("*"),
			expect: datavalues.ToValue(2),
		},
		{
			name:   "count(a+1)",
			expr:   COUNT(ADD("a", 1)),
			expect: datavalues.ToValue(2),
		},
	}

	for _, test := range tests {
		params1 := Map{
			"a": datavalues.ToValue(1),
			"b": datavalues.ToValue(2),
		}
		params2 := Map{
			"a": datavalues.ToValue(3),
			"b": datavalues.ToValue(5),
		}
		_, err := test.expr.Eval(params1)
		assert.Nil(t, err)
		actual, err := test.expr.Eval(params2)
		assert.Nil(t, err)
		assert.Equal(t, test.expect.AsFloat(), actual.AsFloat())

		err = test.expr.Walk(func(e IExpression) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
	}
}
