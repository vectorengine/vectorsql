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
		expect datavalues.IDataValue
	}{
		{
			name:   "sum(1)",
			expr:   SUM(1),
			expect: datavalues.MakeInt(2),
		},
		{
			name:   "sum(a)",
			expr:   SUM("a"),
			expect: datavalues.MakeInt(4),
		},
		{
			name:   "sum(a+1)",
			expr:   SUM(ADD("a", 1)),
			expect: datavalues.MakeInt(6),
		},
		{
			name:   "sum(b)+a",
			expr:   ADD(SUM("b"), "a"),
			expect: datavalues.MakeInt(10),
		},
		{
			name:   "sum(a+(b+1))",
			expr:   SUM(ADD("a", ADD("b", 1))),
			expect: datavalues.MakeInt(13),
		},
		{
			name:   "min(a)",
			expr:   MIN("a"),
			expect: datavalues.MakeInt(1),
		},
		{
			name:   "min(a+1)",
			expr:   MIN(ADD("a", 1)),
			expect: datavalues.MakeInt(2),
		},
		{
			name:   "max(a)",
			expr:   MAX("a"),
			expect: datavalues.MakeInt(3),
		},
		{
			name:   "max(a+1)",
			expr:   MAX(ADD("a", 1)),
			expect: datavalues.MakeInt(4),
		},
		{
			name:   "count(b)",
			expr:   COUNT("b"),
			expect: datavalues.MakeInt(2),
		},
		{
			name:   "count(a)",
			expr:   COUNT("a"),
			expect: datavalues.MakeInt(2),
		},
		{
			name:   "count(a+1)",
			expr:   COUNT(ADD("a", 1)),
			expect: datavalues.MakeInt(2),
		},
	}

	for _, test := range tests {
		params1 := Map{
			"a": datavalues.MakeInt(1),
			"b": datavalues.MakeInt(2),
		}
		params2 := Map{
			"a": datavalues.MakeInt(3),
			"b": datavalues.MakeInt(5),
		}
		_, err := test.expr.Update(params1)
		assert.Nil(t, err)
		actual, err := test.expr.Update(params2)
		assert.Nil(t, err)
		assert.Equal(t, test.expect, actual)

		err = test.expr.Walk(func(e IExpression) (bool, error) {
			return true, nil
		})
		assert.Nil(t, err)
	}
}
