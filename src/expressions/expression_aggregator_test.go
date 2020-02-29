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
		name    string
		expr1   IExpression
		expr2   IExpression
		expect1 datavalues.IDataValue
		expect2 datavalues.IDataValue
	}{
		{
			name:    "sum(1)",
			expr1:   SUM(1),
			expr2:   SUM(1),
			expect1: datavalues.MakeInt(2),
			expect2: datavalues.MakeInt(3),
		},
		{
			name:    "sum(a)",
			expr1:   SUM("a"),
			expr2:   SUM("a"),
			expect1: datavalues.MakeInt(4),
			expect2: datavalues.MakeInt(10),
		},
		{
			name:    "sum(a+1)",
			expr1:   SUM(ADD("a", 1)),
			expr2:   SUM(ADD("a", 1)),
			expect1: datavalues.MakeInt(6),
			expect2: datavalues.MakeInt(13),
		},
		{
			name:    "sum(b)+3+2",
			expr1:   ADD(ADD(SUM("b"), 3), 2),
			expr2:   ADD(ADD(SUM("b"), 3), 2),
			expect1: datavalues.MakeInt(12),
			expect2: datavalues.MakeInt(20),
		},
		{
			name:    "SUM(b)/COUNT(b)+2",
			expr1:   ADD(DIV(SUM("b"), COUNT("b")), 2.0),
			expr2:   ADD(DIV(SUM("b"), COUNT("b")), 2.0),
			expect1: datavalues.ToValue(5.5),
			expect2: datavalues.MakeFloat(7),
		},
		{
			name:    "sum(a+(b+1))",
			expr1:   SUM(ADD("a", ADD("b", 1))),
			expr2:   SUM(ADD("a", ADD("b", 1))),
			expect1: datavalues.MakeInt(13),
			expect2: datavalues.MakeInt(28),
		},
		{
			name:    "min(a)",
			expr1:   MIN("a"),
			expr2:   MIN("a"),
			expect1: datavalues.MakeInt(1),
			expect2: datavalues.MakeInt(1),
		},
		{
			name:    "min(a+1)",
			expr1:   MIN(ADD("a", 1)),
			expr2:   MIN(ADD("a", 1)),
			expect1: datavalues.MakeInt(2),
			expect2: datavalues.MakeInt(2),
		},
		{
			name:    "max(a)",
			expr1:   MAX("a"),
			expr2:   MAX("a"),
			expect1: datavalues.MakeInt(3),
			expect2: datavalues.MakeInt(6),
		},
		{
			name:    "max(a+1)",
			expr1:   MAX(ADD("a", 1)),
			expr2:   MAX(ADD("a", 1)),
			expect1: datavalues.MakeInt(4),
			expect2: datavalues.MakeInt(7),
		},
		{
			name:    "count(b)",
			expr1:   COUNT("b"),
			expr2:   COUNT("b"),
			expect1: datavalues.MakeInt(2),
			expect2: datavalues.MakeInt(3),
		},
		{
			name:    "count(a)",
			expr1:   COUNT("a"),
			expr2:   COUNT("a"),
			expect1: datavalues.MakeInt(2),
			expect2: datavalues.MakeInt(3),
		},
		{
			name:    "count(a+1)",
			expr1:   COUNT(ADD("a", 1)),
			expr2:   COUNT(ADD("a", 1)),
			expect1: datavalues.MakeInt(2),
			expect2: datavalues.MakeInt(3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params1 := Map{
				"a": datavalues.ToValue(1),
				"b": datavalues.ToValue(2),
			}
			params2 := Map{
				"a": datavalues.ToValue(3),
				"b": datavalues.ToValue(5),
			}
			expr1 := test.expr1
			_, err := expr1.Update(params1)
			assert.Nil(t, err)
			_, err = expr1.Update(params2)
			assert.Nil(t, err)
			actual, err := expr1.Result()
			assert.Nil(t, err)
			assert.Equal(t, test.expect1, actual)

			err = expr1.Walk(func(e IExpression) (bool, error) {
				return true, nil
			})
			assert.Nil(t, err)

			// Merge.
			params3 := Map{
				"a": datavalues.ToValue(6),
				"b": datavalues.ToValue(8),
			}
			expr2 := test.expr2
			_, err = expr2.Update(params3)
			assert.Nil(t, err)
			actual, err = expr1.Merge(expr2)

			assert.Nil(t, err)
			assert.Equal(t, test.expect2, actual)
		})
	}
}
