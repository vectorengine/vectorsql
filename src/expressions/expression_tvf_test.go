// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestTVFExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      IExpression
		expect    datavalues.IDataValue
		errstring string
	}{
		{
			name: "RANGETABLE(rows->2, c1->'UInt32', c2->'String')",
			expr: RANGETABLE(3, CONST("UInt32"), CONST("String")),
			expect: datavalues.MakeTuple(
				datavalues.MakeTuple(datavalues.ToValue(0), datavalues.ToValue("string-0")),
				datavalues.MakeTuple(datavalues.ToValue(1), datavalues.ToValue("string-1")),
				datavalues.MakeTuple(datavalues.ToValue(2), datavalues.ToValue("string-2")),
			),
		},
		{
			name:      "RANGETABLE(rows->2)-error",
			expr:      RANGETABLE(3),
			errstring: ("expected at least 2 arguments, but got 1"),
		},
		{
			name: "ZIP(...)",
			expr: ZIP(
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue(2),
				),
				datavalues.MakeTuple(
					datavalues.ToValue("a"),
					datavalues.ToValue("b"),
				),
				datavalues.MakeTuple(
					datavalues.ToValue(11),
					datavalues.ToValue(22),
				),
			),
			expect: datavalues.MakeTuple(
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue("a"),
					datavalues.ToValue(11),
				),
				datavalues.MakeTuple(
					datavalues.ToValue(2),
					datavalues.ToValue("b"),
					datavalues.ToValue(22),
				),
			),
		},
		{
			name: "ZIP-error",
			expr: ZIP(
				datavalues.MakeTuple(
					datavalues.ToValue(1),
					datavalues.ToValue(2),
				),
			),
			errstring: ("expected at least 2 arguments, but got 1"),
		},
	}

	for _, test := range tests {
		actual, err := test.expr.Update(nil)
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
