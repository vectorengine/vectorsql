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
		name   string
		expr   IExpression
		expect *datavalues.Value
	}{
		{
			name:   "simple",
			expr:   ADD("a", "b"),
			expect: datavalues.ToValue(3.0),
		},
		{
			name:   "simple",
			expr:   ADD("a", 3),
			expect: datavalues.ToValue(4.0),
		},
		{
			name:   "simple",
			expr:   ADD("a", CONST(3)),
			expect: datavalues.ToValue(4.0),
		},
		{
			name:   "const",
			expr:   ADD(CONST(1), CONST(3)),
			expect: datavalues.ToValue(4.0),
		},
	}

	for _, test := range tests {
		params := Map{
			"a": datavalues.ToValue(1),
			"b": datavalues.ToValue(2),
		}
		actual := test.expr.Update(params)
		assert.Equal(t, test.expect, actual)
	}
}
