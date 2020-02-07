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
	p1 := datavalues.ToValue(1)
	p2 := datavalues.ToValue(2)
	p3 := datavalues.ToValue(3)

	tests := []struct {
		name   string
		expr   IExpression
		expect *datavalues.Value
	}{
		{
			name:   "simple",
			expr:   ADD(p1, p2),
			expect: datavalues.ToValue(3),
		},
		{
			name:   "complex",
			expr:   ADD(SUB(ADD(p1, p2), SUB(p3, p1)), CONST(p2)),
			expect: datavalues.ToValue(3),
		},
	}

	for _, test := range tests {
		actual := test.expr.Get()
		assert.Equal(t, test.expect, actual)
	}
}
