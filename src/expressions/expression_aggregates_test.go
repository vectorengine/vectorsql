// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestAggregateExpression(t *testing.T) {
	p1 := datavalues.ToValue(1)
	p2 := datavalues.ToValue(2)
	p3 := datavalues.ToValue(3)

	tests := []struct {
		name    string
		expr    IExpression
		updates []IExpression
		expect  *datavalues.Value
	}{
		{
			name:    "sum",
			expr:    SUM(),
			updates: []IExpression{ADD(p1, p2), ADD(p3, p1), ADD(p2, p3)},
			expect:  datavalues.ToValue(12),
		},
		{
			name:    "min",
			expr:    MIN(),
			updates: []IExpression{ADD(p1, p2), ADD(p3, p1), ADD(p2, p3)},
			expect:  datavalues.ToValue(3),
		},
		{
			name:    "max",
			expr:    MAX(),
			updates: []IExpression{ADD(p1, p2), ADD(p3, p1), ADD(p2, p3)},
			expect:  datavalues.ToValue(5),
		},
	}

	for _, test := range tests {
		expr := test.expr
		for _, v := range test.updates {
			expr.Update(v)
		}
		actual := test.expr.Get()
		assert.Equal(t, test.expect, actual)
	}
}
