// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestIFExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      IExpression
		expect    datavalues.IDataValue
		errstring string
	}{
		{
			name:   "IF(c<2, a, b)",
			expr:   IF(LT("c", 2), VAR("a"), VAR("b")),
			expect: datavalues.ToValue(2),
		},
		{
			name:   "IF(c>2, a, b)",
			expr:   IF(GT("c", 2), VAR("a"), VAR("b")),
			expect: datavalues.ToValue(1),
		},
		{
			name:      "if-error",
			expr:      IF(VAR("a")),
			errstring: ("expected at least 2 arguments, but got 1"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := Map{
				"a": datavalues.ToValue(1),
				"b": datavalues.ToValue(2),
				"c": datavalues.ToValue(4),
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
		})
	}
}
