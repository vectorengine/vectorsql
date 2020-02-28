// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionFactory(t *testing.T) {
	tests := []struct {
		name      string
		exprName  string
		args      []interface{}
		expect    IExpression
		errstring string
	}{
		{
			name:     "unary-passed",
			exprName: "SUM",
			args:     []interface{}{nil},
		},
		{
			name:     "binary-passed",
			exprName: "+",
			args:     []interface{}{1, 2},
		},
		{
			name:     "scalar-passed",
			exprName: "if",
			args:     []interface{}{1, 2},
		},
		{
			name:      "notfound-fail",
			exprName:  "notfound",
			errstring: "Unsupported Expression:NOTFOUND",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := ExpressionFactory(test.exprName, test.args)
			if test.errstring != "" {
				assert.Equal(t, test.errstring, err.Error())
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, actual)
			}
		})
	}
}
