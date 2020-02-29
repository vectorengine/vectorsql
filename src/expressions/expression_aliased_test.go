// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAliasedExpression(t *testing.T) {
	expr, err := ExpressionFactory("SUM", []interface{}{1})
	assert.Nil(t, err)

	aliased := ALIASED("xx", expr)
	assert.NotNil(t, aliased)

	err = aliased.Walk(func(e IExpression) (bool, error) {
		return true, nil
	})
	assert.Nil(t, err)
}
