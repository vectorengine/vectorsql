// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"testing"

	"datavalues"

	"github.com/stretchr/testify/assert"
)

func TestExpressionFor(t *testing.T) {
	vals := []interface{}{
		int64(1),
		int32(2),
		int16(2),
		byte(0x01),
		float64(1),
		float32(2),
		datavalues.ToValue(1),
	}
	exprs := expressionsFor(vals...)
	assert.NotNil(t, exprs)

	err := Walk(func(e IExpression) (bool, error) {
		assert.NotNil(t, e)
		return true, nil
	}, exprs...)
	assert.Nil(t, err)
}
