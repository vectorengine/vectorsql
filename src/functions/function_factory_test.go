// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactory(t *testing.T) {
	// OK.
	{
		name := "range"
		fn, err := FunctionFactory(name)
		assert.Nil(t, err)
		assert.Equal(t, "RANGE", fn.Name)
	}

	// Err.
	{
		name := "xx"
		_, err := FunctionFactory(name)
		assert.NotNil(t, err)
	}
}
