// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datatypes"
)

type Function struct {
	Name      string
	Args      [][]string
	Logic     func(...datatypes.Value) (datatypes.Value, error)
	Validator IValidator
}
