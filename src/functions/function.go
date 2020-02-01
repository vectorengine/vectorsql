// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"datavalues"
)

type Function struct {
	Name      string
	Args      [][]string
	Logic     func(...*datavalues.Value) (*datavalues.Value, error)
	Validator IValidator
}
