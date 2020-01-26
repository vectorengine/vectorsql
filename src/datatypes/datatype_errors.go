// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"fmt"
)

type ErrUnexpectedType struct {
	T        interface{}
	DataType IDataType
}

func (err *ErrUnexpectedType) Error() string {
	return fmt.Sprintf("%s: unexpected type %T", err.DataType, err.T)
}
