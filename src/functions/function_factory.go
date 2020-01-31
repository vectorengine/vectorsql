// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"strings"

	"base/errors"
)

var (
	table = map[string]*Function{
		// Compare.
		FuncCompareEqual.Name:        FuncCompareEqual,
		FuncCompareNotEqual.Name:     FuncCompareNotEqual,
		FuncCompareLessThan.Name:     FuncCompareLessThan,
		FuncCompareLessEqual.Name:    FuncCompareLessEqual,
		FuncCompareGreaterThan.Name:  FuncCompareGreaterThan,
		FuncCompareGreaterEqual.Name: FuncCompareGreaterEqual,
		FuncCompareLike.Name:         FuncCompareLike,

		// Logic.
		FuncLogicAnd.Name: FuncLogicAnd,
		FuncLogicOr.Name:  FuncLogicOr,

		// Table valued function.
		FuncTableValuedFunctionRange.Name:      FuncTableValuedFunctionRange,
		FuncTableValuedFunctionRangeTable.Name: FuncTableValuedFunctionRangeTable,
		FuncTableValuedFunctionZip.Name:        FuncTableValuedFunctionZip,
	}
)

func FunctionFactory(name string) (*Function, error) {
	name = strings.ToUpper(name)
	fn, ok := table[name]
	if !ok {
		return nil, errors.Errorf("Unsupported function:%v", name)
	}
	return fn, nil
}
