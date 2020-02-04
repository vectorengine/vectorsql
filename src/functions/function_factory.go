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
		// Compares.
		FuncCompareEqual.Name:        FuncCompareEqual,
		FuncCompareNotEqual.Name:     FuncCompareNotEqual,
		FuncCompareLessThan.Name:     FuncCompareLessThan,
		FuncCompareLessEqual.Name:    FuncCompareLessEqual,
		FuncCompareGreaterThan.Name:  FuncCompareGreaterThan,
		FuncCompareGreaterEqual.Name: FuncCompareGreaterEqual,
		FuncCompareLike.Name:         FuncCompareLike,

		// Logics.
		FuncLogicAnd.Name: FuncLogicAnd,
		FuncLogicOr.Name:  FuncLogicOr,

		// Aggregators.
		FuncAggregatorMin.Name:   FuncAggregatorMin,
		FuncAggregatorMax.Name:   FuncAggregatorMax,
		FuncAggregatorCount.Name: FuncAggregatorCount,

		// Table valued functions.
		FuncTableValuedFunctionRange.Name:      FuncTableValuedFunctionRange,
		FuncTableValuedFunctionRangeTable.Name: FuncTableValuedFunctionRangeTable,
		FuncTableValuedFunctionRandTable.Name:  FuncTableValuedFunctionRandTable,
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
