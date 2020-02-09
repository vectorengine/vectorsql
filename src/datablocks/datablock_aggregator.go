// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datavalues"
	"functions"
)

func (block *DataBlock) Aggregator(fn string, name string) (*datavalues.Value, error) {
	if fn == "" {
		return block.First(name)
	}

	function, err := functions.FunctionFactory(fn)
	if err != nil {
		return nil, err
	}

	i := 0
	it, err := block.ColumnIterator(name)
	if err != nil {
		return nil, err
	}
	values := make([]*datavalues.Value, block.NumRows())
	for it.Next() {
		values[i] = it.Value()
		i++
	}

	if err := function.Validator.Validate(values...); err != nil {
		return nil, err
	}
	return function.Logic(values...)
}
