// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"sort"
	"strings"

	"datatypes"
	"functions"

	"base/errors"
)

type Sorter struct {
	column    string
	direction string
}

func NewSorter(col string, direction string) Sorter {
	return Sorter{
		column:    col,
		direction: direction,
	}
}

func (block *DataBlock) Sort(sorters ...Sorter) error {
	zipFunc, err := functions.FunctionFactory("ZIP")
	if err != nil {
		return err
	}

	// Seqs column.
	max := block.NumRows()
	seqs := make([]*datatypes.Value, max)
	for i := 0; i < max; i++ {
		seqs[i] = datatypes.ToValue(i)
	}

	// Sort columns.
	var tuples []*datatypes.Value
	for _, sorter := range sorters {
		cv, ok := block.valuesmap[sorter.column]
		if !ok {
			return errors.Errorf("Can't find column:%v", sorter.column)
		}
		tuples = append(tuples, datatypes.MakeTuple(cv.values...))
	}
	tuples = append(tuples, datatypes.MakeTuple(seqs...))

	// Zip.
	if err := zipFunc.Validator.Validate(tuples...); err != nil {
		return err
	}
	result, err := zipFunc.Logic(tuples...)
	if err != nil {
		return err
	}

	// Sort.
	matrix := result.AsSlice()
	sort.Slice(matrix[:], func(i, j int) bool {
		irows := matrix[i].AsSlice()
		jrows := matrix[j].AsSlice()
		for x := 0; x < len(irows)-1; x++ {
			cmp, err := datatypes.Compare(irows[x], jrows[x])
			if err != nil {
				return false
			}
			if cmp == datatypes.Equal {
				continue
			}
			switch strings.ToUpper(sorters[x].direction) {
			case "ASC":
				return cmp == datatypes.LessThan
			case "DESC":
				return cmp == datatypes.GreaterThan
			default:
				return cmp == datatypes.LessThan
			}
		}
		return false
	})

	// Final.
	finalSeqs := make([]*datatypes.Value, max)
	for i, tuple := range matrix {
		finalSeqs[i] = tuple.AsSlice()[len(sorters)]
	}
	block.setSeqs(finalSeqs)
	return nil
}
