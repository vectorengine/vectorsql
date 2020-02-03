// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"sort"
	"strings"
	"time"

	"datavalues"
	"functions"

	"base/errors"
	"base/metric"
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
	defer expvar.Get(metric_datablock_sort_sec).(metric.Metric).Record(time.Now())

	if block.NumColumns() == 1 {
		cv := block.values[0]
		matrix := cv.values
		sorter := sorters[0]

		if cv.column.Name != sorter.column {
			return errors.Errorf("Can't find column:%v", sorter.column)
		}
		sort.Slice(matrix[:], func(i, j int) bool {
			cmp, err := datavalues.Compare(matrix[i], matrix[j])
			if err != nil {
				return false
			}
			switch strings.ToUpper(sorter.direction) {
			case "ASC":
				return cmp == datavalues.LessThan
			case "DESC":
				return cmp == datavalues.GreaterThan
			default:
				return cmp == datavalues.LessThan
			}
		})
	} else {
		zipFunc, err := functions.FunctionFactory("ZIP")
		if err != nil {
			return err
		}

		// Seqs column.
		max := block.NumRows()
		seqs := make([]*datavalues.Value, max)
		for i := 0; i < max; i++ {
			seqs[i] = datavalues.ToValue(i)
		}

		// Sort columns.
		var tuples []*datavalues.Value
		for _, sorter := range sorters {
			cv, ok := block.valuesmap[sorter.column]
			if !ok {
				return errors.Errorf("Can't find column:%v", sorter.column)
			}
			tuples = append(tuples, datavalues.MakeTuple(cv.values...))
		}
		tuples = append(tuples, datavalues.MakeTuple(seqs...))

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
				cmp, err := datavalues.Compare(irows[x], jrows[x])
				if err != nil {
					return false
				}
				if cmp == datavalues.Equal {
					continue
				}
				switch strings.ToUpper(sorters[x].direction) {
				case "ASC":
					return cmp == datavalues.LessThan
				case "DESC":
					return cmp == datavalues.GreaterThan
				default:
					return cmp == datavalues.LessThan
				}
			}
			return false
		})

		// Final.
		finalSeqs := make([]*datavalues.Value, max)
		for i, tuple := range matrix {
			finalSeqs[i] = tuple.AsSlice()[len(sorters)]
		}
		block.setSeqs(finalSeqs)
	}
	return nil
}
