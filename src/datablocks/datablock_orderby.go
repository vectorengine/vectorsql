// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"expvar"
	"sort"
	"time"

	"base/metric"
	"datavalues"
	"expressions"
	"planners"
)

func (block *DataBlock) OrderByPlan(fields []string, plan *planners.OrderByPlan) error {
	defer expvar.Get(metric_datablock_filter_sec).(metric.Metric).Record(time.Now())

	// Build the orderby to IExpression.
	exprs := make([]expressions.IExpression, len(plan.Orders))
	for i, order := range plan.Orders {
		expr, err := planners.BuildExpression(order.Expression)
		if err != nil {
			return err
		}
		exprs[i] = expr
	}

	// Orderby column value.
	numRows := block.NumRows()
	tuples := make([]interface{}, len(fields))
	for i, name := range fields {
		it, err := block.ColumnIterator(name)
		if err != nil {
			return err
		}

		k := 0
		colvals := make([]datavalues.IDataValue, numRows)
		for it.Next() {
			colvals[k] = it.Value()
			k++
		}
		tuples[i] = datavalues.MakeTuple(colvals...)
	}
	// Append the Seqs column.
	seqs := make([]datavalues.IDataValue, numRows)
	for i := 0; i < numRows; i++ {
		seqs[i] = datavalues.ToValue(block.seqs[i])
	}
	tuples = append(tuples, datavalues.MakeTuple(seqs...))

	// ZIP to the row format.
	zipFunc, err := expressions.ExpressionFactory("ZIP", tuples)
	if err != nil {
		return err
	}
	result, err := zipFunc.Update(nil)
	if err != nil {
		return err
	}

	// Params.
	iparams := make(expressions.Map, len(fields))
	jparams := make(expressions.Map, len(fields))

	// Sort.
	matrix := datavalues.AsSlice(result)
	sort.Slice(matrix[:], func(i, j int) bool {
		irows := datavalues.AsSlice(matrix[i])
		jrows := datavalues.AsSlice(matrix[j])
		for k := 0; k < len(fields); k++ {
			iparams[fields[k]] = irows[k]
			jparams[fields[k]] = jrows[k]
		}

		for k, order := range plan.Orders {
			ival, err := exprs[k].Update(iparams)
			if err != nil {
				return false
			}
			jval, err := exprs[k].Update(jparams)
			if err != nil {
				return false
			}

			cmp, err := ival.Compare(jval)
			if err != nil {
				return false
			}
			if cmp == datavalues.Equal {
				continue
			}
			switch order.Direction {
			case "desc":
				return cmp == datavalues.GreaterThan
			default:
				return cmp == datavalues.LessThan
			}
		}
		return false
	})

	// Final.
	finalSeqs := make([]int, numRows)
	for i, tuple := range matrix {
		finalSeqs[i] = int(datavalues.AsInt(datavalues.AsSlice(tuple)[len(fields)]))
	}
	block.seqs = finalSeqs
	return nil
}
