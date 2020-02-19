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

func (block *DataBlock) OrderByPlan(plan *planners.OrderByPlan) error {
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

	fields, err := expressions.VariableValues(exprs...)
	if err != nil {
		return err
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
		colvals := make([]*datavalues.Value, numRows)
		for it.Next() {
			colvals[k] = it.Value()
			k++
		}
		tuples[i] = datavalues.MakeTuple(colvals...)
	}
	// Append the Seqs column.
	seqs := make([]*datavalues.Value, numRows)
	for i := 0; i < numRows; i++ {
		seqs[i] = block.seqs[i]
	}
	tuples = append(tuples, datavalues.MakeTuple(seqs...))

	// ZIP to the row format.
	zipFunc, err := expressions.ExpressionFactory("ZIP", tuples)
	if err != nil {
		return err
	}
	result, err := zipFunc.Eval(nil)
	if err != nil {
		return err
	}

	// Params.
	iparams := make(expressions.Map, len(fields))
	jparams := make(expressions.Map, len(fields))

	// Sort.
	matrix := result.AsSlice()
	sort.Slice(matrix[:], func(i, j int) bool {
		irows := matrix[i].AsSlice()
		jrows := matrix[j].AsSlice()
		for k := 0; k < len(fields); k++ {
			iparams[fields[k]] = irows[k]
			jparams[fields[k]] = jrows[k]
		}

		for k, order := range plan.Orders {
			ival, err := exprs[k].Eval(iparams)
			if err != nil {
				return false
			}
			jval, err := exprs[k].Eval(jparams)
			if err != nil {
				return false
			}

			cmp, err := datavalues.Compare(ival, jval)
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
	finalSeqs := make([]*datavalues.Value, numRows)
	for i, tuple := range matrix {
		finalSeqs[i] = tuple.AsSlice()[len(fields)]
	}
	block.seqs = finalSeqs
	return nil
}
