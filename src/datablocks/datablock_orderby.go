// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datavalues"
	"expressions"
	"planners"
	"sort"
	"strings"

	"base/errors"
)

func (block *DataBlock) OrderByPlan(plan *planners.OrderByPlan) error {
	var fields []string

	// Find the column name which all the orderby used.
	if err := plan.Walk(func(p planners.IPlan) (bool, error) {
		switch p := p.(type) {
		case *planners.VariablePlan:
			fields = append(fields, string(p.Value))
		}
		return true, nil
	}); err != nil {
		return err
	}

	// Build the orderby to IExpression.
	exprs := make([]expressions.IExpression, len(plan.Orders))
	for i, order := range plan.Orders {
		expr, err := planners.BuildExpressions(order.Expression)
		if err != nil {
			return err
		}
		exprs[i] = expr
	}

	// Orderby column value.
	tuples := make([]interface{}, len(fields))
	for i, name := range fields {
		cv, ok := block.valuesmap[name]
		if !ok {
			return errors.Errorf("Can't find column:%v", name)
		}
		tuples[i] = datavalues.MakeTuple(cv.values...)
	}
	// Append the Seqs column.
	numRows := block.NumRows()
	seqs := make([]*datavalues.Value, numRows)
	for i := 0; i < numRows; i++ {
		seqs[i] = datavalues.ToValue(i)
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
			switch strings.ToUpper(order.Direction) {
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
	finalSeqs := make([]*datavalues.Value, numRows)
	for i, tuple := range matrix {
		finalSeqs[i] = tuple.AsSlice()[len(fields)]
	}
	block.setSeqs(finalSeqs)
	return nil
}
