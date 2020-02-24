// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/docs"
	"datavalues"
)

func SUM(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "SUM",
		argumentNames: [][]string{},
		description:   docs.Text("Sums Floats, Ints or Durations in the group. You may not mix types."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		updateFn: func(current datavalues.IDataValue, next datavalues.IDataValue) (datavalues.IDataValue, error) {
			if current == nil {
				return next, nil
			} else {
				return datavalues.Add(current, next)
			}
		},
		mergeFn: func(arg0 datavalues.IDataValue, args ...datavalues.IDataValue) (datavalues.IDataValue, error) {
			var err error
			res := arg0

			for _, arg := range args {
				if res, err = datavalues.Add(res, arg); err != nil {
					return nil, err
				}
			}
			return res, nil
		},
	}
}

func MIN(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "MIN",
		argumentNames: [][]string{},
		description:   docs.Text("Takes the minimum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		updateFn: func(current datavalues.IDataValue, next datavalues.IDataValue) (datavalues.IDataValue, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Min(current, next)
		},
		mergeFn: func(arg0 datavalues.IDataValue, args ...datavalues.IDataValue) (datavalues.IDataValue, error) {
			res := arg0

			for _, arg := range args {
				cmp, err := res.Compare(arg)
				if err != nil {
					return nil, err
				}
				if cmp == datavalues.GreaterThan {
					res = arg
				}
			}
			return res, nil
		},
	}
}

func MAX(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "MAX",
		argumentNames: [][]string{},
		description:   docs.Text("Takes the maximum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations."),
		validate: All(
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroInt())),
				AllArgs(TypeOf(datavalues.ZeroFloat())),
			),
		),
		expr: expressionsFor(arg)[0],
		updateFn: func(current datavalues.IDataValue, next datavalues.IDataValue) (datavalues.IDataValue, error) {
			if current == nil {
				return next, nil
			}
			return datavalues.Max(current, next)
		},
		mergeFn: func(arg0 datavalues.IDataValue, args ...datavalues.IDataValue) (datavalues.IDataValue, error) {
			res := arg0

			for _, arg := range args {
				cmp, err := res.Compare(arg)
				if err != nil {
					return nil, err
				}
				if cmp == datavalues.LessThan {
					res = arg
				}
			}
			return res, nil
		},
	}
}

func COUNT(arg interface{}) IExpression {
	return &AggregateExpression{
		name:          "COUNT",
		argumentNames: [][]string{},
		description:   docs.Text("Averages elements in the group."),
		validate:      All(),
		expr:          expressionsFor(arg)[0],
		updateFn: func(current datavalues.IDataValue, next datavalues.IDataValue) (datavalues.IDataValue, error) {
			if current == nil {
				return datavalues.MakeInt(1), nil
			} else {
				return datavalues.Add(current, datavalues.MakeInt(1))
			}
		},
		mergeFn: func(arg0 datavalues.IDataValue, args ...datavalues.IDataValue) (datavalues.IDataValue, error) {
			var err error
			res := arg0

			for _, arg := range args {
				if res, err = datavalues.Add(res, arg); err != nil {
					return nil, err
				}
			}
			return res, nil
		},
	}
}
