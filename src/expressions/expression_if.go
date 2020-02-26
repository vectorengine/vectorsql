// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"base/docs"
	"datavalues"
)

func IF(args ...interface{}) IExpression {
	exprs := expressionsFor(args...)
	return &ScalarExpression{
		name:          "IF",
		argumentNames: [][]string{},
		description:   docs.Text("IF (<cond>, <expr1>, <expr2>). Evaluates <cond>, then evaluates <expr1> if the condition is true, or <expr2> otherwise."),
		validate: All(
			ExactlyNArgs(3),
			Arg(0, TypeOf(datavalues.ZeroBool())),
			SameType(1, 2),
		),
		exprs: exprs,
		updateFn: func(args ...datavalues.IDataValue) (datavalues.IDataValue, error) {
			cond := datavalues.AsBool(args[0])
			cond1 := args[1]
			cond2 := args[2]
			if cond {
				return cond1, nil
			} else {
				return cond2, nil
			}
		},
	}
}
