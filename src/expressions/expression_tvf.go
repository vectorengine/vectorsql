// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"
	"math/rand"

	"base/docs"
	"base/errors"
	"datavalues"
)

func RANGETABLE(args ...interface{}) IExpression {
	exprs := expressionsFor(args...)
	return &ScalarExpression{
		name:          "RANGETABLE",
		argumentNames: [][]string{},
		description:   docs.Text("Returns a list of tuples."),
		validate: All(
			AtLeastNArgs(2),
			Arg(0, TypeOf(datavalues.ZeroInt())),
			Arg(1, TypeOf(datavalues.ZeroString())),
			IfArgPresent(2, Arg(2, TypeOf(datavalues.ZeroString()))),
		),
		exprs: exprs,
		updateFn: func(args ...*datavalues.Value) (*datavalues.Value, error) {
			count := args[0].AsInt()
			values := make([]*datavalues.Value, count)
			for i := 0; i < count; i++ {
				row := make([]*datavalues.Value, len(args)-1)
				for j := 1; j < len(args); j++ {
					arg := args[j].AsString()
					switch arg {
					case "String":
						row[j-1] = datavalues.MakeString(fmt.Sprintf("string-%v", i))
					case "UInt32", "Int32":
						row[j-1] = datavalues.MakeInt(i)
					default:
						return nil, errors.Errorf("Unsupported type:%v", arg)
					}
				}
				values[i] = datavalues.MakeTuple(row...)
			}
			return datavalues.MakeTuple(values...), nil
		},
	}
}

func RANDTABLE(args ...interface{}) IExpression {
	exprs := expressionsFor(args...)
	return &ScalarExpression{
		name:          "RANDTABLE",
		argumentNames: [][]string{},
		description:   docs.Text("Returns a random list of tuples."),
		validate: All(
			AtLeastNArgs(2),
			Arg(0, TypeOf(datavalues.ZeroInt())),
			Arg(1, TypeOf(datavalues.ZeroString())),
			IfArgPresent(2, Arg(2, TypeOf(datavalues.ZeroString()))),
		),
		exprs: exprs,
		updateFn: func(args ...*datavalues.Value) (*datavalues.Value, error) {
			count := args[0].AsInt()
			values := make([]*datavalues.Value, count)
			for i := 0; i < count; i++ {
				row := make([]*datavalues.Value, len(args)-1)
				for j := 1; j < len(args); j++ {
					arg := args[j].AsString()
					switch args[j].AsString() {
					case "String":
						row[j-1] = datavalues.MakeString(fmt.Sprintf("string-%v", rand.Intn(count)))
					case "UInt32", "Int32", "UInt64", "Int64":
						row[j-1] = datavalues.MakeInt(rand.Intn(count))
					default:
						return nil, errors.Errorf("Unsupported type:%v", arg)
					}
				}
				values[i] = datavalues.MakeTuple(row...)
			}
			return datavalues.MakeTuple(values...), nil
		},
	}
}

func ZIP(args ...interface{}) IExpression {
	exprs := expressionsFor(args...)
	return &ScalarExpression{
		name:          "ZIP",
		argumentNames: [][]string{},
		description:   docs.Text("Returns a zip list of tuples."),
		validate: All(
			AtLeastNArgs(2),
			OneOf(
				AllArgs(TypeOf(datavalues.ZeroTuple())),
			),
		),
		exprs: exprs,
		updateFn: func(args ...*datavalues.Value) (*datavalues.Value, error) {
			argsize := len(args)
			tuplesize := len(args[0].AsSlice())
			values := make([]*datavalues.Value, tuplesize)

			for i := 0; i < tuplesize; i++ {
				row := make([]*datavalues.Value, argsize)
				for j := 0; j < argsize; j++ {
					row[j] = args[j].AsSlice()[i]
				}
				values[i] = datavalues.MakeTuple(row...)
			}
			return datavalues.MakeTuple(values...), nil
		},
	}
}

func LOGMOCK(args ...interface{}) IExpression {
	exprs := expressionsFor(args...)
	return &ScalarExpression{
		name:          "LOGMOCK",
		argumentNames: [][]string{},
		description:   docs.Text("Returns a mock log table."),
		validate:      All(),
		exprs:         exprs,
		updateFn: func(args ...*datavalues.Value) (*datavalues.Value, error) {
			servera := "192.168.0.1"
			serverb := "192.168.0.2"

			values := make([]*datavalues.Value, 15)
			i := 0
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/login"), datavalues.MakeString("POST"), datavalues.MakeInt(200), datavalues.MakeInt(10))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/login"), datavalues.MakeString("POST"), datavalues.MakeInt(500), datavalues.MakeInt(13))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/login"), datavalues.MakeString("POST"), datavalues.MakeInt(500), datavalues.MakeInt(13))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(10))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(11))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(12))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(12))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(12))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(servera), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(500), datavalues.MakeInt(10))
			i++

			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/login"), datavalues.MakeString("POST"), datavalues.MakeInt(200), datavalues.MakeInt(10))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/login"), datavalues.MakeString("POST"), datavalues.MakeInt(500), datavalues.MakeInt(12))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(10))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(14))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(200), datavalues.MakeInt(10))
			i++
			values[i] = datavalues.MakeTuple(datavalues.MakeString(serverb), datavalues.MakeString("/index"), datavalues.MakeString("GET"), datavalues.MakeInt(500), datavalues.MakeInt(11))
			return datavalues.MakeTuple(values...), nil
		},
	}
}
