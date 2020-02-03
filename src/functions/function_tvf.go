// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"expvar"
	"fmt"
	"math/rand"
	"time"

	"datavalues"

	"base/metric"
)

var FuncTableValuedFunctionRange = &Function{
	Name: "RANGE",
	Args: [][]string{
		{"left", "right"},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		defer expvar.Get(metric_function_tvf_range_sec).(metric.Metric).Record(time.Now())

		v1 := args[0].AsInt()
		v2 := args[1].AsInt()
		values := make([]*datavalues.Value, v2-v1)

		for j, i := 0, v1; i < v2; j, i = j+1, i+1 {
			row := make([]*datavalues.Value, 1)
			row[0] = datavalues.MakeInt(i)
			values[j] = datavalues.MakeTuple(row...)
		}
		return datavalues.MakeTuple(values...), nil
	},
	Validator: All(
		ExactlyNArgs(2),
		All(
			AllArgs(TypeOf(datavalues.ZeroInt())),
		),
	),
}

var FuncTableValuedFunctionRangeTable = &Function{
	Name: "RANGETABLE",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		defer expvar.Get(metric_function_tvf_rangetable_sec).(metric.Metric).Record(time.Now())

		count := args[0].AsInt()
		values := make([]*datavalues.Value, count)
		for i := 0; i < count; i++ {
			row := make([]*datavalues.Value, len(args)-1)
			for j := 1; j < len(args); j++ {
				switch args[j].AsString() {
				case "String":
					row[j-1] = datavalues.MakeString(fmt.Sprintf("string-%v", i))
				case "UInt32", "Int32":
					row[j-1] = datavalues.MakeInt(i)
				}
			}
			values[i] = datavalues.MakeTuple(row...)
		}
		return datavalues.MakeTuple(values...), nil
	},
	Validator: All(
		AtLeastNArgs(2),
		Arg(0, TypeOf(datavalues.ZeroInt())),
		Arg(1, TypeOf(datavalues.ZeroString())),
		IfArgPresent(2, Arg(2, TypeOf(datavalues.ZeroString()))),
	),
}

var FuncTableValuedFunctionRandTable = &Function{
	Name: "RANDTABLE",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		defer expvar.Get(metric_function_tvf_randtable_sec).(metric.Metric).Record(time.Now())

		count := args[0].AsInt()
		values := make([]*datavalues.Value, count)
		for i := 0; i < count; i++ {
			row := make([]*datavalues.Value, len(args)-1)
			for j := 1; j < len(args); j++ {
				switch args[j].AsString() {
				case "String":
					row[j-1] = datavalues.MakeString(fmt.Sprintf("string-%v", rand.Intn(count)))
				case "UInt32", "Int32":
					row[j-1] = datavalues.MakeInt(rand.Intn(count))
				}
			}
			values[i] = datavalues.MakeTuple(row...)
		}
		return datavalues.MakeTuple(values...), nil
	},
	Validator: All(
		AtLeastNArgs(2),
		Arg(0, TypeOf(datavalues.ZeroInt())),
		Arg(1, TypeOf(datavalues.ZeroString())),
		IfArgPresent(2, Arg(2, TypeOf(datavalues.ZeroString()))),
	),
}

var FuncTableValuedFunctionZip = &Function{
	Name: "ZIP",
	Args: [][]string{
		{""},
	},
	Logic: func(args ...*datavalues.Value) (*datavalues.Value, error) {
		defer expvar.Get(metric_function_tvf_zip_sec).(metric.Metric).Record(time.Now())

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
	Validator: All(
		AtLeastNArgs(2),
		OneOf(
			AllArgs(TypeOf(datavalues.ZeroTuple())),
		),
	),
}
