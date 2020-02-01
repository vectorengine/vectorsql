// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"testing"

	"sort"

	"github.com/davecgh/go-spew/spew"
)

func TestValuesCompare(t *testing.T) {
	tuple := MakeTuple(
		MakeTuple(
			ToValue(0),
			ToValue(5),
			ToValue("a"),
			ToValue("x"),
		),
		MakeTuple(
			ToValue(1),
			ToValue(2),
			ToValue("b"),
			ToValue("x"),
		),
		MakeTuple(
			ToValue(2),
			ToValue(1),
			ToValue("a"),
			ToValue("y"),
		),
	)

	slices := tuple.AsSlice()
	t.Logf("%+v", spew.Sdump(slices))

	sort.SliceStable(slices[:], func(i, j int) bool {
		a := slices[i].AsSlice()
		b := slices[j].AsSlice()

		cmp, err := Compare(a[1], b[1])
		if err != nil {
			panic(err)
		}
		return cmp < Equal
	})

	sort.SliceStable(slices[:], func(i, j int) bool {
		a := slices[i].AsSlice()
		b := slices[j].AsSlice()
		cmp, err := Compare(a[2], b[2])
		if err != nil {
			panic(err)
		}
		return cmp < Equal
	})

	t.Logf("%+v", spew.Sdump(slices))
}
