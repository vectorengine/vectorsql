// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"fmt"
	"sort"

	"base/errors"
)

func (block *DataBlock) GroupBy(cols []string) ([]*DataBlock, error) {
	var keys []string
	var colidxs []int

	for _, name := range cols {
		idx, ok := block.indexmap[name]
		if !ok {
			return nil, errors.Errorf("Can't find column:%v", name)
		}
		colidxs = append(colidxs, idx)
	}

	groups := make(map[string]*DataBlock)
	it := newDataBlockRowIterator(block)
	for it.Next() {
		var key string
		values := it.Value()

		for _, idx := range colidxs {
			key += fmt.Sprintf("%+v", values[idx].ToRawValue())
		}
		if group := groups[key]; group != nil {
			if err := group.WriteRow(values); err != nil {
				return nil, err
			}
		} else {
			keys = append(keys, key)
			block := NewDataBlock(block.Columns())
			if err := block.WriteRow(values); err != nil {
				return nil, err
			}
			groups[key] = block
		}
	}

	sort.Strings(keys)
	blocks := make([]*DataBlock, len(keys))
	for i, key := range keys {
		blocks[i] = groups[key]
	}
	return blocks, nil
}
