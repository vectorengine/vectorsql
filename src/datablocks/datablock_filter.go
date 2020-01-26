// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"datatypes"
)

func (block *DataBlock) Filter(checks []datatypes.Value) (*DataBlock, error) {
	columns := block.Columns()
	cloneBlock := NewDataBlock(columns)

	for _, v := range block.values {
		for i, check := range checks {
			if check.AsBool() {
				if err := cloneBlock.Insert(v.Column().Name, v.Values()[i]); err != nil {
					return nil, err
				}
			}
		}
	}
	return cloneBlock, nil
}
