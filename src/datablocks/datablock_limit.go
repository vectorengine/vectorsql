// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

func (block *DataBlock) Limit(offset, limit int) (*DataBlock, error) {
	result := block.Clone()

	rows := block.NumRows()
	if rows <= offset {
		return result, nil
	}
	end := limit + offset
	if end > rows {
		end = rows
	}

	for i := range block.values {
		result.values[i].values = block.values[i].values[offset:end]
	}
	return result, nil
}
