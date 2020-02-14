// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datavalues

import (
	"encoding/binary"
	"hash/fnv"

	"base/errors"
)

type HashMap struct {
	count     int
	container map[uint64][]entry
}

func NewHashMap() *HashMap {
	return &HashMap{
		container: make(map[uint64][]entry),
	}
}

type entry struct {
	key   *Value
	value interface{}
}

func fastHash(key *Value) (uint64, error) {
	hashes := fnv.New64()
	err := binary.Write(hashes, binary.LittleEndian, []byte(key.String()))
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return hashes.Sum64(), nil
}

func (hm *HashMap) Set(key *Value, value interface{}) error {
	hash, err := fastHash(key)
	if err != nil {
		return errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := hm.container[hash]
	for i := range list {
		if AreEqual(list[i].key, key) {
			list[i].value = value
			return nil
		}
	}
	hm.container[hash] = append(list, entry{
		key:   key,
		value: value,
	})
	hm.count++
	return nil
}

func (hm *HashMap) SetByHash(key *Value, hash uint64, value interface{}) error {
	list := hm.container[hash]
	for i := range list {
		if AreEqual(list[i].key, key) {
			list[i].value = value
			return nil
		}
	}
	hm.container[hash] = append(list, entry{
		key:   key,
		value: value,
	})
	hm.count++
	return nil
}

func (hm *HashMap) Get(key *Value) (interface{}, uint64, bool, error) {
	hash, err := fastHash(key)
	if err != nil {
		return nil, 0, false, errors.Wrapf(err, "couldn't hash %+v", key)
	}

	list := hm.container[hash]
	for i := range list {
		if AreEqual(list[i].key, key) {
			return list[i].value, hash, true, nil
		}
	}
	return nil, hash, false, nil
}

func (hm *HashMap) Count() int {
	return hm.count
}

func (hm *HashMap) GetIterator() *HashMapIterator {
	hashes := make([]uint64, 0, len(hm.container))
	for k := range hm.container {
		hashes = append(hashes, k)
	}

	return &HashMapIterator{
		hm:             hm,
		hashes:         hashes,
		hashesPosition: 0,
		listPosition:   0,
	}
}

type HashMapIterator struct {
	hm             *HashMap
	hashes         []uint64
	hashesPosition int
	listPosition   int
}

func (iter *HashMapIterator) Next() (interface{}, bool) {
	if iter.hashesPosition == len(iter.hashes) {
		return nil, false
	}

	// Save current item location
	outHashPos := iter.hashesPosition
	outListPos := iter.listPosition

	// Advance iterator to next item
	if iter.listPosition+1 == len(iter.hm.container[iter.hashes[iter.hashesPosition]]) {
		iter.hashesPosition++
		iter.listPosition = 0
	} else {
		iter.listPosition++
	}

	outEntry := iter.hm.container[iter.hashes[outHashPos]][outListPos]
	return outEntry.value, true
}
