// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSession(t *testing.T) {
	session := NewSession()
	defer session.Close()

	assert.Equal(t, "system", session.GetDatabase())

	session.SetDatabase("xx")
	assert.Equal(t, "xx", session.GetDatabase())

	// progress
	pv := &ProgressValues{}
	pv.ReadRows.Add(11)
	session.UpdateProgress(pv)
	got := session.GetProgress()
	assert.Equal(t, pv, got)
}
