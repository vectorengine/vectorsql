// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sqltypes

import (
	"reflect"
	"testing"

	querypb "parsers/sqlparser/dependency/query"
)

func TestEventTokenMinimum(t *testing.T) {
	testcases := []struct {
		ev1      *querypb.EventToken
		ev2      *querypb.EventToken
		expected *querypb.EventToken
	}{{
		ev1:      nil,
		ev2:      nil,
		expected: nil,
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 123,
		},
		ev2:      nil,
		expected: nil,
	}, {
		ev1: nil,
		ev2: &querypb.EventToken{
			Timestamp: 123,
		},
		expected: nil,
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 123,
		},
		ev2: &querypb.EventToken{
			Timestamp: 456,
		},
		expected: &querypb.EventToken{
			Timestamp: 123,
		},
	}, {
		ev1: &querypb.EventToken{
			Timestamp: 456,
		},
		ev2: &querypb.EventToken{
			Timestamp: 123,
		},
		expected: &querypb.EventToken{
			Timestamp: 123,
		},
	}}

	for _, tcase := range testcases {
		got := EventTokenMinimum(tcase.ev1, tcase.ev2)
		if tcase.expected == nil && got != nil {
			t.Errorf("expected nil result for Minimum(%v, %v) but got: %v", tcase.ev1, tcase.ev2, got)
			continue
		}

		if !reflect.DeepEqual(got, tcase.expected) {
			t.Errorf("got %v but expected %v for Minimum(%v, %v)", got, tcase.expected, tcase.ev1, tcase.ev2)
		}
	}
}
