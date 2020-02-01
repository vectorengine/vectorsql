// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package functions

import (
	"testing"

	"datavalues"
)

func Test_exactlyNArgs(t *testing.T) {
	type args struct {
		n    int
		args []*datavalues.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "matching number",
			args: args{
				n:    2,
				args: []*datavalues.Value{datavalues.MakeInt(7), datavalues.MakeString("a")},
			},
			wantErr: false,
		},
		{
			name: "non-matching number - too long",
			args: args{
				n:    2,
				args: []*datavalues.Value{datavalues.MakeInt(7), datavalues.MakeString("a"), datavalues.MakeBool(true)},
			},
			wantErr: true,
		},
		{
			name: "non-matching number - too short",
			args: args{
				n:    4,
				args: []*datavalues.Value{datavalues.MakeInt(7), datavalues.MakeString("a"), datavalues.MakeBool(true)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ExactlyNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("ExactlyNArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atLeastNArgs(t *testing.T) {
	type args struct {
		n    int
		args []*datavalues.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]*datavalues.Value{datavalues.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				1,
				[]*datavalues.Value{datavalues.MakeInt(1), datavalues.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				1,
				[]*datavalues.Value{},
			},
			wantErr: true,
		},
		{
			name: "one arg - fail",
			args: args{
				2,
				[]*datavalues.Value{datavalues.MakeInt(1)},
			},
			wantErr: true,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]*datavalues.Value{datavalues.MakeInt(1), datavalues.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - fail",
			args: args{
				2,
				[]*datavalues.Value{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AtLeastNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atLeastOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_atMostNArgs(t *testing.T) {
	type args struct {
		n    int
		args []*datavalues.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "one arg - pass",
			args: args{
				1,
				[]*datavalues.Value{datavalues.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - fail",
			args: args{
				1,
				[]*datavalues.Value{datavalues.MakeInt(1), datavalues.MakeString("hello")},
			},
			wantErr: true,
		},
		{
			name: "zero args - pass",
			args: args{
				1,
				[]*datavalues.Value{},
			},
			wantErr: false,
		},
		{
			name: "one arg - pass",
			args: args{
				2,
				[]*datavalues.Value{datavalues.MakeInt(1)},
			},
			wantErr: false,
		},
		{
			name: "two args - pass",
			args: args{
				2,
				[]*datavalues.Value{datavalues.MakeInt(1), datavalues.MakeString("hello")},
			},
			wantErr: false,
		},
		{
			name: "zero args - pass",
			args: args{
				2,
				[]*datavalues.Value{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AtMostNArgs(tt.args.n).Validate(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("atMostOneArg() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_wantedType(t *testing.T) {
	type args struct {
		wantedType *datavalues.Value
		arg        *datavalues.Value
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "int - int - pass",
			args: args{
				datavalues.ZeroInt(),
				datavalues.MakeInt(7),
			},
			wantErr: false,
		},
		{
			name: "int - float - fail",
			args: args{
				datavalues.ZeroInt(),
				datavalues.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "int - string - fail",
			args: args{
				datavalues.ZeroInt(),
				datavalues.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "float - float - pass",
			args: args{
				datavalues.ZeroFloat(),
				datavalues.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - float - pass",
			args: args{
				datavalues.ZeroFloat(),
				datavalues.MakeFloat(7.0),
			},
			wantErr: false,
		},
		{
			name: "float - string - fail",
			args: args{
				datavalues.ZeroFloat(),
				datavalues.MakeString("aaa"),
			},
			wantErr: true,
		},
		{
			name: "bool - bool - pass",
			args: args{
				datavalues.ZeroBool(),
				datavalues.MakeBool(false),
			},
			wantErr: false,
		},
		{
			name: "string - string - pass",
			args: args{
				datavalues.ZeroString(),
				datavalues.MakeString("nice"),
			},
			wantErr: false,
		},
		{
			name: "string - int - fail",
			args: args{
				datavalues.ZeroString(),
				datavalues.MakeInt(7),
			},
			wantErr: true,
		},
		{
			name: "string - float - fail",
			args: args{
				datavalues.ZeroString(),
				datavalues.MakeFloat(7.0),
			},
			wantErr: true,
		},
		{
			name: "string - string - pass",
			args: args{
				datavalues.ZeroString(),
				datavalues.MakeString("aaa"),
			},
			wantErr: false,
		},
		{
			name: "tuple - tuple - pass",
			args: args{
				datavalues.ZeroTuple(),
				datavalues.MakeTuple(datavalues.MakeInt(1), datavalues.MakeInt(2), datavalues.MakeInt(3)),
			},
			wantErr: false,
		},
		{
			name: "tuple - int - fail",
			args: args{
				datavalues.ZeroTuple(),
				datavalues.MakeInt(4),
			},
			wantErr: true,
		},
		{
			name: "object - object - pass",
			args: args{
				datavalues.ZeroObject(),
				datavalues.MakeObject(map[string]*datavalues.Value{}),
			},
			wantErr: false,
		},
		{
			name: "object - int - fail",
			args: args{
				datavalues.ZeroObject(),
				datavalues.MakeInt(4),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		if err := TypeOf(tt.args.wantedType).Validate(tt.args.arg); (err != nil) != tt.wantErr {
			t.Errorf("basicType() error = %v, wantErr %v", err, tt.wantErr)
		}
	}
}
