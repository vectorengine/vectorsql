// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package collections

type Stack struct {
	len int
	top *node
}

type node struct {
	value interface{}
	prev  *node
}

func NewStack() *Stack {
	return &Stack{}
}

func (stack *Stack) Len() int {
	return stack.len
}

func (stack *Stack) Peek() interface{} {
	if stack.len == 0 {
		return nil
	}
	return stack.top.value
}

func (stack *Stack) Pop() interface{} {
	if stack.len == 0 {
		return nil
	}
	n := stack.top
	stack.top = stack.top.prev
	stack.len--
	return n.value
}

func (stack *Stack) Push(value interface{}) {
	n := &node{
		value: value,
		prev:  stack.top,
	}
	stack.top = n
	stack.len++
}
