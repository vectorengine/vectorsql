// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package processors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPipelineParallelDAG(t *testing.T) {
	numbers := 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink1 := NewSink("sink1")
	source1 := NewSource("source1")
	t11 := NewMockSleepTransform("t11", 1100)

	pipeline1 := NewPipeline(ctx).
		Add(source1).
		Add(t11)

	source2 := NewSource("source2")
	t21 := NewMockSleepTransform("t21", 1000)
	t22 := NewMockAddTransform("t22")
	t22.From(t11)
	t23 := NewMockMultiTransform("t23")
	pipeline2 := NewPipeline(ctx).
		Add(source2).
		Add(t21).
		Add(t22).
		Add(t23).
		Add(sink1)

	pipeline1.Run()
	pipeline2.Run()

	go func() {
		out := source1.Out()
		defer out.Close()
		for i := 0; i < numbers; i++ {
			out.Send(i)
		}
	}()

	go func() {
		out := source2.Out()
		defer out.Close()
		for i := 0; i < numbers; i++ {
			out.Send(i)
		}
	}()

	err := pipeline2.Wait(func(x interface{}) error {
		/*
			if (x.(int) % 100) == 0 {
				fmt.Print("Pipeline Metrcis:\n")
				for _, metric := range pipeline2.Metrics() {
					fmt.Printf("%+v\n", metric)
				}
				fmt.Print("\n")
			}
		*/
		return nil
	})
	assert.Nil(t, err)
}

func TestPipelineSequentialDAG(t *testing.T) {
	numbers := 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink1 := NewSink("sink1")
	source1 := NewSource("source1")
	t11 := NewMockSleepTransform("t11", 1100)
	t21 := NewMockSleepTransform("t21", 1000)
	t22 := NewMockAddTransform("t22")
	t23 := NewMockMultiTransform("t23")

	pipeline1 := NewPipeline(ctx).
		Add(source1).
		Add(t11).
		Add(t21).
		Add(t22).
		Add(t23).
		Add(sink1)

	pipeline1.Run()

	go func() {
		out := source1.Out()
		defer out.Close()
		for i := 0; i < numbers; i++ {
			out.Send(i)
		}
	}()

	err := pipeline1.Wait(func(x interface{}) error {
		return nil
	})
	assert.Nil(t, err)
}

func TestPipelinePauseAndResume(t *testing.T) {
	numbers := 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink1 := NewSink("sink1")
	source1 := NewSource("source1")
	t11 := NewMockSleepTransform("t11", 10)
	t12 := NewMockSleepTransform("t12", 10)

	pipeline1 := NewPipeline(ctx).
		Add(source1).
		Add(t11).
		Add(t12).
		Add(sink1)

	pipeline1.Run()

	go func() {
		out := source1.Out()
		defer out.Close()
		for i := 0; i < numbers; i++ {
			out.Send(i)
		}
	}()

	err := pipeline1.Wait(func(x interface{}) error {
		if x.(int)%50 == 0 {
			pipeline1.Pause()
			time.Sleep(time.Second)
			pipeline1.Resume()
		}
		return nil
	})
	assert.Nil(t, err)
}

func TestPipelineCancel(t *testing.T) {
	numbers := 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink1 := NewSink("sink1")
	source1 := NewSource("source1")
	t11 := NewMockSleepTransform("t11", 10)
	t12 := NewMockAddTransform("t22")
	t13 := NewMockMultiTransform("t23")

	pipeline1 := NewPipeline(ctx).
		Add(source1).
		Add(t11).
		Add(t12).
		Add(t13).
		Add(sink1)

	pipeline1.Run()

	go func() {
		out := source1.Out()
		defer out.Close()
		for i := 0; i < numbers; i++ {
			out.Send(i)
		}
	}()

	err := pipeline1.Wait(func(x interface{}) error {
		if x.(int) > 2 {
			cancel()
		}
		return nil
	})
	assert.Equal(t, "context canceled", err.Error())
}

func BenchmarkPipeline(b *testing.B) {
	channels := 50

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink1 := NewSink("sink1")
	source1 := NewSource("source1")

	pipeline1 := NewPipeline(ctx).Add(source1)
	for i := 0; i < channels; i++ {
		t := NewMockAddTransform("t")
		pipeline1.Add(t)
	}
	pipeline1.Add(sink1)
	pipeline1.Run()

	go func() {
		out := source1.Out()
		defer out.Close()
		for i := 0; i < b.N; i++ {
			out.Send(i)
		}
	}()

	_ = pipeline1.Wait(func(x interface{}) error {
		return nil
	})
}
