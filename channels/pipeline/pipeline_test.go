package pipeline

import (
	"reflect"
	"testing"
	"time"
)

func drain[T any](ch <-chan T) []T {
	var result []T
	for v := range ch {
		result = append(result, v)
	}
	return result
}

func TestGenerate_EmitsValuesInOrder(t *testing.T) {
	got := drain(generate(1, 2, 3, 4, 5))
	want := []int{1, 2, 3, 4, 5}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("generate() = %v, want %v", got, want)
	}
}

func TestGenerate_NoValues_ClosesEmpty(t *testing.T) {
	got := drain(generate[int]())

	if got != nil {
		t.Fatalf("generate() без аргументов должен дать пустой канал, got %v", got)
	}
}

func TestGenerate_WorksWithAnyType(t *testing.T) {
	got := drain(generate("a", "b", "c"))
	want := []string{"a", "b", "c"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("generate() = %v, want %v", got, want)
	}
}

func TestProcess_AppliesActionToEachValue(t *testing.T) {
	square := func(v int) int { return v * v }

	got := drain(process(generate(1, 2, 3, 4, 5), square))
	want := []int{1, 4, 9, 16, 25}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("process() = %v, want %v", got, want)
	}
}

func TestProcess_PreservesOrder(t *testing.T) {
	identity := func(v int) int { return v }

	got := drain(process(generate(5, 4, 3, 2, 1), identity))
	want := []int{5, 4, 3, 2, 1}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("process() нарушил порядок: got %v, want %v", got, want)
	}
}

func TestProcess_EmptyInput_ClosesEmpty(t *testing.T) {
	square := func(v int) int { return v * v }

	got := drain(process(generate[int](), square))

	if got != nil {
		t.Fatalf("process() пустого канала должен дать пустой канал, got %v", got)
	}
}

func TestPipeline_MultiStage(t *testing.T) {
	square := func(v int) int { return v * v }
	addOne := func(v int) int { return v + 1 }

	stage1 := generate(1, 2, 3)
	stage2 := process(stage1, square)
	stage3 := process(stage2, addOne)

	got := drain(stage3)
	want := []int{2, 5, 10} // 1²+1, 2²+1, 3²+1

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pipeline = %v, want %v", got, want)
	}
}

func TestPipeline_ClosesDownstreamChannel(t *testing.T) {
	done := make(chan struct{})

	go func() {
		drain(process(generate(1, 2, 3), func(v int) int { return v }))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("downstream-канал не закрылся: process не закаскадировал close()")
	}
}
