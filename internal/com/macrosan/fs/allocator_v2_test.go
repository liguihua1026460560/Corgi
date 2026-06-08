package fs

import (
	//"fmt"
	"math/rand"
	"sync"
	"testing"
	//"time"
)

func TestAllocatorNewAvailableEqualsDiskSize(t *testing.T) {
	const diskSize = int64(12345)
	a := NewAllocatorV2(diskSize)
	if a.Total() != diskSize {
		t.Fatalf("Total()=%d, want %d", a.Total(), diskSize)
	}
	if a.Available() != diskSize {
		t.Fatalf("Available()=%d, want %d", a.Available(), diskSize)
	}
}

func TestAllocatorSequentialAllocateOffsetsIncrease(t *testing.T) {
	a := NewAllocatorV2(100)

	first, err := a.Allocate(10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.Allocate(5)
	if err != nil {
		t.Fatal(err)
	}
	third, err := a.Allocate(20)
	if err != nil {
		t.Fatal(err)
	}

	assertSingleExtent(t, first, 0, 10)
	assertSingleExtent(t, second, 10, 5)
	assertSingleExtent(t, third, 15, 20)
}

func TestAllocatorAllocateDecreasesAvailable(t *testing.T) {
	a := NewAllocatorV2(100)
	if _, err := a.Allocate(25); err != nil {
		t.Fatal(err)
	}
	if got, want := a.Available(), int64(75); got != want {
		t.Fatalf("Available()=%d, want %d", got, want)
	}
}

func TestAllocatorReleaseIncreasesAvailable(t *testing.T) {
	a := NewAllocatorV2(100)
	res, err := a.Allocate(25)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Release(res); err != nil {
		t.Fatal(err)
	}
	if got, want := a.Available(), int64(100); got != want {
		t.Fatalf("Available()=%d, want %d", got, want)
	}
}

func TestAllocatorReleaseThenAllocateReusesSpace(t *testing.T) {
	a := NewAllocatorV2(100)
	first, err := a.Allocate(10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Allocate(10); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(first); err != nil {
		t.Fatal(err)
	}

	reused, err := a.Allocate(4)
	if err != nil {
		t.Fatal(err)
	}
	assertSingleExtent(t, reused, 0, 4)
}

func TestAllocatorInitAllocatedPreventsAllocation(t *testing.T) {
	a := NewAllocatorV2(10)
	if err := a.InitAllocated(0, 5); err != nil {
		t.Fatal(err)
	}

	res, err := a.Allocate(5)
	if err != nil {
		t.Fatal(err)
	}
	assertSingleExtent(t, res, 5, 5)
}

func TestAllocatorInsufficientSpaceReturnsErrorWithoutChangingAvailable(t *testing.T) {
	a := NewAllocatorV2(5)
	before := a.Available()

	res, err := a.Allocate(6)
	if err == nil {
		t.Fatalf("expected allocate error, got results=%v", res)
	}
	if len(res) != 0 {
		t.Fatalf("Allocate returned results on error: %v", res)
	}
	if got := a.Available(); got != before {
		t.Fatalf("Available changed after failed allocate: got %d, want %d", got, before)
	}
}

func TestAllocatorFragmentedAllocateReturnsMultipleExtents(t *testing.T) {
	a := NewAllocatorV2(10)
	if _, err := a.Allocate(10); err != nil {
		t.Fatal(err)
	}
	if err := a.Release([]Result{
		{Offset: 1, Size: 2},
		{Offset: 5, Size: 3},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := a.Allocate(5)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("len(results)=%d, want 2: %v", len(res), res)
	}
	if res[0] != (Result{Offset: 1, Size: 2}) || res[1] != (Result{Offset: 5, Size: 3}) {
		t.Fatalf("unexpected fragmented allocation: %v", res)
	}
}

func TestAllocatorDoubleReleaseReturnsErrorAndDoesNotExceedTotal(t *testing.T) {
	a := NewAllocatorV2(10)
	res, err := a.Allocate(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Release(res); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(res); err == nil {
		t.Fatal("expected double release error")
	}
	if got, want := a.Available(), a.Total(); got != want {
		t.Fatalf("Available()=%d, want total %d", got, want)
	}

	res, err = a.Allocate(2)
	if err != nil {
		t.Fatal(err)
	}
	before := a.Available()
	if err := a.Release([]Result{res[0], res[0]}); err == nil {
		t.Fatal("expected same-call duplicate release error")
	}
	if got := a.Available(); got != before {
		t.Fatalf("Available changed after duplicate release: got %d, want %d", got, before)
	}
}

func TestAllocatorOutOfBoundsOperationsReturnError(t *testing.T) {
	a := NewAllocatorV2(10)
	if _, err := a.Allocate(10); err != nil {
		t.Fatal(err)
	}

	if err := a.Release([]Result{{Offset: 9, Size: 2}}); err == nil {
		t.Fatal("expected out-of-bounds release error")
	}
	if err := a.InitAllocated(9, 2); err == nil {
		t.Fatal("expected out-of-bounds InitAllocated error")
	}
	if err := a.InitFree(9, 2); err == nil {
		t.Fatal("expected out-of-bounds InitFree error")
	}
	if got := a.Available(); got != 0 {
		t.Fatalf("Available()=%d, want 0", got)
	}
}

func TestAllocatorConcurrentAllocateReleaseNoDuplicateAllocation(t *testing.T) {
	const total = int64(2000000000)
	a := NewAllocatorV2(total)

	var allocatedMu sync.Mutex
	allocated := make(map[int64]struct{})

	var wg sync.WaitGroup
	for i := 0; i < int(16); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//start := time.Now()
			res, err := a.Allocate(1)
			if err != nil {
				t.Errorf("Allocate(1) failed: %v", err)
				return
			}
			//fmt.Printf("allocated 1 block in %s\n", time.Since(start))
			if len(res) != 1 || res[0].Size != 1 {
				t.Errorf("unexpected result: %v", res)
				return
			}
			allocatedMu.Lock()
			if _, ok := allocated[res[0].Offset]; ok {
				t.Errorf("duplicate allocation at offset %d", res[0].Offset)
			}
			allocated[res[0].Offset] = struct{}{}
			allocatedMu.Unlock()
		}()
	}
	wg.Wait()

	if len(allocated) != int(total) {
		t.Fatalf("allocated count=%d, want %d", len(allocated), total)
	}
	if got := a.Available(); got != 0 {
		t.Fatalf("Available()=%d, want 0", got)
	}

	wg = sync.WaitGroup{}
	for offset := range allocated {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			if err := a.Release([]Result{{Offset: offset, Size: 1}}); err != nil {
				t.Errorf("Release(%d) failed: %v", offset, err)
			}
		}(offset)
	}
	wg.Wait()

	if got := a.Available(); got != total {
		t.Fatalf("Available() after release=%d, want %d", got, total)
	}
}

func TestAllocatorRandomAgainstBoolModel(t *testing.T) {
	const total = int64(257)
	a := NewAllocatorV2(total)
	model := make([]bool, total)
	live := make([]Result, 0)
	rng := rand.New(rand.NewSource(1))

	for step := 0; step < 10000; step++ {
		if len(live) == 0 || rng.Intn(100) < 65 {
			want := int64(rng.Intn(32) + 1)
			before := a.Available()
			freeBefore := modelFree(model)

			res, err := a.Allocate(want)
			if freeBefore < want {
				if err == nil {
					t.Fatalf("step %d: expected allocate error for want=%d free=%d, got %v", step, want, freeBefore, res)
				}
				if got := a.Available(); got != before {
					t.Fatalf("step %d: failed allocate changed Available: got %d, want %d", step, got, before)
				}
				continue
			}
			if err != nil {
				t.Fatalf("step %d: Allocate(%d) failed with free=%d: %v", step, want, freeBefore, err)
			}
			if got := resultBlocks(res); got != want {
				t.Fatalf("step %d: allocated blocks=%d, want %d, res=%v", step, got, want, res)
			}
			for _, r := range res {
				assertModelRangeFree(t, step, model, r)
				setModelRange(model, r, true)
				live = append(live, r)
			}
		} else {
			idx := rng.Intn(len(live))
			r := live[idx]
			if err := a.Release([]Result{r}); err != nil {
				t.Fatalf("step %d: Release(%v) failed: %v", step, r, err)
			}
			setModelRange(model, r, false)
			live[idx] = live[len(live)-1]
			live = live[:len(live)-1]
		}

		if got, want := a.Available(), modelFree(model); got != want {
			t.Fatalf("step %d: Available()=%d, model free=%d", step, got, want)
		}
	}
}

func assertSingleExtent(t *testing.T, results []Result, offset, size int64) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("len(results)=%d, want 1: %v", len(results), results)
	}
	if results[0].Offset != offset || results[0].Size != size {
		t.Fatalf("result=%+v, want offset=%d size=%d", results[0], offset, size)
	}
}

func assertModelRangeFree(t *testing.T, step int, model []bool, r Result) {
	t.Helper()
	for i := r.Offset; i < r.Offset+r.Size; i++ {
		if i < 0 || i >= int64(len(model)) {
			t.Fatalf("step %d: result out of bounds: %+v", step, r)
		}
		if model[i] {
			t.Fatalf("step %d: allocator returned allocated block %d in %+v", step, i, r)
		}
	}
}

func setModelRange(model []bool, r Result, allocated bool) {
	for i := r.Offset; i < r.Offset+r.Size; i++ {
		model[i] = allocated
	}
}

func modelFree(model []bool) int64 {
	var free int64
	for _, allocated := range model {
		if !allocated {
			free++
		}
	}
	return free
}
