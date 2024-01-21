package signal

import (
	"context"
	"testing"
	"time"

	"github.com/mazzegi/mbox/mathx"
	"github.com/mazzegi/mbox/testx"
)

func TestSignal(t *testing.T) {
	var topic1 string = "topic_1"
	var topic2 string = "topic_2"

	sig := New()
	emitAfter := func(t string, dur time.Duration) {
		time.AfterFunc(dur, func() { sig.Emit(t) })
	}

	ctx := context.Background()

	t0 := time.Now()
	emitAfter(topic1, 20*time.Millisecond)
	ok := sig.WaitContext(ctx, topic1, 50*time.Millisecond)
	if !ok {
		t.Fatalf("wait failed but shouldn't")
	}
	testx.AssertInRange(t, time.Since(t0), 20*time.Millisecond, 30*time.Millisecond)

	emitAfter(topic1, 50*time.Millisecond)
	ok = sig.WaitContext(ctx, topic1, 20*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}
	ok = sig.WaitContext(ctx, topic1, 50*time.Millisecond)
	if !ok {
		t.Fatalf("wait failed but shouldn't")
	}

	emitAfter(topic2, 20*time.Millisecond)
	ok = sig.WaitContext(ctx, topic1, 50*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer dcancel()
	emitAfter(topic1, 50*time.Millisecond)
	ok = sig.WaitContext(dctx, topic1, 20*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}
}

func FuzzSignal(f *testing.F) {
	var topic1 string = "topic_1"
	ctx := context.Background()
	emitAfter := func(sig *Signals, t string, dur time.Duration) {
		time.AfterFunc(dur, func() { sig.Emit(t) })
	}

	wait := 50 * time.Millisecond
	testcasesMs := []uint{40, 50, 60, 150}
	for _, tc := range testcasesMs {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, emitAfterMS uint) {
		// add 20ms to ensure we have a positive (not == 0) input
		emitAfterMS = 20 + emitAfterMS

		sig := New()
		emitAfterDur := time.Millisecond * time.Duration(emitAfterMS)
		emitAfter(sig, topic1, emitAfterDur)
		ok := sig.WaitContext(ctx, topic1, wait)

		if mathx.Abs(emitAfterDur-wait) < 10*time.Millisecond {
			// we cannot estimate whats ok
		} else {
			expOk := emitAfterDur < wait
			if ok != expOk {
				t.Fatalf("emit-after-dur=%s; expect wait to return %t, got %t", emitAfterDur, expOk, ok)
			}
		}
	})

}
