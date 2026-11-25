package qpubsub

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pablo-botella/quevu/qkernel"
)

// These tests run against stand-ins for a topic and a subscription. They
// prove the logic of this package, not that the real service agrees with
// it: nothing here has talked to Google.

type published struct {
	data  string
	attrs map[string]string
}

type fakeTopic struct {
	mu      sync.Mutex
	got     []published
	refuse  map[string]error
	stopped bool
}

func (f *fakeTopic) publish(_ context.Context, data []byte, attrs map[string]string) func(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.refuse[string(data)]
	if err == nil {
		f.got = append(f.got, published{string(data), attrs})
	}
	return func(context.Context) error { return err }
}

func (f *fakeTopic) stop() { f.stopped = true }

// fakeSub delivers its messages and, like the service, delivers again
// the ones that were not acknowledged.
type fakeSub struct {
	mu      sync.Mutex
	pending []string
	acked   []string
	fail    error
	closed  bool
}

func (f *fakeSub) receive(ctx context.Context, fn func([]byte) bool) error {
	if f.fail != nil {
		return f.fail
	}
	for ctx.Err() == nil {
		f.mu.Lock()
		var m string
		have := len(f.pending) > 0
		if have {
			m = f.pending[0]
			f.pending = f.pending[1:]
		}
		f.mu.Unlock()
		if !have {
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		if fn([]byte(m)) {
			f.mu.Lock()
			f.acked = append(f.acked, m)
			f.mu.Unlock()
		} else {
			f.mu.Lock()
			f.pending = append(f.pending, m)
			f.mu.Unlock()
		}
	}
	return nil
}

func (f *fakeSub) close() { f.closed = true }

func (f *fakeSub) left() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.pending...)
}

type log struct {
	mu     sync.Mutex
	got    []string
	refuse string
}

func (l *log) Write(p []byte) (int, error) { return len(p), nil }

func (l *log) Append(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.refuse != "" && string(data) == l.refuse {
		return errors.New("log closed")
	}
	l.got = append(l.got, string(data))
	return nil
}

func (l *log) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got...)
}

func chunks(texts ...string) []qkernel.Chunk {
	out := make([]qkernel.Chunk, len(texts))
	for i, t := range texts {
		out[i] = qkernel.Chunk{Seq: uint64(i + 1), Time: time.Unix(1700000000, int64(i)), Data: []byte(t)}
	}
	return out
}

func wait(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("never happened: " + what)
		}
	}
}

func TestOneChunkOneMessageAsItIs(t *testing.T) {
	top := &fakeTopic{}
	c := &Writer{pub: top, timeout: time.Second}
	// Pub/Sub carries bytes: nothing needs encoding, not even nothing.
	in := []string{"text áñ", "\x00\xff binary", ""}
	if err := c.Write(chunks(in...)); err != nil {
		t.Fatal(err)
	}
	if len(top.got) != 3 {
		t.Fatalf("published %d", len(top.got))
	}
	for i, p := range top.got {
		if p.data != in[i] {
			t.Fatalf("message %d: %q", i, p.data)
		}
	}
	// Where a chunk came from travels with it.
	if top.got[1].attrs[attrSeq] != "2" || top.got[1].attrs[attrTime] == "" {
		t.Fatalf("attributes: %v", top.got[1].attrs)
	}
	c.Close()
	if !top.stopped {
		t.Fatal("publisher not stopped")
	}
}

func TestAChunkTooLargeIsLeftOutAndSaid(t *testing.T) {
	top := &fakeTopic{}
	c := &Writer{pub: top, timeout: time.Second}
	err := c.Write(chunks("before", strings.Repeat("x", MaxMessage), "after"))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if len(top.got) != 2 {
		t.Fatalf("published %d: the rest of the batch should have gone", len(top.got))
	}
}

func TestWhatTheServiceDoesNotConfirmIsAnError(t *testing.T) {
	boom := errors.New("permission denied")
	top := &fakeTopic{refuse: map[string]error{"b": boom}}
	c := &Writer{pub: top, timeout: time.Second}
	err := c.Write(chunks("a", "b", "c"))
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("got %v", err)
	}
	if len(top.got) != 2 {
		t.Fatalf("published %d", len(top.got))
	}
}

func TestAMessageIsAcknowledgedOnlyAfterTheLogTookIt(t *testing.T) {
	sub := &fakeSub{pending: []string{"one", "two", "\x00binary", ""}}
	var l log
	s := &Source{sub: sub, into: &l}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("started twice")
	}
	wait(t, "four messages in the log", func() bool { return len(l.all()) == 4 })
	if got := strings.Join(l.all(), "|"); got != "one|two|\x00binary|" {
		t.Fatalf("log: %q", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-s.Done(); err != nil {
		t.Fatalf("a stop reported as a failure: %v", err)
	}
	if len(sub.acked) != 4 || len(sub.left()) != 0 || s.Taken() != 4 {
		t.Fatalf("acked %d, left %d, taken %d", len(sub.acked), len(sub.left()), s.Taken())
	}
	s.Close()
	if !sub.closed {
		t.Fatal("not closed")
	}
}

func TestWhatTheLogRefusesIsDeliveredAgain(t *testing.T) {
	sub := &fakeSub{pending: []string{"one", "two", "three"}}
	l := &log{refuse: "two"}
	s := &Source{sub: sub, into: l}
	s.Start(context.Background())
	select {
	case err := <-s.Done():
		if err == nil || !strings.Contains(err.Error(), "log closed") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the source went on with a log that takes nothing")
	}
	// "one" is in the log and acknowledged; "two" was not acknowledged,
	// so the service still has it, and "three" was never reached.
	if got := strings.Join(l.all(), "|"); got != "one" {
		t.Fatalf("log: %q", got)
	}
	if strings.Join(sub.acked, "|") != "one" {
		t.Fatalf("acked: %v", sub.acked)
	}
	left := strings.Join(sub.left(), "|")
	if !strings.Contains(left, "two") || !strings.Contains(left, "three") {
		t.Fatalf("left with the service: %q", left)
	}
}

func TestASubscriptionThatFailsIsAFailure(t *testing.T) {
	sub := &fakeSub{fail: errors.New("subscription not found")}
	var l log
	s := &Source{sub: sub, into: &l}
	s.Start(context.Background())
	if err := <-s.Done(); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("got %v", err)
	}
	// And it can be started again once that is fixed.
	sub.fail = nil
	sub.pending = []string{"back"}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait(t, "the message after the fix", func() bool { return len(l.all()) == 1 })
	s.Stop(context.Background())
}

func TestConfigIsChecked(t *testing.T) {
	ctx := context.Background()
	if _, err := NewWriter(ctx, Config{ProjectID: "p"}); err == nil {
		t.Fatal("no topic accepted")
	}
	if _, err := NewWriter(ctx, Config{Topic: "t"}); err == nil {
		t.Fatal("no project accepted")
	}
	if _, err := NewSource(ctx, Config{ProjectID: "p", Subscription: "s"}, nil); err == nil {
		t.Fatal("no log accepted")
	}
	if _, err := NewSource(ctx, Config{ProjectID: "p"}, &log{}); err == nil {
		t.Fatal("no subscription accepted")
	}
	// Building a client for an emulator needs no network and no account:
	// the connection is made when it is first used.
	c, err := NewWriter(ctx, Config{ProjectID: "example-project", Topic: "example-topic", Endpoint: "127.0.0.1:1"})
	if err != nil || c == nil {
		t.Fatalf("client: %v", err)
	}
	c.Close()
}
