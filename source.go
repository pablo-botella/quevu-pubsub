package qpubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub/v2"
	"github.com/pablo-botella/quevu/qkernel"
)

// receiver is the part of a Pub/Sub subscription this package uses. It
// is what tests replace. receive blocks, calling f for every message,
// until its context is done; f says whether the message was taken.
type receiver interface {
	receive(ctx context.Context, f func(data []byte) (taken bool)) error
	close()
}

type subscription struct {
	s      *pubsub.Subscriber
	client *pubsub.Client
}

func (s subscription) receive(ctx context.Context, f func([]byte) bool) error {
	return s.s.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		if f(m.Data) {
			m.Ack()
		} else {
			// Not acknowledged: the service delivers it again.
			m.Nack()
		}
	})
}

func (s subscription) close() { s.client.Close() }

// Source reads a subscription and feeds what arrives to a log.
type Source struct {
	sub  receiver
	into qkernel.Intake

	mu     sync.Mutex
	cancel context.CancelFunc
	fin    chan struct{}
	done   chan error

	taken atomic.Uint64
}

// NewSource connects to the subscription for receiving. into is the log
// every message is appended to, as one chunk each.
func NewSource(ctx context.Context, cfg Config, into qkernel.Intake) (*Source, error) {
	if into == nil {
		return nil, errors.New("qpubsub: no log to feed")
	}
	if cfg.Subscription == "" {
		return nil, errors.New("qpubsub: no subscription")
	}
	c, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Source{sub: subscription{s: c.Subscriber(cfg.Subscription), client: c}, into: into}, nil
}

// Start begins receiving. It returns at once.
func (s *Source) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fin != nil {
		select {
		case <-s.fin:
		default:
			return errors.New("qpubsub: already running")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	fin := make(chan struct{})
	done := make(chan error, 1)
	s.cancel, s.fin, s.done = cancel, fin, done
	go func() {
		err := s.run(ctx, cancel)
		cancel()
		done <- err
		close(done)
		close(fin)
	}()
	return nil
}

// run receives until it is told to stop or the log refuses a message.
func (s *Source) run(ctx context.Context, cancel context.CancelFunc) error {
	var mu sync.Mutex
	var refused error
	err := s.sub.receive(ctx, func(data []byte) bool {
		// A message is acknowledged only once it is in the log, so the
		// worst that can happen is that it arrives twice.
		if aerr := s.into.Append(data); aerr != nil {
			mu.Lock()
			if refused == nil {
				refused = fmt.Errorf("qpubsub: the log did not take a message: %w", aerr)
			}
			mu.Unlock()
			// A log that takes nothing is the end of this input.
			cancel()
			return false
		}
		s.taken.Add(1)
		return true
	})
	mu.Lock()
	defer mu.Unlock()
	if refused != nil {
		return refused
	}
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("qpubsub: %w", err)
	}
	return nil
}

// Stop ends the receiving in an orderly way: messages already handed to
// the log are acknowledged before it returns.
func (s *Source) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel, fin := s.cancel, s.fin
	s.mu.Unlock()
	if fin == nil {
		return nil
	}
	cancel()
	select {
	case <-fin:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done yields the result of the current run when it ends.
func (s *Source) Done() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Close releases the connection. It is for a source that will not be
// started again.
func (s *Source) Close() error {
	s.sub.close()
	return nil
}

// Taken returns how many messages the log has accepted.
func (s *Source) Taken() uint64 { return s.taken.Load() }

var (
	_ qkernel.Unit   = (*Source)(nil)
	_ qkernel.Exiter = (*Source)(nil)
)
