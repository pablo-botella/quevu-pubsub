// Package qpubsub connects quevu to Google Cloud Pub/Sub in both
// directions.
//
//   - Writer is a writer of a hub (quevu.Writer): what the log
//     distributes is published to a topic.
//   - Source is an input (a quevu.Unit): what arrives at a subscription
//     is fed to a log, and follows the normal way to its writers.
//
// The two directions are not symmetric here. Output goes to a topic;
// input comes from a subscription to that topic, which is a different
// thing with a different name, created on the Google side. The two are
// configured separately.
//
// A message that has been received must be acknowledged, or it is
// delivered again. So receiving is "at least once": the source
// acknowledges a message only after the log has accepted it, and a
// message may arrive twice.
//
// One chunk is one message.
package qpubsub

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub/v2"
	quevu "github.com/pablo-botella/quevu-kernel"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// MaxMessage is the largest publish request Pub/Sub takes.
	MaxMessage = pubsub.MaxPublishRequestBytes
	// overhead is room left for the attributes this package adds and
	// for the envelope of the request.
	overhead = 1024

	attrSeq  = "quevu-seq"
	attrTime = "quevu-time"
)

// ErrTooLarge is returned for a chunk that does not fit in one message.
// The chunk is not published and not split: the log counts it as failed.
var ErrTooLarge = errors.New("qpubsub: chunk larger than a Pub/Sub message")

// Config says which project, and as whom. What is left empty comes from
// the environment, the way the Google libraries look for it (Application
// Default Credentials).
type Config struct {
	// ProjectID is the Google Cloud project.
	ProjectID string
	// Topic is where a Writer publishes: its name, or its full path.
	Topic string
	// Subscription is where a Source reads from: its name, or its full
	// path. It is a subscription to the topic, not the topic.
	Subscription string
	// CredentialsFile is the path of a service account key file, and
	// CredentialsJSON its contents. Only that kind of credential is
	// accepted here, on purpose: the Google library warns against loading
	// a credential configuration of unknown type. Both empty: from the
	// environment.
	// They belong in the configuration of the deployment, never in
	// source code.
	CredentialsFile string
	CredentialsJSON []byte
	// Endpoint is the address of an emulator. When set, the connection
	// is made without credentials and without encryption: it is for
	// local testing only.
	Endpoint string
}

func connect(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	if cfg.ProjectID == "" {
		return nil, errors.New("qpubsub: no project")
	}
	var opts []option.ClientOption
	switch {
	case cfg.Endpoint != "":
		opts = append(opts,
			option.WithEndpoint(cfg.Endpoint),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	case len(cfg.CredentialsJSON) > 0:
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, cfg.CredentialsJSON))
	case cfg.CredentialsFile != "":
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	}
	c, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("qpubsub: %w", err)
	}
	return c, nil
}

// publisher is the part of a Pub/Sub topic this package uses. It is what
// tests replace. publish hands a message over and returns how to wait
// for the service to confirm it.
type publisher interface {
	publish(ctx context.Context, data []byte, attrs map[string]string) (confirm func(context.Context) error)
	stop()
}

type topic struct {
	p      *pubsub.Publisher
	client *pubsub.Client
}

func (t topic) publish(ctx context.Context, data []byte, attrs map[string]string) func(context.Context) error {
	res := t.p.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs})
	return func(ctx context.Context) error {
		_, err := res.Get(ctx)
		return err
	}
}

func (t topic) stop() {
	t.p.Stop()
	t.client.Close()
}

// Writer publishes the chunks of a log to a topic.
type Writer struct {
	fails quevu.Failures // the body of Run, and what it could not write

	pub publisher
	// timeout bounds the wait for a batch to be confirmed.
	timeout time.Duration
}

// NewWriter connects to the topic for publishing.
func NewWriter(ctx context.Context, cfg Config) (*Writer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("qpubsub: no topic")
	}
	c, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{pub: topic{p: c.Publisher(cfg.Topic), client: c}, timeout: 60 * time.Second}, nil
}

// Write publishes a batch, one message per chunk, and returns when the
// service has confirmed every one of them or said why not. A chunk too
// large for a message is left out and reported with ErrTooLarge; the
// rest are published.
func (c *Writer) Write(batch []quevu.Chunk) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var errs []error
	var confirms []func(context.Context) error
	for _, ch := range batch {
		if len(ch.Data)+overhead > MaxMessage {
			errs = append(errs, fmt.Errorf("%w: position %d, %d bytes", ErrTooLarge, ch.Seq, len(ch.Data)))
			continue
		}
		// A message may be empty as long as it has attributes, and it
		// always has these two: where the chunk was in its log.
		confirms = append(confirms, c.pub.publish(ctx, ch.Data, map[string]string{
			attrSeq:  strconv.FormatUint(ch.Seq, 10),
			attrTime: strconv.FormatInt(ch.Time.UnixNano(), 10),
		}))
	}
	failed := 0
	var first error
	for _, confirm := range confirms {
		if err := confirm(ctx); err != nil {
			failed++
			if first == nil {
				first = err
			}
		}
	}
	if failed > 0 {
		errs = append(errs, fmt.Errorf("qpubsub: %d of %d messages not confirmed; first: %w", failed, len(confirms), first))
	}
	return errors.Join(errs...)
}

// Close sends what is still waiting and releases the connection.
func (c *Writer) Close() error {
	c.pub.stop()
	return nil
}

var _ quevu.Writer = (*Writer)(nil)

// Run receives from the channel the hub gave this writer until it is
// closed, writing what arrives in batches, and closes the writer at the
// end. It is what makes it a quevu.Writer.
func (c *Writer) Run(in <-chan quevu.Chunk) error { return c.fails.Drain(in, 0, c) }

// Failed returns the chunks lost so far in failed batches, and the last
// error.
func (c *Writer) Failed() (uint64, error) { return c.fails.Failed() }
