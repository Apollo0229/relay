package relay

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher produces messages with franz-go's defaults: an idempotent
// producer that waits for all in-sync replicas, so an acknowledged message is
// durable and a retried send is never written twice.
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher connects to the given brokers. Extra options are appended
// to the defaults.
func NewKafkaPublisher(brokers []string, opts ...kgo.Opt) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(0), // a batch is already in hand; send it now
	}, opts...)...)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{client: client}, nil
}

// Ping checks that at least one broker is reachable.
func (p *KafkaPublisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

func (p *KafkaPublisher) Close() { p.client.Close() }

// Publish sends msgs and waits for every acknowledgement. Messages that share
// a key land on the same partition, in order.
func (p *KafkaPublisher) Publish(ctx context.Context, msgs []Message) []error {
	records := make([]*kgo.Record, len(msgs))
	index := make(map[*kgo.Record]int, len(msgs))
	for i, m := range msgs {
		records[i] = &kgo.Record{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload}
		index[records[i]] = i
	}

	errs := make([]error, len(msgs))
	// Results arrive in completion order, not input order.
	for _, res := range p.client.ProduceSync(ctx, records...) {
		errs[index[res.Record]] = res.Err
	}
	return errs
}
