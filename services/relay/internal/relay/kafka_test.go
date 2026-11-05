package relay

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaPublishKeepsEachKeyOnOnePartitionInOrder(t *testing.T) {
	brokers := os.Getenv("RELAY_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("RELAY_TEST_KAFKA_BROKERS is not set")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("relay-test-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pub, err := NewKafkaPublisher(seeds, kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	var msgs []Message
	for i := range 6 {
		key := []string{"stripe", "github"}[i%2]
		msgs = append(msgs, Message{ID: int64(i), Topic: topic, Key: key, Payload: fmt.Appendf(nil, `{"n":%d}`, i)})
	}
	for i, err := range pub.Publish(ctx, msgs) {
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	partitions := map[string]int32{}
	got := map[string][]string{}
	for n := 0; n < len(msgs); {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("received %d of %d messages: %v", n, len(msgs), err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			key := string(r.Key)
			if p, seen := partitions[key]; seen && p != r.Partition {
				t.Errorf("key %q split across partitions %d and %d", key, p, r.Partition)
			}
			partitions[key] = r.Partition
			got[key] = append(got[key], string(r.Value))
			n++
		})
	}

	want := map[string][]string{
		"stripe": {`{"n":0}`, `{"n":2}`, `{"n":4}`},
		"github": {`{"n":1}`, `{"n":3}`, `{"n":5}`},
	}
	for key, values := range want {
		if strings.Join(got[key], " ") != strings.Join(values, " ") {
			t.Errorf("%s received %v, want %v", key, got[key], values)
		}
	}
}
