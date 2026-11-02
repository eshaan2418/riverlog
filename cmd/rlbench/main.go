// Command rlbench drives the same workloads against riverlog, Kafka and
// NATS JetStream.
//
//	rlbench -system riverlog|kafka|nats -mode throughput|latency [flags]
//
// throughput: P producers send N records of S bytes as fast as acks allow
// (async, batched); reports records/s, MB/s and ack latency percentiles.
// latency: one producer sends at a fixed rate; a consumer reads; reports
// produce-to-consume latency percentiles.
//
// Output is one JSON line.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eshaan2418/riverlog/client"
)

var (
	system     = flag.String("system", "riverlog", "riverlog | kafka | nats")
	addr       = flag.String("addr", "", "server address (default per system)")
	mode       = flag.String("mode", "throughput", "throughput | latency")
	topic      = flag.String("topic", "", "topic/stream name (default: unique per run)")
	partitions = flag.Int("partitions", 1, "partitions (riverlog, kafka); NATS streams have one")
	producers  = flag.Int("producers", 1, "producer goroutines, each with its own connection")
	records    = flag.Int("records", 1_000_000, "records per producer (throughput)")
	size       = flag.Int("size", 100, "record value size in bytes")
	linger     = flag.Duration("linger", time.Millisecond, "producer linger")
	batchBytes = flag.Int("batch-bytes", 256<<10, "producer batch size")
	rate       = flag.Int("rate", 10000, "records/s (latency mode)")
	duration   = flag.Duration("duration", 10*time.Second, "latency mode run time")
	kafkaFsync = flag.Bool("kafka-fsync", false, "create the Kafka topic with flush.messages=1 (fsync each batch)")
)

type result struct {
	System     string  `json:"system"`
	Mode       string  `json:"mode"`
	Partitions int     `json:"partitions"`
	Producers  int     `json:"producers"`
	Size       int     `json:"size"`
	Records    int64   `json:"records"`
	Seconds    float64 `json:"seconds"`
	RecordsPS  float64 `json:"records_per_s"`
	MBPS       float64 `json:"mb_per_s"`
	P50ms      float64 `json:"p50_ms"`
	P99ms      float64 `json:"p99_ms"`
	P999ms     float64 `json:"p999_ms"`
	Errors     int64   `json:"errors"`
}

// A driver produces and consumes on one system.
type driver interface {
	setup(name string) error
	// producer returns a send function: send(value, done) calls done(err)
	// when the record is acknowledged, and a flush function.
	producer() (send func([]byte, func(error)), flush func(), err error)
	// consume calls fn for each value, from the start of the topic, until ctx ends.
	consume(ctx context.Context, name string, fn func([]byte)) error
	close()
}

func main() {
	flag.Parse()
	name := *topic
	if name == "" {
		name = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	var d driver
	switch *system {
	case "riverlog":
		d = &riverlogDriver{addr: def(*addr, "127.0.0.1:9192")}
	case "kafka":
		d = &kafkaDriver{addr: def(*addr, "127.0.0.1:9092")}
	case "nats":
		d = &natsDriver{addr: def(*addr, "nats://127.0.0.1:4222")}
	default:
		log.Fatalf("unknown system %q", *system)
	}
	if err := d.setup(name); err != nil {
		log.Fatal(err)
	}
	defer d.close()
	var r result
	switch *mode {
	case "throughput":
		r = throughput(d)
	case "latency":
		r = latency(d, name)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	r.System, r.Mode, r.Partitions, r.Producers, r.Size = *system, *mode, *partitions, *producers, *size
	if *system == "nats" {
		r.Partitions = 1
	}
	json.NewEncoder(os.Stdout).Encode(r)
}

func def(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func percentiles(r *result, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	at := func(p float64) float64 {
		return float64(lat[int(p*float64(len(lat)-1))]) / 1e6
	}
	r.P50ms, r.P99ms, r.P999ms = at(0.5), at(0.99), at(0.999)
}

func throughput(d driver) result {
	var errs atomic.Int64
	lats := make([][]time.Duration, *producers)
	var wg sync.WaitGroup
	start := time.Now()
	for p := 0; p < *producers; p++ {
		send, flush, err := d.producer()
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			lat := make([]time.Duration, *records)
			val := make([]byte, *size)
			var done sync.WaitGroup
			done.Add(*records)
			for i := 0; i < *records; i++ {
				i := i
				t0 := time.Now()
				send(val, func(err error) {
					if err != nil {
						errs.Add(1)
					}
					lat[i] = time.Since(t0)
					done.Done()
				})
			}
			flush()
			done.Wait()
			lats[p] = lat
		}(p)
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	n := int64(*records) * int64(*producers)
	r := result{Records: n, Seconds: secs, RecordsPS: float64(n) / secs, MBPS: float64(n) * float64(*size) / secs / 1e6, Errors: errs.Load()}
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	percentiles(&r, all)
	return r
}

func latency(d driver, name string) result {
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var lat []time.Duration
	base := time.Now()
	consumed := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		defer close(consumed)
		once := sync.Once{}
		go func() { time.Sleep(500 * time.Millisecond); once.Do(func() { close(ready) }) }()
		d.consume(ctx, name, func(v []byte) {
			if len(v) < 8 {
				return
			}
			sent := time.Duration(binary.LittleEndian.Uint64(v))
			l := time.Since(base) - sent
			mu.Lock()
			lat = append(lat, l)
			mu.Unlock()
		})
	}()
	<-ready // consumer subscribed
	send, flush, err := d.producer()
	if err != nil {
		log.Fatal(err)
	}
	var errs atomic.Int64
	n := int(float64(*rate) * duration.Seconds())
	interval := time.Second / time.Duration(*rate)
	start := time.Now()
	for i := 0; i < n; i++ {
		next := start.Add(time.Duration(i) * interval)
		if dt := time.Until(next); dt > 0 {
			time.Sleep(dt)
		}
		val := make([]byte, max(*size, 8))
		binary.LittleEndian.PutUint64(val, uint64(time.Since(base)))
		send(val, func(err error) {
			if err != nil {
				errs.Add(1)
			}
		})
	}
	flush()
	// Let the consumer drain.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(lat)
		mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-consumed
	secs := time.Since(start).Seconds()
	mu.Lock()
	defer mu.Unlock()
	r := result{Records: int64(len(lat)), Seconds: secs, RecordsPS: float64(len(lat)) / secs, Errors: errs.Load() + int64(n-len(lat))}
	percentiles(&r, lat)
	return r
}

// --- riverlog ---

type riverlogDriver struct {
	addr  string
	topic string
	conns []*client.Conn
}

func (r *riverlogDriver) setup(name string) error {
	r.topic = name
	cn, err := client.Dial(r.addr)
	if err != nil {
		return err
	}
	r.conns = append(r.conns, cn)
	return cn.CreateTopic(name, *partitions)
}

func (r *riverlogDriver) producer() (func([]byte, func(error)), func(), error) {
	cn, err := client.Dial(r.addr)
	if err != nil {
		return nil, nil, err
	}
	r.conns = append(r.conns, cn)
	p, err := client.NewProducer(cn, r.topic, client.ProducerConfig{Linger: *linger, BatchBytes: *batchBytes, MaxInFlight: 16})
	if err != nil {
		return nil, nil, err
	}
	send := func(v []byte, done func(error)) {
		p.Produce(nil, v, func(res client.Result) { done(res.Err) })
	}
	return send, p.Flush, nil
}

func (r *riverlogDriver) consume(ctx context.Context, name string, fn func([]byte)) error {
	var wg sync.WaitGroup
	for part := 0; part < *partitions; part++ {
		cn, err := client.Dial(r.addr)
		if err != nil {
			return err
		}
		r.conns = append(r.conns, cn)
		wg.Add(1)
		go func(part uint32) {
			defer wg.Done()
			var off uint64
			for ctx.Err() == nil {
				recs, _, err := cn.Fetch(name, part, off, 1<<20, 100*time.Millisecond)
				if err != nil {
					return
				}
				for _, rec := range recs {
					fn(rec.Value)
					off = rec.Offset + 1
				}
			}
		}(uint32(part))
	}
	wg.Wait()
	return nil
}

func (r *riverlogDriver) close() {
	for _, c := range r.conns {
		c.Close()
	}
}

// --- Kafka ---

type kafkaDriver struct {
	addr    string
	topic   string
	clients []*kgo.Client
}

func (k *kafkaDriver) setup(name string) error {
	k.topic = name
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.addr))
	if err != nil {
		return err
	}
	k.clients = append(k.clients, cl)
	adm := kadm.NewClient(cl)
	cfg := map[string]*string{}
	if *kafkaFsync {
		one := "1"
		cfg["flush.messages"] = &one
	}
	resp, err := adm.CreateTopics(context.Background(), int32(*partitions), 1, cfg, name)
	if err != nil {
		return err
	}
	for _, t := range resp {
		if t.Err != nil {
			return t.Err
		}
	}
	return nil
}

func (k *kafkaDriver) producer() (func([]byte, func(error)), func(), error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.addr),
		kgo.DefaultProduceTopic(k.topic),
		kgo.ProducerLinger(*linger),
		kgo.ProducerBatchMaxBytes(int32(*batchBytes)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(), // riverlog and the NATS driver are not idempotent either
		kgo.MaxBufferedRecords(1<<20),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	if err != nil {
		return nil, nil, err
	}
	k.clients = append(k.clients, cl)
	send := func(v []byte, done func(error)) {
		cl.Produce(context.Background(), &kgo.Record{Value: v}, func(_ *kgo.Record, err error) { done(err) })
	}
	flush := func() { cl.Flush(context.Background()) }
	return send, flush, nil
}

func (k *kafkaDriver) consume(ctx context.Context, name string, fn func([]byte)) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.addr),
		kgo.ConsumeTopics(name),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	if err != nil {
		return err
	}
	k.clients = append(k.clients, cl)
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) { fn(r.Value) })
	}
	return nil
}

func (k *kafkaDriver) close() {
	for _, c := range k.clients {
		c.Close()
	}
}

// --- NATS JetStream ---

type natsDriver struct {
	addr    string
	subject string
	conns   []*nats.Conn
	js      jetstream.JetStream
}

func (n *natsDriver) setup(name string) error {
	n.subject = name + ".data"
	nc, err := nats.Connect(n.addr)
	if err != nil {
		return err
	}
	n.conns = append(n.conns, nc)
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	n.js = js
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name: name, Subjects: []string{n.subject}, Storage: jetstream.FileStorage, Replicas: 1,
	})
	return err
}

func (n *natsDriver) producer() (func([]byte, func(error)), func(), error) {
	nc, err := nats.Connect(n.addr)
	if err != nil {
		return nil, nil, err
	}
	n.conns = append(n.conns, nc)
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(4096))
	if err != nil {
		return nil, nil, err
	}
	// Acks arrive in publish order, so one goroutine resolves them in order.
	type pendingAck struct {
		f    jetstream.PubAckFuture
		done func(error)
	}
	acks := make(chan pendingAck, 8192)
	resolved := make(chan struct{})
	go func() {
		defer close(resolved)
		for a := range acks {
			select {
			case <-a.f.Ok():
				a.done(nil)
			case err := <-a.f.Err():
				a.done(err)
			}
		}
	}()
	send := func(v []byte, done func(error)) {
		f, err := js.PublishAsync(n.subject, v)
		if err != nil {
			done(err)
			return
		}
		acks <- pendingAck{f, done}
	}
	flush := func() {
		<-js.PublishAsyncComplete()
		close(acks)
		<-resolved
	}
	return send, flush, nil
}

func (n *natsDriver) consume(ctx context.Context, name string, fn func([]byte)) error {
	nc, err := nats.Connect(n.addr)
	if err != nil {
		return err
	}
	n.conns = append(n.conns, nc)
	js, _ := jetstream.New(nc)
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return err
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		return err
	}
	cc, err := cons.Consume(func(m jetstream.Msg) { fn(m.Data()) })
	if err != nil {
		return err
	}
	<-ctx.Done()
	cc.Stop()
	return nil
}

func (n *natsDriver) close() {
	for _, c := range n.conns {
		c.Close()
	}
}
