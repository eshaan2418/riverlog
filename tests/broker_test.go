package tests

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/eshaan2418/riverlog/client"
	"github.com/eshaan2418/riverlog/internal/broker"
	rlog "github.com/eshaan2418/riverlog/internal/log"
	"github.com/eshaan2418/riverlog/internal/wire"
)

func startBroker(t *testing.T, dir string) (*broker.Broker, string) {
	t.Helper()
	opts := rlog.DefaultOptions()
	opts.SegmentBytes = 64 << 10
	b, err := broker.Open(broker.Config{Dir: dir, LogOpts: opts})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go b.Serve(ln)
	return b, ln.Addr().String()
}

func TestProduceFetchOrdering(t *testing.T) {
	b, addr := startBroker(t, t.TempDir())
	defer b.Close()
	cn, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	if err := cn.CreateTopic("events", 4); err != nil {
		t.Fatal(err)
	}
	if err := cn.CreateTopic("events", 4); !wire.IsCode(err, wire.TopicExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	p, err := client.NewProducer(cn, "events", client.ProducerConfig{Linger: time.Millisecond, BatchBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	const n = 20000
	var mu sync.Mutex
	acked := map[string]client.Result{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user-%d", i%37)
		val := fmt.Sprintf("%s:%06d", key, i)
		p.Produce([]byte(key), []byte(val), func(r client.Result) {
			if r.Err != nil {
				t.Error(r.Err)
			}
			mu.Lock()
			acked[val] = r
			mu.Unlock()
		})
	}
	p.Flush()
	if len(acked) != n {
		t.Fatalf("acked %d", len(acked))
	}
	// Read every partition back: per-key order preserved, offsets match acks.
	seen := 0
	lastPerKey := map[string]int{}
	for part := uint32(0); part < 4; part++ {
		var off uint64
		for {
			recs, end, err := cn.Fetch("events", part, off, 1<<16, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range recs {
				var key string
				var i int
				fmt.Sscanf(string(r.Value), "user-%s", &key)
				fmt.Sscanf(string(r.Value[len(r.Value)-6:]), "%d", &i)
				k := string(r.Key)
				if prev, ok := lastPerKey[k]; ok && i <= prev {
					t.Fatalf("key %s out of order", k)
				}
				lastPerKey[k] = i
				a := acked[string(r.Value)]
				if a.Partition != part || a.Offset != r.Offset {
					t.Fatalf("ack %+v vs record %v", a, r)
				}
				seen++
			}
			if len(recs) > 0 {
				off = recs[len(recs)-1].Offset + 1
			}
			if off >= end {
				break
			}
		}
	}
	if seen != n {
		t.Fatalf("read %d of %d", seen, n)
	}
}

func TestLongPollFetch(t *testing.T) {
	b, addr := startBroker(t, t.TempDir())
	defer b.Close()
	cn, _ := client.Dial(addr)
	defer cn.Close()
	cn.CreateTopic("lp", 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		c2, _ := client.Dial(addr)
		p, _ := client.NewProducer(c2, "lp", client.ProducerConfig{})
		p.Produce(nil, []byte("wake"), nil)
		p.Flush()
	}()
	start := time.Now()
	recs, _, err := cn.Fetch("lp", 0, 0, 1024, 5*time.Second)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "wake" {
		t.Fatalf("%v %v", recs, err)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("long poll returned after %v", d)
	}
}

func TestRestartKeepsDataAndOffsets(t *testing.T) {
	dir := t.TempDir()
	b, addr := startBroker(t, dir)
	cn, _ := client.Dial(addr)
	cn.CreateTopic("t", 2)
	p, _ := client.NewProducer(cn, "t", client.ProducerConfig{})
	for i := 0; i < 1000; i++ {
		p.ProduceTo(uint32(i%2), nil, []byte(fmt.Sprint(i)), nil)
	}
	p.Flush()
	c, err := client.NewConsumer(cn, client.ConsumerConfig{Group: "g", Member: "m1", Topics: []string{"t"}, FromStart: true})
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for got < 600 {
		recs, _ := c.Poll(time.Second)
		got += len(recs)
	}
	if err := c.Commit(); err != nil {
		t.Fatal(err)
	}
	c.Close()
	cn.Close()
	b.Close()

	b, addr = startBroker(t, dir)
	defer b.Close()
	cn, _ = client.Dial(addr)
	defer cn.Close()
	for part := uint32(0); part < 2; part++ {
		_, end, err := cn.Offsets("t", part)
		if err != nil || end != 500 {
			t.Fatalf("partition %d end %d %v", part, end, err)
		}
	}
	c, _ = client.NewConsumer(cn, client.ConsumerConfig{Group: "g", Member: "m2", Topics: []string{"t"}, FromStart: true})
	defer c.Close()
	rest := 0
	deadline := time.Now().Add(5 * time.Second)
	for rest < 1000-got && time.Now().Before(deadline) {
		recs, _ := c.Poll(200 * time.Millisecond)
		rest += len(recs)
	}
	if rest != 1000-got {
		t.Fatalf("resumed with %d records, want %d", rest, 1000-got)
	}
}

// Consumers join and leave while records flow. Every record must be
// consumed at least once, and within one generation no partition is owned
// by two members.
func TestConsumerGroupRebalance(t *testing.T) {
	b, addr := startBroker(t, t.TempDir())
	defer b.Close()
	admin, _ := client.Dial(addr)
	defer admin.Close()
	const parts = 6
	admin.CreateTopic("work", parts)
	p, _ := client.NewProducer(admin, "work", client.ProducerConfig{Linger: time.Millisecond})
	const total = 30000
	go func() {
		for i := 0; i < total; i++ {
			p.Produce(nil, []byte(fmt.Sprint(i)), nil)
			if i%1000 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		p.Flush()
	}()

	var mu sync.Mutex
	consumed := map[string]bool{}
	owners := map[uint32]map[uint32]string{} // generation -> partition -> member
	run := func(id string, stop <-chan struct{}) {
		cn, _ := client.Dial(addr)
		defer cn.Close()
		c, err := client.NewConsumer(cn, client.ConsumerConfig{
			Group: "workers", Member: id, Topics: []string{"work"}, FromStart: true,
			Session: 900 * time.Millisecond, MaxWait: 50 * time.Millisecond,
		})
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		for {
			select {
			case <-stop:
				c.Commit()
				return
			default:
			}
			recs, err := c.Poll(50 * time.Millisecond)
			if err != nil {
				t.Error(err)
				return
			}
			gen, assigned := c.Assignment()
			mu.Lock()
			if owners[gen] == nil {
				owners[gen] = map[uint32]string{}
			}
			for _, tp := range assigned {
				if o, ok := owners[gen][tp.Partition]; ok && o != id {
					t.Errorf("generation %d: partition %d owned by %s and %s", gen, tp.Partition, o, id)
				}
				owners[gen][tp.Partition] = id
			}
			for _, r := range recs {
				consumed[string(r.Value)] = true
			}
			mu.Unlock()
			c.Commit()
		}
	}
	stops := map[string]chan struct{}{}
	var wg sync.WaitGroup
	start := func(id string) {
		ch := make(chan struct{})
		stops[id] = ch
		wg.Add(1)
		go func() { defer wg.Done(); run(id, ch) }()
	}
	start("a")
	time.Sleep(300 * time.Millisecond)
	start("b")
	time.Sleep(300 * time.Millisecond)
	start("c")
	time.Sleep(400 * time.Millisecond)
	close(stops["b"]) // leaves
	time.Sleep(400 * time.Millisecond)
	start("d")
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(consumed)
		mu.Unlock()
		if n == total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for id, ch := range stops {
		if id != "b" {
			close(ch)
		}
	}
	wg.Wait()
	if len(consumed) != total {
		var missing []int
		for i := 0; i < total && len(missing) < 10; i++ {
			if !consumed[fmt.Sprint(i)] {
				missing = append(missing, i)
			}
		}
		sort.Ints(missing)
		t.Fatalf("consumed %d of %d; first missing %v", len(consumed), total, missing)
	}
	if len(owners) < 3 {
		t.Fatalf("expected several generations, saw %d", len(owners))
	}
}
