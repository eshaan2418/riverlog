// Package client is the Go client for riverlog: a pipelined connection, a
// batching producer, and a consumer-group consumer.
package client

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"

	rlog "github.com/eshaan2418/riverlog/internal/log"
	"github.com/eshaan2418/riverlog/internal/wire"
)

// Record is a consumed record.
type Record struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Key       []byte
	Value     []byte
}

var ErrClosed = errors.New("riverlog: connection closed")

// Conn is a pipelined connection. Many goroutines may issue requests on it
// at once.
type Conn struct {
	c       net.Conn
	wmu     sync.Mutex
	w       *bufio.Writer
	corr    atomic.Uint32
	mu      sync.Mutex
	waiting map[uint32]chan response
	err     error
	done    chan struct{}
}

type response struct {
	code wire.Code
	body []byte
	err  error
}

func Dial(addr string) (*Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	cn := &Conn{c: c, w: bufio.NewWriterSize(c, 64<<10), waiting: map[uint32]chan response{}, done: make(chan struct{})}
	go cn.readLoop()
	return cn, nil
}

func (cn *Conn) readLoop() {
	r := bufio.NewReaderSize(cn.c, 256<<10)
	for {
		frame, err := wire.ReadFrame(r, nil)
		if err != nil {
			cn.fail(err)
			return
		}
		d := &wire.Dec{B: frame}
		corr := d.U32()
		code := wire.Code(d.U16())
		cn.mu.Lock()
		ch := cn.waiting[corr]
		delete(cn.waiting, corr)
		cn.mu.Unlock()
		if ch != nil {
			ch <- response{code: code, body: d.B}
		}
	}
}

func (cn *Conn) fail(err error) {
	cn.mu.Lock()
	if cn.err == nil {
		cn.err = err
		close(cn.done)
	}
	for k, ch := range cn.waiting {
		ch <- response{err: ErrClosed}
		delete(cn.waiting, k)
	}
	cn.mu.Unlock()
}

func (cn *Conn) Close() error {
	err := cn.c.Close()
	cn.fail(ErrClosed)
	return err
}

// Do sends a request and waits for its response body.
func (cn *Conn) Do(api wire.API, fill func(*wire.Enc)) (*wire.Dec, error) {
	ch, err := cn.Send(api, fill)
	if err != nil {
		return nil, err
	}
	return Await(ch)
}

// Send writes a request and returns a channel for its response, so callers
// can keep many requests in flight.
func (cn *Conn) Send(api wire.API, fill func(*wire.Enc)) (<-chan response, error) {
	corr := cn.corr.Add(1)
	e := wire.Request(api, corr)
	if fill != nil {
		fill(e)
	}
	ch := make(chan response, 1)
	cn.mu.Lock()
	if cn.err != nil {
		cn.mu.Unlock()
		return nil, ErrClosed
	}
	cn.waiting[corr] = ch
	cn.mu.Unlock()
	cn.wmu.Lock()
	_, err := cn.w.Write(e.Finish())
	if err == nil {
		err = cn.w.Flush()
	}
	cn.wmu.Unlock()
	if err != nil {
		cn.fail(err)
		return nil, err
	}
	return ch, nil
}

func Await(ch <-chan response) (*wire.Dec, error) {
	r := <-ch
	if r.err != nil {
		return nil, r.err
	}
	if r.code != wire.OK {
		return nil, &wire.Error{Code: r.code}
	}
	return &wire.Dec{B: r.body}, nil
}

func (cn *Conn) CreateTopic(name string, partitions int) error {
	_, err := cn.Do(wire.CreateTopic, func(e *wire.Enc) { e.Str(name); e.U32(uint32(partitions)) })
	return err
}

func (cn *Conn) Partitions(topic string) (int, error) {
	d, err := cn.Do(wire.Metadata, func(e *wire.Enc) { e.Str(topic) })
	if err != nil {
		return 0, err
	}
	return int(d.U32()), d.Err
}

// Offsets returns the first retained and next offset of a partition.
func (cn *Conn) Offsets(topic string, p uint32) (first, end uint64, err error) {
	d, err := cn.Do(wire.ListOffsets, func(e *wire.Enc) { e.Str(topic); e.U32(p) })
	if err != nil {
		return 0, 0, err
	}
	return d.U64(), d.U64(), d.Err
}

// ProduceBatch appends a raw batch (built with log.Builder) and returns its
// base offset once the broker acknowledges it.
func (cn *Conn) ProduceBatch(topic string, p uint32, batch []byte) (uint64, error) {
	d, err := cn.Do(wire.Produce, func(e *wire.Enc) { e.Str(topic); e.U32(p); e.Blob(batch) })
	if err != nil {
		return 0, err
	}
	return d.U64(), d.Err
}

// Fetch returns records at or after offset, waiting up to wait for data.
// It also returns the partition's visible end offset.
func (cn *Conn) Fetch(topic string, p uint32, offset uint64, maxBytes int, wait time.Duration) ([]Record, uint64, error) {
	d, err := cn.Do(wire.Fetch, func(e *wire.Enc) {
		e.Str(topic)
		e.U32(p)
		e.U64(offset)
		e.U32(uint32(maxBytes))
		e.U32(uint32(wait / time.Millisecond))
	})
	if err != nil {
		return nil, 0, err
	}
	end := d.U64()
	data := d.Blob()
	if d.Err != nil {
		return nil, 0, d.Err
	}
	var out []Record
	err = rlog.EachBatch(data, func(b []byte) error {
		if _, err := rlog.Validate(b); err != nil {
			return err
		}
		return rlog.EachRecord(b, func(o uint64, r rlog.Record) bool {
			if o >= offset {
				out = append(out, Record{Topic: topic, Partition: p, Offset: o, Key: r.Key, Value: r.Value})
			}
			return true
		})
	})
	return out, end, err
}

// ---------------------------------------------------------------------------
// Producer

type ProducerConfig struct {
	Linger      time.Duration // wait this long to fill a batch (0 = send when the previous batch is acked)
	BatchBytes  int           // send a batch once it reaches this size
	MaxInFlight int           // unacknowledged batches per partition
}

// Producer batches records per partition. Records with the same key go to
// the same partition (FNV-1a); nil keys are spread round-robin. Delivery is
// at-least-once if the caller retries failed records.
type Producer struct {
	cn    *Conn
	topic string
	n     int
	cfg   ProducerConfig
	parts []*partBatcher
	rr    atomic.Uint32
}

// Result is the outcome of one produced record.
type Result struct {
	Partition uint32
	Offset    uint64
	Err       error
}

type partBatcher struct {
	mu       sync.Mutex
	p        *Producer
	part     uint32
	b        *rlog.Builder
	cbs      []func(Result)
	timer    *time.Timer
	inflight chan struct{}
	wg       sync.WaitGroup
}

func NewProducer(cn *Conn, topic string, cfg ProducerConfig) (*Producer, error) {
	n, err := cn.Partitions(topic)
	if err != nil {
		return nil, err
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 256 << 10
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 8
	}
	p := &Producer{cn: cn, topic: topic, n: n, cfg: cfg}
	for i := 0; i < n; i++ {
		p.parts = append(p.parts, &partBatcher{p: p, part: uint32(i), b: rlog.NewBuilder(), inflight: make(chan struct{}, cfg.MaxInFlight)})
	}
	return p, nil
}

func (p *Producer) Partitions() int { return p.n }

// Produce queues a record; cb (optional) runs when it is acknowledged.
func (p *Producer) Produce(key, value []byte, cb func(Result)) {
	var part uint32
	if key != nil {
		h := fnv.New32a()
		h.Write(key)
		part = h.Sum32() % uint32(p.n)
	} else {
		part = p.rr.Add(1) % uint32(p.n)
	}
	p.ProduceTo(part, key, value, cb)
}

func (p *Producer) ProduceTo(part uint32, key, value []byte, cb func(Result)) {
	pb := p.parts[part]
	pb.mu.Lock()
	pb.b.Add(rlog.Record{Key: key, Value: value}, time.Now().UnixMilli())
	pb.cbs = append(pb.cbs, cb)
	if pb.b.Size() >= p.cfg.BatchBytes {
		pb.flushLocked()
	} else if pb.timer == nil {
		if p.cfg.Linger <= 0 {
			pb.flushLocked()
		} else {
			pb.timer = time.AfterFunc(p.cfg.Linger, func() {
				pb.mu.Lock()
				pb.timer = nil
				pb.flushLocked()
				pb.mu.Unlock()
			})
		}
	}
	pb.mu.Unlock()
}

func (pb *partBatcher) flushLocked() {
	if pb.timer != nil {
		pb.timer.Stop()
		pb.timer = nil
	}
	if pb.b.Count() == 0 {
		return
	}
	batch := append([]byte(nil), pb.b.Bytes()...)
	cbs := pb.cbs
	pb.cbs = nil
	pb.b.Reset()
	pb.inflight <- struct{}{} // backpressure; order is kept because Send runs under mu
	ch, err := pb.p.cn.Send(wire.Produce, func(e *wire.Enc) { e.Str(pb.p.topic); e.U32(pb.part); e.Blob(batch) })
	pb.wg.Add(1)
	go func() {
		defer pb.wg.Done()
		var base uint64
		if err == nil {
			var d *wire.Dec
			d, err = Await(ch)
			if err == nil {
				base = d.U64()
			}
		}
		<-pb.inflight
		for i, cb := range cbs {
			if cb != nil {
				cb(Result{Partition: pb.part, Offset: base + uint64(i), Err: err})
			}
		}
	}()
}

// Flush sends pending batches and waits for every acknowledgement.
func (p *Producer) Flush() {
	for _, pb := range p.parts {
		pb.mu.Lock()
		pb.flushLocked()
		pb.mu.Unlock()
	}
	for _, pb := range p.parts {
		pb.wg.Wait()
	}
}

// ---------------------------------------------------------------------------
// Group consumer

type ConsumerConfig struct {
	Group    string
	Member   string // unique per consumer instance
	Topics   []string
	Session  time.Duration // default 6s
	MaxWait  time.Duration // fetch long-poll, default 500ms
	MaxBytes int           // per fetch, default 1 MiB
	// FromStart: partitions without a committed offset start at the first
	// retained offset (otherwise at the end).
	FromStart bool
}

// Consumer delivers records from the partitions assigned to it. Poll
// returns the next records; Commit stores the position after the last
// record returned. On a rebalance, Poll re-joins transparently and resumes
// each newly assigned partition from its committed offset, so records after
// the last commit may be delivered again (at-least-once).
type Consumer struct {
	cn         *Conn
	cfg        ConsumerConfig
	mu         sync.Mutex
	generation uint32
	assigned   []wire.TopicPartition
	pos        map[wire.TopicPartition]uint64
	records    chan []Record
	stop       chan struct{}
	fetchers   sync.WaitGroup
	rebalance  atomic.Bool
	closed     atomic.Bool
	hbDone     chan struct{}
}

func NewConsumer(cn *Conn, cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Session == 0 {
		cfg.Session = 6 * time.Second
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = 500 * time.Millisecond
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1 << 20
	}
	c := &Consumer{cn: cn, cfg: cfg, hbDone: make(chan struct{})}
	if err := c.join(); err != nil {
		return nil, err
	}
	go c.heartbeat()
	return c, nil
}

func (c *Consumer) join() error {
	d, err := c.cn.Do(wire.JoinGroup, func(e *wire.Enc) {
		e.Str(c.cfg.Group)
		e.Str(c.cfg.Member)
		e.U16(uint16(len(c.cfg.Topics)))
		for _, t := range c.cfg.Topics {
			e.Str(t)
		}
		e.U32(uint32(c.cfg.Session / time.Millisecond))
	})
	if err != nil {
		return err
	}
	gen := d.U32()
	n := int(d.U32())
	var assigned []wire.TopicPartition
	for i := 0; i < n; i++ {
		assigned = append(assigned, wire.TopicPartition{Topic: d.Str(), Partition: d.U32()})
	}
	if d.Err != nil {
		return d.Err
	}
	pos := map[wire.TopicPartition]uint64{}
	for _, tp := range assigned {
		od, err := c.cn.Do(wire.FetchOffset, func(e *wire.Enc) { e.Str(c.cfg.Group); e.Str(tp.Topic); e.U32(tp.Partition) })
		if err != nil {
			return err
		}
		off := od.U64()
		if off == ^uint64(0) {
			first, end, err := c.cn.Offsets(tp.Topic, tp.Partition)
			if err != nil {
				return err
			}
			off = end
			if c.cfg.FromStart {
				off = first
			}
		}
		pos[tp] = off
	}
	c.mu.Lock()
	c.generation, c.assigned, c.pos = gen, assigned, pos
	c.records = make(chan []Record, 16)
	c.stop = make(chan struct{})
	c.mu.Unlock()
	c.rebalance.Store(false)
	for _, tp := range assigned {
		c.fetchers.Add(1)
		go c.fetch(tp, pos[tp], c.records, c.stop)
	}
	return nil
}

func (c *Consumer) fetch(tp wire.TopicPartition, off uint64, out chan<- []Record, stop <-chan struct{}) {
	defer c.fetchers.Done()
	for {
		select {
		case <-stop:
			return
		default:
		}
		recs, _, err := c.cn.Fetch(tp.Topic, tp.Partition, off, c.cfg.MaxBytes, c.cfg.MaxWait)
		if err != nil {
			if wire.IsCode(err, wire.OffsetOutOfRange) {
				first, _, e2 := c.cn.Offsets(tp.Topic, tp.Partition)
				if e2 == nil && off < first {
					off = first
					continue
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if len(recs) == 0 {
			continue
		}
		off = recs[len(recs)-1].Offset + 1
		select {
		case out <- recs:
		case <-stop:
			return
		}
	}
}

func (c *Consumer) heartbeat() {
	defer close(c.hbDone)
	t := time.NewTicker(c.cfg.Session / 3)
	defer t.Stop()
	for range t.C {
		if c.closed.Load() {
			return
		}
		c.mu.Lock()
		gen := c.generation
		c.mu.Unlock()
		_, err := c.cn.Do(wire.Heartbeat, func(e *wire.Enc) { e.Str(c.cfg.Group); e.Str(c.cfg.Member); e.U32(gen) })
		if wire.IsCode(err, wire.RebalanceInProgress) || wire.IsCode(err, wire.UnknownMember) {
			c.rebalance.Store(true)
		} else if err != nil && c.closed.Load() {
			return
		}
	}
}

// Poll waits up to timeout for records. It handles rebalances: when the
// group changes, fetching stops and the consumer re-joins.
func (c *Consumer) Poll(timeout time.Duration) ([]Record, error) {
	if c.rebalance.Load() {
		c.mu.Lock()
		close(c.stop)
		c.mu.Unlock()
		c.fetchers.Wait()
		var err error
		for attempt := 0; attempt < 5; attempt++ {
			// A member dropped by the broker (missed deadline) joins again as new.
			if err = c.join(); err == nil || !(wire.IsCode(err, wire.UnknownMember) || wire.IsCode(err, wire.RebalanceInProgress)) {
				break
			}
		}
		if err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	ch := c.records
	c.mu.Unlock()
	select {
	case recs := <-ch:
		c.mu.Lock()
		tp := wire.TopicPartition{Topic: recs[0].Topic, Partition: recs[0].Partition}
		if _, ok := c.pos[tp]; ok {
			c.pos[tp] = recs[len(recs)-1].Offset + 1
		}
		c.mu.Unlock()
		return recs, nil
	case <-time.After(timeout):
		return nil, nil
	}
}

// Assignment returns the current generation and assigned partitions.
func (c *Consumer) Assignment() (uint32, []wire.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation, append([]wire.TopicPartition(nil), c.assigned...)
}

// Commit stores the position after the last polled record of every
// assigned partition. It fails with IllegalGeneration if a rebalance
// happened since the records were polled.
func (c *Consumer) Commit() error {
	c.mu.Lock()
	gen := c.generation
	pos := make(map[wire.TopicPartition]uint64, len(c.pos))
	for k, v := range c.pos {
		pos[k] = v
	}
	c.mu.Unlock()
	var first error
	for tp, off := range pos {
		_, err := c.cn.Do(wire.CommitOffset, func(e *wire.Enc) {
			e.Str(c.cfg.Group)
			e.Str(c.cfg.Member)
			e.U32(gen)
			e.Str(tp.Topic)
			e.U32(tp.Partition)
			e.U64(off)
		})
		if err != nil && first == nil {
			first = err
			if wire.IsCode(err, wire.IllegalGeneration) || wire.IsCode(err, wire.UnknownMember) {
				c.rebalance.Store(true)
			}
		}
	}
	return first
}

// Close leaves the group (triggering a rebalance for the others).
func (c *Consumer) Close() error {
	c.closed.Store(true)
	c.mu.Lock()
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	c.mu.Unlock()
	c.fetchers.Wait()
	_, err := c.cn.Do(wire.LeaveGroup, func(e *wire.Enc) { e.Str(c.cfg.Group); e.Str(c.cfg.Member) })
	return err
}

func (r Record) String() string {
	return fmt.Sprintf("%s/%d@%d", r.Topic, r.Partition, r.Offset)
}
