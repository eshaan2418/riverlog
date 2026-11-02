// Package broker serves partition logs and consumer groups over TCP.
package broker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	rlog "github.com/eshaan2418/riverlog/internal/log"
	"github.com/eshaan2418/riverlog/internal/wire"
)

type Config struct {
	Dir     string
	LogOpts rlog.Options
	// MaxWait caps a fetch's long-poll time.
	MaxWait time.Duration
}

type Broker struct {
	cfg    Config
	mu     sync.RWMutex
	topics map[string][]*rlog.Log
	groups *coordinator
	ln     net.Listener
	conns  sync.WaitGroup
	closed chan struct{}
}

var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// Open loads existing topics from cfg.Dir (recovering each partition).
func Open(cfg Config) (*Broker, error) {
	if cfg.MaxWait == 0 {
		cfg.MaxWait = 30 * time.Second
	}
	b := &Broker{cfg: cfg, topics: map[string][]*rlog.Log{}, closed: make(chan struct{})}
	tdir := filepath.Join(cfg.Dir, "topics")
	if err := os.MkdirAll(tdir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(tdir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		parts, _ := os.ReadDir(filepath.Join(tdir, e.Name()))
		var ids []int
		for _, p := range parts {
			if n, err := strconv.Atoi(p.Name()); err == nil {
				ids = append(ids, n)
			}
		}
		sort.Ints(ids)
		var logs []*rlog.Log
		for i, id := range ids {
			if id != i {
				return nil, fmt.Errorf("topic %s: missing partition %d", e.Name(), i)
			}
			l, err := rlog.Open(filepath.Join(tdir, e.Name(), strconv.Itoa(id)), cfg.LogOpts)
			if err != nil {
				return nil, fmt.Errorf("topic %s partition %d: %w", e.Name(), id, err)
			}
			logs = append(logs, l)
		}
		b.topics[e.Name()] = logs
	}
	g, err := newCoordinator(b, filepath.Join(cfg.Dir, "groups"))
	if err != nil {
		return nil, err
	}
	b.groups = g
	return b, nil
}

func (b *Broker) partitions(topic string) uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return uint32(len(b.topics[topic]))
}

// CreateTopic creates a topic with n partitions.
func (b *Broker) CreateTopic(name string, n int) error {
	if !topicName.MatchString(name) || n < 1 || n > 4096 {
		return &wire.Error{Code: wire.BadRequest}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return &wire.Error{Code: wire.TopicExists}
	}
	var logs []*rlog.Log
	for i := 0; i < n; i++ {
		l, err := rlog.Open(filepath.Join(b.cfg.Dir, "topics", name, strconv.Itoa(i)), b.cfg.LogOpts)
		if err != nil {
			return err
		}
		logs = append(logs, l)
	}
	b.topics[name] = logs
	return nil
}

func (b *Broker) partition(topic string, p uint32) (*rlog.Log, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	logs := b.topics[topic]
	if int(p) >= len(logs) {
		return nil, &wire.Error{Code: wire.UnknownTopic}
	}
	return logs[p], nil
}

// Serve accepts connections until Close.
func (b *Broker) Serve(ln net.Listener) error {
	b.ln = ln
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-b.closed:
				return nil
			default:
				return err
			}
		}
		b.conns.Add(1)
		go b.handle(c)
	}
}

// Close stops accepting, closes connections' logs, and flushes everything.
func (b *Broker) Close() error {
	close(b.closed)
	close(b.groups.stop)
	if b.ln != nil {
		b.ln.Close()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var first error
	for _, logs := range b.topics {
		for _, l := range logs {
			if err := l.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func codeOf(err error) wire.Code {
	var we *wire.Error
	switch {
	case err == nil:
		return wire.OK
	case errors.As(err, &we):
		return we.Code
	case errors.Is(err, rlog.ErrOffsetOutOfRange):
		return wire.OffsetOutOfRange
	case errors.Is(err, rlog.ErrCorrupt), errors.Is(err, rlog.ErrShort):
		return wire.CorruptBatch
	default:
		return wire.Internal
	}
}

// handle runs one connection. Requests are read and started in order; a
// writer goroutine sends responses as they complete. Produce appends happen
// inline (so a connection's batches keep their order) and the wait for
// durability happens off the read loop, so pipelined batches share fsyncs.
func (b *Broker) handle(c net.Conn) {
	defer b.conns.Done()
	out := make(chan []byte, 256)
	var pending sync.WaitGroup
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w := bufio.NewWriterSize(c, 64<<10)
		for frame := range out {
			if _, err := w.Write(frame); err != nil {
				break
			}
			if len(out) == 0 {
				if err := w.Flush(); err != nil {
					break
				}
			}
		}
		for range out { // drain after a write error
		}
	}()
	defer func() {
		pending.Wait()
		close(out)
		<-writerDone
		c.Close()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	var buf []byte
	for {
		frame, err := wire.ReadFrame(r, buf[:0])
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
		// The frame buffer is reused; requests that outlive this iteration copy.
		d := &wire.Dec{B: frame}
		api := wire.API(d.U16())
		corr := d.U32()
		if d.Err != nil {
			return
		}
		reply := func(code wire.Code, fill func(*wire.Enc)) {
			e := wire.Response(corr, code)
			if code == wire.OK && fill != nil {
				fill(e)
			}
			out <- e.Finish()
		}
		switch api {
		case wire.CreateTopic:
			name := d.Str()
			n := d.U32()
			if d.Err != nil {
				reply(wire.BadRequest, nil)
				continue
			}
			reply(codeOf(b.CreateTopic(name, int(n))), nil)
		case wire.Metadata:
			name := d.Str()
			n := b.partitions(name)
			if n == 0 {
				reply(wire.UnknownTopic, nil)
				continue
			}
			reply(wire.OK, func(e *wire.Enc) { e.U32(n) })
		case wire.Produce:
			topic, p := d.Str(), d.U32()
			batch := d.Blob()
			if d.Err != nil {
				reply(wire.BadRequest, nil)
				continue
			}
			l, err := b.partition(topic, p)
			if err != nil {
				reply(codeOf(err), nil)
				continue
			}
			base, end, err := l.AppendNoWait(batch)
			if err != nil {
				reply(codeOf(err), nil)
				continue
			}
			pending.Add(1)
			go func() {
				defer pending.Done()
				err := l.WaitDurable(end)
				reply(codeOf(err), func(e *wire.Enc) { e.U64(base) })
			}()
		case wire.Fetch:
			topic, p, off, maxBytes, waitMs := d.Str(), d.U32(), d.U64(), d.U32(), d.U32()
			if d.Err != nil {
				reply(wire.BadRequest, nil)
				continue
			}
			l, err := b.partition(topic, p)
			if err != nil {
				reply(codeOf(err), nil)
				continue
			}
			wait := time.Duration(waitMs) * time.Millisecond
			if wait > b.cfg.MaxWait {
				wait = b.cfg.MaxWait
			}
			pending.Add(1)
			go func() {
				defer pending.Done()
				end := l.Wait(off, wait)
				data, err := l.Read(off, int(maxBytes))
				reply(codeOf(err), func(e *wire.Enc) {
					e.U64(end)
					e.Blob(data)
				})
			}()
		case wire.ListOffsets:
			topic, p := d.Str(), d.U32()
			l, err := b.partition(topic, p)
			if err != nil {
				reply(codeOf(err), nil)
				continue
			}
			first, end := l.Offsets()
			reply(wire.OK, func(e *wire.Enc) { e.U64(first); e.U64(end) })
		case wire.JoinGroup:
			name, id := d.Str(), d.Str()
			n := int(d.U16())
			topics := make([]string, 0, n)
			for i := 0; i < n; i++ {
				topics = append(topics, d.Str())
			}
			session := time.Duration(d.U32()) * time.Millisecond
			if d.Err != nil || session <= 0 {
				reply(wire.BadRequest, nil)
				continue
			}
			pending.Add(1)
			go func() {
				defer pending.Done()
				gen, assigned, err := b.groups.Join(name, id, topics, session)
				reply(codeOf(err), func(e *wire.Enc) {
					e.U32(gen)
					e.U32(uint32(len(assigned)))
					for _, tp := range assigned {
						e.Str(tp.Topic)
						e.U32(tp.Partition)
					}
				})
			}()
		case wire.Heartbeat:
			name, id, gen := d.Str(), d.Str(), d.U32()
			reply(codeOf(b.groups.Heartbeat(name, id, gen)), nil)
		case wire.LeaveGroup:
			name, id := d.Str(), d.Str()
			b.groups.Leave(name, id)
			reply(wire.OK, nil)
		case wire.CommitOffset:
			name, id, gen := d.Str(), d.Str(), d.U32()
			tp := wire.TopicPartition{Topic: d.Str(), Partition: d.U32()}
			off := d.U64()
			if d.Err != nil {
				reply(wire.BadRequest, nil)
				continue
			}
			reply(codeOf(b.groups.Commit(name, id, gen, tp, off)), nil)
		case wire.FetchOffset:
			name := d.Str()
			tp := wire.TopicPartition{Topic: d.Str(), Partition: d.U32()}
			off, ok := b.groups.Committed(name, tp)
			reply(wire.OK, func(e *wire.Enc) {
				if ok {
					e.U64(off)
				} else {
					e.U64(^uint64(0))
				}
			})
		default:
			reply(wire.BadRequest, nil)
		}
		buf = frame
	}
}
