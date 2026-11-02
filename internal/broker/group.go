package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/eshaan2418/riverlog/internal/wire"
)

// Consumer groups.
//
// Assignment is done by the broker (range assignment per topic), not by a
// group leader. A rebalance begins when a member joins, leaves, or misses
// its session deadline. Every member then has to call Join again. The new
// generation is formed once all current members have re-joined, or once
// the rebalance timeout drops those that did not. Heartbeats from the old
// generation get RebalanceInProgress. Offset commits are accepted only
// from a member of the current generation, for a partition assigned to
// it. That fences off a consumer that missed a rebalance.
type member struct {
	id         string
	topics     []string
	session    time.Duration
	lastSeen   time.Time
	joinedGen  uint32 // generation this member has joined (pending or current)
	assignment []wire.TopicPartition
}

type group struct {
	name        string
	mu          sync.Mutex
	cond        *sync.Cond
	members     map[string]*member
	generation  uint32
	rebalancing bool
	deadline    time.Time // rebalance completes by this time
	offsets     map[wire.TopicPartition]uint64
	dir         string
	parts       func(topic string) uint32
}

type coordinator struct {
	b      *Broker
	mu     sync.Mutex
	groups map[string]*group
	dir    string
	stop   chan struct{}
}

const rebalanceTimeout = 3 * time.Second

func newCoordinator(b *Broker, dir string) (*coordinator, error) {
	c := &coordinator{b: b, groups: map[string]*group{}, dir: dir, stop: make(chan struct{})}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var saved []struct {
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			Offset    uint64 `json:"offset"`
		}
		if err := json.Unmarshal(data, &saved); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		g := c.get(nameFromFile(f))
		for _, s := range saved {
			g.offsets[wire.TopicPartition{Topic: s.Topic, Partition: s.Partition}] = s.Offset
		}
	}
	go c.reaper()
	return c, nil
}

func nameFromFile(f string) string {
	base := filepath.Base(f)
	return unescape(base[:len(base)-len(".json")])
}

func (c *coordinator) get(name string) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if g == nil {
		g = &group{name: name, members: map[string]*member{}, offsets: map[wire.TopicPartition]uint64{}, dir: c.dir, parts: c.b.partitions}
		g.cond = sync.NewCond(&g.mu)
		c.groups[name] = g
	}
	return g
}

// reaper expires members that stopped heartbeating, and finishes
// rebalances whose deadline passed.
func (c *coordinator) reaper() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.mu.Lock()
			gs := make([]*group, 0, len(c.groups))
			for _, g := range c.groups {
				gs = append(gs, g)
			}
			c.mu.Unlock()
			for _, g := range gs {
				g.mu.Lock()
				for id, m := range g.members {
					// A member blocked in Join for the pending generation is alive.
					waiting := g.rebalancing && m.joinedGen == g.generation+1
					if !waiting && now.Sub(m.lastSeen) > m.session {
						delete(g.members, id)
						g.startRebalance()
					}
				}
				if g.rebalancing && now.After(g.deadline) {
					g.complete(true)
				}
				g.mu.Unlock()
			}
		}
	}
}

func (g *group) startRebalance() {
	if !g.rebalancing {
		g.rebalancing = true
		g.deadline = time.Now().Add(rebalanceTimeout)
	}
	g.cond.Broadcast()
}

// complete forms the next generation from members that re-joined. With
// force, members that did not re-join by the deadline are dropped.
func (g *group) complete(force bool) {
	next := g.generation + 1
	for id, m := range g.members {
		if m.joinedGen != next {
			if !force {
				return
			}
			delete(g.members, id)
		}
	}
	g.generation = next
	g.rebalancing = false
	g.assign()
	g.cond.Broadcast()
}

// assign: per topic, sort the subscribed members and give each a
// contiguous range of partitions.
func (g *group) assign() {
	for _, m := range g.members {
		m.assignment = nil
	}
	topics := map[string][]*member{}
	for _, m := range g.members {
		for _, t := range m.topics {
			topics[t] = append(topics[t], m)
		}
	}
	for t, ms := range topics {
		sort.Slice(ms, func(i, j int) bool { return ms[i].id < ms[j].id })
		n := int(g.parts(t))
		for i, m := range ms {
			lo, hi := i*n/len(ms), (i+1)*n/len(ms)
			for p := lo; p < hi; p++ {
				m.assignment = append(m.assignment, wire.TopicPartition{Topic: t, Partition: uint32(p)})
			}
		}
	}
}

// Join blocks until the member is part of a completed generation and
// returns that generation and the member's assignment.
func (c *coordinator) Join(name, id string, topics []string, session time.Duration) (uint32, []wire.TopicPartition, error) {
	g := c.get(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.members[id]
	if m == nil || !sameTopics(m.topics, topics) {
		if m == nil {
			m = &member{id: id}
			g.members[id] = m
		}
		m.topics = append([]string(nil), topics...)
		g.startRebalance()
	}
	m.session = session
	m.lastSeen = time.Now()
	if !g.rebalancing {
		// Joining a stable group with no change: current assignment.
		return g.generation, m.assignment, nil
	}
	target := g.generation + 1
	m.joinedGen = target
	g.complete(false)
	for g.generation < target {
		if g.members[id] != m {
			return 0, nil, &wire.Error{Code: wire.UnknownMember}
		}
		m.lastSeen = time.Now()
		g.cond.Wait()
	}
	if g.members[id] != m {
		return 0, nil, &wire.Error{Code: wire.UnknownMember}
	}
	return g.generation, m.assignment, nil
}

func sameTopics(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func (c *coordinator) Heartbeat(name, id string, gen uint32) error {
	g := c.get(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.members[id]
	if m == nil {
		return &wire.Error{Code: wire.UnknownMember}
	}
	m.lastSeen = time.Now()
	if g.rebalancing || gen != g.generation {
		return &wire.Error{Code: wire.RebalanceInProgress}
	}
	return nil
}

func (c *coordinator) Leave(name, id string) {
	g := c.get(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[id]; ok {
		delete(g.members, id)
		g.startRebalance()
		if len(g.members) == 0 {
			g.complete(true)
		}
	}
}

func (c *coordinator) Commit(name, id string, gen uint32, tp wire.TopicPartition, offset uint64) error {
	g := c.get(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.members[id]
	if m == nil {
		return &wire.Error{Code: wire.UnknownMember}
	}
	if gen != g.generation || g.rebalancing {
		return &wire.Error{Code: wire.IllegalGeneration}
	}
	owned := false
	for _, a := range m.assignment {
		if a == tp {
			owned = true
			break
		}
	}
	if !owned {
		return &wire.Error{Code: wire.IllegalGeneration}
	}
	m.lastSeen = time.Now()
	g.offsets[tp] = offset
	return g.persist()
}

func (c *coordinator) Committed(name string, tp wire.TopicPartition) (uint64, bool) {
	g := c.get(name)
	g.mu.Lock()
	defer g.mu.Unlock()
	o, ok := g.offsets[tp]
	return o, ok
}

// persist writes the group's offsets atomically: temp file, fsync, rename,
// fsync of the directory.
func (g *group) persist() error {
	type entry struct {
		Topic     string `json:"topic"`
		Partition uint32 `json:"partition"`
		Offset    uint64 `json:"offset"`
	}
	var out []entry
	for tp, o := range g.offsets {
		out = append(out, entry{tp.Topic, tp.Partition, o})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	data, _ := json.Marshal(out)
	path := filepath.Join(g.dir, escape(g.name)+".json")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(g.dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// escape makes a group name safe as a file name.
func escape(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			out = append(out, c)
		} else {
			out = append(out, fmt.Sprintf("%%%02X", c)...)
		}
	}
	return string(out)
}

func unescape(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			var v byte
			fmt.Sscanf(s[i+1:i+3], "%02X", &v)
			out = append(out, v)
			i += 2
		} else {
			out = append(out, s[i])
		}
	}
	return string(out)
}
