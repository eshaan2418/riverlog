package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// SyncPolicy says when appended data is fsynced.
type SyncPolicy int

const (
	// SyncAlways: Append returns after the data is durable. Concurrent
	// appends share fsyncs (group commit). Readers see only durable data.
	SyncAlways SyncPolicy = iota
	// SyncInterval: a background flusher fsyncs every Interval.
	SyncInterval
	// SyncNever: leave flushing to the OS.
	SyncNever
)

type Options struct {
	SegmentBytes   int64         // roll the active segment beyond this size
	IndexInterval  int           // bytes of log between sparse index entries
	RetentionBytes int64         // delete oldest segments beyond this (0 = keep all)
	RetentionAge   time.Duration // delete segments whose newest record is older (0 = keep all)
	Sync           SyncPolicy
	Interval       time.Duration // for SyncInterval
	// PlainFsync uses fsync(2) instead of os.File.Sync. They differ only on
	// macOS, where File.Sync issues F_FULLFSYNC (flush the drive's cache too)
	// and plain fsync does not. Java's FileChannel.force (used by Kafka) has
	// also issued F_FULLFSYNC on macOS since JDK 9 (JDK-8080589).
	PlainFsync bool
}

func DefaultOptions() Options {
	return Options{SegmentBytes: 256 << 20, IndexInterval: 4096, Sync: SyncAlways, Interval: 50 * time.Millisecond}
}

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrClosed           = errors.New("log closed")
)

type indexEntry struct {
	rel uint32 // offset - segment base
	pos uint32 // byte position of the batch containing that offset
}

type segment struct {
	base       uint64
	f          *os.File
	idx        *os.File
	size       int64
	index      []indexEntry
	next       uint64 // offset after the last batch
	maxTs      int64
	sinceIndex int
}

func segName(dir string, base uint64, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, ext))
}

// Log is one partition. All methods are safe for concurrent use.
type Log struct {
	dir  string
	opts Options

	mu          sync.Mutex
	segs        []*segment // ordered by base; last is active
	next        uint64     // next offset to assign
	visible     uint64     // offsets below this are readable
	durable     uint64     // offsets below this are fsynced
	closed      bool
	arrived     *sync.Cond // signalled when visible advances (long-poll)
	synced      *sync.Cond // signalled when durable advances
	syncing     bool
	stopFlusher chan struct{}
	flusherDone chan struct{}
}

// Open opens or creates the log in dir, recovering from a crash: the active
// segment is scanned and cut at the first incomplete or corrupt batch.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultOptions().SegmentBytes
	}
	if opts.IndexInterval <= 0 {
		opts.IndexInterval = 4096
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, opts: opts}
	l.arrived = sync.NewCond(&l.mu)
	l.synced = sync.NewCond(&l.mu)
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range names {
		if strings.HasSuffix(e.Name(), ".log") {
			b, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
			if err == nil {
				bases = append(bases, b)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	for i, b := range bases {
		s, err := openSegment(dir, b, i == len(bases)-1, opts.IndexInterval)
		if err != nil {
			l.closeFiles()
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	if len(l.segs) == 0 {
		s, err := createSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	l.next = l.segs[len(l.segs)-1].next
	l.visible, l.durable = l.next, l.next
	if opts.Sync == SyncInterval {
		l.stopFlusher = make(chan struct{})
		l.flusherDone = make(chan struct{})
		go l.flusher()
	}
	return l, nil
}

func createSegment(dir string, base uint64) (*segment, error) {
	f, err := os.OpenFile(segName(dir, base, ".log"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	idx, err := os.OpenFile(segName(dir, base, ".idx"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		f.Close()
		return nil, err
	}
	// Make the new files' directory entries durable.
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return &segment{base: base, f: f, idx: idx, next: base}, nil
}

// openSegment scans the segment's batches to rebuild its index and find
// its end. For the active (last) segment, a torn or corrupt tail is
// truncated away; earlier segments must be intact.
func openSegment(dir string, base uint64, active bool, indexInterval int) (*segment, error) {
	f, err := os.OpenFile(segName(dir, base, ".log"), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &segment{base: base, f: f, next: base}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	end := st.Size()
	var pos int64
	hdr := make([]byte, HeaderSize)
	buf := make([]byte, 0, 1<<16)
	since := 0
	for pos < end {
		if end-pos < HeaderSize {
			break
		}
		if _, err := f.ReadAt(hdr, pos); err != nil {
			break
		}
		h, err := ParseHeader(hdr)
		if err != nil || int64(h.Size()) > end-pos || h.BaseOffset != s.next {
			break
		}
		if cap(buf) < h.Size() {
			buf = make([]byte, h.Size())
		}
		buf = buf[:h.Size()]
		if _, err := f.ReadAt(buf, pos); err != nil {
			break
		}
		if _, err := Validate(buf); err != nil {
			break
		}
		if len(s.index) == 0 || since >= indexInterval {
			s.index = append(s.index, indexEntry{uint32(h.BaseOffset - base), uint32(pos)})
			since = 0
		}
		since += h.Size()
		s.next = h.NextOffset()
		if h.MaxTs > s.maxTs {
			s.maxTs = h.MaxTs
		}
		pos += int64(h.Size())
	}
	if pos != end {
		if !active {
			f.Close()
			return nil, fmt.Errorf("segment %d corrupt at byte %d of %d", base, pos, end)
		}
		if err := f.Truncate(pos); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	s.size = pos
	s.sinceIndex = since
	// The index is rebuilt from the log, so the file is rewritten to match.
	idx, err := os.OpenFile(segName(dir, base, ".idx"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.idx = idx
	var ib [8]byte
	for _, e := range s.index {
		binary.LittleEndian.PutUint32(ib[0:], e.rel)
		binary.LittleEndian.PutUint32(ib[4:], e.pos)
		idx.Write(ib[:])
	}
	return s, nil
}

func (l *Log) active() *segment { return l.segs[len(l.segs)-1] }

// Append writes one batch (validated, base offset assigned) and returns its
// base offset. Under SyncAlways it returns once the batch is durable.
func (l *Log) Append(batch []byte) (uint64, error) {
	base, end, err := l.AppendNoWait(batch)
	if err != nil {
		return 0, err
	}
	return base, l.WaitDurable(end)
}

// AppendNoWait writes the batch and returns its base offset and the offset
// after it, without waiting for durability. Callers that need durability
// call WaitDurable(end). Splitting the two lets one connection pipeline
// appends in order while their fsyncs are shared.
func (l *Log) AppendNoWait(batch []byte) (base, end uint64, err error) {
	h, err := Validate(batch)
	if err != nil {
		return 0, 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, 0, ErrClosed
	}
	if l.active().size+int64(len(batch)) > l.opts.SegmentBytes && l.active().size > 0 {
		if err := l.roll(); err != nil {
			return 0, 0, err
		}
	}
	s := l.active()
	base = l.next
	SetBaseOffset(batch, base)
	if _, err := s.f.WriteAt(batch, s.size); err != nil {
		return 0, 0, err
	}
	if len(s.index) == 0 || s.sinceIndex >= l.opts.IndexInterval {
		e := indexEntry{uint32(base - s.base), uint32(s.size)}
		s.index = append(s.index, e)
		var ib [8]byte
		binary.LittleEndian.PutUint32(ib[0:], e.rel)
		binary.LittleEndian.PutUint32(ib[4:], e.pos)
		s.idx.Write(ib[:])
		s.sinceIndex = 0
	}
	s.sinceIndex += len(batch)
	s.size += int64(len(batch))
	l.next = base + uint64(h.Count)
	s.next = l.next
	if h.MaxTs > s.maxTs {
		s.maxTs = h.MaxTs
	}
	if l.opts.Sync != SyncAlways {
		l.visible = l.next
		l.arrived.Broadcast()
	}
	l.retain()
	return base, l.next, nil
}

// WaitDurable blocks until offsets below end are durable (SyncAlways only;
// other policies return immediately). Group commit: one waiter fsyncs
// everything written so far while the others wait; whoever finds no sync
// in flight starts the next one.
func (l *Log) WaitDurable(end uint64) error {
	if l.opts.Sync != SyncAlways {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.durable < end {
		if l.closed {
			return ErrClosed
		}
		if !l.syncing {
			if err := l.syncLocked(); err != nil {
				return err
			}
			continue
		}
		l.synced.Wait()
	}
	return nil
}

// syncLocked fsyncs the active segment with the lock released.
func (l *Log) syncLocked() error {
	l.syncing = true
	upTo := l.next
	f := l.active().f
	l.mu.Unlock()
	err := l.syncFile(f)
	l.mu.Lock()
	l.syncing = false
	if err != nil {
		l.synced.Broadcast()
		return err
	}
	if upTo > l.durable {
		l.durable = upTo
	}
	if l.durable > l.visible {
		l.visible = l.durable
		l.arrived.Broadcast()
	}
	l.synced.Broadcast()
	return nil
}

func (l *Log) syncFile(f *os.File) error {
	if l.opts.PlainFsync {
		return syscall.Fsync(int(f.Fd()))
	}
	return f.Sync()
}

func (l *Log) roll() error {
	old := l.active()
	// The rolled segment must be durable before the next one exists.
	if err := l.syncFile(old.f); err != nil {
		return err
	}
	if err := old.idx.Sync(); err != nil {
		return err
	}
	s, err := createSegment(l.dir, l.next)
	if err != nil {
		return err
	}
	l.segs = append(l.segs, s)
	return nil
}

func (l *Log) retain() {
	for len(l.segs) > 1 {
		old := l.segs[0]
		var total int64
		for _, s := range l.segs {
			total += s.size
		}
		bySize := l.opts.RetentionBytes > 0 && total-old.size >= l.opts.RetentionBytes
		byAge := l.opts.RetentionAge > 0 && old.maxTs > 0 && time.Since(time.UnixMilli(old.maxTs)) > l.opts.RetentionAge
		if !bySize && !byAge {
			return
		}
		old.f.Close()
		old.idx.Close()
		os.Remove(segName(l.dir, old.base, ".log"))
		os.Remove(segName(l.dir, old.base, ".idx"))
		l.segs = l.segs[1:]
	}
}

func (l *Log) flusher() {
	defer close(l.flusherDone)
	t := time.NewTicker(l.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-l.stopFlusher:
			return
		case <-t.C:
			l.mu.Lock()
			if !l.closed && l.durable < l.next && !l.syncing {
				l.syncLocked()
			}
			l.mu.Unlock()
		}
	}
}

// Read returns whole batches starting with the one that contains offset,
// up to maxBytes (at least one batch is returned if available, even if it
// exceeds maxBytes). It returns nil at the end of the log.
func (l *Log) Read(offset uint64, maxBytes int) ([]byte, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, ErrClosed
	}
	if offset < l.segs[0].base || offset > l.visible {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	if offset == l.visible {
		l.mu.Unlock()
		return nil, nil
	}
	i := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].base > offset }) - 1
	s := l.segs[i]
	// Sparse index: last entry at or before offset.
	j := sort.Search(len(s.index), func(j int) bool { return uint64(s.index[j].rel)+s.base > offset }) - 1
	pos := int64(0)
	if j >= 0 {
		pos = int64(s.index[j].pos)
	}
	// Readable end of this segment: everything for closed segments; up to
	// the visible offset for the active one.
	end := s.size
	visible := l.visible
	f := s.f
	l.mu.Unlock()

	hdr := make([]byte, HeaderSize)
	// Skip batches that end at or before offset.
	for pos < end {
		if _, err := f.ReadAt(hdr, pos); err != nil {
			return nil, err
		}
		h, err := ParseHeader(hdr)
		if err != nil {
			return nil, err
		}
		if h.NextOffset() > offset {
			break
		}
		pos += int64(h.Size())
	}
	// Collect batches below the visible offset, up to maxBytes.
	start := pos
	for pos < end {
		if _, err := f.ReadAt(hdr, pos); err != nil {
			return nil, err
		}
		h, err := ParseHeader(hdr)
		if err != nil {
			return nil, err
		}
		if h.BaseOffset >= visible {
			break
		}
		if pos > start && pos-start+int64(h.Size()) > int64(maxBytes) {
			break
		}
		pos += int64(h.Size())
	}
	if pos == start {
		return nil, nil
	}
	out := make([]byte, pos-start)
	if _, err := f.ReadAt(out, start); err != nil && err != io.EOF {
		return nil, err
	}
	return out, nil
}

// Wait blocks until offsets at or beyond offset are readable, the timeout
// passes, or the log closes. It returns the visible end offset.
func (l *Log) Wait(offset uint64, timeout time.Duration) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.visible > offset || timeout <= 0 {
		return l.visible
	}
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		l.mu.Lock()
		l.arrived.Broadcast()
		l.mu.Unlock()
	})
	defer timer.Stop()
	for l.visible <= offset && !l.closed && time.Now().Before(deadline) {
		l.arrived.Wait()
	}
	return l.visible
}

// Offsets returns the first retained offset and the visible end.
func (l *Log) Offsets() (first, end uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segs[0].base, l.visible
}

// Sync forces everything appended so far to disk.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.durable < l.next {
		if !l.syncing {
			if err := l.syncLocked(); err != nil {
				return err
			}
			continue
		}
		l.synced.Wait()
	}
	return nil
}

func (l *Log) Close() error {
	if l.stopFlusher != nil {
		close(l.stopFlusher)
		<-l.flusherDone
		l.stopFlusher = nil
	}
	err := l.Sync()
	l.mu.Lock()
	l.closed = true
	l.arrived.Broadcast()
	l.synced.Broadcast()
	l.closeFiles()
	l.mu.Unlock()
	return err
}

func (l *Log) closeFiles() {
	for _, s := range l.segs {
		s.f.Close()
		if s.idx != nil {
			s.idx.Close()
		}
	}
}
