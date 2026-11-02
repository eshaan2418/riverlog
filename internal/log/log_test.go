package log

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func batchOf(vals ...string) []byte {
	b := NewBuilder()
	for i, v := range vals {
		var key []byte
		if i%2 == 0 {
			key = []byte("k" + v)
		}
		b.Add(Record{Key: key, Value: []byte(v)}, time.Now().UnixMilli())
	}
	return append([]byte(nil), b.Bytes()...)
}

// readAll returns every record value from offset on, in order.
func readAll(t *testing.T, l *Log, from uint64) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	off := from
	for {
		data, err := l.Read(off, 1<<16)
		if err != nil {
			t.Fatalf("read %d: %v", off, err)
		}
		if data == nil {
			return out
		}
		err = EachBatch(data, func(b []byte) error {
			if _, err := Validate(b); err != nil {
				return err
			}
			return EachRecord(b, func(o uint64, r Record) bool {
				if o >= off {
					out[o] = string(r.Value)
					off = o + 1
				}
				return true
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBatchRoundTripAndCorruption(t *testing.T) {
	b := batchOf("a", "bb", "", "dddd")
	h, err := Validate(b)
	if err != nil || h.Count != 4 {
		t.Fatalf("validate: %v %+v", err, h)
	}
	var got []string
	EachRecord(b, func(o uint64, r Record) bool { got = append(got, string(r.Value)); return true })
	if fmt.Sprint(got) != "[a bb  dddd]" {
		t.Fatal(got)
	}
	for i := 16; i < len(b); i++ {
		c := append([]byte(nil), b...)
		c[i] ^= 0x40
		if _, err := Validate(c); err == nil {
			t.Fatalf("flip at %d not detected", i)
		}
	}
	if _, err := Validate(b[:len(b)-1]); err == nil {
		t.Fatal("truncation not detected")
	}
}

func TestAppendReadEveryOffset(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncAlways, SyncInterval, SyncNever} {
		dir := t.TempDir()
		opts := DefaultOptions()
		opts.Sync = policy
		opts.SegmentBytes = 8 << 10 // many segments
		opts.IndexInterval = 512
		l, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(int64(policy)))
		want := map[uint64]string{}
		var next uint64
		for i := 0; i < 500; i++ {
			n := 1 + rng.Intn(8)
			vals := make([]string, n)
			for j := range vals {
				vals[j] = fmt.Sprintf("v%d-%d-%s", i, j, bytes.Repeat([]byte("x"), rng.Intn(100)))
			}
			base, err := l.Append(batchOf(vals...))
			if err != nil {
				t.Fatal(err)
			}
			if base != next {
				t.Fatalf("base %d want %d", base, next)
			}
			for j, v := range vals {
				want[base+uint64(j)] = v
			}
			next += uint64(n)
		}
		if len(l.segs) < 5 {
			t.Fatalf("expected rolling, got %d segments", len(l.segs))
		}
		l.Sync()
		// Every starting offset returns the correct suffix.
		for _, from := range []uint64{0, 1, 7, next / 3, next / 2, next - 1} {
			got := readAll(t, l, from)
			for o := from; o < next; o++ {
				if got[o] != want[o] {
					t.Fatalf("policy %d from %d: offset %d = %q want %q", policy, from, o, got[o], want[o])
				}
			}
		}
		if _, err := l.Read(next+1, 100); err != ErrOffsetOutOfRange {
			t.Fatal("read past end should fail")
		}
		l.Close()
		// Reopen: same contents, appends continue at next.
		l, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, l, 0); len(got) != len(want) {
			t.Fatalf("after reopen %d records want %d", len(got), len(want))
		}
		base, _ := l.Append(batchOf("after"))
		if base != next {
			t.Fatalf("after reopen base %d want %d", base, next)
		}
		l.Close()
	}
}

// Cut the active segment at every kind of position: recovery must keep
// exactly the complete batches before the cut.
func TestRecoveryFromTornWrites(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 60; trial++ {
		dir := t.TempDir()
		l, _ := Open(dir, DefaultOptions())
		var ends []int64 // file size after each batch
		var counts []uint64
		var size int64
		var total uint64
		for i := 0; i < 20; i++ {
			b := batchOf(fmt.Sprint(i), "y")
			l.Append(b)
			size += int64(len(b))
			total += 2
			ends = append(ends, size)
			counts = append(counts, total)
		}
		l.Close()
		path := segName(dir, 0, ".log")
		cut := rng.Int63n(size + 1)
		mode := trial % 3
		data, _ := os.ReadFile(path)
		switch mode {
		case 0: // truncated tail
			data = data[:cut]
		case 1: // garbage tail
			data = append(data[:cut], bytes.Repeat([]byte{0xAB}, 50)...)
		case 2: // bit flip inside the batch containing cut
			if cut < size {
				data[cut] ^= 0x01
			}
		}
		os.WriteFile(path, data, 0o644)
		l, err := Open(dir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		// Expected surviving records: batches entirely before the damage.
		k := sort.Search(len(ends), func(i int) bool { return ends[i] > cut })
		var want uint64
		if k > 0 {
			want = counts[k-1]
		}
		if mode == 2 && cut >= size {
			want = total
		}
		// A flip in a batch's base offset breaks the offset chain; anywhere else
		// the length check or CRC catches it. Either way that batch and every
		// later one are dropped.
		_, end := l.Offsets()
		if end != want {
			t.Fatalf("trial %d mode %d cut %d: end %d want %d", trial, mode, cut, end, want)
		}
		if got := readAll(t, l, 0); uint64(len(got)) != want {
			t.Fatalf("readable %d want %d", len(got), want)
		}
		base, err := l.Append(batchOf("z"))
		if err != nil || base != want {
			t.Fatalf("append after recovery: %d %v", base, err)
		}
		l.Close()
	}
}

func TestGroupCommitConcurrentAppends(t *testing.T) {
	l, _ := Open(t.TempDir(), DefaultOptions())
	defer l.Close()
	const writers, per = 16, 200
	var mu sync.Mutex
	bases := map[uint64]bool{}
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				base, err := l.Append(batchOf(fmt.Sprintf("%d/%d", w, i)))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if bases[base] {
					t.Errorf("duplicate base %d", base)
				}
				bases[base] = true
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	for i := uint64(0); i < writers*per; i++ {
		if !bases[i] {
			t.Fatalf("offset %d missing", i)
		}
	}
	// Per-writer order is preserved in the log.
	got := readAll(t, l, 0)
	last := map[string]int{}
	for o := uint64(0); o < writers*per; o++ {
		var w, i int
		fmt.Sscanf(got[o], "%d/%d", &w, &i)
		if prev, ok := last[fmt.Sprint(w)]; ok && i != prev+1 {
			t.Fatalf("writer %d out of order: %d after %d", w, i, prev)
		}
		last[fmt.Sprint(w)] = i
	}
}

func TestRetention(t *testing.T) {
	opts := DefaultOptions()
	opts.SegmentBytes = 4 << 10
	opts.RetentionBytes = 16 << 10
	l, _ := Open(t.TempDir(), opts)
	defer l.Close()
	for i := 0; i < 1000; i++ {
		l.Append(batchOf(string(bytes.Repeat([]byte("r"), 100))))
	}
	first, end := l.Offsets()
	if first == 0 || end != 1000 {
		t.Fatalf("first %d end %d", first, end)
	}
	if _, err := l.Read(0, 100); err != ErrOffsetOutOfRange {
		t.Fatal("deleted offsets should be out of range")
	}
	files, _ := filepath.Glob(filepath.Join(l.dir, "*.log"))
	if len(files) > 6 {
		t.Fatalf("%d segments retained", len(files))
	}
	if got := readAll(t, l, first); uint64(len(got)) != end-first {
		t.Fatalf("got %d", len(got))
	}
}

func TestWaitWakesOnAppend(t *testing.T) {
	l, _ := Open(t.TempDir(), DefaultOptions())
	defer l.Close()
	start := time.Now()
	if end := l.Wait(0, 30*time.Millisecond); end != 0 || time.Since(start) < 25*time.Millisecond {
		t.Fatal("wait should time out on an empty log")
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		l.Append(batchOf("hello"))
	}()
	start = time.Now()
	end := l.Wait(0, 5*time.Second)
	if end != 1 || time.Since(start) > time.Second {
		t.Fatalf("end %d after %v", end, time.Since(start))
	}
}
