package tests

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eshaan2418/riverlog/client"
)

// buildServer compiles cmd/riverlog once per test binary.
var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func server(t *testing.T) string {
	buildOnce.Do(func() {
		dir, _ := os.MkdirTemp("", "riverlog-bin")
		binPath = filepath.Join(dir, "riverlog")
		out, err := exec.Command("go", "build", "-o", binPath, "../cmd/riverlog").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("%v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func launch(t *testing.T, dir, addr string, extra ...string) *exec.Cmd {
	args := append([]string{"-addr", addr, "-dir", dir, "-segment-bytes", "262144"}, extra...)
	cmd := exec.Command(server(t), args...)
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if strings.Contains(sc.Text(), "listening") {
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("server did not start")
	}
	return cmd
}

type ack struct {
	part uint32
	off  uint64
}

// Producers write while the broker is killed with SIGKILL at random moments
// and restarted. Every acknowledged record must survive at its acknowledged
// offset, exactly once.
func TestCrashKeepsAcknowledgedRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test")
	}
	for _, fsync := range []string{"full", "plain"} {
		t.Run(fsync, func(t *testing.T) { crashRounds(t, fsync) })
	}
}

func crashRounds(t *testing.T, fsync string) {
	dir := t.TempDir()
	addr := freeAddr(t)
	cmd := launch(t, dir, addr, "-sync", "always", "-fsync", fsync, "-create", "crash:3")
	rng := rand.New(rand.NewSource(42))
	var mu sync.Mutex
	acked := map[string]ack{}
	attempted := 0
	const rounds = 8
	for round := 0; round < rounds; round++ {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				cn, err := client.Dial(addr)
				if err != nil {
					return
				}
				defer cn.Close()
				p, err := client.NewProducer(cn, "crash", client.ProducerConfig{Linger: time.Millisecond, BatchBytes: 8 << 10})
				if err != nil {
					return
				}
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					val := fmt.Sprintf("r%d-w%d-%d", round, w, i)
					mu.Lock()
					attempted++
					mu.Unlock()
					p.Produce(nil, []byte(val), func(r client.Result) {
						if r.Err == nil {
							mu.Lock()
							acked[val] = ack{r.Partition, r.Offset}
							mu.Unlock()
						}
					})
					if i%64 == 0 {
						time.Sleep(time.Millisecond)
					}
				}
			}(w)
		}
		time.Sleep(time.Duration(100+rng.Intn(400)) * time.Millisecond)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		close(stop)
		wg.Wait()
		cmd = launch(t, dir, addr, "-sync", "always", "-fsync", fsync)
	}
	defer func() { cmd.Process.Signal(syscall.SIGTERM); cmd.Wait() }()

	cn, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	found := map[string]ack{}
	dups := 0
	for part := uint32(0); part < 3; part++ {
		var off uint64
		_, end, _ := cn.Offsets("crash", part)
		for off < end {
			recs, _, err := cn.Fetch("crash", part, off, 1<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range recs {
				if r.Offset != off {
					t.Fatalf("partition %d: offset gap at %d (got %d)", part, off, r.Offset)
				}
				v := string(r.Value)
				if _, ok := found[v]; ok {
					dups++
				}
				found[v] = ack{part, r.Offset}
				off++
			}
		}
	}
	lost := 0
	for v, a := range acked {
		f, ok := found[v]
		if !ok {
			lost++
			continue
		}
		if f != a {
			t.Fatalf("%s acknowledged at %+v but found at %+v", v, a, f)
		}
	}
	t.Logf("fsync=%s: %d rounds, %d attempted, %d acknowledged, %d in log, %d lost, %d duplicates",
		fsync, rounds, attempted, len(acked), len(found), lost, dups)
	if lost != 0 || dups != 0 {
		t.Fatalf("%d acknowledged records lost, %d duplicates", lost, dups)
	}
	if len(acked) < 1000 {
		t.Fatalf("only %d records acknowledged; test did too little", len(acked))
	}
}
