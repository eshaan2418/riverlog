// Command riverlog runs a broker.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eshaan2418/riverlog/internal/broker"
	rlog "github.com/eshaan2418/riverlog/internal/log"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9192", "listen address")
	dir := flag.String("dir", "data", "data directory")
	syncMode := flag.String("sync", "always", "always (ack after fsync, group commit) | interval | never")
	interval := flag.Duration("sync-interval", 50*time.Millisecond, "flush interval for -sync=interval")
	fsync := flag.String("fsync", "full", "full (F_FULLFSYNC on macOS) | plain (fsync(2))")
	segment := flag.Int64("segment-bytes", 256<<20, "segment size")
	retention := flag.Int64("retention-bytes", 0, "per-partition retention (0 = unlimited)")
	retentionAge := flag.Duration("retention-age", 0, "per-partition retention by age (0 = unlimited)")
	create := flag.String("create", "", "create topic name:partitions at startup if missing")
	flag.Parse()

	opts := rlog.DefaultOptions()
	opts.SegmentBytes = *segment
	opts.RetentionBytes = *retention
	opts.RetentionAge = *retentionAge
	opts.Interval = *interval
	opts.PlainFsync = *fsync == "plain"
	switch *syncMode {
	case "always":
		opts.Sync = rlog.SyncAlways
	case "interval":
		opts.Sync = rlog.SyncInterval
	case "never":
		opts.Sync = rlog.SyncNever
	default:
		log.Fatalf("unknown -sync %q", *syncMode)
	}
	b, err := broker.Open(broker.Config{Dir: *dir, LogOpts: opts})
	if err != nil {
		log.Fatal(err)
	}
	if *create != "" {
		var name string
		var n int
		if _, err := fmt.Sscanf(replaceColon(*create), "%s %d", &name, &n); err != nil {
			log.Fatalf("bad -create %q", *create)
		}
		b.CreateTopic(name, n) // TopicExists is fine
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "riverlog listening on %s (sync=%s fsync=%s)\n", ln.Addr(), *syncMode, *fsync)
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		b.Close()
		os.Exit(0)
	}()
	if err := b.Serve(ln); err != nil {
		log.Fatal(err)
	}
}

func replaceColon(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return s[:i] + " " + s[i+1:]
		}
	}
	return s
}
