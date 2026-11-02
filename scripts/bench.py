#!/usr/bin/env python3
"""riverlog vs Kafka vs NATS JetStream; writes bench/results/bench.json.

Each server starts from an empty data directory, single node, on this
machine. Two durability modes:

  fsync  every acknowledged batch is on stable storage (F_FULLFSYNC on
         macOS for all three: Go's File.Sync, and Java's FileChannel.force
         since JDK 9): riverlog -sync always, Kafka topic flush.messages=1,
         NATS jetstream sync_interval: always.
         Also riverlog with plain fsync(2) ("riverlog-plain"), for reference.
  async  acknowledged when written to the OS:
         riverlog -sync never, Kafka defaults, NATS defaults

Each configuration runs RUNS times; the table uses medians.
"""
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "bin")
KAFKA = subprocess.run(["brew", "--prefix", "kafka"], capture_output=True, text=True).stdout.strip()

WORKLOADS = [
    # name, rlbench args
    ("1 producer, 1 partition, 100 B", ["-mode", "throughput", "-producers", "1", "-partitions", "1", "-size", "100", "-duration", "10s"]),
    ("4 producers, 4 partitions, 100 B", ["-mode", "throughput", "-producers", "4", "-partitions", "4", "-size", "100", "-duration", "10s"]),
    ("4 producers, 4 partitions, 1 KiB", ["-mode", "throughput", "-producers", "4", "-partitions", "4", "-size", "1024", "-duration", "10s"]),
]
# Latency is measured at a rate every system sustains in that mode (NATS with
# per-message F_FULLFSYNC manages a few hundred records/s).
LATENCY = {
    "fsync": ("latency at 200 records/s", ["-mode", "latency", "-partitions", "1", "-size", "100", "-rate", "200", "-duration", "10s"]),
    "async": ("latency at 10k records/s", ["-mode", "latency", "-partitions", "1", "-size", "100", "-rate", "10000", "-duration", "10s"]),
}


def wait_port(port, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"port {port} did not open")


def stop(p):
    p.terminate()
    try:
        p.wait(30)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def start_riverlog(d, durability, fsync="full"):
    args = ["-sync", "always", "-fsync", fsync] if durability == "fsync" else ["-sync", "never"]
    p = subprocess.Popen([f"{BIN}/riverlog", "-addr", "127.0.0.1:9192", "-dir", d, *args], stderr=subprocess.DEVNULL)
    wait_port(9192)
    return p


def start_kafka(d, durability):
    props = open(f"{KAFKA}/libexec/config/server.properties").read()
    props = "\n".join(l for l in props.splitlines() if not l.startswith("log.dirs="))
    props += f"\nlog.dirs={d}/logs\n"
    cfg = f"{d}/server.properties"
    open(cfg, "w").write(props)
    env = dict(os.environ, KAFKA_HEAP_OPTS="-Xms2g -Xmx2g", LOG_DIR=f"{d}/applogs")
    subprocess.run([f"{KAFKA}/bin/kafka-storage", "format", "--standalone", "-t", str(uuid.uuid4()), "-c", cfg],
                   check=True, capture_output=True, env=env)
    p = subprocess.Popen([f"{KAFKA}/bin/kafka-server-start", cfg], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    wait_port(9092, 120)
    time.sleep(3)  # controller election
    return p


def start_nats(d, durability):
    conf = f"{d}/nats.conf"
    sync = "\n  sync_interval: always" if durability == "fsync" else ""
    open(conf, "w").write(f"jetstream {{\n  store_dir: \"{d}/js\"{sync}\n}}\n")
    p = subprocess.Popen(["nats-server", "-c", conf, "-a", "127.0.0.1", "-p", "4222"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_port(4222)
    return p


STARTERS = {
    "riverlog": start_riverlog,
    "riverlog-plain": lambda d, dur: start_riverlog(d, dur, "plain"),
    "kafka": start_kafka,
    "nats": start_nats,
}


def run(system, durability, args):
    extra = ["-kafka-fsync"] if system == "kafka" and durability == "fsync" else []
    sysname = "riverlog" if system == "riverlog-plain" else system
    out = subprocess.run([f"{BIN}/rlbench", "-system", sysname, *args, *extra], check=True, capture_output=True, text=True, timeout=900)
    return json.loads(out.stdout.strip().splitlines()[-1])


def main():
    subprocess.run(["go", "build", "-o", BIN + "/", "./cmd/..."], check=True, cwd=ROOT)
    env = {
        "date": time.strftime("%Y-%m-%d %H:%M"),
        "machine": subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip(),
        "go": subprocess.run(["go", "version"], capture_output=True, text=True).stdout.strip(),
        "kafka": os.path.basename(os.path.realpath(KAFKA)),
        "nats": subprocess.run(["nats-server", "--version"], capture_output=True, text=True).stdout.strip(),
        "runs": RUNS,
    }
    rows = []
    for durability in ["fsync", "async"]:
        systems = ["riverlog", "riverlog-plain", "kafka", "nats"] if durability == "fsync" else ["riverlog", "kafka", "nats"]
        for system in systems:
            # One server per (system, durability); each run uses a fresh topic.
            d = tempfile.mkdtemp(prefix=f"rlbench-{system}-")
            p = STARTERS[system](d, durability)
            try:
                for name, args in WORKLOADS + [LATENCY[durability]]:
                    samples = [run(system, durability, args) for _ in range(RUNS)]
                    med = lambda k: statistics.median(s[k] for s in samples)
                    row = {"system": system, "durability": durability, "workload": name,
                           "records_per_s": med("records_per_s"), "mb_per_s": med("mb_per_s"),
                           "p50_ms": med("p50_ms"), "p99_ms": med("p99_ms"), "p999_ms": med("p999_ms"),
                           "errors": max(s["errors"] for s in samples), "samples": samples}
                    rows.append(row)
                    print(f"{durability:5s} {system:8s} {name:36s} {row['records_per_s']/1e3:10.0f}k rec/s  "
                          f"p50 {row['p50_ms']:8.2f} ms  p99 {row['p99_ms']:8.2f} ms  errors {row['errors']}", flush=True)
            finally:
                stop(p)
                shutil.rmtree(d, ignore_errors=True)
    os.makedirs(f"{ROOT}/bench/results", exist_ok=True)
    json.dump({"env": env, "rows": rows}, open(f"{ROOT}/bench/results/bench.json", "w"), indent=2)


if __name__ == "__main__":
    main()
