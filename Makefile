.PHONY: build test race crash lint bench clean
build:
	go build -o bin/ ./cmd/...
test:
	go test -short ./...
race:
	go test -race -short ./...
crash:
	go test ./tests/ -run Crash -v -count=1
lint:
	gofmt -l . | (! grep .)
	go vet ./...
# Needs Kafka and nats-server installed (brew install kafka nats-server).
bench: build
	python3 scripts/bench.py
clean:
	rm -rf bin
