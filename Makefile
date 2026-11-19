.PHONY: build test test-race demo bench lint

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

# The demo is the end-to-end suite in verbose mode: each scenario prints its
# timeline on the fake clock, then verifies it.
demo:
	go test ./test/e2e -v -count=1

bench:
	go test ./internal/engine -run '^$$' -bench . -benchmem

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	@if command -v staticcheck >/dev/null; then staticcheck ./...; else echo "staticcheck not installed, skipped (CI runs it)"; fi
