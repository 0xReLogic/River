.PHONY: all build build-server build-benchmark test bench lint clean benchmark stress-test

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOLINT=golangci-lint run
GOFLAGS=-ldflags="-s -w"

# Binary directories and names
BINARY_DIR=bin
BINARY_NAME=river
SERVER_BINARY=$(BINARY_DIR)/server
BENCHMARK_BINARY=$(BINARY_DIR)/benchmark

# Default target
all: build

# Create binary directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build all binaries
build: $(BINARY_DIR) build-server build-benchmark

# Build main binary
build-main: $(BINARY_DIR)
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) $(GOFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME) ./...

# Build server binary
build-server: $(BINARY_DIR)
	@echo "Building server..."
	$(GOBUILD) $(GOFLAGS) -o $(SERVER_BINARY) ./cmd/server

# Build benchmark binary
build-benchmark: $(BINARY_DIR)
	@echo "Building benchmark..."
	$(GOBUILD) $(GOFLAGS) -o $(BENCHMARK_BINARY) ./cmd/benchmark

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. ./...

# Run linter
lint:
	@echo "Linting code..."
	$(GOLINT) ./...

# Run benchmark tool
benchmark: build-benchmark
	@echo "Running benchmark tool..."
	$(BENCHMARK_BINARY) -inserts 100000 -queries 1000 -threads 4

# Run stress test
stress-test: build
	@echo "Running stress test..."
	powershell -File scripts/stress-test.ps1 -Iterations 10 -OperationsPerIteration 1000

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BINARY_DIR)
