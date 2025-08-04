.PHONY: build lint test bench

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOLINT=golangci-lint run

# Binary name
BINARY_NAME=river

build:
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) -o $(BINARY_NAME) ./...

test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. ./...

lint:
	@echo "Linting code..."
	$(GOLINT) ./...

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
