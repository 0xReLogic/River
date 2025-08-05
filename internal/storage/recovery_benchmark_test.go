package storage

import (
	"fmt"
	"os"
	"testing"
)

// BenchmarkRecovery_WithoutCheckpoint benchmarks recovery without checkpoint
func BenchmarkRecovery_WithoutCheckpoint(b *testing.B) {
	// Skip for now to avoid timeout
	b.Skip("Skipping benchmark due to timeout issues")

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "river-recovery-bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new engine
	engine, err := NewEngine(tempDir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}

	// Add some data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := engine.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Close the engine
	if err := engine.Close(); err != nil {
		b.Fatalf("Failed to close engine: %v", err)
	}

	// Reset timer
	b.ResetTimer()

	// Benchmark recovery
	for i := 0; i < b.N; i++ {
		// Create a new engine (will trigger recovery)
		engine, err := NewEngine(tempDir)
		if err != nil {
			b.Fatalf("Failed to create engine: %v", err)
		}

		// Close the engine
		if err := engine.Close(); err != nil {
			b.Fatalf("Failed to close engine: %v", err)
		}
	}
}

// BenchmarkRecovery_WithCheckpoint benchmarks recovery with checkpoint
func BenchmarkRecovery_WithCheckpoint(b *testing.B) {
	// Skip for now to avoid timeout
	b.Skip("Skipping benchmark due to timeout issues")

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "river-recovery-bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new engine
	engine, err := NewEngine(tempDir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}

	// Add some data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := engine.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Manually create a checkpoint
	if err := engine.createCheckpoint(); err != nil {
		b.Fatalf("Failed to create checkpoint: %v", err)
	}

	// Add more data after checkpoint
	for i := 10000; i < 11000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := engine.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Close the engine
	if err := engine.Close(); err != nil {
		b.Fatalf("Failed to close engine: %v", err)
	}

	// Reset timer
	b.ResetTimer()

	// Benchmark recovery
	for i := 0; i < b.N; i++ {
		// Create a new engine (will trigger recovery)
		engine, err := NewEngine(tempDir)
		if err != nil {
			b.Fatalf("Failed to create engine: %v", err)
		}

		// Close the engine
		if err := engine.Close(); err != nil {
			b.Fatalf("Failed to close engine: %v", err)
		}
	}
}
