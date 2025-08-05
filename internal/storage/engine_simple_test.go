package storage

import (
	"os"
	"testing"
	"time"
)

// TestSimpleEngineOperations tests basic engine operations
func TestSimpleEngineOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "river-engine-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test with timeout to avoid hanging
	done := make(chan bool)
	go func() {
		// Create a new engine
		engine, err := NewEngine(tempDir)
		if err != nil {
			t.Errorf("Failed to create engine: %v", err)
			done <- true
			return
		}
		defer engine.Close()

		// Test basic operations
		t.Logf("Engine created successfully at %s", tempDir)

		// Try a simple put operation
		key := []byte("test-key")
		value := []byte("test-value")

		err = engine.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put key-value pair: %v", err)
			done <- true
			return
		}

		t.Logf("Successfully put key-value pair")

		// Try to get the value
		result, err := engine.Get(key)
		if err != nil {
			t.Errorf("Failed to get value: %v", err)
			done <- true
			return
		}

		if string(result) != string(value) {
			t.Errorf("Expected value %q, got %q", value, result)
			done <- true
			return
		}

		t.Logf("Successfully retrieved value: %s", result)

		// Signal completion
		done <- true
	}()

	// Wait with timeout
	select {
	case <-done:
		// Test completed successfully
	case <-time.After(10 * time.Second):
		t.Fatalf("Test timed out after 10 seconds")
	}
}
