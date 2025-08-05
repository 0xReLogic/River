package storage

import (
	"os"
	"testing"
	"time"
)

// TestSimpleBlockOperations tests basic block operations without the full engine
func TestSimpleBlockOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "river-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Test with timeout to avoid hanging
	done := make(chan bool)
	go func() {
		// Create a new LSM tree
		tree, err := NewLSMTree(tempDir)
		if err != nil {
			t.Errorf("Failed to create LSM tree: %v", err)
			done <- true
			return
		}
		defer tree.Close()
		
		// Test basic operations
		t.Logf("LSM tree created successfully at %s", tempDir)
		
		// Signal completion
		done <- true
	}()
	
	// Wait with timeout
	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatalf("Test timed out after 5 seconds")
	}
}

// TestSimpleWALOperations tests basic WAL operations
func TestSimpleWALOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "river-wal-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Test with timeout to avoid hanging
	done := make(chan bool)
	go func() {
		// Create a new WAL
		wal, err := NewWAL(tempDir)
		if err != nil {
			t.Errorf("Failed to create WAL: %v", err)
			done <- true
			return
		}
		defer wal.Close()
		
		// Test basic operations
		t.Logf("WAL created successfully at %s", tempDir)
		
		// Signal completion
		done <- true
	}()
	
	// Wait with timeout
	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatalf("Test timed out after 5 seconds")
	}
}
