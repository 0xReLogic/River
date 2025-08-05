package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/0xReLogic/river/internal/storage"
)

// Define custom signal constants for Windows
const (
	// These are not available on Windows, so we define custom values
	// that we'll handle specially in our code
	SIGUSR1 = syscall.Signal(0x10) // Custom signal for child ready
	SIGUSR2 = syscall.Signal(0x11) // Custom signal for graceful restart
)

var (
	// Command line flags
	dataDir   = flag.String("data-dir", "./data", "Directory for storing data")
	httpAddr  = flag.String("http-addr", ":8080", "HTTP server address")
	graceful  = flag.Bool("graceful", false, "Graceful restart (internal use only)")
	parentPid = flag.Int("parent-pid", 0, "Parent PID for graceful restart (internal use only)")
)

func main() {
	// Parse command line flags
	flag.Parse()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create storage engine
	engine, err := storage.NewEngine(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create HTTP server
	server := &http.Server{
		Addr:    *httpAddr,
		Handler: newHandler(engine),
	}

	// Handle graceful restart
	if *graceful && *parentPid > 0 {
		log.Printf("Child process started, parent PID: %d", *parentPid)

		// Signal parent process that we're ready
		parent, err := os.FindProcess(*parentPid)
		if err == nil {
			parent.Signal(SIGUSR1)
		}
	}

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Starting HTTP server on %s", *httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Handle signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, SIGUSR2)

	// Wait for signal
	sig := <-signalChan
	log.Printf("Received signal: %v", sig)

	// Handle graceful restart (SIGUSR2)
	if sig == SIGUSR2 {
		log.Println("Graceful restart requested")

		// Start a new process
		execPath, err := os.Executable()
		if err != nil {
			log.Fatalf("Failed to get executable path: %v", err)
		}

		// Prepare arguments for the new process
		args := []string{
			execPath,
			"-data-dir", *dataDir,
			"-http-addr", *httpAddr,
			"-graceful",
			"-parent-pid", fmt.Sprintf("%d", os.Getpid()),
		}

		// Start the new process
		process, err := os.StartProcess(execPath, args, &os.ProcAttr{
			Dir:   filepath.Dir(execPath),
			Env:   os.Environ(),
			Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		})
		if err != nil {
			log.Fatalf("Failed to start new process: %v", err)
		}

		// Wait for the new process to signal that it's ready
		childReady := make(chan os.Signal, 1)
		signal.Notify(childReady, SIGUSR1)

		select {
		case <-childReady:
			log.Println("Child process ready, shutting down")
		case <-time.After(10 * time.Second):
			log.Println("Timeout waiting for child process, shutting down anyway")
			process.Kill()
		}
	}

	// Shutdown HTTP server
	log.Println("Shutting down HTTP server")
	server.Shutdown(nil)

	// Close storage engine
	log.Println("Closing storage engine")
	engine.Close()

	log.Println("Server stopped")
}

// newHandler creates a new HTTP handler
func newHandler(engine *storage.Engine) http.Handler {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Get endpoint
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		value, err := engine.Get([]byte(key))
		if err != nil {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
			return
		}

		if value == nil {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(value)
	})

	// Put endpoint
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		// Read value from request body
		value, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusInternalServerError)
			return
		}

		if err := engine.Put([]byte(key), value); err != nil {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Delete endpoint
	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		if err := engine.Delete([]byte(key)); err != nil {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Stats endpoint
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		stats := engine.GetStats()

		// Convert stats to JSON
		statsJSON, err := json.Marshal(stats)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(statsJSON)
	})

	return mux
}
