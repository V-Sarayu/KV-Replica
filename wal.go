package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// WALEntry is one durable record. Op is "PUT" or "DELETE".
type WALEntry struct {
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"ts"` // unix nano, used for last-write-wins
}

// WAL is an append-only log file, one JSON entry per line.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: f}, nil
}

// Append writes one entry to disk, fsyncs, and returns it (with
// Timestamp filled in if it was zero).
func (w *WAL) Append(op, key, value string) (WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Op:        op,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
	return entry, w.appendEntry(entry)
}

// AppendEntry writes a pre-built entry (used by followers replicating
// a leader's entry verbatim, timestamp included).
func (w *WAL) AppendEntry(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEntry(entry)
}

func (w *WAL) appendEntry(entry WALEntry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := w.file.Write(b); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads every entry from disk in order and calls fn for each.
// Used on startup to rebuild the in-memory store.
func (w *WAL) Replay(fn func(WALEntry)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry WALEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("corrupt wal line: %w", err)
		}
		fn(entry)
	}
	// seek back to end so subsequent appends go after existing data
	if _, err := w.file.Seek(0, 2); err != nil {
		return err
	}
	return scanner.Err()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
