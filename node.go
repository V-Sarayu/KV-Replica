package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

// Node holds the state every role (leader or follower) needs:
// an in-memory store, a WAL for durability, and bookkeeping for
// role-specific behavior layered on top in leader.go/follower.go.
type Node struct {
	Store *Store
	WAL   *WAL

	mode string // "leader" or "follower"

	// Leader-only: connections to followers, guarded by mu.
	mu        sync.Mutex
	followers map[net.Conn]struct{}
}

// NewNode creates a Node, opens its WAL at walPath, and replays it
// to rebuild in-memory state from any previous run.
func NewNode(mode, walPath string) (*Node, error) {
	store := NewStore()

	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	n := &Node{
		Store:     store,
		WAL:       wal,
		mode:      mode,
		followers: make(map[net.Conn]struct{}),
	}

	replayCount := 0
	err = wal.Replay(func(entry WALEntry) {
		applyEntry(store, entry)
		replayCount++
	})
	if err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}
	log.Printf("[%s] replayed %d WAL entries from %s", mode, replayCount, walPath)

	return n, nil
}

// applyEntry applies one WAL entry's effect to the store. Shared by
// startup replay, leader writes, and follower replication so all
// three paths interpret an entry identically.
func applyEntry(store *Store, entry WALEntry) {
	switch entry.Op {
	case "PUT":
		store.Put(entry.Key, entry.Value)
	case "DELETE":
		store.Delete(entry.Key)
	default:
		log.Printf("warning: unknown WAL op %q, skipping", entry.Op)
	}
}

// sendMessage writes one Message to conn as JSON. Safe to call from
// multiple goroutines only if the caller synchronizes writes to a
// given conn (json.Encoder itself is not safe for concurrent Encode
// calls to the same connection).
func sendMessage(conn net.Conn, msg Message) error {
	return json.NewEncoder(conn).Encode(msg)
}

// receiveMessage blocks until one Message arrives on conn, or returns
// an error (including io.EOF on disconnect).
func receiveMessage(conn net.Conn) (Message, error) {
	var msg Message
	dec := json.NewDecoder(conn)
	err := dec.Decode(&msg)
	return msg, err
}

// startListener opens a TCP listener on addr and returns it. Both
// leader (for clients+followers) and follower (not used — followers
// only dial out) can use this; kept here since it's generic.
func startListener(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}
	log.Printf("listening on %s", addr)
	return ln, nil
}
