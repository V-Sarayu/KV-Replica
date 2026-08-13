package main

import (
	"errors"
	"io"
	"log"
	"net"
	"time"
)

// RunFollower connects to the leader at leaderAddr, registers itself,
// applies the initial snapshot, then continuously applies replicated
// writes. If the connection drops, it retries indefinitely with a
// short backoff — this is what makes "kill follower, restart it"
// result in automatic catch-up rather than requiring a fresh process.
func RunFollower(n *Node, leaderAddr string) error {
	for {
		if err := n.connectAndSync(leaderAddr); err != nil {
			log.Printf("follower: disconnected from leader (%v), retrying in 2s", err)
			time.Sleep(2 * time.Second)
			continue
		}
	}
}

// connectAndSync dials the leader, registers, applies the snapshot,
// then blocks processing REPLICATE messages until the connection
// breaks. Returns the error that ended the loop so the caller can
// decide whether/how to retry.
func (n *Node) connectAndSync(leaderAddr string) error {
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("follower: connected to leader at %s", leaderAddr)

	if err := sendMessage(conn, Message{Type: MsgRegisterFollower}); err != nil {
		return err
	}

	// First message back must be the snapshot.
	snapMsg, err := receiveMessage(conn)
	if err != nil {
		return err
	}
	if snapMsg.Type != MsgSnapshot {
		return errNotSnapshot
	}

	// Full-state resync: load the leader's current data. This
	// overwrites anything stale we had locally, which is exactly the
	// simplification we chose — the leader's current state is always
	// treated as authoritative, no per-key timestamp comparison needed
	// on this path.
	n.Store.LoadSnapshot(snapMsg.Data)
	log.Printf("follower: synced snapshot, %d keys loaded", len(snapMsg.Data))

	// Now loop, applying replicated writes as they stream in.
	for {
		msg, err := receiveMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return err // leader closed connection or we lost it
			}
			return err
		}

		if msg.Type != MsgReplicate || msg.Entry == nil {
			log.Printf("follower: unexpected message type %q, ignoring", msg.Type)
			continue
		}

		entry := *msg.Entry
		if err := n.WAL.AppendEntry(entry); err != nil {
			log.Printf("follower: failed to append replicated entry to WAL: %v", err)
			continue
		}
		applyEntry(n.Store, entry)
		log.Printf("follower: applied replicated %s %s=%s", entry.Op, entry.Key, entry.Value)
	}
}

var errNotSnapshot = errors.New("expected SNAPSHOT as first message from leader")
