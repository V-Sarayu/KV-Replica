package main

import (
	"errors"
	"io"
	"log"
	"net"
)

// RunLeader starts the leader's TCP listener and serves both client
// and follower connections on the same port, dispatching based on
// the first message type received.
func RunLeader(n *Node, addr string) error {
	ln, err := startListener(addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go n.handleConnection(conn)
	}
}

// handleConnection reads the first message on a new connection to
// decide whether it's a follower registering or a client sending a
// single request, then dispatches accordingly.
func (n *Node) handleConnection(conn net.Conn) {
	msg, err := receiveMessage(conn)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("connection read error: %v", err)
		}
		conn.Close()
		return
	}

	if msg.Type == MsgRegisterFollower {
		n.handleFollowerRegistration(conn) // defined in part 2
		return
	}

	// Otherwise treat it as a one-shot client request.
	n.handleClientRequest(conn, msg)
}

// handleClientRequest processes a single PUT or GET from a client and
// closes the connection afterward (our client is one-shot per command,
// simple and sufficient for the demo).
func (n *Node) handleClientRequest(conn net.Conn, msg Message) {
	defer conn.Close()

	switch msg.Type {
	case MsgPut:
		entry, err := n.WAL.Append("PUT", msg.Key, msg.Value)
		if err != nil {
			sendMessage(conn, Message{Type: MsgAck, OK: false, Error: err.Error()})
			return
		}
		n.Store.Put(msg.Key, msg.Value)

		n.broadcastToFollowers(entry) // defined in part 2

		sendMessage(conn, Message{Type: MsgAck, OK: true})

	case MsgGet:
		value, found := n.Store.Get(msg.Key)
		sendMessage(conn, Message{Type: MsgResult, Key: msg.Key, Value: value, Found: found})

	default:
		sendMessage(conn, Message{Type: MsgAck, OK: false, Error: "unknown request type"})
	}
}

// handleFollowerRegistration is called when a connection's first
// message is REGISTER_FOLLOWER. It sends the follower a full snapshot
// of current state (covers both first-time join and reconnect-after-
// downtime catch-up), registers the conn for future broadcasts, then
// blocks reading from the conn just to detect disconnection.
func (n *Node) handleFollowerRegistration(conn net.Conn) {
	log.Printf("follower connected: %s", conn.RemoteAddr())

	snapshot := n.Store.Snapshot()
	if err := sendMessage(conn, Message{Type: MsgSnapshot, Data: snapshot}); err != nil {
		log.Printf("failed to send snapshot to follower: %v", err)
		conn.Close()
		return
	}

	n.mu.Lock()
	n.followers[conn] = struct{}{}
	n.mu.Unlock()

	// Block here reading from the follower conn. Followers don't send
	// anything more after registering, so any read returning (even a
	// real message, which shouldn't happen) means the conn is done or
	// broken — either way we clean up.
	_, _ = receiveMessage(conn)

	n.mu.Lock()
	delete(n.followers, conn)
	n.mu.Unlock()
	conn.Close()
	log.Printf("follower disconnected: %s", conn.RemoteAddr())
}

// broadcastToFollowers sends one replicated WAL entry to every
// currently-registered follower. A follower that fails to receive
// (e.g. it died) is dropped from the set — its next reconnect will
// catch it up via a fresh snapshot, so we don't need retry logic here.
func (n *Node) broadcastToFollowers(entry WALEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()

	msg := Message{Type: MsgReplicate, Entry: &entry}

	for conn := range n.followers {
		if err := sendMessage(conn, msg); err != nil {
			log.Printf("failed to replicate to %s, dropping: %v", conn.RemoteAddr(), err)
			conn.Close()
			delete(n.followers, conn)
		}
	}
}
