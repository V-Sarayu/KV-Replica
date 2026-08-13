package main

import (
	"flag"
	"log"
)

func main() {
	mode := flag.String("mode", "", "node mode: 'leader' or 'follower' (required)")
	addr := flag.String("addr", ":9000", "address this node listens on (leader only)")
	leaderAddr := flag.String("leader", "localhost:9000", "leader address to connect to (follower only)")
	walPath := flag.String("wal", "", "path to WAL file (default: <mode>.wal)")
	flag.Parse()

	if *mode != "leader" && *mode != "follower" {
		log.Fatalf("must specify -mode=leader or -mode=follower")
	}

	wal := *walPath
	if wal == "" {
		wal = *mode + ".wal"
	}

	node, err := NewNode(*mode, wal)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	switch *mode {
	case "leader":
		log.Printf("starting leader on %s (wal: %s)", *addr, wal)
		if err := RunLeader(node, *addr); err != nil {
			log.Fatalf("leader failed: %v", err)
		}

	case "follower":
		log.Printf("starting follower, connecting to leader at %s (wal: %s)", *leaderAddr, wal)
		if err := RunFollower(node, *leaderAddr); err != nil {
			log.Fatalf("follower failed: %v", err)
		}
	}
}
