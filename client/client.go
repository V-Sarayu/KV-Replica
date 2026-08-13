package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

// Message mirrors the leader's protocol.go Message struct. Kept as a
// standalone copy here (rather than importing the node package)
// since this is a separate binary and we're avoiding extra module
// wiring for a demo-scale project.
type Message struct {
	Type  string `json:"type"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"`
	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
}

func main() {
	leaderAddr := flag.String("leader", "localhost:9000", "leader address to connect to")
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		runREPL(*leaderAddr)
		return
	}

	if err := runOnce(*leaderAddr, args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// runOnce sends a single PUT or GET command given as CLI args, e.g.:
//
//	go run ./client PUT foo bar
//	go run ./client GET foo
func runOnce(leaderAddr string, args []string) error {
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PUT":
		if len(args) != 3 {
			return fmt.Errorf("usage: PUT <key> <value>")
		}
		resp, err := send(leaderAddr, Message{Type: "PUT", Key: args[1], Value: args[2]})
		if err != nil {
			return err
		}
		if !resp.OK {
			return fmt.Errorf("leader rejected PUT: %s", resp.Error)
		}
		fmt.Printf("OK\n")

	case "GET":
		if len(args) != 2 {
			return fmt.Errorf("usage: GET <key>")
		}
		resp, err := send(leaderAddr, Message{Type: "GET", Key: args[1]})
		if err != nil {
			return err
		}
		if !resp.Found {
			fmt.Printf("(not found)\n")
		} else {
			fmt.Printf("%s\n", resp.Value)
		}

	default:
		return fmt.Errorf("unknown command %q (expected PUT or GET)", cmd)
	}

	return nil
}

// runREPL starts an interactive loop for the demo: type commands one
// after another without relaunching the binary each time.
func runREPL(leaderAddr string) {
	fmt.Printf("kvreplica client, connected to leader at %s\n", leaderAddr)
	fmt.Println("commands: PUT <key> <value> | GET <key> | quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			break
		}

		fields := strings.Fields(line)
		if err := runOnce(leaderAddr, fields); err != nil {
			fmt.Println("error:", err)
		}
	}
}

// send opens a fresh connection per request (one-shot, matching the
// leader's handleClientRequest which closes after one response),
// sends msg, and returns the decoded response.
func send(leaderAddr string, msg Message) (Message, error) {
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		return Message{}, fmt.Errorf("connect to leader: %w", err)
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return Message{}, fmt.Errorf("send request: %w", err)
	}

	var resp Message
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return Message{}, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}
