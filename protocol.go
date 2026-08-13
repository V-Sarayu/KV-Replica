package main

// MsgType identifies what kind of message is being sent over the wire.
type MsgType string

const (
	// Client <-> Leader
	MsgPut    MsgType = "PUT"
	MsgGet    MsgType = "GET"
	MsgAck    MsgType = "ACK"    // response to PUT
	MsgResult MsgType = "RESULT" // response to GET

	// Follower -> Leader
	MsgRegisterFollower MsgType = "REGISTER_FOLLOWER"

	// Leader -> Follower
	MsgReplicate MsgType = "REPLICATE" // one WAL entry to apply
	MsgSnapshot  MsgType = "SNAPSHOT"  // full state, sent on (re)connect
)

// Message is the single envelope type sent over every TCP connection
// in this system. Not every field is used by every MsgType — e.g. a
// PUT uses Key/Value, a SNAPSHOT uses Data, a RESULT uses Value/Found.
type Message struct {
	Type MsgType `json:"type"`

	// Used by PUT, GET, RESULT
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"` // RESULT: whether GET found the key

	// Used by REPLICATE: the exact WAL entry to apply, timestamp included
	Entry *WALEntry `json:"entry,omitempty"`

	// Used by SNAPSHOT: full key-value state for follower resync
	Data map[string]string `json:"data,omitempty"`

	// Used by ACK / errors
	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
}
