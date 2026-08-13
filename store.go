package main

import "sync"

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot returns a copy of the entire store, used for follower
// full-state resync on reconnect.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]string, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

// LoadSnapshot wipes and replaces the store's contents — used when a
// follower catches up via full resync.
func (s *Store) LoadSnapshot(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(data))
	for k, v := range data {
		s.data[k] = v
	}
}
