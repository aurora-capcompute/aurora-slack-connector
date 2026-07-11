package connector

import "sync"

// seenSet is a bounded set of recently seen keys for event deduplication. Slack
// can deliver the same user message twice (once as app_mention, once as
// message) and retries failed deliveries; both carry the same (channel, ts), so
// a small FIFO of keys is enough to make handling idempotent.
type seenSet struct {
	mu    sync.Mutex
	set   map[string]struct{}
	order []string
	max   int
}

func newSeenSet(max int) *seenSet {
	if max <= 0 {
		max = 1024
	}
	return &seenSet{set: make(map[string]struct{}, max), max: max}
}

// add records a key and reports whether it was new (true) or a duplicate
// (false). The oldest key is evicted once the set is full.
func (s *seenSet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[key]; ok {
		return false
	}
	s.set[key] = struct{}{}
	s.order = append(s.order, key)
	if len(s.order) > s.max {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.set, oldest)
	}
	return true
}

// forget removes a key so it can be recorded again. It undoes an optimistic
// add() when the work that add guarded could not be started (a transient
// error), so a retry or a later redelivery is not silently deduped away.
func (s *seenSet) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[key]; !ok {
		return
	}
	delete(s.set, key)
	for i, k := range s.order {
		if k == key {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}
