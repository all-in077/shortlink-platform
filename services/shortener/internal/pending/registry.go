package pending

import "sync"

type QuotaResult struct {
	Approved bool
	Reason   string
}

type Registry struct {
	mu      sync.Mutex
	waiters map[string]chan QuotaResult
}

func NewRegistry() *Registry {
	return &Registry{
		waiters: make(map[string]chan QuotaResult),
	}
}

func (r *Registry) Register(requestID string) chan QuotaResult {
	ch := make(chan QuotaResult, 1) // буфер на 1: Resolve не блокируется, если хендлер уже ушёл по таймауту
	r.mu.Lock()
	r.waiters[requestID] = ch
	r.mu.Unlock()
	return ch
}

func (r *Registry) Resolve(requestID string, result QuotaResult) {
	r.mu.Lock()
	ch, ok := r.waiters[requestID]
	if ok {
		delete(r.waiters, requestID)
	}
	r.mu.Unlock()

	if ok {
		ch <- result
	}
}

func (r *Registry) Cancel(requestID string) {
	r.mu.Lock()
	delete(r.waiters, requestID)
	r.mu.Unlock()
}
