// Package store holds job records.
package store

import (
	"errors"
	"sort"
	"sync"

	"github.com/LaithAlz/jobsched/internal/job"
)

var (
	ErrNotFound = errors.New("job not found")
	ErrExists   = errors.New("job already exists")
)

// Store is the interface the rest of the program depends on. Declared here rather than in the
// consumers only because both `api` and `worker` need the same view of it; the concrete
// MemoryStore is what main.go wires in. See NOTES.md D-009.
type Store interface {
	Create(j job.Job) error
	Get(id string) (job.Job, error)
	List() []job.Job
	// Counts tallies jobs by status. It exists separately from List because it is called on
	// every metrics scrape, and List's defensive copying (D-012) would clone every job's
	// full attempt history just to count them. See NOTES.md D-038.
	Counts() map[job.Status]int
	// Update applies fn to the stored record while holding the write lock, then returns the
	// updated snapshot. Read-modify-write through this method is atomic; doing it as a
	// separate Get + Put would not be. See NOTES.md D-016.
	Update(id string, fn func(*job.Job)) (job.Job, error)
}

// MemoryStore is a process-local, non-durable Store. See NOTES.md D-008 for the (accepted)
// consequence: a restart loses all job state.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]job.Job
}

func NewMemory() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]job.Job)}
}

func (s *MemoryStore) Create(j job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; ok {
		return ErrExists
	}
	s.jobs[j.ID] = j.Clone()
	return nil
}

func (s *MemoryStore) Get(id string) (job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return job.Job{}, ErrNotFound
	}
	return j.Clone(), nil
}

// List returns all jobs, newest first.
func (s *MemoryStore) List() []job.Job {
	s.mu.RLock()
	out := make([]job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j.Clone())
	}
	s.mu.RUnlock()

	// Sorted outside the lock: it only touches the local copy. Go map iteration order is
	// deliberately randomised, so without this the list endpoint would return a different
	// order on every call.
	sort.Slice(out, func(a, b int) bool {
		return out[a].CreatedAt.After(out[b].CreatedAt)
	})
	return out
}

// Counts tallies jobs by status without copying any records.
func (s *MemoryStore) Counts() map[job.Status]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[job.Status]int, 5)
	for _, j := range s.jobs {
		out[j.Status]++
	}
	return out
}

func (s *MemoryStore) Update(id string, fn func(*job.Job)) (job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return job.Job{}, ErrNotFound
	}
	fn(&j)
	s.jobs[id] = j
	return j.Clone(), nil
}
