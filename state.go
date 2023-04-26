package papillon

import "sync/atomic"

type (
	State uint64
)

func (s *State) String() string {
	switch *s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case ShutDown:
		return "ShutDown"
	default:
		return "Unknown"
	}
}

const (
	Follower State = iota
	Candidate
	Leader
	ShutDown
)

func newState() *State {
	state := new(State)
	state.set(Follower)
	return state
}

func (s *State) set(newState State) {
	atomic.StoreUint64((*uint64)(s), uint64(newState))
}

func (s *State) Get() State {
	return State(atomic.LoadUint64((*uint64)(s)))
}
