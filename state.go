package papillon

import "sync/atomic"

type (
	State uint64
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case ShutDown:
		return "ShutDown"
	default:
		return unknown
	}
}

const (
	Follower State = iota + 1
	Candidate
	Leader
	ShutDown
)

func (s *Raft) _setState(newState State) {
	atomic.StoreUint64((*uint64)(&s.state), uint64(newState))
}

func (s *Raft) GetState() State {
	return State(atomic.LoadUint64((*uint64)(&s.state)))
}
