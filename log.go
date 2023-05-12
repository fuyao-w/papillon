package papillon

import (
	"time"
)

var (
	ErrKeyNotFound = customError{"not found"}
	ErrKeyIsNil    = customError{"key is nil"}
	ErrValueIsNil  = customError{"value is nil"}
	ErrRange       = customError{"from must no bigger than to"}
)

type (
	LogType  uint8
	LogEntry struct {
		Index     uint64
		Term      uint64
		Data      []byte
		Type      LogType
		CreatedAt time.Time
	}
)

const (
	LogCommand LogType = iota + 1
	LogBarrier
	// LogNoop 只用于确认 leader
	LogNoop
	LogCluster
)
