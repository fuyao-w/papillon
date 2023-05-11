package papillon

import (
	"fmt"
	"time"
)

const (
	minCheckInterval     = 10 * time.Millisecond
	minHeartBeatInterval = time.Millisecond * 10
)

type Config struct {
	ElectionTimeout         time.Duration
	HeartbeatTimeout        time.Duration
	LeaderLeaseTimeout      time.Duration
	ApplyBatch              bool
	MaxAppendEntries        uint64
	CommitTimeout           time.Duration
	SnapshotInterval        time.Duration
	SnapshotThreshold       uint64
	TrailingLogs            uint64
	Logger                  Logger
	LocalID                 string
	LeadershipCatchUpRounds uint
	LeadershipLostShutDown  bool
}

type ReloadableConfig struct {
	TrailingLogs      uint64
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}
func ValidateConfig(c *Config) (bool, string) {
	if len(c.LocalID) == 0 {
		return false, "LocalID is blank"
	}
	if c.TrailingLogs < 1 {
		return false, "TrailingLogs must greater than 1"
	}
	if c.SnapshotThreshold < 0 {
		return false, "SnapshotThreshold must greater than 0"
	}
	if c.MaxAppendEntries < 1 {
		return false, "MaxAppendEntries must greater than 1"
	}
	maximumAppendEntries := uint64(1024)
	if c.MaxAppendEntries > maximumAppendEntries {
		return false, fmt.Sprintf("MaxAppendEntries must less than or equal to %d", maximumAppendEntries)
	}
	minimumTimeout := 5 * time.Millisecond
	if c.HeartbeatTimeout < minimumTimeout {
		return false, fmt.Sprintf("HeartbeatTimeout must greater than :%s", minimumTimeout)
	}
	if c.ElectionTimeout < minimumTimeout {
		return false, fmt.Sprintf("ElectionTimeout must greater than :%s", minimumTimeout)
	}

	if c.SnapshotInterval < minimumTimeout {
		return false, fmt.Sprintf("SnapshotInterval must greater than :%s", minimumTimeout)
	}
	if c.LeaderLeaseTimeout < minCheckInterval {
		return false, fmt.Sprintf("LeaderLeaseTimeout must greater than :%s", minCheckInterval)
	}
	minimumCommitTimeout := time.Millisecond
	if c.CommitTimeout < minimumCommitTimeout {
		return false, fmt.Sprintf("CommitTimeout must greater than :%s", minimumCommitTimeout)
	}
	// 处理投票时实现一种租期机制，如果能正常接受到了心跳，则拒绝投票请求，如果选举超时果断则会多发送无效请求
	if c.ElectionTimeout < c.HeartbeatTimeout {
		return false, fmt.Sprintf("ElectionTimeout must greater than or equal HeartbeatTimeout")
	}
	// 确保领导者至少可以先下台
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		return false, fmt.Sprintf("LeaderLeaseTimeout must greater than or equal HeartbeatTimeout")
	}
	return true, ""
}
