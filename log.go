package papillon

import (
	"time"
)

var (
	ErrKeyIsNil   = customError{"key is nil"}
	ErrValueIsNil = customError{"value is nil"}
	ErrRange      = customError{"from must no bigger than to"}
)

type (
	LogType uint8 // 日志类型
	// LogEntry 日志条目，毕传 Data 、Type 字段，Index、Term、CreatedAt 字段会在 applyLog 方法中默认设置
	LogEntry struct {
		Index     uint64    // 日志的日志索引
		Term      uint64    // 创建日志时的任期
		Data      []byte    // 日志内容
		Type      LogType   // 日志类型
		CreatedAt time.Time // 创建时间
	}
)

const (
	LogCommand LogType = iota + 1 // 用户命令
	LogBarrier                    // 用于确认 index 已经被应用到状态机，[LogFuture] 会被状态机线程确认
	LogNoop                       // 禁止操作，用于提交一个空命令帮助 raft 确认之前的索引都已经提交，[LogFuture] 不会被状态机线程确认
	LogCluster                    // 用于存储集群配置
)
