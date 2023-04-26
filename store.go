package papillon

var (
	KeyCurrentTerm  = []byte("CurrentTerm")
	KeyLastVoteFor  = []byte("LastVoteFor")
	KeyLastVoteTerm = []byte("LastVoteTerm")
)

// LogStore 提供日志操作的抽象
type LogStore interface {
	// FirstIndex 返回第一个写入的索引，-1 代表没有
	FirstIndex() (uint64, error)
	// LastIndex 返回最后一个写入的索引，-1 代表没有
	LastIndex() (uint64, error)
	// GetLog 返回指定位置的索引
	GetLog(index uint64) (log *LogEntry, err error)
	// GetLogRange 按指定范围遍历索引，闭区间
	GetLogRange(from, to uint64) (log []*LogEntry, err error)
	// SetLogs 追加日志
	SetLogs(logs []*LogEntry) error
	// DeleteRange 批量删除指定范围的索引内容，用于快照生成
	DeleteRange(from, to uint64) error
}

// KVStorage 提供稳定存储的抽象
type KVStorage interface {
	// Get 用于存储日志
	Get(key []byte) (val []byte, err error)
	// Set 用于存储日志
	Set(key, val []byte) error

	// SetUint64 用于存储任期
	SetUint64(key []byte, val uint64) error
	// GetUint64 用于返回任期
	GetUint64(key []byte) (uint64, error)
}

type ConfigurationStorage interface {
	KVStorage
	SetConfiguration(index uint64, configuration Configuration) error
}
