package papillon

import (
	. "github.com/fuyao-w/common-util"
	"github.com/fuyao-w/deepcopy"
	"time"
)

type memLog struct {
	firstIndex, lastIndex uint64
	log                   map[uint64]*LogEntry
}
type MemoryStore struct {
	kv  *LockItem[map[string]interface{}] // 实现 KVStorage
	log *LockItem[memLog]                 // 实现 LogStore
}

func newMemoryStore() *MemoryStore {
	return &MemoryStore{
		kv: NewLockItem(map[string]interface{}{}),
		log: NewLockItem(memLog{
			log: map[uint64]*LogEntry{},
		}),
	}
}

func (m *MemoryStore) GetLogRange(from, to uint64) (logs []*LogEntry, err error) {
	m.log.Action(func(t *memLog) {
		for i := from; i <= to; i++ {
			log := t.log[i]
			if log == nil {
				continue
			}
			logs = append(logs, log)
		}
	})
	return
}

func (m *MemoryStore) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return nil, ErrKeyIsNil
	}
	kv := m.kv.Lock()
	defer m.kv.Unlock()

	v, ok := (*kv)[string(key)]
	if ok {
		return v.([]byte), nil
	}
	return nil, ErrKeyNotFound
}

func (m *MemoryStore) Set(key []byte, val []byte) (err error) {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	if len(val) == 0 {
		return ErrValueIsNil
	}
	m.kv.Action(func(t *map[string]interface{}) {
		(*t)[string(key)] = val
	})
	return
}

func (m *MemoryStore) SetUint64(key []byte, val uint64) (err error) {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	m.kv.Action(func(t *map[string]interface{}) {
		(*t)[string(key)] = val
	})
	return
}

func (m *MemoryStore) GetUint64(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, ErrKeyIsNil
	}
	kv := m.kv.Lock()
	defer m.kv.Unlock()

	v, ok := (*kv)[string(key)]
	if ok {
		return v.(uint64), nil
	}
	return 0, ErrKeyNotFound
}
func (m *MemoryStore) FirstIndex() (uint64, error) {
	var idx uint64
	m.log.Action(func(t *memLog) {
		idx = (*t).firstIndex
	})
	return idx, nil
}

func (m *MemoryStore) LastIndex() (uint64, error) {
	var idx uint64
	m.log.Action(func(t *memLog) {
		idx = (*t).lastIndex
	})
	return idx, nil
}

func (m *MemoryStore) GetLog(index uint64) (log *LogEntry, err error) {
	m.log.Action(func(t *memLog) {
		s := *t
		l, ok := s.log[index]
		if ok {
			log = deepcopy.Copy(l).(*LogEntry)
		} else {
			err = ErrKeyNotFound
		}
	})
	return
}

func (m *MemoryStore) SetLogs(logs []*LogEntry) (err error) {
	m.log.Action(func(t *memLog) {
		s := *t
		var exists []uint64
		for _, entry := range logs {
			if _, ok := s.log[entry.Index]; ok {
				exists = append(exists, entry.Index)
			}
		}
		for _, entry := range logs {
			s.log[entry.Index] = deepcopy.Copy(entry).(*LogEntry)
			s.log[entry.Index].CreatedAt = time.Now()
			if t.firstIndex == 0 {
				t.firstIndex = entry.Index
			}
			if entry.Index > t.lastIndex {
				t.lastIndex = entry.Index
			}
		}
	})
	return nil
}

func (m *MemoryStore) DeleteRange(min, max uint64) error {
	if min > max {
		return ErrRange
	}
	m.log.Action(func(t *memLog) {
		s := *t
		for i := min; i <= max; i++ {
			delete(s.log, i)
		}
		if min <= s.firstIndex {
			s.firstIndex = max + 1
		}
		if max >= s.lastIndex {
			s.lastIndex = min - 1
		}
		if s.firstIndex > s.lastIndex {
			s.firstIndex = 0
			s.lastIndex = 0
		}
	})
	return nil
}
