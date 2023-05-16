package papillon

import (
	. "github.com/fuyao-w/common-util"
)

// CacheLog 带缓存的 LogStore 用于减少磁盘 IO，只在执行 SetLogs, DeleteRange 时更新 cache 以保证局部性
type CacheLog struct {
	store    LogStore
	buffer   *LockItem[[]*LogEntry]
	capacity uint64
}

// NewCacheLog capacity 必须大于 0
func NewCacheLog(store LogStore, capacity uint64) LogStore {
	if capacity == 0 {
		panic("capacity must bigger than 0")
	}
	return &CacheLog{
		store:    store,
		buffer:   NewLockItem(make([]*LogEntry, capacity)),
		capacity: capacity,
	}
}
func (c *CacheLog) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

func (c *CacheLog) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

func (c *CacheLog) GetLog(index uint64) (log *LogEntry, err error) {
	c.buffer.Action(func(t *[]*LogEntry) {
		log = (*t)[index%c.capacity]
	})
	if log != nil && log.Index == index {
		return
	}
	return c.store.GetLog(index)
}

func (c *CacheLog) GetLogRange(from, to uint64) (logs []*LogEntry, err error) {
	buf := *c.buffer.Lock()
	for i := from; i <= to; i++ {
		if log := buf[i%c.capacity]; log != nil && log.Index == i {
			logs = append(logs, log)
		} else {
			goto LOAD
		}
	}
	c.buffer.Unlock()
	return
LOAD:
	c.buffer.Unlock()
	return c.store.GetLogRange(from, to)
}

func (c *CacheLog) SetLogs(logs []*LogEntry) error {
	if err := c.store.SetLogs(logs); err != nil {
		return err
	}
	c.buffer.Action(func(buf *[]*LogEntry) {
		for _, log := range logs {
			(*buf)[log.Index%c.capacity] = log
		}
	})
	return nil
}

func (c *CacheLog) DeleteRange(from, to uint64) error {
	if err := c.store.DeleteRange(from, to); err != nil {
		return err
	}
	c.buffer.Action(func(buf *[]*LogEntry) {
		for i := from; i <= to; i++ {
			idx := i % c.capacity
			if log := (*buf)[idx]; log != nil && log.Index == i {
				(*buf)[idx] = nil
			}
		}
	})

	return nil
}
