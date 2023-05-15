package papillon

import (
	"encoding/json"
	. "github.com/fuyao-w/common-util"
	"hash/adler32"
	"io"
)

type logHash struct {
	lastHash []byte
}

type kvSchema [2]string

func (s kvSchema) encode(k, v string) []byte {
	s[0], s[1] = k, v
	b, _ := json.Marshal(s)
	return b
}
func (s kvSchema) decode(data []byte) (k, v string) {
	_ = json.Unmarshal(data, &s)
	return s[0], s[1]
}

func (l *logHash) Add(p []byte) {
	hasher := adler32.New()
	hasher.Write(l.lastHash)
	hasher.Write(p)
	l.lastHash = hasher.Sum(nil)
}

type memFSM struct {
	logHash
	lastIndex, lastTerm uint64
	kv                  *LockItem[map[string]string] // 简单提供 kv 功能
	configurations      []ClusterInfo
}

func (m *memFSM) StoreConfiguration(index uint64, configuration ClusterInfo) {
	m.configurations = append(m.configurations, configuration)
}

func newMemFSM() *memFSM {
	return &memFSM{
		kv: NewLockItem(map[string]string{}),
	}
}

func (m *memFSM) getVal(key string) (val string) {
	kv := m.kv.Lock()
	defer m.kv.Unlock()
	return (*kv)[key]
}

type memSnapshotContainer struct {
	LastIndex uint64 `json:"last_index"`
	LastTerm  uint64 `json:"last_term"`
	Content   []byte `json:"content"`
}

func (m *memFSM) Persist(sink SnapshotSink) error {
	fsm := m.kv.Lock()
	defer m.kv.Unlock()
	b, _ := json.Marshal(fsm)
	c, _ := json.Marshal(memSnapshotContainer{
		LastIndex: m.lastIndex,
		LastTerm:  m.lastTerm,
		Content:   b,
	})
	_, err := sink.Write(c)
	return err
}

func (m *memFSM) Release() {
}

type applyItem struct {
	index uint64
	term  uint64
	data  []byte
}

func (m *memFSM) Apply(entry *LogEntry) interface{} {
	if entry.Index < m.lastIndex {
		panic("index error")
	}
	if entry.Term < m.lastTerm {
		panic("term error")
	}
	m.lastTerm = entry.Term
	m.lastIndex = entry.Index
	m.Add(entry.Data)
	if k, v := kvSchema.decode(kvSchema{}, entry.Data); len(k) > 0 {
		m.kv.Action(func(t *map[string]string) {
			(*t)[k] = v
		})
	}
	return nil
}

func (m *memFSM) Snapshot() (FsmSnapshot, error) {
	return &*m, nil
}

func (m *memFSM) ReStore(rc io.ReadCloser) error {
	defer rc.Close()
	var c memSnapshotContainer
	buf, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buf, &c)
	if err != nil {
		return err
	}
	m.lastTerm = c.LastTerm
	m.lastIndex = c.LastIndex
	newKv := map[string]string{}
	err = json.Unmarshal(c.Content, &newKv)
	if err != nil {
		return err
	}
	m.kv.Action(func(t *map[string]string) {
		*t = newKv
	})
	return nil
}
