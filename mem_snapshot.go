package papillon

import (
	"bytes"
	"io"
	"sync"
)

type memSnapshot struct {
	latest *memSnapshotSink
	has    bool
	sync.Mutex
}

func newMemSnapShot() *memSnapshot {
	return &memSnapshot{}
}

type memSnapshotSink struct {
	meta *SnapShotMeta
	buf  *bytes.Buffer
}

func (m *memSnapshotSink) Write(p []byte) (n int, err error) {
	size, err := m.buf.Write(p)
	m.meta.Size += int64(size)
	return size, err
}

func (m *memSnapshotSink) Close() error {
	return nil
}

func (m *memSnapshotSink) ID() string {
	return m.meta.ID
}

func (m *memSnapshotSink) Cancel() error {
	return nil
}

func (m *memSnapshot) Open(id string) (*SnapShotMeta, io.ReadCloser, error) {
	m.Lock()
	defer m.Unlock()
	if !m.has {
		return nil, nil, ErrNotExist
	}
	if m.latest.meta.ID != id {
		return nil, nil, ErrNotExist
	}
	buffer := bytes.NewBuffer(m.latest.buf.Bytes())
	return m.latest.meta, io.NopCloser(buffer), nil
}

func (m *memSnapshot) List() ([]*SnapShotMeta, error) {
	m.Lock()
	defer m.Unlock()
	if !m.has {
		return nil, nil
	}
	return []*SnapShotMeta{m.latest.meta}, nil
}

func (m *memSnapshot) Create(version SnapShotVersion, index, term uint64, configuration Configuration, configurationIndex uint64, rpc RpcInterface) (SnapshotSink, error) {
	m.Lock()
	defer m.Unlock()
	sink := memSnapshotSink{
		meta: &SnapShotMeta{
			Version:            version,
			ID:                 snapshotName(term, index),
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
			Size:               0,
		},
		buf: &bytes.Buffer{},
	}
	m.has = true
	m.latest = &sink
	return &sink, nil
}
