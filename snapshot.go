package papillon

import (
	"fmt"
	"io"
	"time"
)

type (
	SnapshotStore interface {
		Open(id string) (*SnapShotMeta, io.ReadCloser, error)
		List() ([]*SnapShotMeta, error)
		Create(version SnapShotVersion, index, term uint64, configuration Configuration, configurationIndex uint64, rpc RpcInterface) (SnapshotSink, error)
	}
	SnapshotSink interface {
		io.WriteCloser
		ID() string
		Cancel() error
	}
	// SnapShotVersion 表示快照的版本，会在以后的快照结构变更的时候使用
	SnapShotVersion uint64
	SnapShotMeta    struct {
		Version            SnapShotVersion
		ID                 string
		Index              uint64
		Term               uint64
		Configuration      Configuration
		ConfigurationIndex uint64
		Size               int64
	}
)

const (
	SnapShotVersionDefault SnapShotVersion = iota + 1
)

func snapshotName(term, index uint64) string {
	return fmt.Sprintf("%d-%d-%d", term, index, time.Now().UnixMilli())
}

// runSnapshot 快照线程
func (r *Raft) runSnapshot() {
	for {
		select {
		case <-r.shutDown.C:
			return
		case fu := <-r.apiSnapshotBuildCh:
			id, err := r.buildSnapshot()
			fn := func() (*SnapShotMeta, io.ReadCloser, error) {
				return r.snapshotStore.Open(id)
			}
			if err != nil {
				fn = nil
			}
			fu.responded(fn, err)
		case <-time.After(r.conf.Load().SnapshotInterval):
			if !r.shouldBuildSnapshot() {
				continue
			}
			r.buildSnapshot()
		}
	}
}
