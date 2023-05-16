package papillon

import (
	"fmt"
	"io"
	"time"
)

type (
	// SnapshotStore 快照存储的抽象，提供打开快照，创建快照，查询快照列表的能力
	SnapshotStore interface {
		Open(id string) (*SnapshotMeta, io.ReadCloser, error)
		List() ([]*SnapshotMeta, error)
		Create(version SnapshotVersion, index, term uint64, configuration ClusterInfo, configurationIndex uint64, rpc RpcInterface) (SnapshotSink, error)
	}
	// SnapshotSink 快照的抽象提供写入、取消写入、返回快照 ID 的能力
	SnapshotSink interface {
		io.WriteCloser
		ID() string
		Cancel() error
	}
	// SnapshotVersion 表示快照的版本，会在以后的快照结构变更的时候使用
	SnapshotVersion uint64
	// SnapshotMeta 快照元信息
	SnapshotMeta struct {
		Version            SnapshotVersion
		ID                 string
		Index              uint64
		Term               uint64
		Configuration      ClusterInfo
		ConfigurationIndex uint64
		Size               int64
	}
)

const (
	SnapShotVersionDefault SnapshotVersion = iota + 1
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
			fn := func() (*SnapshotMeta, io.ReadCloser, error) {
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
			if id, err := r.buildSnapshot(); err != nil {
				r.logger.Info(r.localInfo.ID, "build snap fail: ", err.Error())
			} else {
				r.logger.Info(r.localInfo.ID, "build snap success ,id :", id)
			}
		}
	}
}
