package papillon

import (
	"io"
)

type FSM interface {
	Apply(*LogEntry) interface{}
	ReStore(reader io.ReadCloser) error // 从快照恢复，需要自行实现觅等
	Snapshot() (FsmSnapshot, error)
}

type FsmSnapshot interface {
	Persist(sink SnapshotSink) error
	Release()
}

type BatchFSM interface {
	FSM
	BatchApply([]*LogEntry) []interface{}
}

// runFSM 状态机线程
func (r *Raft) runFSM() {
	var (
		batchFSM, canBatchApply                   = r.fsm.(BatchFSM)
		configurationStore, canConfigurationStore = r.kvStore.(ConfigurationStorage)
		lastAppliedTerm, lastAppliedIdx           uint64
	)

	processConfiguration := func(fu *LogFuture) {
		if fu.log.Type != LogCluster || !canConfigurationStore {
			return
		}
		configurationStore.SetConfiguration(fu.log.Index, DecodeCluster(fu.log.Data))
	}
	setFsmLastApplied := func(log *LogEntry) {
		lastAppliedIdx = log.Index
		lastAppliedTerm = log.Term
	}
	for {
		select {
		case <-r.shutDown.C:
			return
		case futures := <-r.fsmApplyCh:
			switch {
			case canBatchApply:
				applyBatch(futures, batchFSM, processConfiguration)
				if len(futures) > 0 {
					setFsmLastApplied(futures[len(futures)-1].log)
				}
			default:
				for _, future := range futures {
					applySingle(future, r.fsm, processConfiguration)
					setFsmLastApplied(future.log)
				}
			}
			r.readOnly.notify(lastAppliedIdx)
		case fu := <-r.fsmRestoreCh:
			meta, err := r.recoverSnapshotByID(fu.ID)
			fu.responded(nil, err)
			if err == nil {
				setFsmLastApplied(&LogEntry{Term: meta.Term, Index: meta.Index})
			}
			r.readOnly.notify(lastAppliedIdx)
		case fu := <-r.fsmSnapshotCh:
			r.processSnapshot(fu, lastAppliedTerm, lastAppliedIdx)
		case fu := <-r.readOnly.request:
			if fu.readIndex <= lastAppliedIdx {
				fu.responded(lastAppliedIdx, nil)
				continue
			}
			r.readOnly.observe(fu)
		}
	}
}

func canApply(future *LogFuture) bool {
	switch future.log.Type {
	case LogCommand:
		return true
	}
	return false
}

func (r *Raft) processSnapshot(fu *fsmSnapshotFuture, lastAppliedTerm, lastAppliedIdx uint64) {
	if lastAppliedIdx == 0 {
		fu.fail(ErrNothingNewToSnapshot)
		return
	}
	snapshot, err := r.fsm.Snapshot()
	if err != nil {
		r.logger.Errorf("fsm generate snap shot err :%s", err)
	}
	fu.responded(&SnapShotFutureResp{
		term:        lastAppliedTerm,
		index:       lastAppliedIdx,
		fsmSnapshot: snapshot,
	}, err)
}

func applySingle(fu *LogFuture, fsm FSM, callback func(fu *LogFuture)) error {
	callback(fu)
	if !canApply(fu) {
		fu.responded(nil, nil)
		return nil
	}
	fu.responded(fsm.Apply(fu.log), nil)
	return nil
}
func applyBatch(futures []*LogFuture, batchFSM BatchFSM, callback func(fu *LogFuture)) {
	var (
		logs        []*LogEntry
		respFutures []*LogFuture
	)

	for _, fu := range futures {
		callback(fu)
		if canApply(fu) {
			logs = append(logs, fu.log)
			respFutures = append(respFutures, fu)
		} else {
			fu.success()
		}
	}
	if len(logs) == 0 {
		return
	}
	resp := batchFSM.BatchApply(logs)
	for i, fu := range respFutures {
		fu.responded(resp[i], nil)
	}
}
