package papillon

import "io"

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
	batchFSM, canBatchApply := r.fsm.(BatchFSM)
	configurationStore, canConfigurationStore := r.kvStore.(ConfigurationStorage)
	var (
		lastAppliedIdx, lastAppliedTerm uint64 //用于创建快照
	)
	canApply := func(future *LogFuture) bool {
		switch future.log.Type {
		case LogCommand:
			return true
		}
		return false
	}
	processConfiguration := func(fu *LogFuture) {
		if fu.log.Type != LogCluster || !canConfigurationStore {
			return
		}
		configurationStore.SetConfiguration(fu.log.Index, DecodeCluster(fu.log.Data))

	}
	applyBatch := func(futures []*LogFuture) {
		var (
			logs        []*LogEntry
			respFutures []*LogFuture
		)

		for _, fu := range futures {
			processConfiguration(fu)
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
	applySingle := func(fu *LogFuture) error {
		processConfiguration(fu)
		if !canApply(fu) {
			fu.responded(nil, nil)
			return nil
		}
		fu.responded(r.fsm.Apply(fu.log), nil)
		return nil
	}
	snapshot := func(fu *fsmSnapshotFuture) {
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
	for {
		select {
		case <-r.shutDown.C:
			return
		case futures := <-r.fsmApplyCh:
			if canBatchApply {
				applyBatch(futures)
				if len(futures) > 0 {
					future := futures[len(futures)-1]
					lastAppliedIdx, lastAppliedTerm = future.log.Index, future.log.Term
				}
			} else {
				for _, future := range futures {
					applySingle(future)
					lastAppliedIdx, lastAppliedTerm = future.log.Index, future.log.Term
				}
			}
		case fu := <-r.fsmRestoreCh:
			meta, err := r.recoverSnapshotByID(fu.ID)
			if err == nil {
				lastAppliedIdx, lastAppliedTerm = meta.Index, meta.Term
			}
			fu.responded(nil, err)
		case fu := <-r.fsmSnapshotCh:
			snapshot(fu)
		}
	}
}
