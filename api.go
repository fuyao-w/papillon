package papillon

import (
	"errors"
	"io"
	"time"
)

var (
	ErrNotExist                        = errors.New("not exist")
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
	ErrNotFound                        = customError{"not found"}
	ErrNotLeader                       = errors.New("not leader")
	ErrCantBootstrap                   = errors.New("bootstrap only works on new clusters")
	ErrIllegalConfiguration            = errors.New("illegal clusterInfo")
	ErrShutDown                        = errors.New("shut down")
	ErrNotStarted                      = errors.New("not started")
	ErrLeadershipTransferInProgress    = errors.New("leader ship transfer in progress")
	// ErrAbortedByRestore is returned when a leader fails to commit a log
	// entry because it's been superseded by a user snapshot restore.
	ErrAbortedByRestore       = errors.New("snapshot restored while committing log")
	ErrEnqueueTimeout         = errors.New("timed out enqueuing operation")
	ErrTimeout                = errors.New("time out")
	ErrPipelineShutdown       = errors.New("append pipeline closed")
	ErrNotVoter               = errors.New("not voter")
	ErrLeadershipTransferFail = errors.New("not found transfer peer")
	ErrLeadershipLost         = errors.New("leadership lost")
	ErrNothingNewToSnapshot   = errors.New("nothing new to snapshot")
	ErrEmptyCommit            = errors.New("empty commit")
)

func genTimeoutCh(timeout time.Duration) (tm <-chan time.Time) {
	if timeout > 0 {
		tm = time.After(timeout)
	}
	return tm
}

type customError struct{ string }

func (e customError) Error() string {
	return e.string
}

func (e customError) Is(err error) bool {
	if err == nil {
		return false
	}
	return e.Error() == err.Error()
}

func (r *Raft) BootstrapCluster(configuration ClusterInfo) defaultFuture {
	future := &bootstrapFuture{
		clusterInfo: configuration,
	}
	future.init()
	if r == nil || r.GetState().String() == unknown {
		future.fail(ErrNotStarted)
		return future
	}
	select {
	case <-r.shutDown.C:
		future.fail(ErrShutDown)
	case r.commandCh <- &command{enum: commandBootstrap, callback: future}:
	}
	return future
}

func (r *Raft) LeaderInfo() (ServerID, ServerAddr) {
	info := r.leaderInfo.Get()
	return info.ID, info.Addr
}

// Apply 向 raft 提交日志
func (r *Raft) Apply(data []byte, timeout time.Duration) ApplyFuture {
	return r.apiApplyLog(&LogEntry{Data: data, Type: LogCommand}, timeout)
}
func (r *Raft) apiApplyLog(entry *LogEntry, timeout time.Duration) ApplyFuture {
	var (
		tm <-chan time.Time
	)
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var applyFuture = &LogFuture{
		log: entry,
	}
	applyFuture.init()
	if r == nil || r.GetState().String() == unknown {
		applyFuture.fail(ErrNotStarted)
		return applyFuture
	}
	select {
	case <-tm:
		return &errFuture[nilRespFuture]{errors.New("apply log time out")}
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	case r.apiLogApplyCh <- applyFuture: //batch apply
	case r.commandCh <- &command{enum: commandLogApply, callback: applyFuture}: // 正常提交
	}
	return applyFuture
}

// VerifyLeader 验证当前节点是否是领导人
func (r *Raft) VerifyLeader() Future[bool] {
	vf := &verifyFuture{}
	vf.init()
	if r == nil || r.GetState().String() == unknown {
		vf.fail(ErrNotStarted)
		return vf
	}
	select {
	case <-r.shutDown.C:
		vf.fail(ErrShutDown)
		return &vf.deferResponse
	case r.commandCh <- &command{enum: commandVerifyLeader, callback: vf}:
		return &vf.deferResponse
	}
}

// GetConfiguration 获取集群配置
func (r *Raft) GetConfiguration() ClusterInfo {
	return r.configuration.Load()
}

func (r *Raft) requestClusterChange(req clusterChangeRequest, timeout time.Duration) IndexFuture {
	var tm <-chan time.Time
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var ccf = &clusterChangeFuture{
		req: &req,
	}
	ccf.init()
	if r == nil || r.GetState().String() == unknown {
		ccf.fail(ErrNotStarted)
		return ccf
	}
	select {
	case <-tm:
		return &errFuture[nilRespFuture]{err: errors.New("apply log time out")}
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{err: ErrShutDown}
	case r.commandCh <- &command{enum: commandClusterChange, callback: ccf}:
		return ccf
	}
}

func (r *Raft) AddServer(peer ServerInfo, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestClusterChange(clusterChangeRequest{
		command:   addServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}
func (r *Raft) RemoveServer(peer ServerInfo, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestClusterChange(clusterChangeRequest{
		command:   removeServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) UpdateServer(peer ServerInfo, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestClusterChange(clusterChangeRequest{
		command:   updateServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) SnapShot() Future[OpenSnapshot] {
	fu := &apiSnapshotFuture{}
	fu.init()
	if r == nil || r.GetState().String() == unknown {
		fu.fail(ErrNotStarted)
		return fu
	}
	select {
	case <-r.shutDown.C:
		return &errFuture[OpenSnapshot]{ErrShutDown}
	case r.apiSnapshotBuildCh <- fu:
		return fu
	}
}

// StateCh 状态变化的通知
func (r *Raft) StateCh() <-chan *StateChange {
	return r.stateChangeCh
}

func (r *Raft) LastContact() time.Time {
	return r.lastContact.Load()
}

func (r *Raft) LatestIndex() uint64 {
	return r.getLatestIndex()
}

func (r *Raft) LastApplied() uint64 {
	return r.getLastApply()
}

func (r *Raft) LeaderTransfer(id ServerID, address ServerAddr, timeout time.Duration) defaultFuture {
	tm := genTimeoutCh(timeout)
	future := &leadershipTransferFuture{
		Peer: ServerInfo{
			ID:   id,
			Addr: address,
		},
	}
	future.init()
	if r == nil || r.GetState().String() == unknown {
		future.fail(ErrNotStarted)
		return future
	}
	if len(id) > 0 && id == r.localInfo.ID {
		future.fail(errors.New("can't transfer to itself"))
		return future
	}
	select {
	case r.commandCh <- &command{enum: commandLeadershipTransfer, callback: future}:
		return future
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	case <-tm:
		return &errFuture[nilRespFuture]{ErrEnqueueTimeout}
	}
}

func (r *Raft) ReloadConfig(rc ReloadableConfig) error {
	r.confReloadMu.Lock()
	defer r.confReloadMu.Unlock()
	if r == nil || r.GetState().String() == unknown {
		return ErrNotStarted
	}
	oldConf := *r.Conf()
	newConf := rc.apply(oldConf)
	ok, hint := ValidateConfig(&newConf)
	if !ok {
		return errors.New(hint)
	}
	r.conf.Store(&newConf)
	if newConf.HeartbeatTimeout <= oldConf.HeartbeatTimeout {
		return nil
	}
	select {
	case <-r.shutDown.C:
		return ErrShutDown
	case r.commandCh <- &command{enum: commandConfigReload, callback: &newConf}:
	}
	return nil
}

func (r *Raft) ReStoreSnapshot(meta *SnapshotMeta, reader io.ReadCloser) error {
	fu := &userRestoreFuture{
		meta:   meta,
		reader: reader,
	}
	fu.init()
	if r == nil || r.GetState().String() == unknown {
		return ErrNotStarted
	}
	select {
	case r.commandCh <- &command{enum: commandSnapshotRestore, callback: fu}:
	case <-r.shutDown.C:
		return ErrShutDown
	}
	_, err := fu.Response()
	if err != nil {
		return err
	}
	applyFu := r.apiApplyLog(&LogEntry{Type: LogNoop}, 0)
	_, err = applyFu.Response()
	return err
}

func (r *Raft) ShutDown() defaultFuture {
	var resp *shutDownFuture
	if r == nil || r.GetState().String() == unknown {
		fu := new(defaultDeferResponse)
		fu.init()
		fu.fail(ErrNotStarted)
		return fu
	}
	r.shutDown.done(func(oldState bool) {
		resp = new(shutDownFuture)
		if !oldState {
			resp.raft = r
		}
		r.setShutDown()
	})
	return resp
}

func (r *Raft) RaftStats() Future[map[string]interface{}] {
	fu := new(deferResponse[map[string]interface{}])
	fu.init()
	if r == nil || r.GetState().String() == unknown {
		fu.fail(ErrNotStarted)
		return fu
	}
	r.commandCh <- &command{
		enum:     commandRaftStats,
		callback: fu,
	}
	return fu
}

func (r *Raft) ReadIndex(timeout time.Duration) Future[uint64] {
	tm := genTimeoutCh(timeout)
	fu := new(deferResponse[uint64])
	fu.init()
	if r == nil || r.GetState().String() == unknown {
		fu.fail(ErrNotStarted)
		return fu
	}
	select {
	case <-tm:
		fu.fail(ErrTimeout)
	case r.commandCh <- &command{
		enum:     commandReadIndex,
		callback: fu,
	}:
	}
	return fu
}

func (r *Raft) Barrier(readIndex uint64, timeout time.Duration) Future[uint64] {
	tm := genTimeoutCh(timeout)
	fu := &readOnlyFuture{
		readIndex: readIndex,
	}
	fu.init()
	if r == nil || r.GetState().String() == unknown {
		fu.fail(ErrNotStarted)
		return fu
	}
	select {
	case <-tm:
		fu.fail(ErrTimeout)
	case r.readOnly.request <- fu:
	}
	return fu
}
