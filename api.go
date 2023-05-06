package papillon

import (
	"errors"
	"io"
	"time"
)

var (
	ErrNotExist                        = errors.New("not exist")
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
	ErrNotFoundLog                     = customError{"not found log"}
	ErrNotLeader                       = errors.New("not leader")
	ErrCantBootstrap                   = errors.New("bootstrap only works on new clusters")
	ErrIllegalConfiguration            = errors.New("illegal configuration")
	ErrShutDown                        = errors.New("shut down")
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
)

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

func (r *Raft) BootstrapCluster(configuration Configuration) defaultFuture {
	future := &bootstrapFuture{
		configuration: configuration,
	}
	future.init()
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
	var tm <-chan time.Time
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var applyFuture = &LogFuture{
		log: entry,
	}
	applyFuture.init()
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
	select {
	case <-r.shutDown.C:
		vf.fail(ErrShutDown)
		return &vf.deferResponse
	case r.commandCh <- &command{enum: commandVerifyLeader, callback: vf}:
		return &vf.deferResponse
	}
}

// GetConfiguration 获取集群配置
func (r *Raft) GetConfiguration() Configuration {
	return r.configuration.Load()
}

func (r *Raft) requestClusterChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var tm <-chan time.Time
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var ccf = &configurationChangeFuture{
		req: &req,
	}
	ccf.init()
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
	return r.requestClusterChange(configurationChangeRequest{
		command:   addServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}
func (r *Raft) RemoveServer(peer ServerInfo, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestClusterChange(configurationChangeRequest{
		command:   removeServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) UpdateServer(peer ServerInfo, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestClusterChange(configurationChangeRequest{
		command:   updateServer,
		peer:      peer,
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) SnapShot() Future[OpenSnapShot] {
	fu := &apiSnapshotFuture{}
	fu.init()
	select {
	case <-r.shutDown.C:
		return &errFuture[OpenSnapShot]{ErrShutDown}
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
	return r.getLastApplied()
}

func (r *Raft) LeaderTransfer(id ServerID, address ServerAddr) defaultFuture {
	future := &leadershipTransferFuture{
		Peer: ServerInfo{
			ID:   id,
			Addr: address,
		},
	}
	if len(id) > 0 && id == r.localInfo.ID {
		future.fail(errors.New("can't transfer to itself"))
		return future
	}
	future.init()
	select {
	case r.commandCh <- &command{enum: commandLeadershipTransfer, callback: future}:
		return future
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	default:
		return &errFuture[nilRespFuture]{ErrEnqueueTimeout}
	}
}

func (r *Raft) ReloadConfig(rc ReloadableConfig) error {
	r.confReloadMu.Lock()
	defer r.confReloadMu.Unlock()
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

func (r *Raft) ReStoreSnapshot(meta *SnapShotMeta, reader io.ReadCloser) error {
	fu := &userRestoreFuture{
		meta:   meta,
		reader: reader,
	}
	fu.init()
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
	r.shutDown.done(func(oldState bool) {
		resp = new(shutDownFuture)
		if !oldState {
			resp.raft = r
		}
		r.setShutDown()
	})
	return resp
}
