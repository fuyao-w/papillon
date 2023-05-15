package papillon

import (
	"container/list"
	"errors"
	"fmt"
	. "github.com/fuyao-w/common-util"
	"github.com/fuyao-w/log"
	"golang.org/x/sync/errgroup"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

func NewRaft(conf *Config,
	fsm FSM,
	rpc RpcInterface,
	logStore LogStore,
	kvStore KVStorage,
	snapshotStore SnapshotStore) (*Raft, error) {
	var (
		_lastLog lastLog
	)
	ok, hint := ValidateConfig(conf)
	if !ok {
		return nil, fmt.Errorf("config validate err :%s", hint)
	}
	if conf.Logger == nil {
		conf.Logger = log.NewLogger()
	}

	lastIndex, err := logStore.LastIndex()
	if err != nil {
		if !errors.Is(ErrNotFoundLog, err) {
			conf.Logger.Errorf("init index error")
			return nil, fmt.Errorf("recover last log err :%s", err)
		}
	}
	if lastIndex > 0 {
		log, err := logStore.GetLog(lastIndex)
		if err != nil {
			conf.Logger.Errorf("get lastLog error")
			return nil, fmt.Errorf("recover last log err :%s", err)
		}
		_lastLog = lastLog{
			index: log.Index,
			term:  log.Term,
		}
	}

	currentTerm, err := kvStore.GetUint64(KeyCurrentTerm)
	if err != nil && !errors.Is(ErrKeyNotFound, err) {
		conf.Logger.Errorf("init current term error :%s", err)
		return nil, fmt.Errorf("recover current term err :%s", err)
	}

	raft := &Raft{
		commitIndex:          0,
		lastApplied:          0,
		currentTerm:          currentTerm,
		conf:                 NewAtomicVal(conf),
		configuration:        NewAtomicVal[ClusterInfo](),
		state:                0,
		run:                  nil,
		lastContact:          NewAtomicVal[time.Time](),
		localInfo:            ServerInfo{Addr: rpc.LocalAddr(), ID: ServerID(conf.LocalID)},
		lastEntry:            NewLockItem(lastEntry{log: _lastLog}),
		cluster:              cluster{},
		leaderInfo:           NewLockItem[ServerInfo](),
		funcEg:               new(errgroup.Group),
		shutDown:             newShutDown(),
		rpcCh:                rpc.Consumer(),
		apiLogApplyCh:        make(chan *LogFuture),
		commitCh:             make(chan struct{}, 1),
		logger:               conf.Logger,
		leaderState:          leaderState{},
		stateChangeCh:        make(chan *StateChange, 1),
		commandCh:            make(chan *command),
		fsm:                  fsm,
		fsmApplyCh:           make(chan []*LogFuture, 128),
		fsmSnapshotCh:        make(chan *fsmSnapshotFuture),
		fsmRestoreCh:         make(chan *restoreFuture, 64),
		readOnly:             readOnly{notifySet: map[*readOnlyFuture]struct{}{}, request: make(chan *readOnlyFuture)},
		apiSnapshotBuildCh:   make(chan *apiSnapshotFuture),
		apiSnapshotRestoreCh: make(chan *userRestoreFuture),
		rpc:                  rpc,
		kvStore:              kvStore,
		logStore:             logStore,
		snapshotStore:        snapshotStore,
	}

	if err = raft.recoverSnapshot(); err != nil {
		return nil, fmt.Errorf("recover snap shot|%s", err)
	}
	if err = raft.recoverCluster(); err != nil {
		return nil, fmt.Errorf("recover cluster|%s", err)
	}
	raft.rpc.SetHeartbeatFastPath(raft.heartBeatFastPath)
	raft.setFollower()
	raft.goFunc(raft.runState, raft.runFSM, raft.runSnapshot)
	return raft, nil
}

// recoverCluster 从日志恢复集群配置
func (r *Raft) recoverCluster() (err error) {
	entry := r.lastEntry.Get()
	for i := entry.snapshot.index + 1; i < entry.log.index; i++ {
		_entry, err := r.logStore.GetLog(i)
		if err != nil {
			r.logger.Errorf("recoverCluster|GetLog err :%s ,index :%d", err, i)
			return err
		}
		r.saveConfiguration(_entry)
	}
	return nil
}

func (r *Raft) recoverSnapshotByID(id string) (*SnapShotMeta, error) {
	meta, readCloser, err := r.snapshotStore.Open(id)
	if err != nil {
		r.logger.Errorf("")
		return nil, fmt.Errorf("open id %s err:%s", id, err)
	}
	defer readCloser.Close()
	if err = r.fsm.ReStore(readCloser); err != nil {
		r.logger.Errorf("")
		return nil, fmt.Errorf("restore snapshot err:%s", err)
	}
	return meta, nil
}

// recoverSnapshot 将本地最新的快照恢复至状态机，并更新本地的索引、任期状态。只在初始化使用
func (r *Raft) recoverSnapshot() (err error) {
	metaList, err := r.snapshotStore.List()
	if err != nil {
		r.Conf().Logger.Errorf("list snapshot error :%s", err)
		return fmt.Errorf("list snapshot err :%s", err)
	}
	if len(metaList) == 0 {
		return nil
	}
	meta, err := r.recoverSnapshotByID(metaList[0].ID)
	if err != nil {
		return err
	}
	r.setLatestSnapshot(meta.Term, meta.Index)
	r.setLastApplied(meta.Index)
	// 生成快照默认用稳定的配置，这里已提交和最新全部用一个就行
	r.setCommitConfiguration(meta.ConfigurationIndex, meta.Configuration)
	r.setLatestConfiguration(meta.ConfigurationIndex, meta.Configuration)
	return nil
}

func shouldApply(log *LogEntry) bool {
	switch log.Type {
	case LogCluster, LogCommand, LogBarrier:
		return true
	}
	return false
}

// applyLogToFsm 将 lastApplied 到 index 的日志应用到状态机
func (r *Raft) applyLogToFsm(toIndex uint64, ready map[uint64]*LogFuture) {
	if toIndex <= r.getLastApplied() {
		return
	}
	var (
		fromIndex  = r.getLastApplied() + 1
		maxAppend  = r.Conf().MaxAppendEntries
		logFutures = make([]*LogFuture, 0, maxAppend)
	)
	applyBatch := func(futures []*LogFuture) {
		if len(futures) == 0 {
			return
		}
		select {
		case r.fsmApplyCh <- futures:
		case <-r.shutDown.C:
			for _, future := range futures {
				future.fail(ErrShutDown)
			}
		}
	}

	for i := fromIndex; i <= toIndex; i++ {
		var fu *LogFuture
		if fu = ready[i]; fu == nil {
			log, err := r.logStore.GetLog(i)
			if err != nil {
				panic(fmt.Errorf("apply Log to fsm,err :%s", err))
				return
			}
			fu = &LogFuture{log: log}
			fu.init()
		}
		if !shouldApply(fu.log) {
			fu.success()
			continue
		}
		if logFutures = append(logFutures, fu); len(logFutures) >= int(maxAppend) {
			applyBatch(logFutures)
			logFutures = make([]*LogFuture, 0, maxAppend)
		}
	}
	applyBatch(logFutures)

	r.setLastApplied(toIndex)
}

func (r *Raft) shouldBuildSnapshot() bool {
	_, snapshotIndex := r.getLatestSnapshot()
	_, logIndex := r.getLatestLog()
	return logIndex-snapshotIndex > r.Conf().SnapshotThreshold
}
func (r *Raft) buildSnapshot() (id string, err error) {
	fu := new(fsmSnapshotFuture)
	fu.init()
	select {
	case r.fsmSnapshotCh <- fu:
	case <-r.shutDown.C:
		return "", ErrShutDown
	}
	resp, err := fu.Response()
	if err != nil {
		r.logger.Errorf("buildSnapshot err :%s", err)
		return "", err
	}

	clusterFu := new(clusterGetFuture)
	clusterFu.init()
	select {
	case r.commandCh <- &command{enum: commandClusterGet, callback: clusterFu}:
	case <-r.shutDown.C:
		return "", ErrShutDown
	}
	cResp, err := clusterFu.Response()
	if err != nil {
		return "", err
	}
	commit, commitIndex := cResp.commit, cResp.commitIndex

	if resp.index < commitIndex { // 快照如果没赶上已提交的集群配置 index ，则不能生成快照
		return "", fmt.Errorf("no stable snapshot")
	}
	sink, err := r.snapshotStore.Create(SnapShotVersionDefault, resp.index, resp.term,
		commit, commitIndex, r.rpc)
	if err != nil {
		r.logger.Errorf("")
		return
	}

	if err = resp.fsmSnapshot.Persist(sink); err != nil {
		r.logger.Errorf("snap shot persist err :%s", err)
		sink.Cancel()
		return
	}
	if err = sink.Close(); err != nil {
		r.logger.Errorf("")
		return "", err
	}
	r.setLatestSnapshot(resp.term, resp.index)
	_ = r.compactLog()
	return sink.ID(), nil
}

// onShutDown 关机时处理一些清理逻辑
func (r *Raft) onShutDown() {
	// 清理未处理的 command 请求
	for i := 0; i < 100; i++ {
		select {
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		default:
			return
		}
	}
}

// cycleFollower 更随者主线程
func (r *Raft) cycleFollower() {
	leader := r.leaderInfo.Get()
	r.logger.Info("entering follower state", r.localInfo.ID, "leader-address", leader.Addr, "leader-id", leader.ID)
	r.heartbeatTimeout = randomTimeout(r.Conf().HeartbeatTimeout)
	warnOnce := new(sync.Once)
	warn := func(args ...interface{}) {
		warnOnce.Do(func() {
			r.logger.Warn(args...)
		})
	}
	doFollower := func() (stop bool) {
		select {
		case <-r.shutDown.C:
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.heartbeatTimeout:
			r.processHeartBeatTimeout(warn)
		}
		return
	}

	r.cycle(Follower, doFollower)

}

// processRPC 处理 rpc 请求
func (r *Raft) processRPC(cmd *RPC) {
	switch req := cmd.Request.(type) {
	case *VoteRequest:
		r.processVote(req, cmd)
	case *AppendEntryRequest:
		r.processAppendEntry(req, cmd)
	case *FastTimeoutRequest:
		r.processFastTimeout(req, cmd)
	case *InstallSnapshotRequest:
		r.processInstallSnapshot(req, cmd)
	}
}

// heartBeatFastPath 心跳的 fastPath ，不用经过主线程
func (r *Raft) heartBeatFastPath(cmd *RPC) bool {
	req, ok := cmd.Request.(*AppendEntryRequest)
	if cmd.CmdType == CmdAppendEntry && ok && len(req.Entries) > 0 {
		r.processAppendEntry(req, cmd)
		return true
	}
	return false
}

func (r *Raft) deleteLog(from, to uint64) error {
	pageSize := uint64(5)
	for i := from; i < to; i += pageSize {
		logs, err := r.logStore.GetLogRange(i, i+pageSize)
		if err != nil {
			return err
		}
		if err := r.logStore.DeleteRange(i, i+pageSize); err != nil {
			return err
		}
		if len(logs) < 5 {
			break
		}
	}
	return nil
}

// saveConfiguration 从日志里更新集群配置，由于集群变更只允许单次一个节点的变更，所以当有新日志
// 到来的时候就可以默认为原有的 latest ClusterInfo 已经提交 ，该函数只能用于启东时恢复或者主线程更新
func (r *Raft) saveConfiguration(entry *LogEntry) {
	if entry.Type != LogCluster {
		return
	}
	r.setCommitConfiguration(r.cluster.latestIndex, r.cluster.latest)
	r.setLatestConfiguration(entry.Index, DecodeCluster(entry.Data))
}

// storeEntries 持久化日志，需要校验 PrevLogIndex、PrevLogTerm 是否匹配
func (r *Raft) storeEntries(req *AppendEntryRequest) ([]*LogEntry, error) {
	var (
		newEntries              []*LogEntry
		latestTerm, latestIndex = r.getLatestEntry()
	)

	// 校验日志是否匹配
	// 只要有和 prevTerm、prevIndex 匹配的日志就行，不一定是最新的，日志可能会被领导人覆盖，lastIndex 可能回退
	if req.PrevLogIndex > 0 { // 第一个日志 PrevLogIndex 为 0 ，所以这里加判断
		var prevLogTerm uint64
		if req.PrevLogIndex == latestIndex {
			prevLogTerm = latestTerm
		} else {
			log, err := r.logStore.GetLog(req.PrevLogIndex)
			if err != nil {
				if errors.Is(ErrNotFoundLog, err) {
					r.logger.Errorf("not fround prev log ,idx : %d ,id : %s", req.PrevLogIndex, r.localInfo.ID)
				}
				return nil, err
			}
			prevLogTerm = log.Term

		}
		if prevLogTerm != req.PrevLogTerm {
			r.logger.Error("storeEntries|prev log term not match :", prevLogTerm, req.PrevLogTerm)
			return nil, errors.New("prev log term not match")
		}
	}

	for i, entry := range req.Entries {
		if entry.Index > latestIndex { // 绝对是最新的了
			newEntries = req.Entries[i:]
			break
		}
		log, err := r.logStore.GetLog(entry.Index)
		if err != nil {
			return nil, err // 小于 latestIndex 的不应该有空洞，所以不判断 ErrNotFoundLog
		}
		if log.Term != entry.Term {
			if err = r.logStore.DeleteRange(entry.Index, latestIndex); err != nil {
				r.logger.Errorf("delete log err :%s  .from :%d ,to :%d", err, entry.Index, latestIndex)
				return nil, err
			}
			newEntries = req.Entries[i:]
			break
		}

	}
	if len(newEntries) == 0 {
		return nil, nil
	}
	if err := r.logStore.SetLogs(newEntries); err != nil {
		r.logger.Errorf("store log err :%s", err)
		return nil, err
	}
	return newEntries, nil
}

// processInstallSnapshot 复制远端的快照到本地，并且引用到状态机，这个方法使用在领导者复制过程中待复制日志在快照中的情况
// 隐含意义就是跟随者的有效日志比领导者的快照旧
func (r *Raft) processInstallSnapshot(req *InstallSnapshotRequest, cmd *RPC) {
	var (
		succ bool
		meta = req.SnapshotMeta
	)
	defer func() {
		cmd.Response <- &InstallSnapshotResponse{
			RPCHeader: r.buildRPCHeader(),
			Success:   succ,
			Term:      r.getCurrentTerm(),
		}
		io.Copy(io.Discard, cmd.Reader)
	}()
	r.logger.Debug("processInstallSnapshot ", r.getCurrentTerm(), req.Term)
	if req.Term < r.getCurrentTerm() {
		return
	}
	if req.Term > r.getCurrentTerm() {
		r.setFollower()
		r.setCurrentTerm(req.Term)
	}
	if len(req.ID) > 0 {
		r.leaderInfo.Set(ServerInfo{
			ID:   req.ID,
			Addr: req.Addr,
		})
	}

	sink, err := r.snapshotStore.Create(SnapShotVersionDefault, meta.Index, meta.Term, meta.Configuration, meta.ConfigurationIndex, r.rpc)
	if err != nil {
		r.logger.Errorf("processInstallSnapshot|crate err :%s", err)
		return
	}
	reader := newCounterReader(cmd.Reader)
	written, err := io.Copy(sink, reader)
	if err != nil {
		r.logger.Errorf("processInstallSnapshot|Copy err :%s", err)
		sink.Cancel()
		return
	}
	if written != meta.Size {
		r.logger.Errorf("receive snap shot size not enough")
		sink.Cancel()
		return
	}
	if err = sink.Close(); err != nil {
		r.logger.Errorf("processInstallSnapshot|close sink err :%s", err)
		return
	}
	fu := &restoreFuture{ID: sink.ID()}
	fu.init()

	select {
	case <-r.shutDown.C:
		return
	case r.fsmRestoreCh <- fu:
	}
	if _, err = fu.Response(); err != nil {
		r.logger.Errorf("restore snap shot err :%s", err)
		return
	}
	r.setLatestSnapshot(meta.Term, meta.Index)
	r.setLastApplied(meta.Index)

	r.setCommitConfiguration(meta.Index, meta.Configuration)
	r.setLatestConfiguration(meta.Index, meta.Configuration)
	r.setLastContact()
	_ = r.compactLog()
	succ = true
}

// compactLog 压缩日志，至少保留 Config.TrailingLogs 长度日志
func (r *Raft) compactLog() error {
	trailingLogs := r.Conf().TrailingLogs
	firstIndex, err := r.logStore.FirstIndex()
	if err != nil {
		r.logger.Errorf("compact log fail ,get first index err :%s", err)
		return err
	}
	_, logIndex := r.getLatestLog()
	_, snapshotIndex := r.getLatestSnapshot()
	idx := Min(snapshotIndex, logIndex-trailingLogs)
	if idx < firstIndex {
		return nil
	}
	if err = r.logStore.DeleteRange(firstIndex, idx); err != nil {
		r.logger.Errorf("compact log fail ,delete log err :%s", err)
		return err
	}
	return nil
}

// processFastTimeout 跟随者快速超时成为候选人
func (r *Raft) processFastTimeout(req *FastTimeoutRequest, cmd *RPC) {
	var succ bool
	defer func() {
		cmd.Response <- &FastTimeoutResponse{
			RPCHeader: r.buildRPCHeader(),
			Success:   succ,
		}
	}()
	if req.Term < r.getCurrentTerm() {
		return
	}
	r.setCandidate()
	r.candidateFromLeaderTransfer = req.LeaderShipTransfer
	succ = true
}

// processAppendEntry 处理日志复制，当日志长度为 0 的时候当做心跳使用
func (r *Raft) processAppendEntry(req *AppendEntryRequest, cmd *RPC) {
	var (
		succ bool
	)
	defer func() {
		cmd.Response <- &AppendEntryResponse{
			RPCHeader:   r.buildRPCHeader(),
			Term:        r.getCurrentTerm(),
			Succ:        succ,
			LatestIndex: r.getLatestIndex(),
		}
	}()
	if req.Term < r.getCurrentTerm() {
		return
	}
	// 如果我们正在进行 leader transfer 则不用退回成跟随者，继续发起选举
	if req.Term > r.getCurrentTerm() && !r.candidateFromLeaderTransfer {
		r.setCurrentTerm(req.Term)
		r.setFollower()
	}

	if len(req.ID) > 0 {
		r.leaderInfo.Set(ServerInfo{
			Addr: req.Addr,
			ID:   req.ID,
		})
	}
	entries, err := r.storeEntries(req)
	if err != nil {
		r.logger.Debug("processAppendEntry|storeEntries err:", err)
		return
	}

	// 更新本地最近的 index
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		r.setLatestLog(last.Term, last.Index)
		// 处理集群配置更新
		for _, entry := range entries {
			r.saveConfiguration(entry)
		}
	}
	// 更新已提交的配置
	if req.LeaderCommit > r.cluster.latestIndex {
		r.setCommitConfiguration(r.cluster.latestIndex, r.cluster.latest)
	}
	if req.LeaderCommit > r.getCommitIndex() {
		// 更新 commit index
		r.setCommitIndex(Min(req.LeaderCommit, r.getLatestIndex()))
		// 应用到状态机错误不用返回给 leader
		r.applyLogToFsm(r.getCommitIndex(), nil)
	}
	succ = true
	r.setLastContact()
}

func (r *Raft) processVote(req *VoteRequest, cmd *RPC) {
	var granted bool
	defer func() {
		cmd.Response <- &VoteResponse{
			RPCHeader: r.buildRPCHeader(),
			Term:      r.getCurrentTerm(),
			Granted:   granted,
		}
	}()
	if len(r.getLatestCluster()) > 0 {
		if !r.inConfiguration(req.ID) {
			r.logger.Warn("rejecting vote request since node is not in clusterInfo",
				"from", req.Addr)
			return
		} else if !r.canVote(req.ID) {
			r.logger.Warn("rejecting vote request since node is not a voter", "from", req.Addr)
			return
		}
	}
	// 如果跟随者确认当前有合法领导人则直接拒绝，保持集群稳定
	if info := r.leaderInfo.Get(); len(info.Addr) != 0 && info.Addr != req.Addr && !req.LeaderTransfer {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from addr", req.Addr,
			"from id", req.ID,
			"leader", r.leaderInfo.Get().ID)
		return
	}
	if req.Term < r.getCurrentTerm() {
		return
	}
	if req.Term > r.getCurrentTerm() {
		r.setCurrentTerm(req.Term)
		r.setFollower()
	}

	candidateId, err := r.kvStore.Get(KeyLastVoteFor)
	if err != nil && !errors.Is(ErrKeyNotFound, err) {
		r.logger.Errorf("get last voter for err :%s", err)
		return
	}
	candidateTerm, err := r.kvStore.GetUint64(KeyLastVoteTerm)
	if err != nil && !errors.Is(ErrKeyNotFound, err) {
		r.logger.Errorf("")
		return
	}

	// 每个任期只能投一票
	if candidateTerm == req.Term && len(candidateId) != 0 {
		return
	}
	if err = r.kvStore.Set(KeyLastVoteFor, Str2Bytes(req.ID)); err != nil {
		r.logger.Errorf("save vote candidate id err:%s ,id :%s", err, req.ID)
		return
	}
	if err = r.kvStore.SetUint64(KeyLastVoteTerm, req.Term); err != nil {
		r.logger.Errorf("save vote term err:%s ,id :%s", err, req.Term)
		return
	}
	granted = true
	if req.LeaderTransfer {
		r.logger.Info("vote for", req.ID, " cause leader transfer")
	}
}
func (r *Raft) cycle(state State, f func() (stop bool)) {
	for r.GetState() == state && !f() {
	}
}

// setCurrentTerm 更新任期。需要持久化以在重启时恢复
func (r *Raft) setCurrentTerm(term uint64) {
	if err := r.kvStore.SetUint64(KeyCurrentTerm, term); err != nil {
		panic(err)
		return
	}
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *Raft) cycleCandidate() {
	r.setCurrentTerm(r.getCurrentTerm() + 1)
	r.logger.Infof("entering candidate state , term : %d", r.getCurrentTerm())
	var (
		voteCount  uint
		quorumSize = r.quorumSize()
		electionCh = r.launchElection()
	)
	defer func() {
		r.candidateFromLeaderTransfer = false
	}()
	r.electionTimeout = randomTimeout(r.Conf().ElectionTimeout)

	doCandidate := func() (stop bool) {
		select {
		case <-r.shutDown.C:
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.electionTimeout:
			r.logger.Warn("election timeout reached, restarting election")
			return true
		case result := <-electionCh:
			if result.Term > r.getCurrentTerm() {
				r.logger.Infof("newer term discovered, fallback to follower", "term", result.Term)
				r.setCurrentTerm(result.Term)
				r.setFollower()
				return
			}
			if !result.Granted {
				return
			}
			voteCount++
			r.logger.Debug("vote granted", "from", result.ID, "term", result.Term, "tally", voteCount)
			if voteCount >= quorumSize {
				r.logger.Info("election won", "term", result.Term, "tally", voteCount, "id", r.localInfo.ID)
				r.setLeader()
			}
		}
		return
	}

	r.cycle(Candidate, doCandidate)
}

func (r *Raft) setupLeaderState() {
	servers := r.getLatestCluster()
	r.leaderState.inflight = list.New()
	r.leaderState.startIndex = r.getLatestIndex()
	r.leaderState.stepDown = make(chan ServerID, 1)
	r.leaderState.replicate = make(map[ServerID]*replication, len(servers))
	r.leaderState.matchIndex = make(map[ServerID]uint64)
	for _, info := range servers {
		if info.isVoter() {
			r.leaderState.matchIndex[info.ID] = 0
		}
	}
}

func (r *Raft) stopReplication() {
	for _, repl := range r.leaderState.replicate {
		repl.stop <- true // 尽力复制
		close(repl.stop)
		<-repl.done
	}
}

// recalculate 计算 commit index 必须在 leaderState 的锁里执行
func (r *Raft) recalculate() uint64 {
	list := make([]uint64, 0, len(r.leaderState.matchIndex))
	for _, idx := range r.leaderState.matchIndex {
		list = append(list, idx)
	}
	SortSlice(list)
	return list[(len(list)-1)/2]
}
func (l *leaderState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&l.commitIndex)
}
func (l *leaderState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&l.commitIndex, index)
}
func (r *Raft) updateMatchIndex(id ServerID, latestIndex uint64) {
	r.leaderState.matchLock.Lock()
	defer r.leaderState.matchLock.Unlock()
	// commit index 不允许回退，不允许比新任期提交的第一条日志小
	if idx, ok := r.leaderState.matchIndex[id]; ok && latestIndex > idx {
		r.leaderState.matchIndex[id] = latestIndex
		r.calcCommitIndex()
	}
}
func (r *Raft) calcCommitIndex() {
	commitIndex := r.recalculate()
	if commitIndex > r.leaderState.getCommitIndex() && commitIndex > r.leaderState.startIndex {
		r.leaderState.setCommitIndex(commitIndex)
		asyncNotify(r.commitCh)
	}
}

func (r *Raft) onConfigurationUpdate() {
	r.leaderState.matchLock.Lock()
	defer r.leaderState.matchLock.Unlock()
	oldMatch := r.leaderState.matchIndex
	r.leaderState.matchIndex = make(map[ServerID]uint64)
	for _, info := range r.getLatestCluster() {
		if info.isVoter() {
			r.leaderState.matchIndex[info.ID] = oldMatch[info.ID]
		}
	}
	r.calcCommitIndex()
}

// buildAppendEntryReq 构建 AppendEntryRequest ，填充 replication.nextIndex 到 latestIndex 之前的日志以及计算 PrevLogIndex 等信息
// 如果日志有空洞则会返回 ErrNotFoundLog 错误，通知调用方应该发送快照对跟随者的日志进行覆盖
func (r *Raft) buildAppendEntryReq(nextIndex, latestIndex uint64) (*AppendEntryRequest, error) {
	var (
		snapshotTerm, snapshotIndex = r.getLatestSnapshot()
		req                         = &AppendEntryRequest{
			RPCHeader:    r.buildRPCHeader(),
			Term:         r.getCurrentTerm(),
			LeaderCommit: r.getCommitIndex(),
		}
	)
	latestIndex = clacLatestIndex(nextIndex, latestIndex, r.Conf().MaxAppendEntries)
	setupLogEntries := func() (err error) {
		if latestIndex == 0 || nextIndex > latestIndex {
			return nil
		}
		req.Entries, err = r.logStore.GetLogRange(nextIndex, latestIndex)
		if err != nil {
			return err
		}
		if uint64(len(req.Entries)) != latestIndex-nextIndex+1 {
			return ErrNotFoundLog
		}
		return nil
	}
	// prev log 即使不传递新日志也需要设置，因为需要让新加入集群的干净节点追赶进度
	setupPrevLog := func() error {
		var prevIndex uint64
		if len(req.Entries) != 0 {
			prevIndex = req.Entries[0].Index - 1
		} else {
			prevIndex = latestIndex
		}
		switch {
		case prevIndex == 0: // 第一个日志
		case prevIndex == snapshotIndex: // 上一个 index 正好是快照的最后一个日志，避免触发快照安装
			req.PrevLogTerm, req.PrevLogIndex = snapshotTerm, snapshotIndex
		default:
			log, err := r.logStore.GetLog(prevIndex)
			if err != nil {
				return err
			}
			req.PrevLogTerm, req.PrevLogIndex = log.Term, log.Index
		}
		return nil
	}
	for _, f := range []func() error{
		setupLogEntries,
		setupPrevLog,
	} {
		if err := f(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// leaderLease 领导人线程通知自己下台
func (r *Raft) leaderLease(fr *replication) {
	asyncNotify(r.leaderState.stepDown, fr.peer.Get().ID)
	fr.notifyAll(false)
}

// updateLatestCommit 更新最新的提交索引，并且回调 replication 的 verifyFuture 请求
func (r *Raft) updateLatestCommit(fr *replication, entries []*LogEntry) {
	if len(entries) > 0 {
		peer := fr.peer.Get()
		last := entries[len(entries)-1]
		fr.setNextIndex(last.Index + 1)
		r.updateMatchIndex(peer.ID, last.Index)
		if diff := r.getCommitIndex() - fr.getNextIndex(); diff > 0 && diff < 50 {
			r.logger.Infof("peer :%s catch up", peer.ID)
		}
	}
	fr.notifyAll(true)
}

// reloadReplication 重新加载跟随者的复制、心跳线程
func (r *Raft) reloadReplication() {
	var (
		set       = map[ServerID]bool{}
		nextIndex = r.getLatestIndex() + 1
	)
	// 开启新的跟随者线程
	for _, server := range r.getLatestCluster() {
		set[server.ID] = true
		if server.ID == r.localInfo.ID {
			continue
		}
		if fr := r.leaderState.replicate[server.ID]; fr != nil {
			if fr.peer.Get().Addr != server.Addr {
				r.logger.Info("updating peer", "peer", server.ID)
			}
			fr.peer.Set(server)
			continue
		}
		fr := &replication{
			nextIndex:     nextIndex,
			peer:          NewLockItem(server),
			stop:          make(chan bool, 1),
			heartBeatStop: make(chan struct{}),
			heartBeatDone: make(chan struct{}),
			notifyCh:      make(chan struct{}, 1),
			done:          make(chan struct{}),
			trigger:       make(chan *defaultDeferResponse, 1),
			lastContact:   NewLockItem(time.Now()),
			notify:        NewLockItem(map[*verifyFuture]struct{}{}),
		}
		r.leaderState.replicate[server.ID] = fr
		r.logger.Info("begin replicate", server.ID, nextIndex)
		r.goFunc(
			func() { r.heartBeat(fr) },
			func() { r.replicate(fr) },
		)
		// 立即触发心跳，防止领导权检查下台
		asyncNotify(fr.notifyCh)
	}
	// 删除已经不在集群的跟随者线程
	for _, repl := range r.leaderState.replicate {
		id := repl.peer.Get().ID
		if set[id] {
			continue
		}
		repl.notifyAll(true)
		// 删除
		delete(r.leaderState.replicate, id)
		repl.stop <- true // 尽力复制
		close(repl.stop)
		//<-repl.done 没必要等待
	}
}

// clearLeaderState 领导人下台时的清理逻辑
func (r *Raft) clearLeaderState() {
	for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
		e.Value.(*LogFuture).fail(ErrNotLeader)
		r.leaderState.inflight.Remove(e)
	}
	for _, repl := range r.leaderState.replicate {
		repl.notify.Action(func(t *map[*verifyFuture]struct{}) {
			for future := range *t {
				future.fail(ErrLeadershipLost)
			}
		})
	}
	r.leaderState.inflight = nil
	r.leaderState.replicate = nil
	r.leaderState.startIndex = 0
	r.leaderState.matchIndex = nil
	r.leaderState.commitIndex = 0
	r.leaderState.stepDown = nil
}

// checkLeadership 计算领导权
func (r *Raft) checkLeadership() (leader bool, maxDiff time.Duration) {
	var (
		now               = time.Now()
		quorumSize        = r.quorumSize()
		leaseTimeout      = r.Conf().LeaderLeaseTimeout
		voteForCount uint = 1
	)
	for id, repl := range r.leaderState.replicate {
		if !Ptr(repl.peer.Get()).isVoter() {
			continue
		}
		if diff := now.Sub(repl.lastContact.Get()); diff <= leaseTimeout {
			voteForCount++
			maxDiff = Max(diff, maxDiff)
		} else {
			if diff < 3*leaseTimeout {
				r.logger.Warn("failed to contact", "server-id", id, "time", diff)
			} else {
				r.logger.Debug("failed to contact too long", "server-id", id, "time", diff)
			}
		}
	}
	return voteForCount >= quorumSize, maxDiff
}

// broadcastReplicate 通知所有副本强制进行复制
func (r *Raft) broadcastReplicate() {
	for _, repl := range r.leaderState.replicate {
		asyncNotify(repl.trigger)
	}
}

// applyLog 想本地提交日志然后通知复制到跟随者
func (r *Raft) applyLog(future ...*LogFuture) {
	var (
		index = r.getLatestIndex()
		term  = r.getCurrentTerm()
		logs  = make([]*LogEntry, 0, len(future))
		now   = time.Now()
	)
	if len(future) == 0 {
		return
	}
	for _, fu := range future {
		index++
		fu.log.Index = index
		fu.log.Term = term
		fu.log.CreatedAt = now
		logs = append(logs, fu.log)
	}
	if err := r.logStore.SetLogs(logs); err != nil {
		r.logger.Errorf("applyLog|SetLogs err :%s", err)
		for _, fu := range future {
			fu.fail(err)
		}
		// 如果本地存储出问题了直接放弃领导权
		r.setFollower()
		return
	}
	for _, fu := range future {
		r.leaderState.inflight.PushBack(fu)
	}
	// 更新本地最新 index
	r.setLatestLog(term, index)
	// 更新 commitIndex
	r.updateMatchIndex(r.localInfo.ID, index)
	// 通知复制
	r.broadcastReplicate()
}

// processLeaderCommit 只在 cycleLeader 中调用，将所有可提交的日志提交到状态机
func (r *Raft) processLeaderCommit() {
	var (
		oldCommitIndex = r.getCommitIndex()
		newCommitIndex = r.leaderState.getCommitIndex()
		readyFuture    = map[uint64]*LogFuture{}
		readElem       []*list.Element
		stepDown       bool
	)
	//r.logger.Debug("commit index update :", newCommitIndex)
	r.setCommitIndex(newCommitIndex)

	if r.cluster.latestIndex > oldCommitIndex && r.cluster.latestIndex <= newCommitIndex {
		r.logger.Info("cluster stable ,old :", oldCommitIndex, " new:", newCommitIndex)
		r.setCommitConfiguration(r.cluster.latestIndex, r.cluster.latest)
		if !r.canVote(r.localInfo.ID) { // 集群配置提交后，如果当前领导人不在集群内，则下台
			stepDown = true
		}
	}

	for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
		if future := e.Value.(*LogFuture); future.Index() <= newCommitIndex {
			readyFuture[future.Index()] = future
			readElem = append(readElem, e)
		}
	}
	//r.logger.Infof("leader commit ori :%d,cur:%d", oldCommitIndex, newCommitIndex)
	r.applyLogToFsm(newCommitIndex, readyFuture)
	for _, element := range readElem {
		r.leaderState.inflight.Remove(element)
	}
	if stepDown {
		if r.Conf().LeadershipLostShutDown {
			r.ShutDown()
		} else {
			r.setFollower()
		}
	}
}

// hasExistTerm 判断节点是否干净
func (r *Raft) hasExistTerm() (exist bool, err error) {
	for _, f := range []func() (bool, error){
		func() (bool, error) {
			existTerm, err := r.kvStore.GetUint64(KeyCurrentTerm)
			if errors.Is(ErrKeyNotFound, err) {
				err = nil
			}
			return existTerm > 0, err
		},
		func() (bool, error) {
			lastIndex, err := r.logStore.LastIndex()
			return lastIndex > 0, err
		},
		func() (bool, error) {
			snapshots, err := r.snapshotStore.List()
			return len(snapshots) > 0, err
		},
	} {
		if exist, err = f(); exist || err != nil {
			if err != nil {
				r.logger.Errorf("hasExistTerm err :%s", err)
			}
			return
		}
	}
	return
}

func (r *Raft) clacNewConfiguration(req *clusterChangeRequest) (newConfiguration ClusterInfo, err error) {
	switch req.command {
	case addServer:
		for _, server := range r.getLatestCluster() {
			if server.ID == req.peer.ID {
				return ClusterInfo{}, errors.New("peer duplicate")
			}
			newConfiguration.Servers = append(newConfiguration.Servers, server)
		}
		newConfiguration.Servers = append(newConfiguration.Servers, req.peer)
	case updateServer:
		found := false
		for _, server := range r.getLatestCluster() {
			if server.ID == req.peer.ID {
				if server.Suffrage == req.peer.Suffrage {
					return ClusterInfo{}, errors.New("suffrage no change")
				}
				server.Suffrage = req.peer.Suffrage
				found = true
			}
			newConfiguration.Servers = append(newConfiguration.Servers, server)
		}
		if !found {
			return ClusterInfo{}, errors.New("not found")
		}
	case removeServer:
		found := false
		for _, server := range r.getLatestCluster() {
			if server.ID == req.peer.ID {
				found = true
				continue
			} else {
				newConfiguration.Servers = append(newConfiguration.Servers, server)
			}
		}
		if !found {
			return ClusterInfo{}, errors.New("not found")
		}
	}

	return
}

// cycleLeader Leader 状态主线程
func (r *Raft) cycleLeader() {
	r.logger.Debug("cycle leader ", r.localInfo.ID)
	r.setupLeaderState()
	r.reloadReplication()
	defer func() {
		r.stopReplication()
		r.clearLeaderState()
		// 因为会把 LastContact 返回给 API 调用者，这里更新下时间避免跟随者状态获取的时间太旧
		r.setLastContact()
		r.logger.Info("leave leader", r.localInfo.ID)
	}()
	// 提交一个空日志，用于确认 commitIndex
	future := &LogFuture{log: &LogEntry{Type: LogNoop}}
	future.init()
	r.applyLog(future)
	leaderLeaseTimeout := time.After(r.Conf().LeaderLeaseTimeout)

	leaderLoop := func() (stop bool) {
		select {
		case <-r.shutDown.C:
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.commitCh:
			r.processLeaderCommit()
		case <-leaderLeaseTimeout:
			if leader, maxDiff := r.checkLeadership(); leader {
				leaderLeaseTimeout = time.After(Max(r.Conf().LeaderLeaseTimeout-maxDiff, minCheckInterval))
			} else {
				r.logger.Infof("leader ship check fail, term :%d ,step down", r.getCurrentTerm())
				r.setFollower()
			}
		case id := <-r.leaderState.stepDown:
			r.logger.Info("leader ship step down, term :", r.getCurrentTerm(), "notify from :", id)
			r.setFollower()
		}
		return
	}
	r.cycle(Leader, leaderLoop)
}

func (r *Raft) quorumSize() (c uint) {
	for _, server := range r.getLatestCluster() {
		if server.isVoter() {
			c++
		}
	}
	return c>>1 + 1
}

// launchElection 发起选举，只由 cycleCandidate 方法调用
func (r *Raft) launchElection() chan *voteResult {
	var (
		list                = r.getLatestCluster()
		lastTerm, lastIndex = r.getLatestEntry()
		header              = r.buildRPCHeader()
		currentTerm         = r.getCurrentTerm()
		result              = make(chan *voteResult, len(list))
	)
	for _, info := range list {
		if !info.isVoter() {
			continue
		}
		if info.ID == r.localInfo.ID {
			if err := r.kvStore.Set(KeyLastVoteFor, Str2Bytes(info.ID)); err != nil {
				r.logger.Errorf("launchElection vote for self error :%s", err)
				continue
			}
			result <- &voteResult{
				VoteResponse: &VoteResponse{
					RPCHeader: header,
					Term:      currentTerm,
					Granted:   true,
				},
				ServerID: info.ID,
			}
		} else {
			info := info
			r.goFunc(func() {
				resp, err := r.rpc.VoteRequest(&info, &VoteRequest{
					RPCHeader:      header,
					Term:           currentTerm,
					CandidateID:    info.ID,
					LastLogIndex:   lastIndex,
					LastLogTerm:    lastTerm,
					LeaderTransfer: r.candidateFromLeaderTransfer,
				})
				if err != nil {
					r.logger.Errorf("launchElection|VoteRequest err :%s", err)
					return
				}
				result <- &voteResult{
					VoteResponse: resp,
					ServerID:     info.ID,
				}
			})
		}
	}
	return result
}
