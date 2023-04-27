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

type (
	Raft struct {
		commitIndex   uint64                    // 集群已提交的日志，初始化为 0 只有再提交过日志后才可以更新
		lastApplied   uint64                    // 已提交给状态机的最新 index , 注意：不代表状态机已经应用
		currentTerm   uint64                    // 当前任期，需要持久化
		configuration *AtomicVal[Configuration] // 集群配置的副本
		conf          *AtomicVal[*Config]       // 参数配置信息
		confReloadMu  sync.Mutex
		state         State                 // 节点状态
		run           func()                // 主线程函数
		lastContact   *AtomicVal[time.Time] // 与领导者上次联系的时间
		localInfo     ServerInfo            // 当前节点地址
		lastEntry     *LockItem[lastEntry]  // 节点最新的索引、任期
		cluster       cluster               // 集群配置
		leaderInfo    *LockItem[ServerInfo] // 领导人地址
		funcEg        *errgroup.Group
		shutDown      shutDown
		logger        Logger // 日志
		//-------主线程-----------
		rpcCh <-chan *RPC // 处理 RPC 命令

		commitCh chan struct{} // 日志已提交通知，可以应用到 FSM 可以有 buffer

		leaderState                 leaderState      // 领导人上下文
		heartbeatTimeout            <-chan time.Time // 由主线程设置
		electionTimeout             <-chan time.Time // 由主线程设置
		candidateFromLeaderTransfer bool             // 当前节点在领导权转移过程中
		//-------fsm-----------
		fsm           FSM                     // 状态机，日志提交后由此应用
		fsmApplyCh    chan []*LogFuture       // 状态机线程的日志提交通知
		fsmSnapshotCh chan *fsmSnapshotFuture // 从状态机取快照
		fsmRestoreCh  chan *restoreFuture     // 通知状态机重新应用快照

		//-----API--------------------
		//clusterGetCh         chan *clusterGetFuture  // 获取集群配置
		apiSnapshotBuildCh   chan *apiSnapshotFuture // 生成快照
		apiSnapshotRestoreCh chan *userRestoreFuture // 重新应用快照的时候不能接收新的日志，需要从 runState 线程触发
		apiLogApplyCh        chan *LogFuture         // 日志提交请求，由于需要支持批量提交，所以单独提出来
		commandCh            chan *command           // 对节点发起的命令，包括领导权验证等
		stateChangeCh        chan State
		//-----组件------
		rpc           RpcInterface  // RPC 组件
		kvStore       KVStorage     // 任期、投票信息持久化组件
		logStore      LogStore      // 日志持久化组件
		snapshotStore SnapshotStore // 快照组件
	}
	lastEntry struct {
		// 快照中存的最新日志
		snapshot lastLog
		log      lastLog
		// 已经落到稳定存储的最新日志
		//logIndex uint64
		//logTerm  uint64
	}
	lastLog struct {
		index uint64
		term  uint64
	}
	// leaderState 领导人上下文
	leaderState struct {
		sync.Mutex
		commitIndex        uint64                    // 通过计算副本得出的已提交索引，只能由新日志提交触发更新
		matchIndex         map[ServerID]uint64       // 每个跟随者对应的已复制的 index
		replicate          map[ServerID]*replication // 所有的跟随者
		inflight           *list.List                // 等待提交并应用到状态机的 LogFuture
		startIndex         uint64                    // 记录任期开始时的最新一条索引，防止在日志提交的时候发生 commit index 回退
		stepDown           chan struct{}             // 领导人下台通知
		leadershipTransfer int32                     // 是否发生领导权转移 1 ：是 ，0 ：否
	}
	// replication 领导人复制时每个跟随者维护的上下文状态
	replication struct {
		peer                               *LockItem[ServerInfo]                 // 跟随者的 server 信息
		nextIndex                          uint64                                // 待复制给跟随者的下一条日志索引，初始化为领导人最新的日志索引
		heartBeatStop, heartBeatDone, done chan struct{}                         // 心跳停止、复制线程结束、pipeline 返回结果处理线程结束
		trigger                            chan *defaultDeferResponse            // 强制复制，不需要复制结果可以投递 nil
		notifyCh                           chan struct{}                         // 强制心跳
		stop                               chan bool                             // 复制停止通知，true 代表需要在停机前尽力复制
		lastContact                        *LockItem[time.Time]                  // 上次与跟随者联系的时间，用于计算领导权
		notify                             *LockItem[map[*verifyFuture]struct{}] // VerifyLeader 请求跟踪
	}
)

const (
	Voter Suffrage = iota
	NonVoter
)

type (
	ServerAddr string
	ServerID   string
	Suffrage   int
	// ServerInfo 节点的地址信息
	ServerInfo struct {
		Suffrage Suffrage
		Addr     ServerAddr
		ID       ServerID
	}
)

func (r *Raft) Conf() *Config {
	return r.conf.Load()
}
func (r *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}
func (r *Raft) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *Raft) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}
func (fr *replication) getNextIndex() uint64 {
	return atomic.LoadUint64(&fr.nextIndex)
}
func (fr *replication) setNextIndex(newNextIndex uint64) {
	atomic.StoreUint64(&fr.nextIndex, newNextIndex)
}
func (fr *replication) notifyAll(leadership bool) {
	fr.notify.Action(func(t *map[*verifyFuture]struct{}) {
		for v := range *t {
			v.vote(leadership)
		}
		*t = map[*verifyFuture]struct{}{}
	})
}
func (fr *replication) observe(v *verifyFuture) {
	fr.notify.Action(func(t *map[*verifyFuture]struct{}) {
		(*t)[v] = struct{}{}
	})
}
func (l *leaderState) setupLeadershipTransfer(status bool) (succ bool) {
	old, newVal := int32(1), int32(0)
	if status {
		old = 0
		newVal = 1
	}
	return atomic.CompareAndSwapInt32(&l.leadershipTransfer, old, newVal)
}
func (l *leaderState) getLeadershipTransfer() (status bool) {
	return atomic.LoadInt32(&l.leadershipTransfer) == 1
}

func (s *ServerInfo) isVoter() bool {
	return s.Suffrage == Voter
}

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
		configuration:        NewAtomicVal[Configuration](),
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
		commitCh:             make(chan struct{}, 16),
		logger:               conf.Logger,
		leaderState:          leaderState{},
		stateChangeCh:        make(chan State, 1),
		commandCh:            make(chan *command),
		fsm:                  fsm,
		fsmApplyCh:           make(chan []*LogFuture),
		fsmSnapshotCh:        make(chan *fsmSnapshotFuture),
		fsmRestoreCh:         make(chan *restoreFuture),
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
	raft.rpc.SetHeartbeatFastPath(raft.processHeartBeat)
	raft.setFollower()
	raft.goFunc(raft.runState, raft.runFSM, raft.runSnapshot)
	return raft, nil
}
func (r *Raft) setLatestConfiguration(index uint64, configuration Configuration) {
	r.cluster.setLatest(index, configuration)
	r.configuration.Store(configuration)
}
func (r *Raft) setCommitConfiguration(index uint64, configuration Configuration) {
	r.cluster.setCommit(index, configuration)
}
func (r *Raft) getLatestIndex() uint64 {
	entry := r.lastEntry.Get()
	return Max(entry.log.index, entry.snapshot.index)
}
func (r *Raft) getLatestTerm() uint64 {
	entry := r.lastEntry.Get()
	return Max(entry.log.term, entry.snapshot.term)
}

func (r *Raft) getLatestEntry() (term uint64, index uint64) {
	entry := r.lastEntry.Get()
	return Max(entry.log.term, entry.snapshot.term), Max(entry.log.index, entry.snapshot.index)
}

// recoverCluster 从日志恢复集群配置
func (r *Raft) recoverCluster() (err error) {
	entry := r.lastEntry.Get()
	for i := entry.snapshot.index; i < entry.log.index; i++ {
		if i <= 0 {
			continue
		}
		entry, err := r.logStore.GetLog(i)
		if err != nil {
			r.logger.Errorf("")
			return fmt.Errorf("get log err :%s", err)
		}
		r.saveConfiguration(entry)
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

// recoverSnapshot 将本地最新的快照恢复至状态机，并更新本地的索引、任期状态
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
	// 生成快照默认用稳定的配置
	r.setCommitConfiguration(meta.ConfigurationIndex, meta.Configuration)
	r.setLatestConfiguration(meta.ConfigurationIndex, meta.Configuration)
	return nil
}

func (r *Raft) clearLeaderInfo() {
	r.updateLeaderInfo(func(s *ServerInfo) {
		*s = ServerInfo{}
	})
}

func (r *Raft) updateLeaderInfo(act func(s *ServerInfo)) {
	r.leaderInfo.Action(act)
}
func (r *Raft) goFunc(funcList ...func()) {
	for _, f := range funcList {
		f := f
		r.funcEg.Go(func() error {
			f()
			return nil
		})
	}
}
func (r *Raft) waitShutDown() {

}

func (r *Raft) buildRPCHeader() *RPCHeader {
	header := &RPCHeader{
		ID:   r.localInfo.ID,
		Addr: r.localInfo.Addr,
	}
	return header
}
func (r *Raft) setLastContact() {
	r.lastContact.Store(time.Now())
}
func (r *Raft) getLastContact() time.Time {
	return r.lastContact.Load()
}
func (fr *replication) setLastContact() {
	fr.lastContact.Set(time.Now())
}
func (fr *replication) getLastContact() time.Time {
	return fr.lastContact.Get()
}
func (r *Raft) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}
func (r *Raft) setCommitIndex(commitIndex uint64) {
	atomic.StoreUint64(&r.commitIndex, commitIndex)
}

// runState 运行主线程
func (r *Raft) runState() {
	for {
		select {
		case <-r.shutDown.C:
			return
		default:
		}
		r.run()
	}
}
func shouldApply(log *LogEntry) bool {
	switch log.Type {
	case LogConfiguration, LogCommand, LogBarrier:
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
				panic(err)
				return
			}
			fu = &LogFuture{log: log}
			fu.init()
		}
		if !shouldApply(fu.log) {
			fu.success()
			continue
		}
		logFutures = append(logFutures, fu)
		if len(logFutures) >= maxAppend {
			applyBatch(logFutures)
			logFutures = make([]*LogFuture, 0, maxAppend)
		}
	}
	applyBatch(logFutures)

	r.setLastApplied(toIndex)
}

func (r *Raft) shouldBuildSnapshot() bool {
	_, snapshotIndex := r.getLatestSnapshot()
	logIndex, _ := r.logStore.LastIndex()
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
	case r.commandCh <- &command{typ: commandClusterGet, item: clusterFu}:
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
		r.logger.Errorf("")
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

func (r *Raft) setFollower() {
	r.state.set(Follower)
	r.run = r.cycleFollower
	overrideNotify(r.stateChangeCh, Follower)
}
func (r *Raft) setCandidate() {
	r.state.set(Candidate)
	r.run = r.cycleCandidate
	overrideNotify(r.stateChangeCh, Candidate)
}
func (r *Raft) setLeader() {
	r.state.set(Leader)
	r.leaderInfo.Action(func(t *ServerInfo) {
		t.ID = r.localInfo.ID
		t.Addr = r.localInfo.Addr
	})
	r.run = r.cycleLeader
	overrideNotify(r.stateChangeCh, Leader)
}

func (r *Raft) setShutDown() {
	r.state.set(ShutDown)
	r.run = nil
	overrideNotify(r.stateChangeCh, ShutDown)
}

// onShutDown 关机时处理一些清理逻辑
func (r *Raft) onShutDown() {
	// 清理未处理的 command 请求
	for {
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
	r.logger.Info("entering follower state", "follower", "leader-address", leader.Addr, "leader-id", leader.ID)
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
			r.onShutDown()
			return true
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.heartbeatTimeout:
			tm := r.Conf().HeartbeatTimeout
			r.heartbeatTimeout = randomTimeout(tm)
			if time.Now().Before(r.getLastContact().Add(tm)) {
				return
			}
			oldLeaderInfo := r.leaderInfo.Get()
			r.clearLeaderInfo()
			switch {
			case r.cluster.latestIndex == 0:
				warn("no cluster members")
			case r.cluster.commitIndex == r.cluster.latestIndex && !r.canVote(r.localInfo.ID):
				warn("no part of stable Configuration, aborting election")
			case r.canVote(r.localInfo.ID):
				warn("heartbeat abortCh reached, starting election", "last-leader-addr", oldLeaderInfo.Addr, "last-leader-id", oldLeaderInfo.ID)
				r.setCandidate()
			default:
				warn("heartbeat abortCh reached, not part of a stable Configuration or a non-voter, not triggering a leader election")
			}
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

// processHeartBeat 心跳的 fastPath ，不用经过主线程
func (r *Raft) processHeartBeat(cmd *RPC) bool {
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
// 到来的时候就可以默认为原有的 latest Configuration 已经提交 ，该函数只能用于启东时恢复或者主线程更新
func (r *Raft) saveConfiguration(entry *LogEntry) {
	if entry.Type != LogConfiguration {
		return
	}
	r.setCommitConfiguration(r.cluster.latestIndex, r.cluster.latest)
	r.setLatestConfiguration(entry.Index, DecodeConfiguration(entry.Data))
}

// storeEntries 持久化日志，需要校验 PrevLogIndex、PrevLogTerm 是否匹配
func (r *Raft) storeEntries(req *AppendEntryRequest) ([]*LogEntry, error) {
	if len(req.Entries) == 0 {
		return nil, nil
	}
	var (
		newEntries              []*LogEntry
		latestTerm, latestIndex = r.getLatestEntry()
	)

	if req.PrevLogIndex+1 != req.Entries[0].Index {
		return nil, errors.New("param errorx")
	}
	// 校验日志是否匹配
	// 只要有和 prevTerm、prevIndex 匹配的日志就行，不一定是最新的，日志可能会被领导人覆盖，lastIndex 可能回退
	if req.PrevLogIndex > 0 { // 第一个日志 PrevLogIndex 为 0 ，所以这里加判断
		var prevlLogTerm uint64
		if req.PrevLogIndex == latestIndex {
			prevlLogTerm = latestTerm
		} else {
			log, err := r.logStore.GetLog(req.PrevLogIndex)
			if err != nil {
				if err.Error() != ErrNotFoundLog.Error() {
					r.logger.Errorf("")
				}
				return nil, err
			}
			prevlLogTerm = log.Term

		}
		if prevlLogTerm != req.PrevLogTerm {
			return nil, errors.New("prev log not match")
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
			if err := r.logStore.DeleteRange(entry.Index, latestIndex); err != nil {
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
		r.logger.Errorf("")
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
	}()
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
		return
	}
	reader := newCounterReader(cmd.Reader)
	written, err := io.Copy(sink, reader)
	if err != nil {
		r.logger.Errorf("")
		sink.Cancel()
		return
	}
	if written != meta.Size {
		r.logger.Errorf("")
		sink.Cancel()
		return
	}
	if err = sink.Close(); err != nil {
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
		r.logger.Errorf("")
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
		r.logger.Errorf("")
		return err
	}
	_, logIndex := r.getLatestLog()
	_, snapshotIndex := r.getLatestSnapshot()
	idx := Min(snapshotIndex, logIndex-trailingLogs)
	if idx < firstIndex {
		return nil
	}

	if err = r.logStore.DeleteRange(firstIndex, idx); err != nil {
		r.logger.Errorf("")
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
	r.candidateFromLeaderTransfer = true
	succ = true
}

// processAppendEntry 处理日志复制，当日志长度为 0 的时候当做心跳使用
func (r *Raft) processAppendEntry(req *AppendEntryRequest, cmd *RPC) {
	var (
		succ bool
		term = r.getLatestTerm()
	)

	defer func() {
		cmd.Response <- &AppendEntryResponse{
			RPCHeader:   r.buildRPCHeader(),
			Term:        r.getCurrentTerm(),
			Succ:        succ,
			LatestIndex: term,
		}
		io.Copy(io.Discard, cmd.Reader)
	}()
	if req.Term < r.getCurrentTerm() {
		return
	}

	// 如果我们正在进行 leader transfer 则不用退回成跟随者，继续发起选举
	if req.Term > r.getCurrentTerm() || (r.state.Get() != Follower && !r.candidateFromLeaderTransfer) {
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
		return
	}
	// 更新本地最近的 index
	last := entries[len(entries)-1]
	r.setLatestLog(last.Term, last.Index)
	// 处理集群配置更新
	for _, entry := range entries {
		r.saveConfiguration(entry)
	}
	// 更新已提交的配置
	if req.LeaderCommit > r.cluster.latestIndex {
		r.setCommitConfiguration(r.cluster.latestIndex, r.cluster.latest)
	}
	if req.LeaderCommit > r.getCommitIndex() {
		// 更新 commit index
		index := r.getLatestIndex()
		r.setCommitIndex(Min(req.LeaderCommit, index))
		// 应用到状态机错误不用返回给 leader
		r.applyLogToFsm(r.getCommitIndex(), nil)
	}
	succ = true
	r.setLastContact()
}
func (r *Raft) applyToFsm() {

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
	if len(r.getLatestConfiguration()) > 0 {
		if r.inConfiguration(req.ID) {
			r.logger.Warn("rejecting vote request since node is not in configuration",
				"from", req.Addr)
		} else if !r.canVote(req.ID) {
			r.logger.Warn("rejecting vote request since node is not a voter", "from", req.Addr)
		}
		return
	}
	// 如果跟随者确认当前有合法领导人则直接拒绝，保持集群稳定
	if info := r.leaderInfo.Get(); len(info.Addr) != 0 && info.Addr != req.Addr && !req.LeaderTransfer {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", req.Addr,
			"leader", r.leaderInfo.Get().ID,
			"leader-id", string(req.ID))
		return
	}
	if req.Term < r.getCurrentTerm() {
		return
	}
	if req.Term > r.getCurrentTerm() {
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.setCurrentTerm(req.Term)
		r.setFollower()
	}

	candidateId, err := r.kvStore.Get(KeyLastVoteFor)
	if err != nil {
		r.logger.Errorf("")
		return
	}
	candidateTerm, err := r.kvStore.GetUint64(KeyLastVoteTerm)
	if err != nil {
		r.logger.Errorf("")
		return
	}

	// 每个任期只能投一票
	if candidateTerm == req.Term && len(candidateId) != 0 {
		return
	}
	if err := r.kvStore.Set(KeyLastVoteFor, Str2Bytes(string(req.ID))); err != nil {
		r.logger.Errorf("")
		return
	}
	if err := r.kvStore.SetUint64(KeyLastVoteTerm, req.Term); err != nil {
		r.logger.Errorf("")
		return
	}
	granted = true
}
func (r *Raft) cycle(state State, f func() (stop bool)) {
	for r.state.Get() == state && !f() {
	}
}

// setCurrentTerm 更新任期。需要持久化以在重启时恢复
func (r *Raft) setCurrentTerm(term uint64) {
	if err := r.kvStore.Set(KeyCurrentTerm, iToBytes(term)); err != nil {
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
			r.onShutDown()
			return true
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.electionTimeout:
			r.logger.Warn("election timeout reached, restarting election")
			return true
		case result := <-electionCh:
			if result.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower", "term", result.Term)
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
	servers := r.getLatestConfiguration()
	index := r.getLatestIndex()
	r.leaderState.inflight = list.New()
	r.leaderState.startIndex = index
	r.leaderState.stepDown = make(chan struct{})
	r.leaderState.replicate = make(map[ServerID]*replication, len(servers))
	r.leaderState.matchIndex = make(map[ServerID]uint64)
	for _, info := range r.getLatestConfiguration() {
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

const minHeartBeatInterval = time.Millisecond * 10

// recalculate 计算 commit index 必须在 leaderState 的锁里执行
func (r *Raft) recalculate() uint64 {
	list := make([]uint64, len(r.leaderState.matchIndex))
	for _, idx := range r.leaderState.matchIndex {
		list = append(list, idx)
	}
	SortSlice(list)
	return list[len(list)>>1]
}
func (l *leaderState) getCommitIndex() uint64 {
	return l.commitIndex
}
func (r *Raft) updateMatchIndex(id ServerID, latestIndex uint64) {
	r.leaderState.Lock()
	defer r.leaderState.Unlock()
	// commit index 不允许回退，不允许比新任期提交的第一条日志小
	if idx, ok := r.leaderState.matchIndex[id]; ok && latestIndex > idx {
		r.leaderState.matchIndex[id] = latestIndex
		r.calcCommitIndex()
	}
}
func (r *Raft) calcCommitIndex() {
	commitIndex := r.recalculate()
	if commitIndex > r.leaderState.getCommitIndex() && commitIndex > r.leaderState.startIndex {
		r.leaderState.commitIndex = commitIndex
		asyncNotify(r.commitCh)
	}
}

func (r *Raft) onConfigurationUpdate() {
	r.leaderState.Lock()
	defer r.leaderState.Unlock()
	oldMatch := r.leaderState.matchIndex
	r.leaderState.matchIndex = make(map[ServerID]uint64)
	for _, info := range r.getLatestConfiguration() {
		if info.isVoter() {
			r.leaderState.matchIndex[info.ID] = oldMatch[info.ID]
		}
	}
	r.calcCommitIndex()
}

// heartBeat 想跟随者发起心跳，跟随 replicate 关闭
func (r *Raft) heartBeat(fr *replication) {
	var (
		ticker = time.NewTicker(Max(r.Conf().HeartbeatTimeout/10, minHeartBeatInterval))
	)
	defer func() {
		ticker.Stop()
		close(fr.heartBeatDone)
	}()
	for {
		select {
		case <-fr.heartBeatStop:
			return
		case <-ticker.C:
		case <-fr.notifyCh:
		}
		r.replicateTo(fr, 0)
	}
}

func (r *Raft) buildAppendEntryReq(fr *replication, latestIndex uint64) (*AppendEntryRequest, error) {
	var (
		snapshotTerm, snapshotIndex = r.getLatestSnapshot()
		entries                     []*LogEntry
		req                         = &AppendEntryRequest{
			RPCHeader:    r.buildRPCHeader(),
			Term:         r.getCurrentTerm(),
			LeaderCommit: r.getCommitIndex(),
		}
	)
	setupLogEntries := func() (err error) {
		if fr.getNextIndex() < latestIndex {
			return nil
		}
		entries, err = r.logStore.GetLogRange(fr.nextIndex, latestIndex)
		if err != nil {
			return err
		}
		if uint64(len(entries)) != latestIndex-fr.nextIndex+1 {
			return ErrNotFoundLog
		}
		return nil
	}

	setupPrevLog := func() error {
		if len(entries) == 0 {
			return nil
		}
		prevIndex := entries[0].Index - 1
		switch {
		case prevIndex == 0: // 第一个日志
		case prevIndex == snapshotIndex: // 上一个 index 正好是快照的最后一个日志，避免触发快照安装
			req.PrevLogTerm, req.PrevLogIndex = snapshotTerm, snapshotIndex
		default:
			log, err := r.logStore.GetLog(prevIndex)
			if err == nil {
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
func (r *Raft) replicateHelper(fr *replication) (stop bool) {
	var (
		ticker = time.NewTicker(r.Conf().CommitTimeout)
	)
	defer ticker.Stop()
	for !stop {
		select {
		case shouldSend := <-fr.stop:
			if shouldSend {
				r.replicateTo(fr, r.getLatestIndex())
			}
			return true
		case <-ticker.C:
			stop = r.replicateTo(fr, r.getLatestIndex())
		case fu := <-fr.trigger:
			stop = r.replicateTo(fr, r.getLatestIndex())
			if fu == nil {
				continue
			}
			if stop {
				fu.success()
			} else {
				fu.fail(errors.New("replication failed"))
			}

		}
	}
	return
}

// leaderLease 领导人线程通知自己下台
func (r *Raft) leaderLease(term uint64, fr *replication) {
	r.setFollower()
	r.setCurrentTerm(term)
	asyncNotify(r.leaderState.stepDown)
	fr.notifyAll(false)
}

// sendLatestSnapshot 发送最新的快照
func (r *Raft) sendLatestSnapshot(fr *replication) error {
	list, err := r.snapshotStore.List()
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return errors.New("snapshot not exist")
	}
	latestID := list[0].ID
	meta, readCloser, err := r.snapshotStore.Open(latestID)
	if err != nil {
		return err
	}
	defer func() {
		readCloser.Close()
	}()
	resp, err := r.rpc.InstallSnapShot(Ptr(fr.peer.Get()), &InstallSnapshotRequest{
		RPCHeader:    r.buildRPCHeader(),
		SnapshotMeta: meta,
	}, readCloser)
	if err != nil {
		return err
	}
	if resp.Term > r.getCurrentTerm() {

		r.leaderLease(resp.Term, fr)
		return nil
	}
	fr.setLastContact()
	fr.notifyAll(resp.Success)
	return nil
}

// updateLatestCommit 更新最新的提交索引，并且回调 replication 的 verifyFuture 请求
func (r *Raft) updateLatestCommit(fr *replication, entries []*LogEntry) {
	if len(entries) > 0 {
		peer := fr.peer.Get()
		last := entries[len(entries)-1]
		fr.setNextIndex(last.Index + 1)
		r.updateMatchIndex(peer.ID, last.Index)
		if r.getCommitIndex()-fr.nextIndex < 50 {
			r.logger.Infof("peer :%s catch up", peer.ID)
		}
	}
	fr.notifyAll(true)
}

func (r *Raft) replicateTo(fr *replication, latestIndex uint64) (stop bool) {
	hasMore := func() bool {
		select {
		case <-fr.stop:
			return false
		default:
			return fr.getNextIndex() < latestIndex
		}
	}
	for {
		req, err := r.buildAppendEntryReq(fr, latestIndex)
		if err != nil {
			if errors.Is(ErrNotFoundLog, err) {
				_ = r.sendLatestSnapshot(fr)
				return
			}
		}
		resp, err := r.rpc.AppendEntries(Ptr(fr.peer.Get()), req)
		if err != nil {
			r.logger.Errorf("")
			return
		}
		if resp.Term > r.getCurrentTerm() {
			r.leaderLease(resp.Term, fr)
			return true
		}
		fr.setLastContact()
		if resp.Succ {
			r.updateLatestCommit(fr, req.Entries)
		} else {
			fr.setNextIndex(Max(1, Min(fr.getNextIndex()-1, resp.LatestIndex)))
		}
		if !hasMore() {
			break
		}
	}

	return
}

// processPipelineResult 处理 pipeline 的结果
func (r *Raft) processPipelineResult(fr *replication, pipeline AppendEntryPipeline, finishCh, pipelineStopCh chan struct{}) {
	defer close(pipelineStopCh)
	for {
		select {
		case <-finishCh:
			return
		case fu := <-pipeline.Consumer():
			resp, err := fu.Response()
			if err != nil {
				r.logger.Errorf("")
				continue
			}
			if resp.Term > r.getCurrentTerm() {
				r.leaderLease(resp.Term, fr)
				return
			}
			fr.setLastContact()
			if resp.Succ {
				r.updateLatestCommit(fr, fu.Request().Entries)
			} else {
				fr.setNextIndex(Max(1, Min(fr.getNextIndex()-1, resp.LatestIndex)))
			}
		}

	}
}
func (r *Raft) pipelineReplicateTo(fr *replication, pipeline AppendEntryPipeline) (stop bool) {
	req, err := r.buildAppendEntryReq(fr, r.getLatestIndex())
	if err != nil {
		r.logger.Errorf("")
		return true
	}
	_, err = pipeline.AppendEntries(req)
	if err != nil {
		r.logger.Errorf("")
		return true
	}
	if n := len(req.Entries); n > 0 {
		fr.setNextIndex(req.Entries[n-1].Index + 1)
	}
	return
}
func (r *Raft) pipelineReplicateHelper(fr *replication) (stop bool) {
	var (
		ticker                   = time.NewTicker(r.Conf().CommitTimeout)
		finishCh, pipelineStopCh = make(chan struct{}), make(chan struct{})
	)
	defer ticker.Stop()
	pipeline, err := r.rpc.AppendEntryPipeline(Ptr(fr.peer.Get()))
	if err != nil {
		return
	}
	r.goFunc(func() {
		r.processPipelineResult(fr, pipeline, finishCh, pipelineStopCh)
	})

	for stop {
		select {
		case shouldSend := <-fr.stop:
			if shouldSend {
				r.pipelineReplicateTo(fr, pipeline)
			}
			stop = true
		case <-ticker.C:
			stop = r.pipelineReplicateTo(fr, pipeline)
		case fu := <-fr.trigger:
			stop = r.pipelineReplicateTo(fr, pipeline)
			if fu != nil {
				if stop {
					fu.fail(errors.New("replication failed"))
				} else {
					fu.success()
				}
			}
		}
	}
	close(finishCh)
	select {
	case <-pipelineStopCh:
	case <-r.shutDown.C:
	}
	return
}

// replicate 复制到制定的跟随者，先短连接（可以发送快照），后长链接
func (r *Raft) replicate(fr *replication) {
	for stop := r.replicateHelper(fr); !stop; stop = r.replicateHelper(fr) {
		stop = r.pipelineReplicateHelper(fr)
	}
	close(fr.heartBeatStop)
	<-fr.heartBeatDone
	close(fr.done)
}

// reloadReplication 重新加载跟随者的复制、心跳线程
func (r *Raft) reloadReplication() {
	var (
		set   = map[ServerID]bool{}
		index = r.getLatestIndex()
	)
	index++
	// 开启新的跟随者线程
	for _, server := range r.getLatestConfiguration() {
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
			nextIndex:     index,
			peer:          NewLockItem(server),
			stop:          make(chan bool),
			heartBeatStop: make(chan struct{}),
			notifyCh:      make(chan struct{}),
			done:          make(chan struct{}),
			trigger:       make(chan *defaultDeferResponse),
			lastContact:   NewLockItem[time.Time](),
			notify:        NewLockItem(map[*verifyFuture]struct{}{}),
		}
		r.leaderState.replicate[server.ID] = fr
		r.goFunc(func() {
			r.heartBeat(fr)
		}, func() {
			r.replicate(fr)
		})
	}
	// 删除已经不在集群的跟随者线程
	for _, rep := range r.leaderState.replicate {
		id := rep.peer.Get().ID
		if set[id] {
			continue
		}
		rep.notifyAll(false)
		// 删除
		delete(r.leaderState.replicate, id)
		rep.stop <- true // 尽力复制
		close(rep.stop)
	}
}

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
		now          = time.Now()
		quorumSize   = r.quorumSize()
		leaseTimeout = r.Conf().LeaderLeaseTimeout
	)
	for _, peer := range r.getLatestConfiguration() {
		if !peer.isVoter() {
			continue
		}
		if peer.ID == r.localInfo.ID {
			quorumSize--
			continue
		}
		diff := now.Sub(r.leaderState.replicate[peer.ID].lastContact.Get())
		if diff <= leaseTimeout {
			quorumSize--
			maxDiff = Max(diff, maxDiff)
		} else {
			if diff < 3*leaseTimeout {
				r.logger.Warn("failed to contact", "server-id", peer.ID, "time", diff)
			} else {
				r.logger.Debug("failed to contact", "server-id", peer.ID, "time", diff)
			}
		}

	}
	return quorumSize <= 0, maxDiff
}

// broadcastReplicate 通知所有副本强制进行复制
func (r *Raft) broadcastReplicate() {
	for _, repl := range r.leaderState.replicate {
		asyncNotify(repl.trigger)
	}
}

// applyLog 想本地提交日志然后通知复制到跟随者
func (r *Raft) applyLog(future []*LogFuture) {
	var (
		index = r.getLatestIndex()
		term  = r.getCurrentTerm()
		logs  = make([]*LogEntry, 0, len(future))
		now   = time.Now()
	)
	for _, fu := range future {
		index++
		fu.log.Index = index
		fu.log.Term = term
		fu.log.CreatedAt = now
		log := fu.log
		logs = append(logs, log)
	}
	err := r.logStore.SetLogs(logs)
	if err != nil {
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

// leaderCommit 只在 cycleLeader 中调用，将所有可提交的日志提交到状态机
func (r *Raft) leaderCommit() (stepDown bool) {
	var (
		oldCommitIndex = r.getCommitIndex()
		newCommitIndex = r.leaderState.getCommitIndex()
		readyFuture    = map[uint64]*LogFuture{}
		readElem       []*list.Element
	)

	r.logger.Infof("leader commit ori :%d,cur:%d", oldCommitIndex, newCommitIndex)
	r.setCommitIndex(newCommitIndex)
	if r.cluster.latestIndex > oldCommitIndex && r.cluster.latestIndex <= newCommitIndex {
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
	if len(readElem) > 0 {
		r.applyLogToFsm(newCommitIndex, readyFuture)
	}
	for _, element := range readElem {
		r.leaderState.inflight.Remove(element)
	}

	return false
}

// apiProcessRestore 从本地应用快照
func (r *Raft) apiProcessRestore(fu *restoreFuture) {

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

func (r *Raft) clacNewConfiguration(req *configurationChangeRequest) (newConfiguration Configuration, err error) {

	switch req.command {
	case updateServer:
		found := false
		for _, server := range r.getLatestConfiguration() {
			if server.ID == req.peer.ID {
				newConfiguration.Servers = append(newConfiguration.Servers, req.peer)
			} else {
				newConfiguration.Servers = append(newConfiguration.Servers, server)
			}
		}
		if !found {
			return Configuration{}, errors.New("not found")
		}
	case addServer:
		for _, server := range r.getLatestConfiguration() {
			if server.ID == req.peer.ID {
				return Configuration{}, errors.New("peer duplicate")
			}
			newConfiguration.Servers = append(newConfiguration.Servers, server)
		}
		newConfiguration.Servers = append(newConfiguration.Servers, req.peer)
	case removeServer:
		found := false
		for _, server := range r.getLatestConfiguration() {
			if server.ID == req.peer.ID {
				continue
			} else {
				newConfiguration.Servers = append(newConfiguration.Servers, server)
			}
		}
		if !found {
			return Configuration{}, errors.New("not found")
		}

	}

	return
}

func (r *Raft) cycleLeader() {
	r.logger.Debug("cycle leader ", r.leaderInfo.Get().ID)
	r.setupLeaderState()
	r.reloadReplication()
	defer func() {
		r.logger.Info("leave leader")
		r.stopReplication()
		r.clearLeaderState()
		// TODO 注意下，是否需要调用 setLastContact 避免跟随者错误处理逻辑
	}()
	// 提交一个空日志，用于确认 commitIndex
	future := &LogFuture{log: &LogEntry{Type: LogNoop}}
	future.init()
	r.applyLog([]*LogFuture{future})
	leaderLeaseTimeout := time.After(r.Conf().LeaderLeaseTimeout)

	leaderLoop := func() (stop bool) {
		select {
		case <-r.shutDown.C:
			r.onShutDown()
			r.logger.Debug("shut down")
			return true
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case cmd := <-r.commandCh:
			r.processCommand(cmd)
		case <-r.commitCh:
			if stepDown := r.leaderCommit(); stepDown {
				// TODO 更新 step down 策略
				r.setShutDown()
			}
		case <-leaderLeaseTimeout:
			if leader, maxDiff := r.checkLeadership(); leader {
				leaderLeaseTimeout = time.After(Max(r.Conf().LeaderLeaseTimeout-maxDiff, minCheckInterval))
			} else {
				r.logger.Infof("leader ship check fail, term :%d ,step down", r.getCurrentTerm())
				r.setFollower()
			}
		case <-r.leaderState.stepDown:
			r.logger.Infof("leader ship step down, term :%d", r.getCurrentTerm())
			r.setFollower()
		}
		return
	}
	r.cycle(Leader, leaderLoop)
}

func (r *Raft) setLatestLog(term, index uint64) {
	r.lastEntry.Action(func(t *lastEntry) {
		t.log = lastLog{index: index, term: term}
	})
}
func (r *Raft) getLatestLog() (term, index uint64) {
	entry := r.lastEntry.Get()
	return entry.log.term, entry.log.index
}
func (r *Raft) getLatestSnapshot() (term, index uint64) {
	entry := r.lastEntry.Get()
	return entry.snapshot.term, entry.snapshot.index
}
func (r *Raft) setLatestSnapshot(term, index uint64) {
	r.lastEntry.Action(func(t *lastEntry) {
		t.snapshot = lastLog{index: index, term: term}
	})
}
func (r *Raft) getLatestConfiguration() []ServerInfo {
	return r.cluster.latest.Servers
}
func (r *Raft) inConfiguration(id ServerID) bool {
	for _, server := range r.cluster.latest.Servers {
		if server.ID == id {
			return true
		}
	}
	return false
}

func (r *Raft) canVote(id ServerID) bool {
	for _, serverInfo := range r.getLatestConfiguration() {
		if serverInfo.ID == id {
			return serverInfo.isVoter()
		}
	}
	return false
}

func (r *Raft) quorumSize() (c uint) {
	for _, server := range r.getLatestConfiguration() {
		if server.Suffrage == Voter {
			c++
		}
	}
	return c>>1 + 1
}

func (r *Raft) launchElection() (result chan *voteResult) {
	var (
		list                = r.getLatestConfiguration()
		lastTerm, lastIndex = r.getLatestEntry()
		header              = r.buildRPCHeader()
		currentTerm         = r.getCurrentTerm()
	)
	result = make(chan *voteResult, len(list))
	for _, info := range list {
		if info.ID == r.localInfo.ID {
			if err := r.kvStore.Set(KeyLastVoteFor, Str2Bytes(string(info.ID))); err != nil {
				r.logger.Errorf("launchElection vote for self errorx :%s", err)
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
					return
				}
				result <- &voteResult{
					VoteResponse: resp,
					ServerID:     info.ID,
				}
			})

		}
	}
	return
}
