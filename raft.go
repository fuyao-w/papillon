package papillon

import (
	"container/list"
	"fmt"
	. "github.com/fuyao-w/common-util"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
	"time"
)

type (
	Raft struct {
		commitIndex   uint64                  // 集群已提交的日志，初始化为 0 只有再提交过日志后才可以更新
		lastApplied   uint64                  // 已提交给状态机的最新 index , 注意：不代表状态机已经应用
		currentTerm   uint64                  // 当前任期，需要持久化
		configuration *AtomicVal[ClusterInfo] // 集群配置的副本
		conf          *AtomicVal[*Config]     // 参数配置信息
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
		readOnly      readOnly                // 跟踪只读查询的请求
		//-----API--------------------
		apiSnapshotBuildCh   chan *apiSnapshotFuture // 生成快照
		apiSnapshotRestoreCh chan *userRestoreFuture // 重新应用快照的时候不能接收新的日志，需要从 runState 线程触发
		apiLogApplyCh        chan *LogFuture         // 日志提交请求，由于需要支持批量提交，所以单独提出来
		commandCh            chan *command           // 对节点发起的命令，包括领导权验证等
		stateChangeCh        chan *StateChange       // 状态切换时的通知
		//-----组件------
		rpc           RpcInterface  // RPC 组件
		kvStore       KVStorage     // 任期、投票信息持久化组件
		logStore      LogStore      // 日志持久化组件
		snapshotStore SnapshotStore // 快照组件
	}
	lastEntry struct {
		snapshot lastLog // 快照中存的最新日志
		log      lastLog // LogStore 中的最新日志
	}
	lastLog struct {
		index uint64
		term  uint64
	}
	// leaderState 领导人上下文
	leaderState struct {
		commitIndex        uint64                    // 通过计算副本得出的已提交索引，只能由新日志提交触发更新
		startIndex         uint64                    // 记录任期开始时的最新一条索引，防止在日志提交的时候发生 commit index 回退
		leadershipTransfer uint64                    // 是否发生领导权转移 1 ：是 ，0 ：否
		matchIndex         map[ServerID]uint64       // 每个跟随者对应的已复制的 index
		replicate          map[ServerID]*replication // 所有的跟随者
		inflight           *list.List                // 等待提交并应用到状态机的 LogFuture
		stepDown           chan ServerID             // 领导人下台通知
		matchLock          sync.Mutex
	}
	StateChange struct {
		Before, After State
	}
	// readOnly 跟踪只读请求，确保状态机已经应用完制定索引，用于实现线性一致性读
	readOnly struct {
		notifySet map[*readOnlyFuture]struct{} // 待响应的只读请求，只由状态机线程处理
		request   chan *readOnlyFuture         // 只读请求
	}
)

const (
	Voter Suffrage = iota
	NonVoter
)
const (
	unknown     = "Unknown"
	voterStr    = "Voter"
	nonVoterStr = "NonVoter"
)

type (
	ServerAddr string
	ServerID   string
	Suffrage   int // 是否有选举权，枚举： Voter NonVoter
	// ServerInfo 节点的地址信息
	ServerInfo struct {
		Suffrage Suffrage
		Addr     ServerAddr
		ID       ServerID
	}
)

// observe 将请求添加进 pending 集合
func (r *readOnly) observe(future *readOnlyFuture) {
	r.notifySet[future] = struct{}{}
}

// notify 回调集合中达到索引位置的请求
func (r *readOnly) notify(index uint64) {
	for future := range r.notifySet {
		if future.readIndex <= index {
			future.responded(index, nil)
			delete(r.notifySet, future)
		}
	}
	// 释放内存
	if len(r.notifySet) == 0 {
		r.notifySet = map[*readOnlyFuture]struct{}{}
	}
}

func (s *Suffrage) MarshalText() (text []byte, err error) {
	if suffrage := s.String(); suffrage == unknown {
		return nil, fmt.Errorf("unknown suffrage :%d", *s)
	} else {
		return Str2Bytes(suffrage), nil
	}
}
func (s *Suffrage) UnmarshalText(text []byte) error {
	switch Bytes2Str(text) {
	case voterStr:
		*s = Voter
	case nonVoterStr:
		*s = NonVoter
	default:
		return fmt.Errorf("unknown suffrage :%d", *s)
	}
	return nil
}

func (s *Suffrage) String() string {
	switch *s {
	case Voter:
		return voterStr
	case NonVoter:
		return nonVoterStr
	default:
		return unknown
	}
}

func (r *Raft) Conf() *Config {
	return r.conf.Load()
}
func (r *Raft) setLatestConfiguration(index uint64, configuration ClusterInfo) {
	r.cluster.setLatest(index, configuration)
	r.configuration.Store(configuration)
}
func (r *Raft) setCommitConfiguration(index uint64, configuration ClusterInfo) {
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
func (r *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}
func (r *Raft) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *Raft) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}
func (l *leaderState) setupLeadershipTransfer(status bool) (succ bool) {
	old, newVal := uint64(1), uint64(0)
	if status {
		old = 0
		newVal = 1
	}
	return atomic.CompareAndSwapUint64(&l.leadershipTransfer, old, newVal)
}
func (l *leaderState) getLeadershipTransfer() (status bool) {
	return atomic.LoadUint64(&l.leadershipTransfer) == 1
}

func (s *ServerInfo) isVoter() bool {
	return s.Suffrage == Voter
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

func (r *Raft) setState(state State) {
	before := r.GetState()
	r._setState(state)
	overrideNotify(r.stateChangeCh, &StateChange{
		Before: before,
		After:  state,
	})
}
func (r *Raft) setFollower() {
	r.setState(Follower)
	r.run = r.cycleFollower
}
func (r *Raft) setCandidate() {
	r.setState(Candidate)
	r.run = r.cycleCandidate
}
func (r *Raft) setLeader() {
	r.setState(Leader)
	r.leaderInfo.Action(func(t *ServerInfo) {
		t.ID = r.localInfo.ID
		t.Addr = r.localInfo.Addr
	})
	r.run = r.cycleLeader
}

// setShutDown 只能由 Raft.ShutDown 调用
func (r *Raft) setShutDown() {
	r.setState(ShutDown)
	r.run = nil
	r.onShutDown()
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
func (r *Raft) getLatestCluster() []ServerInfo {
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
	for _, serverInfo := range r.getLatestCluster() {
		if serverInfo.ID == id {
			return serverInfo.isVoter()
		}
	}
	return false
}
