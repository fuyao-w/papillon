package papillon

import (
	"errors"
	"fmt"
	. "github.com/fuyao-w/common-util"
	"io"
	"sync"
	"time"
)

type (
	reject interface {
		reject(state State)
	}

	commandEnum int
	command     struct {
		enum     commandEnum
		callback reject
	}
	commandMap map[commandEnum]map[State]func(*Raft, any)
)

// testFuture 测试用
type testFuture struct {
	deferResponse[string]
}

const (
	commandTest commandEnum = iota + 1 // 用于单测
	commandClusterGet
	commandBootstrap
	commandLogApply
	commandSnapshotRestore
	commandClusterChange
	commandVerifyLeader
	commandConfigReload
	commandLeadershipTransfer
	commandRaftStats
	commandReadIndex
)

var channelCommand commandMap

func init() {
	channelCommand = commandMap{
		commandTest: {
			Leader: func(raft *Raft, i interface{}) {
				i.(*testFuture).responded("succ", nil)
			},
		},
		commandClusterGet: {
			Leader:    (*Raft).processClusterGet,
			Candidate: (*Raft).processClusterGet,
			Follower:  (*Raft).processClusterGet,
		},
		commandLogApply: {
			Leader: (*Raft).processLogApply,
		},
		commandBootstrap: {
			Follower: (*Raft).processBootstrap,
		},
		commandSnapshotRestore: {
			Leader: (*Raft).processSnapshotRestore,
		},
		commandLeadershipTransfer: {
			Leader: (*Raft).processLeadershipTransfer,
		},
		commandConfigReload: {
			Follower:  (*Raft).processReloadConfig,
			Leader:    (*Raft).processReloadConfig,
			Candidate: (*Raft).processReloadConfig,
		},
		commandVerifyLeader: {
			Leader: (*Raft).processVerifyLeader,
		},
		commandClusterChange: {
			Leader: (*Raft).processClusterChange,
		},
		commandRaftStats: {
			Follower:  (*Raft).processRaftStats,
			Candidate: (*Raft).processRaftStats,
			Leader:    (*Raft).processRaftStats,
		},
		commandReadIndex: {
			Leader: (*Raft).processReadIndex,
		},
	}
}
func (d *deferResponse[_]) reject(state State) {
	if state == ShutDown {
		d.fail(ErrShutDown)
		return
	}
	d.fail(fmt.Errorf("current state %s can't process", state.String()))
}

func (c *Config) reject(State) {}

// processCommand 处理 raft 的命令，需要在 channelCommand 注册 commandEnum + 对应 State 的处理函数，
// 如果该请求当前没有对应的 State 处理函数，则会强制转成 reject interface 执行拒绝回调
// 如果未在 channelCommand 注册，则会直接 panic
func (r *Raft) processCommand(cmd *command) {
	cc, ok := channelCommand[cmd.enum]
	if !ok {
		panic(fmt.Sprintf("command type :%d not register", cmd.enum))
	}
	state := r.GetState()
	f, ok := cc[state]
	if ok {
		f(r, cmd.callback)
	} else {
		cmd.callback.reject(state)
	}
}

// processHeartBeatTimeout 处理心跳超时判断，只在 cycleFollower 中调用！
func (r *Raft) processHeartBeatTimeout(warn func(...interface{})) {
	tm := r.Conf().HeartbeatTimeout
	r.heartbeatTimeout = randomTimeout(tm)

	if time.Now().Before(r.getLastContact().Add(tm)) {
		return
	}
	oldLeaderInfo := r.leaderInfo.Get()
	r.clearLeaderInfo() // 必须清掉，否则在投票时候选人会被拒绝
	// 区分下不能开始选举的原因
	switch {
	case r.cluster.latestIndex == 0:
		warn("no cluster members")
	case r.cluster.stable() && !r.canVote(r.localInfo.ID):
		warn("no part of stable ClusterInfo, aborting election")
	case r.canVote(r.localInfo.ID):
		warn("heartbeat abortCh reached, starting election", "last-leader-addr", oldLeaderInfo.Addr, "last-leader-id", oldLeaderInfo.ID)
		r.setCandidate()
	default:
		warn("heartbeat abortCh reached, not part of a stable ClusterInfo or a non-voter, not triggering a leader election")
	}
}

// processClusterGet 用于内部从主线程获取集群配置
func (r *Raft) processClusterGet(i interface{}) {
	fu := i.(*clusterGetFuture)
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	fu.responded(r.cluster.Clone(), nil)
}

// processLogApply 只在 cycleLeader 中调用，将日志提交到本次，并通知跟随者进行复制
func (r *Raft) processLogApply(item interface{}) {
	var (
		fu      = item.(*LogFuture)
		futures = []*LogFuture{fu}
	)
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}

	if r.Conf().ApplyBatch {
	BREAK:
		for i := uint64(0); i < r.Conf().MaxAppendEntries; i++ {
			select {
			case applyFu := <-r.apiLogApplyCh:
				futures = append(futures, applyFu)
			default:
				break BREAK
			}
		}
	}

	r.applyLog(futures...)
}

// processBootstrap 引导集群启动，节点必须是干净的（日志、快照、任期都是 0 ）
// 将配置作为第一条日志存储到本地，然后当前节点就可以发起选举，并最终将日志复制到集群副本中
func (r *Raft) processBootstrap(item interface{}) {
	var (
		fu = item.(*bootstrapFuture)
	)
	if !fu.clusterInfo.hasVoter(r.localInfo.ID) {
		fu.fail(ErrNotVoter)
		return
	}
	if !validateConfiguration(fu.clusterInfo) {
		fu.fail(ErrIllegalConfiguration)
		return
	}
	exist, err := r.hasExistTerm()
	if err != nil {
		fu.fail(err)
		return
	}
	if exist {
		fu.fail(ErrCantBootstrap)
		return
	}
	entry := &LogEntry{
		Index:     1,
		Term:      1,
		Data:      EncodeCluster(fu.clusterInfo),
		Type:      LogCluster,
		CreatedAt: time.Now(),
	}
	if err = r.logStore.SetLogs([]*LogEntry{entry}); err != nil {
		r.logger.Errorf("processBootstrap|SetLogs err:%s", err)
		fu.fail(err)
		return
	}
	r.setCurrentTerm(1)
	r.setLatestLog(1, 1)
	r.saveConfiguration(entry)
	fu.success()
}

// processSnapshotRestore 从外部接收一个快照并应用，但是不进行日志压缩，外部看起来就像多执行了一些命令一样
// 当前节点会基于最新的配置和日志索引创建快照，并且会把最新索引 + 1 。这样做的目的是更快的触发快照发送
// 所有的集群副本最终在状态机中应用了两个命令集合：一个原有的 + 从外部引入的
// 注意：这个请求执行后的最新快照索引和最新日志索引均是最新的。在下一次完整快照生成之前，如果我们重启节点，
// 将会丢失最新快照索引 SnapshotMeta.Index 之前的所有日志。该命令仅应在崩溃恢复时使用
// TODO 可以在崩溃恢复后的下一次快照检查强制生成快照，尽量保证日志的安全
func (r *Raft) processSnapshotRestore(item interface{}) {
	var (
		fu = item.(*userRestoreFuture)
	)
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	if !r.cluster.stable() {
		fu.fail(fmt.Errorf("cannot restore snapshot now, wait until the clusterInfo entry at %v has been applied (have applied %v)",
			r.cluster.latestIndex, r.cluster.commitIndex))
		return
	}
	for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
		e.Value.(*LogFuture).fail(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}
	index := r.getLatestIndex() + 1
	sink, err := r.snapshotStore.Create(SnapShotVersionDefault, index, r.getCurrentTerm(),
		r.cluster.commit, r.cluster.commitIndex, r.rpc)
	if err != nil {
		fu.fail(err)
		return
	}
	defer fu.reader.Close()
	written, err := io.Copy(sink, fu.reader)
	if err != nil {
		fu.fail(err)
		sink.Cancel()
		return
	}
	if written != fu.meta.Size {
		fu.fail(fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", written, fu.meta.Size))
		sink.Cancel()
		return
	}
	if err := sink.Close(); err != nil {
		fu.fail(err)
		return
	}
	r.logger.Info("copied to local snapshot", "bytes", written)

	fsmFu := &restoreFuture{ID: sink.ID()}
	fsmFu.init()
	select {
	case r.fsmRestoreCh <- fsmFu:
	case <-r.shutDown.C:
		fu.fail(ErrShutDown)
		return
	}
	if _, err = fsmFu.Response(); err != nil {
		panic(fmt.Errorf("failed to restore snapshot: %v", err))
	}

	r.setLatestLog(r.getCurrentTerm(), index)
	r.setLatestSnapshot(r.getCurrentTerm(), index)
	r.setLastApply(index)
	r.logger.Info("restored user snapshot", "index", index)
	fu.success()
}

// pickLatestPeer 寻找进度最新的跟随者
func (r *Raft) pickLatestPeer() *replication {
	var (
		latest      *replication
		latestIndex uint64
	)
	for _, info := range r.getLatestCluster() {
		if !info.isVoter() {
			continue
		}
		fr, ok := r.leaderState.replicate[info.ID]
		if !ok {
			continue
		}
		if fr.getNextIndex() > latestIndex {
			latestIndex = fr.getNextIndex()
			latest = fr
		}
	}
	return latest
}

// leadershipTransfer 进行领导权转移，必须等到跟随者的进度赶上当前节点才可以发起请求
func (r *Raft) leadershipTransfer(fr *replication) error {
	var (
		rounds = r.Conf().LeadershipCatchUpRounds
		i      uint
	)

	for ; i < rounds && r.getLatestIndex() > fr.getNextIndex(); i++ {
		fu := new(defaultDeferResponse)
		fu.init()
		select {
		case <-r.shutDown.C:
			return ErrShutDown
		case <-fr.stop:
			return nil
		case fr.trigger <- fu:
			if _, err := fu.Response(); err != nil {
				return err
			}
		}
	}
	if i > rounds {
		return errors.New("reach the maximum number of catch-up rounds")
	}
	resp, err := r.rpc.FastTimeout(Ptr(fr.peer.Get()), &FastTimeoutRequest{
		RPCHeader:          r.buildRPCHeader(),
		Term:               r.getCurrentTerm(),
		LeaderShipTransfer: true,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New("peer reject time out")
	}
	return nil
}

// processLeadershipTransfer 处理领导权转移，只能由领导人执行
// 首先挑选出最新的跟随者，然后停止接收日志提交请求并等待跟随者赶上当前领导人的进度，
// 最后调用 RpcInterface.FastTimeout 通知其快速超时发起选举
// 追赶进度时将等待固定的轮次，超过次数则返回失败
func (r *Raft) processLeadershipTransfer(item interface{}) {
	var (
		fu = item.(*leadershipTransferFuture)
		fr *replication
	)
	if id := fu.Peer.ID; len(id) > 0 {
		fr = r.leaderState.replicate[id]
	} else {
		fr = r.pickLatestPeer()
	}
	if fr == nil {
		fu.fail(errors.New("no suitable peer"))
		return
	}
	if !r.leaderState.setupLeadershipTransfer(true) {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	r.logger.Info("leader transfer choice", fr.peer.Get().ID)
	go func() {
		fu.responded(nil, r.leadershipTransfer(fr))
		r.leaderState.setupLeadershipTransfer(false)
	}()
}

// processReloadConfig 验证当前节点是否还是领导人
func (r *Raft) processReloadConfig(item interface{}) {
	var (
		oldConf = item.(*Config)
		newConf = r.Conf()
	)
	switch r.GetState() {
	case Follower:
		if oldConf.HeartbeatTimeout != newConf.HeartbeatTimeout {
			r.heartbeatTimeout = time.After(0)
		}
	case Leader:
		if oldConf.HeartbeatTimeout != newConf.HeartbeatTimeout {
			for _, replication := range r.leaderState.replicate {
				asyncNotify(replication.notifyCh)
			}
		}

	case Candidate:
		if oldConf.ElectionTimeout != newConf.ElectionTimeout {
			r.electionTimeout = randomTimeout(newConf.ElectionTimeout)
		}
	default:
		panic(fmt.Errorf("except state :%d ", r.GetState()))
	}
}

// processVerifyLeader 验证当前节点是否还是领导人
func (r *Raft) processVerifyLeader(item interface{}) {
	var (
		fu = item.(*verifyFuture)
	)
	fu.quorumCount = r.quorumSize()
	fu.voteGranted = 0
	fu.reportOnce = new(sync.Once)
	fu.stepDown = r.leaderState.stepDown
	// 先投自己一票
	fu.vote(true)
	for _, repl := range r.leaderState.replicate {
		repl.observe(fu)
		asyncNotify(repl.notifyCh)
	}
}

// processClusterChange 集群配置更新，只能等到上次更新已提交后才可以开始新的变更
func (r *Raft) processClusterChange(item interface{}) {
	var (
		fu = item.(*clusterChangeFuture)
	)
	if !r.cluster.stable() {
		fu.fail(errors.New("no stable cluster"))
		return
	}
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	if fu.req.pervIndex > 0 && r.cluster.latestIndex != fu.req.pervIndex {
		fu.fail(errors.New("clusterInfo index not match"))
		return
	}
	newConfiguration, err := r.clacNewConfiguration(fu.req)
	if err != nil {
		fu.fail(err)
		return
	}
	logFu := &LogFuture{
		log: &LogEntry{
			Data: EncodeCluster(newConfiguration),
			Type: LogCluster,
		},
	}
	logFu.init()
	r.applyLog(logFu)
	r.setLatestConfiguration(logFu.Index(), newConfiguration)
	r.onConfigurationUpdate()
	r.reloadReplication()
	fu.success()
}

// processRaftStats 获取 Raft 状态上下文
func (r *Raft) processRaftStats(item interface{}) {
	var (
		fu = item.(*deferResponse[map[string]interface{}])
	)
	formatTime := func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return time.Now().Sub(t).String()
	}
	fu.responded(func() map[string]interface{} {
		snapshotTerm, snapshotIndex := r.getLatestSnapshot()
		logTerm, logIndex := r.getLatestLog()
		leader := r.leaderInfo.Get()
		var m = map[string]interface{}{
			"state":                 r.state.String(),
			"current_term":          r.getCurrentTerm(),
			"latest_log_term":       logTerm,
			"latest_log_index":      logIndex,
			"latest_snapshot_term":  snapshotTerm,
			"latest_snapshot_index": snapshotIndex,
			"latest_cluster_index":  r.cluster.latestIndex,
			"commit_cluster_index":  r.cluster.commitIndex,
			"commit_cluster":        r.getLatestCluster(),
			"commit_index":          r.getCommitIndex(),
			"leader_id":             leader.ID,
			"leader_addr":           leader.Addr,
			"fsm_pending":           len(r.fsmApplyCh) + len(r.fsmRestoreCh),
		}
		switch r.GetState() {
		case Leader:
			var (
				lastContact = make(map[ServerID]string, len(r.leaderState.replicate))
				nextIndex   = make(map[ServerID]uint64, len(r.leaderState.replicate))
			)
			for id, repl := range r.leaderState.replicate {
				lastContact[id] = formatTime(repl.lastContact.Get())
				nextIndex[id] = repl.getNextIndex()
			}
			m["last_contact"] = lastContact
			m["next_index"] = nextIndex
			m["num_peers"] = func() (count int) {
				if !r.canVote(r.localInfo.ID) {
					return 0
				}
				for _, server := range r.getLatestCluster() {
					if server.isVoter() {
						count++
					}
				}
				return
			}()
		default:
			m["last_contact"] = formatTime(r.getLastContact())
		}
		return m
	}(), nil)
}

// processReadIndex 获取 Raft 状态上下文
func (r *Raft) processReadIndex(item interface{}) {
	var (
		fu = item.(*deferResponse[uint64])
	)
	commitIndex := r.getCommitIndex()
	if commitIndex == 0 {
		fu.fail(ErrEmptyCommit)
		return
	}
	fu.responded(commitIndex, nil)
}
