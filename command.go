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
	commandTyp int
	command    struct {
		typ  commandTyp
		item interface{}
	}
	commandMap map[commandTyp]map[State]func(*Raft, interface{})
)
type testFuture struct {
	deferResponse[string]
}

const (
	commandTest commandTyp = iota + 1 // 用于单测
	commandClusterGet
	commandBootstrap
	commandLogApply
	commandSnapshotRestore
	commandClusterChange
	commandVerifyLeader
	commandConfigReload
	commandLeadershipTransfer
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
			Leader: (*Raft).processClusterGet,
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
	}
}

func (r *Raft) processCommand(cmd *command) {
	cc, ok := channelCommand[cmd.typ]
	if !ok {
		panic(fmt.Sprintf("command type :%d not register", cmd.typ))
	}
	state := r.state.Get()
	f, ok := cc[state]
	if ok {
		f(r, cmd.item)
	} else {
		cmd.item.(reject).reject(state)
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
		batch   = r.Conf().ApplyBatch
	)
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
BREAK:
	for i := 0; i < batch; i++ {
		select {
		case applyFu := <-r.apiLogApplyCh:
			futures = append(futures, applyFu)
		default:
			break BREAK
		}
	}
	r.applyLog(futures)
}

// processBootstrap 引导集群启动，节点必须是干净的（日志、快照、任期都是 0 ）
// 将配置作为第一条日志存储到本地，然后当前节点就可以发起选举，并最终将日志复制到集群副本中
func (r *Raft) processBootstrap(item interface{}) {
	var (
		fu = item.(*bootstrapFuture)
	)
	if !validateConfiguration(fu.configuration) {
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
		Data:      EncodeConfiguration(fu.configuration),
		Type:      LogConfiguration,
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
// 将会丢失最新快照索引 SnapShotMeta.Index 之前的所有日志。该命令仅应在崩溃恢复时使用
// TODO 可以在崩溃恢复后的下一次快照检查强制生成快照，尽量保证日志的安全
func (r *Raft) processSnapshotRestore(item interface{}) {
	var (
		fu = item.(*userRestoreFuture)
	)
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	if r.cluster.commitIndex != r.cluster.latestIndex {
		fu.fail(fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			r.cluster.latestIndex, r.cluster.commitIndex))
		return
	}
	for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
		e.Value.(*LogFuture).fail(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}
	index := r.getLatestIndex() + 1
	sink, err := r.snapshotStore.Create(SnapShotVersionDefault, index, r.getCurrentTerm(), r.cluster.latest, r.cluster.latestIndex, nil)
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
	r.setLastApplied(index)
	r.logger.Info("restored user snapshot", "index", index)
	fu.success()
}

// pickLatestPeer 寻找进度最新的跟随者
func (r *Raft) pickLatestPeer() *replication {
	var (
		latest      *replication
		latestIndex uint64
		rep         = r.leaderState.replicate
	)
	for _, info := range r.getLatestConfiguration() {
		if !info.isVoter() {
			continue
		}
		fr, ok := rep[info.ID]
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
	if i >= rounds {
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
	switch r.state.Get() {
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
		panic(fmt.Errorf("except state :%d ", r.state.Get()))
	}
}

// processVerifyLeader 验证当前节点是否还是领导人
func (r *Raft) processVerifyLeader(item interface{}) {
	var (
		fu = item.(*verifyFuture)
	)
	// 先计算自己一票
	fu.quorumCount = r.quorumSize()
	fu.voteGranted = 0
	fu.reportOnce = new(sync.Once)
	fu.stepDown = r.leaderState.stepDown
	fu.vote(true)
	for _, repl := range r.leaderState.replicate {
		repl.observe(fu)
		asyncNotify(repl.notifyCh)
	}
}

// processClusterChange 集群配置更新，只能等到上次更新已提交后才可以开始新的变更
func (r *Raft) processClusterChange(item interface{}) {
	var (
		fu = item.(*configurationChangeFuture)
	)
	if r.cluster.commitIndex != r.cluster.latestIndex {
		fu.fail(errors.New("no stable configuration"))
		return
	}
	if r.leaderState.getLeadershipTransfer() {
		fu.fail(ErrLeadershipTransferInProgress)
		return
	}
	if r.cluster.latestIndex != fu.req.pervIndex {
		fu.fail(errors.New("configuration index not match"))
		return
	}
	newConfiguration, err := r.clacNewConfiguration(fu.req)
	if err != nil {
		fu.fail(err)
		return
	}
	logFu := &LogFuture{
		log: &LogEntry{
			Data: EncodeConfiguration(newConfiguration),
			Type: LogCommand,
		},
	}
	logFu.init()
	r.applyLog([]*LogFuture{logFu})

	r.cluster.setLatest(logFu.Index(), newConfiguration)
	r.onConfigurationUpdate()
	r.reloadReplication()

}
