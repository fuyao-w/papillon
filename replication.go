package papillon

import (
	"errors"
	. "github.com/fuyao-w/common-util"
	"math"
	"sync/atomic"
	"time"
)

type (
	// replication 领导人复制时每个跟随者维护的上下文状态
	replication struct {
		failures                           int                                   // Raft.replicateTo 支持退避重试，放到这里用于只支持短连接情况
		peer                               *LockItem[ServerInfo]                 // 跟随者的 server 信息
		nextIndex                          *atomic.Uint64                        // 待复制给跟随者的下一条日志索引，初始化为领导人最新的日志索引
		heartBeatStop, heartBeatDone, done chan struct{}                         // 心跳停止、复制线程结束、pipeline 返回结果处理线程结束
		trigger                            chan *defaultDeferResponse            // 强制复制，不需要复制结果可以投递 nil
		notifyCh                           chan struct{}                         // 强制心跳
		stop                               chan bool                             // 复制停止通知，true 代表需要在停机前尽力复制
		lastContact                        *LockItem[time.Time]                  // 上次与跟随者联系的时间，用于计算领导权
		notify                             *LockItem[map[*verifyFuture]struct{}] // Raft.VerifyLeader 请求跟踪
		// allowPipeline 是否允许通过 Raft.pipelineReplicateHelper 复制，正常情况下先短连接复制，并可能发送快照，当跟随者追赶上进度后
		// 可以通过长链接进行复制
		allowPipeline bool
	}
)

func (fr *replication) getNextIndex() uint64 {
	return fr.nextIndex.Load()
}
func (fr *replication) setNextIndex(newNextIndex uint64) {
	fr.nextIndex.Store(newNextIndex)
}

// notifyAll 同步所有的 verifyFuture 验证结果，然后清空 notify
func (fr *replication) notifyAll(leadership bool) {
	fr.notify.Action(func(t *map[*verifyFuture]struct{}) {
		for v := range *t {
			v.vote(leadership)
		}
		*t = map[*verifyFuture]struct{}{}
	})
}

// observe 增加 Raft.VerifyLeader 的跟踪请求
func (fr *replication) observe(v *verifyFuture) {
	fr.notify.Action(func(t *map[*verifyFuture]struct{}) {
		(*t)[v] = struct{}{}
	})
}
func (fr *replication) setLastContact() {
	fr.lastContact.Set(time.Now())
}
func (fr *replication) getLastContact() time.Time {
	return fr.lastContact.Get()
}

// heartbeat 想跟随者发起心跳，跟随 replicate 关闭
func (r *Raft) heartbeat(fr *replication) {
	var (
		req = &AppendEntryRequest{
			RPCHeader: r.buildRPCHeader(),
			Term:      r.getCurrentTerm(),
		}
		failures int
	)
	defer close(fr.heartBeatDone)
	for {
		select {
		case <-fr.heartBeatStop:
			return
		case <-randomTimeout(r.Conf().HeartbeatTimeout / 10):
		case <-fr.notifyCh:
		}
		resp, err := r.rpc.AppendEntries(Ptr(fr.peer.Get()), req)
		if err != nil {
			failures++
			select {
			case <-time.After(exponentialBackoff(backoffBaseDuration, r.Conf().HeartbeatTimeout, failures, maxBackoffRounds)):
			case <-fr.heartBeatStop:
				return
			}
			r.logger.Errorf("AppendEntries :%s", err)
			continue
		}
		failures = 0
		fr.setLastContact()
		// 由于我们没有追加日志与校验 prev log， 所以 resp.Success 结果只会受任期校验影响，true 或者 false 都是 VerifyLeader 请求可信的
		fr.notifyAll(resp.Success)
	}
}

// sendLatestSnapshot 发送最新的快照
func (r *Raft) sendLatestSnapshot(fr *replication) (stop bool) {
	peer := fr.peer.Get()
	list, err := r.snapshotStore.List()
	if err != nil {
		return false
	}
	if len(list) == 0 {
		r.logger.Errorf("sendLatestSnapshot|snapshot not exist")
		return
	}
	latestID := list[0].ID
	meta, readCloser, err := r.snapshotStore.Open(latestID)
	if err != nil {
		r.logger.Errorf("sendLatestSnapshot|open :%s err :%s", latestID, err)
		return
	}
	defer func() {
		readCloser.Close()
	}()
	resp, err := r.rpc.InstallSnapShot(Ptr(fr.peer.Get()), &InstallSnapshotRequest{
		RPCHeader:    r.buildRPCHeader(),
		Term:         r.getCurrentTerm(),
		SnapshotMeta: meta,
	}, readCloser)
	if err != nil {
		r.logger.Errorf("sendLatestSnapshot|InstallSnapShot err :%s", err)
		return
	}
	if resp.Term > r.getCurrentTerm() {
		r.leaderLease(fr)
		return true
	}
	if resp.Success {
		fr.setNextIndex(meta.Index + 1)
		r.updateMatchIndex(peer.ID, meta.Index)
	}
	fr.setLastContact()
	fr.notifyAll(resp.Success)
	r.logger.Debug("update next index ", fr.getNextIndex(), peer.ID)
	return
}

// clacLatestIndex 计算复制到远端节点的最大索引
func clacLatestIndex(nextIndex uint64, latestIndex, maxAppendEntries uint64) uint64 {
	return Min(latestIndex, nextIndex+maxAppendEntries-1)
}

// clacNextIndex 计算 replication.nextIndex
func clacNextIndex(nextIndex, peerLatestIndex uint64) uint64 {
	return Max(1, Min(nextIndex-1, peerLatestIndex))
}

// replicateTo 负责短连接的跟随者复制，如果 replication.nextIndex 到 latestIndex 之前有空洞，则发送快照
// 短连接需要保证跟随者尽快追赶上进度，所以需要循环执行
func (r *Raft) replicateTo(fr *replication, latestIndex uint64) (stop bool) {
	for {
		req, err := r.buildAppendEntryReq(fr.getNextIndex(), latestIndex)
		if err != nil {
			if errors.Is(ErrNotFound, err) {
				return r.sendLatestSnapshot(fr)
			}
			r.logger.Errorf("buildAppendEntryReq err :%s ,latest index", err, latestIndex)
			return true
		}
		if len(req.Entries) > 0 {
			//r.logger.Debug("AppendEntries to id :", fr.peer.Get().ID, " next index:", fr.getNextIndex(),
			//	" latestIndex:", latestIndex)
		}
		resp, err := r.rpc.AppendEntries(Ptr(fr.peer.Get()), req)
		if err != nil {
			fr.failures++
			select {
			case <-fr.stop:
				return true
			case <-time.After(exponentialBackoff(backoffBaseDuration, time.Minute, fr.failures, math.MaxInt)):
			}
			r.logger.Errorf("AppendEntries :%s", err)
			return
		}
		fr.failures = 0
		if resp.Term > r.getCurrentTerm() {
			r.leaderLease(fr)
			r.logger.Info("replicate to leader Lease", fr.peer.Get().ID)
			return true
		}
		fr.setLastContact()
		if len(req.Entries) > 0 {
			//r.logger.Debug("replicate to id:", fr.peer.Get().ID, ",latestIndex:", latestIndex, ",result:", resp.Success,
			//	",peer latest index", resp.LatestIndex)
		}
		if resp.Success {
			r.updateLatestCommit(fr, req.Entries)
			fr.allowPipeline = true
		} else {
			fr.setNextIndex(clacNextIndex(fr.getNextIndex(), resp.LatestIndex))
		}
		// has more
		select {
		case <-fr.stop:
			return true
		default:
			if fr.getNextIndex() > latestIndex || stop {
				return
			}
		}
	}
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
				r.logger.Errorf("pipeline result err :%s, id :%s ", err, fr.peer.Get().ID)
				continue
			}
			if resp.Term > r.getCurrentTerm() {
				r.leaderLease(fr)
				return
			}
			fr.setLastContact()
			if resp.Success {
				r.updateLatestCommit(fr, fu.Request().Entries)
			} else {
				fr.setNextIndex(clacNextIndex(fr.getNextIndex(), resp.LatestIndex))
				r.logger.Debug("processPipelineResult", fr.getNextIndex(), resp.LatestIndex, fr.peer.Get().ID)
			}
		}

	}
}

// pipelineReplicateTo 执行长链接复制
func (r *Raft) pipelineReplicateTo(fr *replication, pipeline AppendEntryPipeline) (stop, hasMore bool) {
	req, err := r.buildAppendEntryReq(fr.getNextIndex(), r.getLatestIndex())
	if err != nil {
		r.logger.Errorf("pipelineReplicateTo|buildAppendEntryReq err:%s ,next index:%d  ,latest index :%d ,%s", err, fr.getNextIndex(), r.getLatestIndex(), fr.peer.Get().ID)
		return true, false
	}
	_, err = pipeline.AppendEntries(req)
	if err != nil {
		r.logger.Errorf("pipelineReplicateTo|AppendEntries err:%s", err)
		return true, false
	}
	// 因为请求结果在 processPipelineResult 函数线程异步接收，这里需要立即更新 nextIndex 防止重复发送
	if n := len(req.Entries); n > 0 {
		fr.setNextIndex(req.Entries[n-1].Index + 1)
		r.logger.Debug("setNextIndex ", fr.getNextIndex(), fr.peer.Get().ID)
		return false, r.getLatestIndex()-fr.getNextIndex() > 0
	}
	return
}

// pipelineReplicateHelper 负责长链接复制，仅当确认跟随者上一次已经追赶上进度后才会执行，不具备快照发送能力
func (r *Raft) pipelineReplicateHelper(fr *replication) {
	var (
		finishCh, pipelineStopCh = make(chan struct{}), make(chan struct{})
		hasMore                  bool
		timeout                  = r.Conf().CommitTimeout
		ticker                   = time.NewTimer(timeout)
	)
	defer ticker.Stop()
	fr.allowPipeline = false
	peer := fr.peer.Get()
	pipeline, err := r.rpc.AppendEntryPipeline(&peer)
	if err != nil {
		r.logger.Error("append entry pipeline err :", err, " peer id :", peer.ID)
		return
	}
	r.goFunc(func() {
		r.processPipelineResult(fr, pipeline, finishCh, pipelineStopCh)
	})

	for stop := false; !stop; {
		select {
		case <-pipelineStopCh:
			stop = true
		case shouldSend := <-fr.stop:
			if shouldSend {
				r.pipelineReplicateTo(fr, pipeline)
			}
			stop = true
		case <-ticker.C: // trigger 有几率丢失通知(如果 rpc 延迟过大，可能会略过最新的通知)，所以通过定时任务作为补充
			stop, hasMore = r.pipelineReplicateTo(fr, pipeline)
			if hasMore { // 加速追赶
				ticker.Reset(0)
			} else {
				ticker.Reset(timeout)
			}
		case fu := <-fr.trigger:
			stop, _ = r.pipelineReplicateTo(fr, pipeline)
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
	r.logger.Info("pipelineReplicateHelper stop", peer.ID)
	return
}

// replicate 复制到制定的跟随者，先短连接（可以发送快照），后长链接
func (r *Raft) replicate(fr *replication) {
	for stop := false; !stop; {
		stop = r.replicateHelper(fr)
		if !stop && fr.allowPipeline {
			r.pipelineReplicateHelper(fr)
		}
	}
	close(fr.heartBeatStop)
	<-fr.heartBeatDone
	r.logger.Info("replicate ", fr.peer.Get().ID, " stop")
	close(fr.done)
}

// replicateHelper 短链复制的触发函数，通过定时器、channel 通知触发复制
func (r *Raft) replicateHelper(fr *replication) (stop bool) {
	var (
		ticker = time.NewTicker(r.Conf().CommitTimeout)
	)
	defer ticker.Stop()
	defer func() {
		r.logger.Info("replicateHelper end ,id:", fr.peer.Get().ID, ", stop:", stop)
	}()
	for !stop && !fr.allowPipeline {
		select {
		case shouldSend := <-fr.stop:
			if shouldSend {
				r.replicateTo(fr, r.getLatestIndex())
			}
			return true
		case <-ticker.C: // trigger 有几率丢失通知(如果 rpc 延迟过大，可能会略过最新的通知)，所以通过定时任务作为补充
			stop = r.replicateTo(fr, r.getLatestIndex())
		case fu := <-fr.trigger:
			stop = r.replicateTo(fr, r.getLatestIndex())
			if fu != nil {
				if !stop {
					fu.success()
				} else {
					fu.fail(errors.New("replication failed"))
				}
			}
		}
	}
	return
}
