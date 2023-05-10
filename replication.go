package papillon

import (
	"errors"
	. "github.com/fuyao-w/common-util"
	"sync/atomic"
	"time"
)

type (
	// replication 领导人复制时每个跟随者维护的上下文状态
	replication struct {
		peer                               *LockItem[ServerInfo]                 // 跟随者的 server 信息
		nextIndex                          uint64                                // 待复制给跟随者的下一条日志索引，初始化为领导人最新的日志索引
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
	return atomic.LoadUint64(&fr.nextIndex)
}
func (fr *replication) setNextIndex(newNextIndex uint64) {
	atomic.StoreUint64(&fr.nextIndex, newNextIndex)
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

// replicateTo 负责短连接的跟随者复制，如果 replication.nextIndex 到 latestIndex 之前有空洞，则发送快照
func (r *Raft) replicateTo(fr *replication, latestIndex uint64) (stop bool) {
	hasMore := func() bool {
		select {
		case <-fr.stop:
			return false
		default:
			return fr.getNextIndex() <= latestIndex
		}
	}

	for !stop {
		req, err := r.buildAppendEntryReq(fr.getNextIndex(), latestIndex)
		if err != nil {
			if errors.Is(ErrNotFoundLog, err) {
				return r.sendLatestSnapshot(fr)
			}
			r.logger.Errorf("buildAppendEntryReq err :%s ,latest index", err, latestIndex)
			return true
		}
		if len(req.Entries) > 0 {
			r.logger.Debug("AppendEntries to ", fr.peer.Get().ID, latestIndex)
		}
		resp, err := r.rpc.AppendEntries(Ptr(fr.peer.Get()), req)
		if err != nil {
			r.logger.Errorf("heart beat err :%s", err)
			return
		}
		if resp.Term > r.getCurrentTerm() {
			r.leaderLease(resp.Term, fr)
			return true
		}
		fr.setLastContact()
		if len(req.Entries) > 0 {
			r.logger.Debug("replicate to ", fr.peer.Get().ID, latestIndex, resp.Succ)
		}
		if resp.Succ {
			r.updateLatestCommit(fr, req.Entries)
			fr.allowPipeline = true
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
				r.logger.Errorf("pipeline result err :%s, id :%s ", err, fr.peer.Get().ID)
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
				r.logger.Debug("processPipelineResult", fr.getNextIndex(), fr.peer.Get().ID)
			}
		}

	}
}

// pipelineReplicateTo 执行长链接复制
func (r *Raft) pipelineReplicateTo(fr *replication, pipeline AppendEntryPipeline) (stop bool) {
	req, err := r.buildAppendEntryReq(fr.getNextIndex(), r.getLatestIndex())
	if err != nil {
		r.logger.Errorf("pipelineReplicateTo|buildAppendEntryReq err:%s ,next index:%d  ,latest index :%d ,%s", err, fr.getNextIndex(), r.getLatestIndex(), fr.peer.Get().ID)
		return true
	}
	_, err = pipeline.AppendEntries(req)
	if err != nil {
		r.logger.Errorf("pipelineReplicateTo|AppendEntries err:%s", err)
		return true
	}
	// 因为请求结果在 processPipelineResult 函数线程异步接收，这里需要立即更新 nextIndex 防止重复发送
	if n := len(req.Entries); n > 0 {
		fr.setNextIndex(req.Entries[n-1].Index + 1)
	}
	return
}

// pipelineReplicateHelper 负责长链接复制，仅当确认跟随者上一次已经追赶上进度后才会执行，不具备快照发送能力
func (r *Raft) pipelineReplicateHelper(fr *replication) (stop bool) {
	var (
		finishCh, pipelineStopCh = make(chan struct{}), make(chan struct{})
	)
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

	for !stop {
		select {
		case shouldSend := <-fr.stop:
			if shouldSend {
				r.pipelineReplicateTo(fr, pipeline)
			}
			stop = true
		case <-time.After(r.Conf().CommitTimeout):
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
		if fr.allowPipeline {
			stop = r.pipelineReplicateHelper(fr)
		}
	}
	close(fr.heartBeatStop)
	<-fr.heartBeatDone
	close(fr.done)
}

// replicateHelper 短链复制的触发函数，通过定时器、channel 通知触发复制
func (r *Raft) replicateHelper(fr *replication) (stop bool) {
	var (
		ticker = time.NewTicker(r.Conf().CommitTimeout)
	)
	defer ticker.Stop()
	for !stop && !fr.allowPipeline {
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
			if !stop {
				fu.success()
			} else {
				fu.fail(errors.New("replication failed"))
			}
		}
	}
	return
}
