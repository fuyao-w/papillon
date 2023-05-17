package papillon

import (
	"io"
)

type (

	// AppendEntryRequest 追加日志
	AppendEntryRequest struct {
		*RPCHeader
		Term         uint64
		PrevLogIndex uint64
		PrevLogTerm  uint64
		Entries      []*LogEntry
		LeaderCommit uint64
	}
	AppendEntryResponse struct {
		*RPCHeader
		Term        uint64
		Succ        bool
		LatestIndex uint64 // peer 当前保存最新的日志 index ，用于新节点快速定位 nextIndex
	}
	// VoteRequest 投票
	VoteRequest struct {
		*RPCHeader
		Term           uint64
		CandidateID    ServerID
		LastLogIndex   uint64
		LastLogTerm    uint64
		LeaderTransfer bool
	}
	VoteResponse struct {
		*RPCHeader
		Term    uint64
		Granted bool
	}
	// InstallSnapshotRequest 安装快照
	InstallSnapshotRequest struct {
		*RPCHeader
		SnapshotMeta *SnapshotMeta
		Term         uint64
	}
	InstallSnapshotResponse struct {
		*RPCHeader
		Term    uint64
		Success bool
	}
	// FastTimeoutRequest 引导 leader 直接超时
	FastTimeoutRequest struct {
		*RPCHeader
		Term               uint64
		LeaderShipTransfer bool
	}
	FastTimeoutResponse struct {
		*RPCHeader
		Success bool
	}
	// voteResult 投票结果
	voteResult struct {
		*VoteResponse
		ServerID ServerID
	}

	RPCHeader struct {
		ID   ServerID
		Addr ServerAddr
	}
)

type (
	// rpcType rpc 接口类型，因为要编码在 rpc 协议中，并且只占用一个 byte ，所以类型也需要是 byte 防止溢出
	rpcType byte
	// RPC rpc 请求的封装
	RPC struct {
		RpcType  rpcType
		Request  any
		Response chan any
		Reader   io.Reader // 链接读接口，安装快照的时候用
	}
	RpcInterface interface {
		// Consumer 返回一个可消费的 Chan
		Consumer() <-chan *RPC
		// VoteRequest 发起投票请求
		VoteRequest(*ServerInfo, *VoteRequest) (*VoteResponse, error)
		// AppendEntries 追加日志
		AppendEntries(*ServerInfo, *AppendEntryRequest) (*AppendEntryResponse, error)
		// AppendEntryPipeline 以 pipe 形式追加日志
		AppendEntryPipeline(*ServerInfo) (AppendEntryPipeline, error)
		// InstallSnapShot 安装快照
		InstallSnapShot(*ServerInfo, *InstallSnapshotRequest, io.Reader) (*InstallSnapshotResponse, error)
		// SetHeartbeatFastPath 用于快速处理，不用经过主流程，不支持也没关系
		SetHeartbeatFastPath(cb fastPath)
		// FastTimeout 快速超时转换为候选人
		FastTimeout(*ServerInfo, *FastTimeoutRequest) (*FastTimeoutResponse, error)

		LocalAddr() ServerAddr
		EncodeAddr(info *ServerInfo) []byte
		DecodeAddr([]byte) ServerAddr
	}

	AppendEntryPipeline interface {
		AppendEntries(*AppendEntryRequest) (AppendEntriesFuture, error)
		Consumer() <-chan AppendEntriesFuture
		Close() error
	}
)
