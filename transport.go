package papillon

import (
	"bufio"
	"encoding/json"
	"net"
	"time"
)

func (c rpcType) String() string {
	switch c {
	case RpcVoteRequest:
		return "VoteRequest"
	case RpcAppendEntryPipeline:
		return "AppendEntryPipeline"
	case RpcAppendEntry:
		return "AppendEntry"
	case RpcInstallSnapshot:
		return "InstallSnapshot"
	case RpcFastTimeout:
		return "FastTimeout"
	default:
		return unknown
	}
}

const (
	RpcVoteRequest rpcType = iota + 1
	RpcAppendEntry
	RpcAppendEntryPipeline
	RpcInstallSnapshot
	RpcFastTimeout
)
const (
	rpcMaxPipeline = 128
	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB
)

// NetLayer 网络层抽象
type NetLayer interface {
	net.Listener
	// Dial is used to create a new outgoing connection
	Dial(peer ServerAddr, timeout time.Duration) (net.Conn, error)
}

type (
	WithPeers interface {
		Connect(addr ServerAddr, rpc RpcInterface)
		Disconnect(addr ServerAddr)
		DisconnectAll()
	}
	fastPath func(cb *RPC) bool

	PackageParser interface {
		Encode(writer *bufio.Writer, cmdType rpcType, data []byte) (err error)
		Decode(reader *bufio.Reader) (rpcType, []byte, error)
	}

	RpcConvert interface {
		Deserialization(data []byte, i interface{}) error
		Serialization(i interface{}) (bytes []byte, err error)
	}

	// JsonRpcHandler 提供 json 的序列化能力
	JsonRpcHandler struct{}
)

var defaultCmdConverter = new(JsonRpcHandler)

func (j *JsonRpcHandler) Deserialization(data []byte, i interface{}) error {
	return json.Unmarshal(data, i)
}

func (j *JsonRpcHandler) Serialization(i interface{}) (bytes []byte, err error) {
	return json.Marshal(i)
}
