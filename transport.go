package papillon

import (
	"bufio"
	"encoding/json"
	"net"
	"time"
)

func (c rpcType) String() string {
	switch c {
	case CmdVoteRequest:
		return "VoteRequest"
	case CmdAppendEntryPipeline:
		return "AppendEntryPipeline"
	case CmdAppendEntry:
		return "AppendEntry"
	case CmdInstallSnapshot:
		return "InstallSnapshot"
	case CmdFastTimeout:
		return "FastTimeout"
	default:
		return "UNKNOWN"
	}
}

const (
	CmdVoteRequest rpcType = iota + 1
	CmdAppendEntry
	CmdAppendEntryPipeline
	CmdInstallSnapshot
	CmdFastTimeout
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

	CmdConvert interface {
		Deserialization(data []byte, i interface{}) error
		Serialization(i interface{}) (bytes []byte, err error)
	}

	// JsonCmdHandler 提供 json 的序列化能力
	JsonCmdHandler struct{}
)

var defaultCmdConverter = new(JsonCmdHandler)

func (j *JsonCmdHandler) Deserialization(data []byte, i interface{}) error {
	return json.Unmarshal(data, i)
}

func (j JsonCmdHandler) Serialization(i interface{}) (bytes []byte, err error) {
	return json.Marshal(i)
}
