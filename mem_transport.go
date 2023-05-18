package papillon

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

type memRPC struct {
	sync.Mutex
	consumerCh chan *RPC
	localAddr  ServerAddr
	peerMap    map[ServerAddr]RpcInterface
	pipeline   list.List
	timeout    time.Duration
	shutDown   shutDown
	fastPath   fastPath
}

func NewMemRpc(localAddr string) *memRPC {
	return &memRPC{
		localAddr:  ServerAddr(localAddr),
		consumerCh: make(chan *RPC),
		peerMap:    map[ServerAddr]RpcInterface{},
		timeout:    time.Second,
		shutDown:   newShutDown(),
	}
}

func (m *memRPC) Connect(addr ServerAddr, rpc RpcInterface) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.peerMap[addr]; ok {
		return
	}
	m.peerMap[addr] = rpc
}

func (m *memRPC) Disconnect(addr ServerAddr) {
	m.Lock()
	defer m.Unlock()
	delete(m.peerMap, addr)
	for e := m.pipeline.Front(); e != nil; e.Next() {
		if p := e.Value.(*menAppendEntryPipeline); p.peer.localAddr == addr {
			m.pipeline.Remove(e)
		}
	}
}

func (m *memRPC) DisconnectAll() {
	m.Lock()
	defer m.Unlock()
	m.peerMap = map[ServerAddr]RpcInterface{}
	for e := m.pipeline.Front(); e != nil; e = e.Next() {
		e.Value.(*menAppendEntryPipeline).Close()
	}
	m.pipeline.Init()
}

type menAppendEntryPipeline struct {
	peer, rpc    *memRPC
	processedCh  chan AppendEntriesFuture
	inProgressCh chan *memAppendEntriesInflight
	shutDownCh   chan struct{}
	shutDownOnce sync.Once
}

type memAppendEntriesInflight struct {
	af  *appendEntriesFuture
	rpc *RPC
}

func newMenAppendEntryPipeline(peer, rpc *memRPC) *menAppendEntryPipeline {
	return &menAppendEntryPipeline{
		peer:         peer,
		rpc:          rpc,
		shutDownCh:   make(chan struct{}),
		inProgressCh: make(chan *memAppendEntriesInflight),
		processedCh:  make(chan AppendEntriesFuture),
	}
}

func (pipe *menAppendEntryPipeline) decodeResponse() {
	timeout := pipe.rpc.timeout
	for {
		select {
		case <-pipe.shutDownCh:
			return
		case inflight := <-pipe.inProgressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case rpcResp := <-inflight.rpc.Response:
				resp := rpcResp.(*AppendEntryResponse)
				inflight.af.responded(resp, nil)
				select {
				case pipe.processedCh <- inflight.af:
				case <-pipe.shutDownCh:
					return
				}
			case <-timeoutCh:
				inflight.af.responded(nil, ErrTimeout)
				select {
				case pipe.processedCh <- inflight.af:
				case <-pipe.shutDownCh:
					return
				}
			case <-pipe.shutDownCh:
				return
			}
		}
	}
}
func (pipe *menAppendEntryPipeline) AppendEntries(request *AppendEntryRequest) (AppendEntriesFuture, error) {
	var (
		af      = newAppendEntriesFuture(request)
		timeout <-chan time.Time
	)
	if t := pipe.rpc.timeout; t > 0 {
		timeout = time.After(t)
	}

	cmd := RPC{
		RpcType:  RpcAppendEntry,
		Request:  request,
		Response: make(chan interface{}, 1),
	}

	select {
	case pipe.peer.consumerCh <- &cmd:
	case <-timeout:
		return nil, ErrTimeout
	case <-pipe.shutDownCh:
		return nil, ErrShutDown
	}
	select {
	case pipe.inProgressCh <- &memAppendEntriesInflight{af: af, rpc: &cmd}:
	case <-pipe.shutDownCh:
		return nil, ErrPipelineShutdown
	}
	return af, nil
}

func (pipe *menAppendEntryPipeline) Consumer() <-chan AppendEntriesFuture {
	return pipe.processedCh
}

func (pipe *menAppendEntryPipeline) Close() error {
	pipe.shutDownOnce.Do(func() {
		close(pipe.shutDownCh)
	})
	return nil
}

func (m *memRPC) getPeer(addr ServerAddr) *memRPC {
	m.Lock()
	defer m.Unlock()
	return m.peerMap[addr].(*memRPC)
}

func (m *memRPC) Consumer() <-chan *RPC {
	return m.consumerCh
}
func (m *memRPC) doRpc(cmdType rpcType, peer *memRPC, request interface{}, reader io.Reader) (interface{}, error) {
	timeout := m.timeout
	rpc := &RPC{
		RpcType:  cmdType,
		Request:  request,
		Reader:   reader,
		Response: make(chan interface{}),
	}
	now := time.Now()
	select {
	case peer.consumerCh <- rpc:
		timeout = time.Now().Sub(now)
	case <-time.After(timeout):
		if cmdType == RpcAppendEntry {
			fmt.Println("time out------")
		}
		return nil, errors.New("time out")
	}

	select {
	case resp := <-rpc.Response:
		return resp, nil
	case <-time.After(m.timeout):
		return nil, errors.New("time out")
	}
}

func (m *memRPC) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	resp, err := m.doRpc(RpcVoteRequest, m.getPeer(info.Addr), request, nil)
	if err != nil {
		return nil, err
	}
	return resp.(*VoteResponse), nil
}

func (m *memRPC) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	resp, err := m.doRpc(RpcAppendEntry, m.getPeer(info.Addr), request, nil)
	if err != nil {
		return nil, err
	}
	return resp.(*AppendEntryResponse), nil
}

func (m *memRPC) AppendEntryPipeline(info *ServerInfo) (AppendEntryPipeline, error) {
	peer := m.getPeer(info.Addr)
	m.Lock()
	defer m.Unlock()

	pipe := newMenAppendEntryPipeline(peer, m)
	m.pipeline.PushBack(pipe)
	go pipe.decodeResponse()
	return pipe, nil
}

func (m *memRPC) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest, reader io.Reader) (*InstallSnapshotResponse, error) {
	peer := m.getPeer(info.Addr)
	resp, err := m.doRpc(RpcInstallSnapshot, peer, request, reader)
	if err != nil {
		return nil, err
	}
	return resp.(*InstallSnapshotResponse), nil
}

func (m *memRPC) SetHeartbeatFastPath(cb fastPath) {
	m.fastPath = cb
}

func (m *memRPC) FastTimeout(info *ServerInfo, request *FastTimeoutRequest) (*FastTimeoutResponse, error) {
	resp, err := m.doRpc(RpcFastTimeout, m.getPeer(info.Addr), request, nil)
	if err != nil {
		return nil, err
	}
	return resp.(*FastTimeoutResponse), nil
}

func (m *memRPC) LocalAddr() ServerAddr {
	return m.localAddr
}

func (m *memRPC) EncodeAddr(info *ServerInfo) []byte {
	return []byte(info.Addr)
}

func (m *memRPC) DecodeAddr(bytes []byte) ServerAddr {
	return ServerAddr(bytes)
}

func batchConn(rpc ...*memRPC) {
	for _, outer := range rpc {
		for _, inner := range rpc {
			outer.Connect(inner.LocalAddr(), inner)
			inner.Connect(outer.LocalAddr(), outer)
		}
	}
}
