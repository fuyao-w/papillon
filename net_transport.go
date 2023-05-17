package papillon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	. "github.com/fuyao-w/common-util"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type (
	netConn struct {
		remote ServerAddr
		c      net.Conn
		rw     *bufio.ReadWriter
	}
	ServerAddrProvider interface {
		GetAddr(id ServerID) (ServerAddr, error)
	}
	typConnPool  map[ServerAddr][]*netConn
	NetTransport struct {
		logger             Logger
		shutDown           shutDown
		timeout            time.Duration
		cmdChan            chan *RPC
		netLayer           NetLayer
		connPoll           *connPool
		serverAddrProvider ServerAddrProvider
		processor          Processor
		heartbeatFastPath  fastPath
		TimeoutScale       int64
		ctx                *LockItem[ctx]
	}
	ctx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	connPool struct {
		pool             *LockItem[typConnPool]
		maxSinglePoolNum int
	}
)

func (n *NetTransport) EncodeAddr(info *ServerInfo) []byte {
	return []byte(info.Addr)
}

func (n *NetTransport) DecodeAddr(bytes []byte) ServerAddr {
	return ServerAddr(bytes)
}

func (n *NetTransport) LocalAddr() ServerAddr {
	return ServerAddr(n.netLayer.Addr().String())
}

func (n *NetTransport) Consumer() <-chan *RPC {
	return n.cmdChan
}

func (n *NetTransport) getServerAddr(info *ServerInfo) ServerAddr {
	if n.serverAddrProvider == nil {
		return info.Addr
	}
	addr, err := n.serverAddrProvider.GetAddr(info.ID)
	if err != nil {
		return info.Addr
	}
	return addr
}

func (n *NetTransport) sendRpc(conn *netConn, cmdType rpcType, request interface{}) error {
	data, err := defaultCmdConverter.Serialization(request)
	if err != nil {
		return err
	}
	if err = defaultPackageParser.Encode(conn.rw.Writer, cmdType, data); err != nil {
		return err
	}
	return conn.rw.Flush()
}
func (n *NetTransport) recvRpc(conn *netConn, resp interface{}) error {
	_, data, err := defaultPackageParser.Decode(conn.rw.Reader)
	if err != nil {
		return err
	}
	err = defaultCmdConverter.Deserialization(data, resp)
	if err != nil {
		return err
	}
	return nil
}
func (n *NetTransport) genericRPC(info *ServerInfo, cmdType rpcType, request, response interface{}) (err error) {
	conn, err := n.getConn(info)
	if err != nil {
		return err
	}
	if n.timeout > 0 {
		conn.c.SetDeadline(time.Now().Add(n.timeout))
	}
	defer func() {
		if err != nil {
			conn.Close()
			data, _ := json.Marshal(request)
			n.logger.Infof("genericRPC errorx : %s , rpcType :%d , req :%s", err, cmdType, data)
		} else {
			n.connPoll.PutConn(conn)
		}
	}()
	if err = n.sendRpc(conn, cmdType, request); err != nil {
		return
	}

	return n.recvRpc(conn, response)
}
func (n *NetTransport) getConn(info *ServerInfo) (*netConn, error) {
	addr := n.getServerAddr(info)
	if conn := n.connPoll.GetConn(addr); conn != nil {
		return conn, nil
	}
	conn, err := n.netLayer.Dial(info.Addr, n.timeout)
	if err != nil {
		return nil, err
	}
	return newNetConn(info.Addr, conn), nil
}
func newNetConn(addr ServerAddr, conn net.Conn) *netConn {
	return &netConn{
		remote: addr,
		c:      conn,
		rw:     bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
}
func (n *NetTransport) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	var resp = new(VoteResponse)
	return resp, n.genericRPC(info, RpcVoteRequest, request, resp)
}

func (n *NetTransport) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	var resp = new(AppendEntryResponse)
	return resp, n.genericRPC(info, RpcAppendEntry, request, resp)
}

func (n *NetTransport) AppendEntryPipeline(info *ServerInfo) (AppendEntryPipeline, error) {
	conn, err := n.getConn(info)
	if err != nil {
		return nil, err
	}
	return newNetPipeline(n, conn), err
}

func (n *NetTransport) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest, r io.Reader) (*InstallSnapshotResponse, error) {
	conn, err := n.getConn(info)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if n.timeout > 0 {
		conn.c.SetDeadline(time.Now().Add(Max(n.timeout*time.Duration(request.SnapshotMeta.Size/n.TimeoutScale), n.timeout)))
	}
	if err = n.sendRpc(conn, RpcInstallSnapshot, request); err != nil {
		return nil, err
	}
	if _, err = io.Copy(conn.rw, r); err != nil {
		return nil, err
	}
	if err = conn.rw.Flush(); err != nil {
		n.logger.Errorf("InstallSnapShot|Flush errorx :%s", err)
		return nil, err
	}

	var resp = new(InstallSnapshotResponse)
	if err = n.recvRpc(conn, resp); err != nil {
		n.logger.Errorf("InstallSnapShot|recvRpc errorx :%s", err)
		return nil, err
	}
	return resp, nil
}

func (n *NetTransport) SetHeartbeatFastPath(cb fastPath) {
	n.processor.SetFastPath(cb)
}

func (n *NetTransport) FastTimeout(info *ServerInfo, req *FastTimeoutRequest) (*FastTimeoutResponse, error) {
	var resp = new(FastTimeoutResponse)
	return resp, n.genericRPC(info, RpcFastTimeout, req, resp)
}
func newConnPool(maxSinglePoolNum int) *connPool {
	p := &connPool{
		pool:             NewLockItem[typConnPool](map[ServerAddr][]*netConn{}),
		maxSinglePoolNum: maxSinglePoolNum,
	}
	return p
}
func (c *connPool) GetConn(addr ServerAddr) (conn *netConn) {
	c.pool.Action(func(t *typConnPool) {
		if list, ok := (*t)[addr]; ok {
			if len(list) == 0 {
				return
			}
			conn = list[len(list)-1]
			list = list[:len(list)-1]
			(*t)[addr] = list
		}
	})
	return
}

func (c *connPool) PutConn(conn *netConn) {
	c.pool.Action(func(t *typConnPool) {
		if c.maxSinglePoolNum <= len((*t)[conn.remote]) {
			conn.Close()
			return
		}
		(*t)[conn.remote] = append((*t)[conn.remote], conn)
	})
}

func (n *netConn) Close() {
	n.c.Close()
}

type NetWorkTransportConfig struct {
	ServerAddressProvider ServerAddrProvider

	Logger Logger

	NetLayer NetLayer

	MaxPool int

	Timeout time.Duration
}

func NewNetTransport(conf *NetWorkTransportConfig) *NetTransport {
	cmdCh := make(chan *RPC)
	logConnCtx, cancel := context.WithCancel(context.Background())
	t := &NetTransport{
		logger:             conf.Logger,
		timeout:            conf.Timeout,
		cmdChan:            cmdCh,
		netLayer:           conf.NetLayer,
		connPoll:           newConnPool(conf.MaxPool),
		serverAddrProvider: conf.ServerAddressProvider,
		processor:          newProcessorProxy(cmdCh),
		TimeoutScale:       DefaultTimeoutScale,
		shutDown:           newShutDown(),
		ctx: NewLockItem(ctx{
			ctx:    logConnCtx,
			cancel: cancel,
		}),
	}
	go t.Start()
	return t
}

func genCtx() ctx {
	c, cancel := context.WithCancel(context.Background())
	return ctx{
		ctx:    c,
		cancel: cancel,
	}
}

func (n *NetTransport) CloseConnections() {
	n.connPoll.pool.Action(func(t *typConnPool) {
		for _, connLise := range *t {
			for _, conn := range connLise {
				conn.Close()
			}
		}
		*t = map[ServerAddr][]*netConn{}
	})
	n.ctx.Action(func(t *ctx) {
		t.cancel()
		*t = genCtx()
	})
}
func (n *NetTransport) Close() error {
	n.shutDown.done(func(_ bool) {
		n.netLayer.Close()
	})
	return nil
}

func (n *NetTransport) Start() {
	var c int64
	for {
		conn, err := n.netLayer.Accept()
		if err != nil {
			if n.processError(err, c) {
				return
			}
		}
		c = 0
		go n.handleConn(n.ctx.Get().ctx, newNetConn("", conn))
	}
}

const baseDelay = 5 * time.Millisecond
const maxDelay = 1 * time.Second

func (n *NetTransport) processError(err error, count int64) (needEnd bool) {
	select {
	case <-n.shutDown.C:
		log.Printf("server shut down")
		return true
	default:
	}
	e, ok := err.(net.Error)
	if !ok {
		return true
	}
	switch {
	case e.Timeout():
		log.Printf("listener|Accept|errorx %n ", err)
		time.Sleep(func() (delay time.Duration) {
			delay = time.Duration(count) * baseDelay
			if delay > maxDelay {
				delay = maxDelay
			}
			return delay
		}() * time.Millisecond)
		return false
	}
	return true
}

func (n *NetTransport) handleConn(ctx context.Context, conn *netConn) {
	defer conn.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			cmdType, data, err := defaultPackageParser.Decode(conn.rw.Reader)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					n.logger.Errorf("processConnection|Decode errorx : %s\n", err)
				}
				return
			}
			respData, err := n.processor.Do(cmdType, data, conn.rw)
			if err != nil {
				n.logger.Errorf("NetTransport|processor errorx:%s", err)
				return
			}
			if err = defaultPackageParser.Encode(conn.rw.Writer, cmdType, respData.([]byte)); err != nil {
				n.logger.Errorf("NetTransport|Encode errorx:%s", err)
				return
			}
			if err := conn.rw.Flush(); err != nil {
				n.logger.Errorf("NetTransport|Flush errorx:%s", err)
				return
			}
		}
	}
}

type netPipeline struct {
	conn         *netConn
	trans        *NetTransport
	doneCh       chan AppendEntriesFuture
	inProgressCh chan *appendEntriesFuture
	shutdownCh   chan struct{}
	shutDownOnce sync.Once
}

func (n *netPipeline) AppendEntries(request *AppendEntryRequest) (AppendEntriesFuture, error) {
	af := newAppendEntriesFuture(request)
	if err := n.trans.sendRpc(n.conn, RpcAppendEntry, af.req); err != nil {
		return nil, err
	}
	select {
	case <-n.shutdownCh:
		return nil, ErrShutDown
	case n.inProgressCh <- af:
	}

	return af, nil
}

func (n *netPipeline) Consumer() <-chan AppendEntriesFuture {
	return n.doneCh
}

func (n *netPipeline) Close() error {
	n.shutDownOnce.Do(func() {
		n.conn.Close()
		close(n.shutdownCh)
	})
	return nil
}

func newNetPipeline(trans *NetTransport, conn *netConn) *netPipeline {
	pipeline := &netPipeline{
		conn:         conn,
		trans:        trans,
		doneCh:       make(chan AppendEntriesFuture, rpcMaxPipeline),
		inProgressCh: make(chan *appendEntriesFuture, rpcMaxPipeline),
		shutdownCh:   make(chan struct{}),
	}
	go pipeline.decodeResponses()
	return pipeline
}

func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case <-n.shutdownCh:
			return
		case af := <-n.inProgressCh:
			if timeout > 0 {
				n.conn.c.SetDeadline(time.Now().Add(timeout))
			}
			var resp *AppendEntryResponse
			err := n.trans.recvRpc(n.conn, &resp)
			if err != nil {
				n.trans.logger.Errorf("decodeResponses|recvRpc errorx :%s", err)

			}
			af.responded(resp, err)
			select {
			case <-n.shutdownCh:
				return
			case n.doneCh <- af:
			}
		}
	}
}
