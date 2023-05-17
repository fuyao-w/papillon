package papillon

import (
	"io"
	"time"
)

type (
	Processor interface {
		Do(rpcType, interface{}, io.Reader) (interface{}, error)
		SetFastPath(cb fastPath)
	}
	// ProcessorProxy 服务器接口 handler 代理，提供将序列化数据，解析成接口 struct 指针的功能
	ProcessorProxy struct {
		Processor
	}
	// ServerProcessor 服务器接口 handler ，提供具体的接口处理逻辑
	ServerProcessor struct {
		cmdChan  chan *RPC
		fastPath fastPath
	}
)

func (d *ProcessorProxy) SetFastPath(cb fastPath) {
	d.Processor.SetFastPath(cb)
}
func (d *ServerProcessor) SetFastPath(cb fastPath) {
	d.fastPath = cb
}

// Do ServerProcessor 不关心上层协议，所以不用处理第一个参数（rpcType）
func (d *ServerProcessor) Do(typ rpcType, req interface{}, reader io.Reader) (resp interface{}, err error) {
	resCh := make(chan any, 1)
	rpc := &RPC{
		Request:  req,
		Response: resCh,
	}
	switch typ {
	case RpcAppendEntry:
		request := req.(*AppendEntryRequest)
		if len(request.Entries) == 0 && d.fastPath != nil && d.fastPath(rpc) {
			return <-resCh, nil
		}
	case RpcInstallSnapshot:
		rpc.Reader = io.LimitReader(reader, req.(*InstallSnapshotRequest).SnapshotMeta.Size)
	}

	d.cmdChan <- rpc
	return <-resCh, nil
}

type processorOption struct {
	Processor
	RpcConvert
}

func newProcessorProxy(cmdCh chan *RPC, options ...func(opt *processorOption)) Processor {
	proxy := &ProcessorProxy{
		Processor: &ServerProcessor{
			cmdChan: cmdCh,
		},
	}
	var opt processorOption
	for _, do := range options {
		do(&opt)
	}
	if opt.Processor != nil {
		proxy.Processor = opt.Processor
	}
	//if opt.RpcConvert != nil {
	//	proxy.RpcConvert = opt.RpcConvert
	//}
	return proxy
}

func (p *ProcessorProxy) Do(cmdType rpcType, reqBytes interface{}, reader io.Reader) (respBytes interface{}, err error) {
	date := reqBytes.([]byte)
	var req interface{}

	switch cmdType {
	case RpcVoteRequest:
		req = new(VoteRequest)
	case RpcAppendEntry:
		req = new(AppendEntryRequest)
	case RpcAppendEntryPipeline:
		req = new(AppendEntryRequest)
	case RpcInstallSnapshot:
		req = new(InstallSnapshotRequest)
	}
	err = defaultCmdConverter.Deserialization(date, req)
	if err != nil {
		return
	}
	resp, err := p.Processor.Do(cmdType, req, reader)
	if err != nil {
		return nil, err
	}
	return defaultCmdConverter.Serialization(resp)
}

func doWithTimeout(timeout time.Duration, do func()) bool {
	wrapper := func() chan struct{} {
		done := make(chan struct{})
		go do()
		return done
	}
	select {
	case <-time.After(timeout):
		return false
	case <-wrapper():
		return true
	}
}
