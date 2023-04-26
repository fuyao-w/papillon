package papillon

import (
	"errors"
	"github.com/fuyao-w/log"
	"net"
	"time"
)

type TcpLayer struct {
	listener  net.Listener
	advertise net.Addr
}

func NewTCPTransport(bindAddr string, maxPool int, timeout time.Duration) (*NetTransport, error) {
	return newTcpTransport(bindAddr, func(layer NetLayer) *NetTransport {
		return NewNetTransport(&NetWorkTransportConfig{
			ServerAddressProvider: nil,
			Logger:                log.NewLogger(),
			NetLayer:              layer,
			MaxPool:               maxPool,
			Timeout:               timeout,
		})
	})
}
func newTcpTransport(bindAddr string, transportCreator func(layer NetLayer) *NetTransport) (*NetTransport, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	layer := NewTcpLayer(listener, nil)

	addr, ok := layer.Addr().(*net.TCPAddr)
	if !ok {
		listener.Close()
		return nil, errors.New("add not tcp")
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		listener.Close()
		return nil, errors.New("err not advertisable")
	}
	return transportCreator(layer), nil
}
func NewTcpLayer(l net.Listener, advertise net.Addr) NetLayer {
	return &TcpLayer{
		listener:  l,
		advertise: advertise,
	}
}
func (t *TcpLayer) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *TcpLayer) Close() error {
	return t.listener.Close()
}

func (t *TcpLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

func (t *TcpLayer) Dial(peer ServerAddr, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(peer), timeout)
}
