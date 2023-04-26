package papillon

import (
	"errors"
	"fmt"
	common_util "github.com/fuyao-w/common-util"
	"io"
	"sync"
	"time"
)

var (
	FutureErrTimeout   = errors.New("time out")
	FutureErrNotLeader = errors.New("not leader")
)

// OpenSnapShot 用于 API 请求执行完快照后再需要的时候延迟打开快照
type OpenSnapShot = func() (*SnapShotMeta, io.ReadCloser, error)

// nilRespFuture Future 默认不需要返回值的类型
type nilRespFuture = interface{}

// Future 用于异步提交，Response 会同步返回，可以重复调用
type Future[T any] interface {
	Response() (T, error)
}

// defaultFuture 默认不需要返回值的 Future
type defaultFuture = Future[nilRespFuture]

type defaultDeferResponse = deferResponse[nilRespFuture]

type reject interface {
	reject(state State)
}

type deferResponse[T any] struct {
	err        error
	once       *sync.Once
	errCh      chan error
	response   T
	ShutdownCh <-chan struct{}
}

func (d *deferResponse[_]) reject(state State) {
	if state == ShutDown {
		d.fail(ErrShutDown)
		return
	}
	d.fail(fmt.Errorf("current state %s can't process", state.String()))
}

func (d *deferResponse[_]) init() {
	d.errCh = make(chan error, 1)
	d.once = new(sync.Once)
}
func (d *deferResponse[_]) setTimeout() {
	d.errCh = make(chan error, 1)
	d.once = new(sync.Once)
}

func (d *deferResponse[T]) Response() (T, error) {
	d.once.Do(func() {
		select {
		case d.err = <-d.errCh:
		case <-d.ShutdownCh:
			d.err = ErrShutDown
		}
	})
	return d.response, d.err
}

type LogFuture struct {
	deferResponse[any]
	log *LogEntry
}

func (l *LogFuture) Index() uint64 {
	return l.log.Index
}

// responded 返回响应结果，在调用该方法后 Response 就会返回，该方法不支持重复调用
func (d *deferResponse[T]) responded(resp T, err error) {
	d.response = resp
	select {
	case d.errCh <- err:
	default:
		panic("defer response not init")
	}
	close(d.errCh)
}

func (d *deferResponse[T]) success() {
	d.responded(common_util.Zero[T](), nil)
}
func (d *deferResponse[T]) fail(err error) {
	d.responded(common_util.Zero[T](), err)
}

type AppendEntriesFuture interface {
	Future[*AppendEntryResponse]
	StartAt() time.Time
	Request() *AppendEntryRequest
}

type appendEntriesFuture struct {
	deferResponse[*AppendEntryResponse]
	startAt time.Time
	req     *AppendEntryRequest
}

func newAppendEntriesFuture(req *AppendEntryRequest) *appendEntriesFuture {
	af := &appendEntriesFuture{
		startAt: time.Now(),
		req:     req,
	}
	af.init()
	return af
}
func (a *appendEntriesFuture) StartAt() time.Time {
	return a.startAt
}

func (a *appendEntriesFuture) Request() *AppendEntryRequest {
	return a.req
}

type configurationChangeFuture struct {
	LogFuture
	req *configurationChangeRequest
}

type configurationChangeCommend uint64

const (
	addServer configurationChangeCommend = iota + 1
	removeServer
	updateServer
)

type configurationChangeRequest struct {
	command   configurationChangeCommend
	peer      ServerInfo
	pervIndex uint64
}

type (
	verifyFuture struct {
		deferResponse[bool]
		sync.Mutex
		quorumCount uint
		voteGranted uint
		reportOnce  *sync.Once
		stepDown    chan struct{}
	}
)

func (v *verifyFuture) report(leadership bool) {
	v.reportOnce.Do(func() {
		v.responded(leadership, nil)
		if !leadership {
			asyncNotify(v.stepDown)
		}
	})
}
func (v *verifyFuture) vote(leadership bool) {
	v.Lock()
	defer v.Unlock()
	if leadership {
		v.voteGranted++
		if v.voteGranted >= v.quorumCount {
			v.report(true)
		}
	} else {
		v.report(false)
	}
}

type userRestoreFuture struct {
	defaultDeferResponse
	meta   *SnapShotMeta
	reader io.ReadCloser
}

type leadershipTransferFuture struct {
	defaultDeferResponse
	Peer ServerInfo
}

type clusterGetFuture struct {
	deferResponse[cluster]
}

type apiClusterGetFuture struct {
	deferResponse[Configuration]
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	defaultDeferResponse

	// configuration is the proposed bootstrap configuration to apply.
	configuration Configuration
}

type (
	fsmSnapshotFuture struct {
		deferResponse[*SnapShotFutureResp]
	}
	SnapShotFutureResp struct {
		term, index uint64
		fsmSnapshot FsmSnapshot
	}
)

// apiSnapshotFuture is used for waiting on a user-triggered snapshot to
// complete.
type apiSnapshotFuture struct {
	deferResponse[OpenSnapShot]
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	defaultDeferResponse
	ID string
}

type shutDownFuture struct {
	raft *Raft
}

func (s *shutDownFuture) Response() (nilRespFuture, error) {
	if s.raft == nil {
		return nil, nil
	}
	s.raft.waitShutDown()

	if inter, ok := s.raft.rpc.(interface {
		Close() error
	}); ok {
		inter.Close()
	}
	return nil, nil
}

type ApplyFuture interface {
	IndexFuture
	Future[nilRespFuture]
}
type IndexFuture interface {
	Index() uint64
	defaultFuture
}

type errFuture[T any] struct {
	err error
}

func (e *errFuture[T]) Index() uint64 {
	return 0
}

func (e *errFuture[T]) Response() (t T, _ error) {
	return t, e.err
}
