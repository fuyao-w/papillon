package papillon

import (
	"errors"
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
type nilRespFuture = any

// Future 用于异步提交，Response 会同步返回，可以重复调用
type Future[T any] interface {
	Response() (T, error)
}

// defaultFuture 默认不需要返回值的 Future
type defaultFuture = Future[nilRespFuture]

type defaultDeferResponse = deferResponse[nilRespFuture]

// deferResponse Future[T any] 的实现，用于异步返回结果
type deferResponse[T any] struct {
	err      error
	once     *sync.Once
	errCh    chan error
	response T
}

func (d *deferResponse[_]) init() {
	d.errCh = make(chan error, 1)
	d.once = new(sync.Once)
}

func (d *deferResponse[T]) Response() (T, error) {
	d.once.Do(func() { d.err = <-d.errCh })
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

type clusterChangeFuture struct {
	LogFuture
	req *clusterChangeRequest
}

type clusterChangeCommend uint64

const (
	addServer clusterChangeCommend = iota + 1
	removeServer
	updateServer
)

type clusterChangeRequest struct {
	command   clusterChangeCommend
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
		stepDown    chan ServerID
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

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	defaultDeferResponse

	// clusterInfo is the proposed bootstrap clusterInfo to apply.
	clusterInfo ClusterInfo
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
