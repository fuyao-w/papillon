package papillon

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cast"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	raft1, rpc1 := buildRaft("1", nil, nil)
	raft2, rpc2 := buildRaft("2", nil, nil)
	raft3, rpc3 := buildRaft("3", nil, nil)
	batchConn(rpc1.(*memRPC), rpc2.(*memRPC), rpc3.(*memRPC))
	go func() {
		time.Sleep(time.Second)
		raft1.BootstrapCluster(ClusterInfo{Servers: []ServerInfo{
			{Voter, "1", "1"},
			//{Voter, "2", "2"},
			//{Voter, "3", "3"},
		}})
	}()
	raftList := []*Raft{raft1, raft2, raft3}
	http.Handle("/get", &getHandle{raftList})
	http.Handle("/set", &setHandle{raftList})
	http.Handle("/verify", &verifyHandle{raftList})
	http.Handle("/config", &configGetHandle{raftList})
	http.Handle("/get_log", &getLogHandle{raftList})
	http.Handle("/leader_transfer", &leaderTransferHandle{raftList})
	http.Handle("/snapshot", &snapshotHandle{raftList})
	http.Handle("/restore", &userRestoreSnapshotHandle{raftList})
	http.Handle("/add_peer", &reloadPeerHandle{raftList})
	http.Handle("/raft_state", &raftGetHandle{raftList})
	http.Handle("/read_only", &readOnlyHandle{raftList})
	http.Handle("/read_index", &readIndexHandle{raftList})
	http.ListenAndServe("localhost:8080", nil)
}

func buildRaft(localID string, rpc RpcInterface, store interface {
	LogStore
	KVStorage
}) (*Raft, RpcInterface) {

	conf := &Config{
		LocalID: localID,
		//HeartBeatCycle:    time.Second * 2,
		//MemberList:        nil,
		HeartbeatTimeout:        time.Second * 2,
		SnapshotInterval:        time.Second * 15,
		ElectionTimeout:         time.Second * 5,
		CommitTimeout:           time.Millisecond * 50,
		LeaderLeaseTimeout:      time.Second * 1,
		MaxAppendEntries:        10,
		SnapshotThreshold:       1000,
		TrailingLogs:            1000,
		ApplyBatch:              true,
		LeadershipCatchUpRounds: 500,
		//LeadershipLostShutDown:  true,
	}
	if store == nil {
		store = newMemoryStore()
	}
	if rpc == nil {
		rpc = newMemRpc(localID)
	}
	fsm := newMemFSM()
	//fileSnapshot, err := NewFileSnapshot("./testsnapshot/snapshot"+localID, false, 3)
	//if err != nil {
	//	panic(err)
	//}
	raft, err := NewRaft(conf, fsm, rpc, store, store, newMemSnapShot())
	if err != nil {
		panic(err)
	}
	return raft, rpc
}

func getLeader(rafts ...*Raft) *Raft {
	for _, raft := range rafts {
		_, err := raft.VerifyLeader().Response()
		if err != nil {
			continue
		}
		return raft
	}
	return nil
}

type (
	getHandle struct {
		raftList []*Raft
	}
	setHandle struct {
		raftList []*Raft
	}
	verifyHandle struct {
		raftList []*Raft
	}
	configGetHandle struct {
		raftList []*Raft
	}
	getLogHandle struct {
		raftList []*Raft
	}
	leaderTransferHandle struct {
		raftList []*Raft
	}
	snapshotHandle struct {
		raftList []*Raft
	}
	userRestoreSnapshotHandle struct {
		raftList []*Raft
	}
	reloadPeerHandle struct {
		raftList []*Raft
	}

	raftGetHandle struct {
		raftList []*Raft
	}
	readIndexHandle struct {
		raftList []*Raft
	}
	readOnlyHandle struct {
		raftList []*Raft
	}
)

func (g *leaderTransferHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	raft := getLeader(g.raftList...)
	fu := raft.LeaderTransfer("", "", 0)
	_, err := fu.Response()
	if err == nil {
		writer.Write([]byte("succ"))
	} else {

		writer.Write([]byte(err.Error()))
	}
}
func (g *getHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	key := request.URL.Query().Get("key")
	raft := getLeader(g.raftList...)
	fmt.Println("getleader", raft.localInfo.ID)
	fsm := raft.fsm.(*memFSM)
	val := fsm.getVal(key)
	if len(val) > 0 {
		writer.Write([]byte(val))
	} else {
		writer.Write([]byte("not found"))
	}
	writer.WriteHeader(200)
}
func (s *setHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	key := request.URL.Query().Get("key")
	value := request.URL.Query().Get("value")
	raft := getLeader(s.raftList...)
	fu := raft.Apply(kvSchema{}.encode(key, value), time.Second)
	_, err := fu.Response()
	if err != nil {
		writer.Write([]byte("fail set handle " + err.Error()))
	} else {
		writer.Write([]byte("succ"))
	}

}
func (s *verifyHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	idx := cast.ToInt(request.URL.Query().Get("idx"))
	if idx < 0 || idx >= len(s.raftList) {
		writer.Write([]byte("param err"))
		return
	}
	fu := s.raftList[idx].VerifyLeader()
	_, err := fu.Response()

	if err != nil {
		writer.Write([]byte("fail" + err.Error() + " " + cast.ToString(idx)))
	} else {
		writer.Write([]byte("succ"))
	}

}
func (s *configGetHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	idx := cast.ToUint(request.URL.Query().Get("idx"))
	if idx >= uint(len(s.raftList)) {
		writer.Write([]byte("idx not exist"))
		return
	}
	cluster := s.raftList[idx].getLatestCluster()
	b, _ := json.Marshal(cluster)
	writer.Write([]byte(b))

}
func (s *getLogHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	from := cast.ToUint64(request.URL.Query().Get("from"))
	to := cast.ToUint64(request.URL.Query().Get("to"))
	idx := cast.ToInt(request.URL.Query().Get("idx"))
	if idx < 0 || idx > len(s.raftList)-1 {
		writer.Write([]byte("params error"))
		return
	}
	raft := s.raftList[idx]
	logs, err := raft.logStore.GetLogRange(from, to)
	if err != nil {
		writer.Write([]byte("fail" + err.Error()))
	} else {
		b, _ := json.MarshalIndent(logs, "", "    ")
		writer.Write([]byte(b))
	}

}
func (s *snapshotHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	raft := getLeader(s.raftList...)
	fu := raft.SnapShot()
	open, err := fu.Response()
	if err != nil {
		writer.Write([]byte(err.Error()))
		return
	}
	meta, reader, err := open()
	if err != nil {
		writer.Write([]byte(err.Error()))
		return
	}
	defer reader.Close()
	io.Copy(writer, reader)
	b, _ := json.MarshalIndent(&meta, "", "    ")
	writer.Write(b)
}
func (s *reloadPeerHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	addr := request.URL.Query().Get("addr")
	id := request.URL.Query().Get("id")
	suffrage := request.URL.Query().Get("suffrage")
	action := cast.ToInt(request.URL.Query().Get("action"))
	leader := getLeader(s.raftList...)
	var fu IndexFuture
	if action == 0 {
		fu = leader.AddServer(ServerInfo{
			Suffrage: Suffrage(cast.ToInt(suffrage)),
			Addr:     ServerAddr(addr),
			ID:       ServerID(id),
		}, 0, time.Second)
	} else if action == 1 {
		fu = leader.RemoveServer(ServerInfo{
			Suffrage: Suffrage(cast.ToInt(suffrage)),
			Addr:     ServerAddr(addr),
			ID:       ServerID(id),
		}, 0, time.Second)
	} else {
		fu = leader.UpdateServer(ServerInfo{
			Suffrage: Suffrage(cast.ToInt(suffrage)),
			Addr:     ServerAddr(addr),
			ID:       ServerID(id),
		}, 0, time.Second)
	}

	_, err := fu.Response()
	if err != nil {
		writer.Write([]byte(err.Error()))
	} else {
		writer.Write([]byte("succ"))
	}
}
func (s *raftGetHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	idx := cast.ToInt(request.URL.Query().Get("idx"))
	if idx < 0 || idx >= len(s.raftList) {
		writer.Write([]byte("param err"))
		return
	}
	fu := s.raftList[idx].RaftState()
	stat, _ := fu.Response()
	writer.Write([]byte(stat))
}
func (s *readIndexHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	idx := cast.ToInt(request.URL.Query().Get("idx"))
	if idx < 0 || idx >= len(s.raftList) {
		writer.Write([]byte("param err"))
		return
	}
	fu := s.raftList[idx].ReadIndex(0)
	ri, err := fu.Response()
	if err != nil {
		writer.Write([]byte(err.Error()))
	} else {
		writer.Write([]byte(cast.ToString(ri)))
	}
}
func (s *userRestoreSnapshotHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//raft := getLeader(s.raftList...)
	//snapshot := raft.snapShotStore
	//list, err := snapshot.List()
	//if err != nil {
	//	writer.Write([]byte(err.Error()))
	//	return
	//}
	//if len(list) == 0 {
	//	writer.Write([]byte("have no snapshot"))
	//	return
	//}
	//meta, rc, err := snapshot.Open(list[0].ID)
	//if len(list) == 0 {
	//	writer.Write([]byte("open" + "err:" + err.Error()))
	//	return
	//}
	//defer rc.Close()
	//
	//err = raft.RestoreSnapshot(meta, rc)
	//if err != nil {
	//	writer.Write([]byte(err.Error()))
	//} else {
	//	writer.Write([]byte("succ"))
	//}
}

func (s *readOnlyHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	idx := cast.ToUint64(request.URL.Query().Get("idx"))
	readIndex := cast.ToUint64(request.URL.Query().Get("index"))
	fu := s.raftList[idx].Barrier(readIndex, 0)
	i, err := fu.Response()
	if err != nil {
		writer.Write([]byte(err.Error()))
	} else {
		writer.Write([]byte(cast.ToString(i)))
	}
}
