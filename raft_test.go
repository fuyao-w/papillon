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
	raft, rpc := buildRaft("1", nil, nil)
	_ = rpc
	_ = raft
	go func() {
		time.Sleep(time.Second)
		raft.BootstrapCluster(Configuration{Servers: []ServerInfo{{Voter, "1", "1"}}})
	}()
	http.Handle("/get", &getHandle{raftList: []*Raft{raft}})
	http.Handle("/set", &setHandle{raftList: []*Raft{raft}})
	http.Handle("/verify", &verifyHandle{raft})
	http.Handle("/config", &configGetHandle{raft})
	http.Handle("/get_log", &getLogHandle{raftList: []*Raft{raft}})
	http.Handle("/leader_transfer", &leaderTransferHandle{raftList: []*Raft{raft}})
	http.Handle("/snapshot", &snapshotHandle{raftList: []*Raft{raft}})
	http.Handle("/restore", &userRestoreSnapshotHandle{raftList: []*Raft{raft}})
	http.Handle("/add_peer", &addPeerHandle{raftList: []*Raft{raft}})
	http.ListenAndServe("localhost:8080", nil)
}

func buildRaft(localID string, rpc RpcInterface, store interface {
	LogStore
	KVStorage
}) (*Raft, *memFSM) {

	conf := &Config{
		LocalID: localID,
		//HeartBeatCycle:    time.Second * 2,
		//MemberList:        nil,
		HeartbeatTimeout:        time.Second * 2,
		SnapshotInterval:        time.Second * 15,
		ElectionTimeout:         time.Second * 5,
		CommitTimeout:           time.Second,
		LeaderLeaseTimeout:      time.Second * 1,
		MaxAppendEntries:        10,
		SnapshotThreshold:       100,
		TrailingLogs:            1000,
		ApplyBatch:              1,
		LeadershipCatchUpRounds: 500,
		//ShutdownOnRemove: false,
	}
	if store == nil {
		store = newMemoryStore()
	}
	if rpc == nil {
		rpc = newMemRpc(localID)
	}
	fsm := newMemFSM()
	fileSnapshot, err := NewFileSnapshot("./testsnapshot/snapshot"+localID, false, 3)
	if err != nil {
		panic(err)
	}
	raft, err := NewRaft(conf, fsm, rpc, store, store, fileSnapshot)
	if err != nil {
		panic(err)
	}
	return raft, fsm
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
		*Raft
	}
	configGetHandle struct {
		*Raft
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
	addPeerHandle struct {
		raftList []*Raft
	}
)

func (g *leaderTransferHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	raft := getLeader(g.raftList...)
	fu := raft.LeaderTransfer("", "")
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
		writer.Write([]byte("fail" + err.Error()))
	} else {
		writer.Write([]byte("succ"))
	}

}
func (s *verifyHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fu := s.Raft.VerifyLeader()
	_, err := fu.Response()

	if err != nil {
		writer.Write([]byte("fail" + err.Error()))
	} else {
		writer.Write([]byte("succ"))
	}

}
func (s *configGetHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {

	cluster := s.Raft.GetConfiguration()
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
func (s *addPeerHandle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//addr := request.URL.Query().Get("addr")
	//id := request.URL.Query().Get("id")
	//
	//leader := getLeader(s.raftList...)
	//fu := leader.AddVoter(ServerID(id), ServerAddr(addr), 0, time.Second)
	//_, err := fu.Response()
	//if err != nil {
	//	writer.Write([]byte(err.Error()))
	//} else {
	//	writer.Write([]byte("succ"))
	//}
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
