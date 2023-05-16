package papillon

import (
	. "github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestCreate(t *testing.T) {
	Convey("create", t, func() {
		content := []byte("a,b,c,d")
		meta := &SnapshotMeta{
			Version: 1,
			ID:      "",
			Index:   1,
			Term:    1,
			Configuration: ClusterInfo{
				[]ServerInfo{{
					Voter, "aa", "xxx",
				}},
			},
			ConfigurationIndex: 11,
			Size:               0,
		}
		snapshot := newMemSnapShot()
		sink, _ := snapshot.Create(meta.Version, meta.Index, meta.Term, meta.Configuration, meta.ConfigurationIndex, nil)
		sink.Write(content)

		sink.Cancel()
		list, _ := snapshot.List()
		So(len(list), ShouldEqual, 1)
		newMeta := *list[0]
		newMeta.Size = 0
		newMeta.ID = ""
		So(reflect.DeepEqual(&newMeta, meta), ShouldBeTrue)

		meta, reader, _ := snapshot.Open(list[0].ID)
		buf := make([]byte, meta.Size)
		reader.Read(buf)
		So(reflect.DeepEqual(content, buf), ShouldBeTrue)
	})
}
