package papillon

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCommand(t *testing.T) {
	Convey("", t, func() {
		fu := &testFuture{}
		fu.init()
		new(Raft).processCommand(&command{
			enum:     commandTest,
			callback: fu,
		})
		_, err := fu.Response()
		So(err, ShouldNotBeNil)
		fu = &testFuture{}
		fu.init()
		r := Raft{
			state: Leader,
		}
		r.processCommand(&command{
			enum:     commandTest,
			callback: fu,
		})
		resp, err := fu.Response()
		So(resp, ShouldEqual, "succ")
		So(err, ShouldBeNil)
	})

}
