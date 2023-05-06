package papillon

import (
	"bufio"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"

	"testing"
)

type testRW struct {
	buf []byte
}

func (t *testRW) Read(p []byte) (n int, err error) {
	copy(p, t.buf)
	if len(p) < len(t.buf) {
		t.buf = t.buf[len(p):]
		n = len(p)
		return
	}
	n = len(t.buf)
	t.buf = nil
	return
}

func (t *testRW) Write(p []byte) (n int, err error) {
	t.buf = append(t.buf, p...)
	return len(p), nil
}

func TestProto(t *testing.T) {
	rw := &testRW{}
	testCase := []struct {
		typ  rpcType
		data string
	}{
		{
			1, string(make([]byte, 1024*4)),
		}, {
			100, "",
		},
	}
	for i, s := range testCase {
		Convey(fmt.Sprintf("TestProto :%d", i), t, func() {
			w := bufio.NewWriter(rw)
			So(defaultPackageParser.Encode(w, s.typ, []byte(s.data)), ShouldBeNil)
			w.Flush()
			r := bufio.NewReader(rw)
			typ, data, err := defaultPackageParser.Decode(r)
			So(err, ShouldBeNil)
			So(typ, ShouldEqual, s.typ)
			So(string(data), ShouldEqual, s.data)
		})
	}
	rw = &testRW{
		buf: []byte("!"),
	}
	_, _, err := defaultPackageParser.Decode(bufio.NewReader(rw))
	Convey("test magic ", t, func() {
		So(err, ShouldEqual, errUnrecognizedRequest)
	})
}
