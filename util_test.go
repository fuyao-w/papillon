package papillon

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	Convey("", t, func() {
		d := exponentialBackoff(backoffBaseDuration, time.Second, 1, maxBackoffRounds)
		So(d, ShouldEqual, backoffBaseDuration)
		d = exponentialBackoff(backoffBaseDuration, time.Second, 2, maxBackoffRounds)
		So(d, ShouldEqual, backoffBaseDuration)
		d = exponentialBackoff(backoffBaseDuration, time.Second, 3, maxBackoffRounds)
		So(d, ShouldEqual, backoffBaseDuration*2)
		d = exponentialBackoff(backoffBaseDuration, time.Second, 4, maxBackoffRounds)
		So(d, ShouldEqual, backoffBaseDuration*4)
		d = exponentialBackoff(backoffBaseDuration, time.Second, maxBackoffRounds+1, maxBackoffRounds)
		So(d, ShouldEqual, time.Second)
		d = exponentialBackoff(backoffBaseDuration, time.Hour*50, maxBackoffRounds+2, 11)
		So(d, ShouldEqual, 5120*time.Millisecond)
	})
}
