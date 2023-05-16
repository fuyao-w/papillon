package papillon

import (
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkPoolSlice(b *testing.B) {
	b.Run("allocate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			list := make([]uint64, 0, 4)
			for i := uint64(0); i < 4; i++ {
				list = append(list, i)
			}
		}
	})
	b.Run("not allocate", func(b *testing.B) {
		var x = make([]uint64, 0)
		for i := 0; i < b.N; i++ {
			for i := uint64(0); i < 4; i++ {
				x = append(x, i)
			}
			x = x[:0]
		}
	})

}

func BenchmarkPoolAppendEntriesRequest(b *testing.B) {
	r := Raft{localInfo: ServerInfo{
		Suffrage: 0,
		Addr:     "1",
		ID:       "1",
	}, currentTerm: new(atomic.Uint64),
		//appendEntriesPool: initAppendEntriesPool(),
		commitIndex: new(atomic.Uint64),
	}
	r.currentTerm.Store(111)
	r.commitIndex.Store(1231)
	b.Run("allocate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = &AppendEntryRequest{
				RPCHeader:    r.buildRPCHeader(),
				Term:         r.getCurrentTerm(),
				LeaderCommit: r.getCommitIndex(),
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	b.Run("not allocate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				//_, recycle := r.retrieveAppendEntryRequest()
				//defer recycle()
				//time.Sleep(10 * time.Millisecond)
			}()

		}
	})
}
