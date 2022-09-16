package raft

import (
	"fmt"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func TestNewLog(t *testing.T) {

	storage := NewMemoryStorage()
	entries := []eraftpb.Entry{}
	for i := 0; i < 5; i++ {

		entries = append(entries, eraftpb.Entry{
			EntryType: eraftpb.EntryType_EntryNormal,
			Term:      1,
			Index:     uint64(i),
			Data:      []byte("hahahha@" + fmt.Sprint(i)),
		})
	}

	storage.Append(entries)

	fmt.Println(storage.ents)

	l := newLog(storage)
	fmt.Println(l)

}
