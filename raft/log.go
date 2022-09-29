// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 大多数状态机都确认过提交的日志位置
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 当前node，日志应用到了什么位置，还没有commit
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 已经持久化保存的日志的位置,也就说持久化到了什么位置
	stabled uint64

	// all entries that have not yet compact.
	// 日志
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 当前节点正在应用的快照
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// first index in storage
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	first, err := storage.FirstIndex()
	last, err := storage.LastIndex()

	entries, _ := storage.Entries(first, last+1)

	hardState, _, err := storage.InitialState()
	if err != nil {
		return nil
	}
	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    first - 1,
		stabled:    last,
		entries:    entries,
		firstIndex: first,
	}
}
func (l *RaftLog) appendEntries(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled+1 >= l.firstIndex {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		start := l.applied - l.firstIndex + 1
		end := min(l.committed-l.firstIndex+1, uint64(len(l.entries)))
		return l.entries[start:end]
	}
	return nil
}
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	if lo >= l.firstIndex && hi-l.firstIndex <= uint64(len(l.entries)) {
		return l.entries[lo-l.firstIndex : hi-l.firstIndex]
	}
	ents, _ := l.storage.Entries(lo, hi)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	index, _ := l.storage.FirstIndex()
	return index - 1
}
func (l *RaftLog) RemoveAfter(index uint64) {
	// 因为删除了日志，所以需要调整l.stable指针
	l.stabled = min(l.stabled, index-1)

	if index-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index-l.firstIndex]
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.firstIndex {
		if i > l.LastIndex() {
			return 0, errors.New("ErrUnavailable")
		}
		return l.entries[i-l.firstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, errors.New("storageTermErr")
	}
	return term, nil
}
