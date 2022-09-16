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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	// 表示我当前投给那个node, 值为NodeId
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 当前票数统计
	voteCount int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// 从持久化数据库中恢复当前任何和选票
	heartState, confState, _ := c.Storage.InitialState()

	if c.peers == nil {
		// 集群中所包含的全部NodeId
		c.peers = confState.Nodes
	}

	r := &Raft{
		id:      c.ID,
		Term:    heartState.GetTerm(),
		Vote:    heartState.GetVote(),
		Prs:     make(map[uint64]*Progress),
		RaftLog: newLog(c.Storage),
		// 初始为跟随者
		State: StateFollower,
		votes: make(map[uint64]bool),
		msgs:  make([]pb.Message, 0),
		Lead:  None,
		// 心跳超时时间
		heartbeatTimeout: c.HeartbeatTick,
		// 选举超时时间
		electionTimeout: c.ElectionTick,
	}

	r.RaftLog.applied = c.Applied
	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		// 如果当前节点是leader，并且触发心跳超时，则发送一个本地消息
		// 触发领导者去发送心跳消息
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	default:
		// 如果当前节点处于其他角色，并且触发了选举超时，则发送一个本地消息
		// 触发当前节点发起选举
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// 当领导者发现有比自己大的term时就直接讲自己降为Follower
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.voteCount = 0

	//TODO ???:这个值有疑问
	r.leadTransferee = 0
	// 复位超时
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Vote = r.id
	r.Term++
	//TODO ???: 这个还有必要吗，有了voteCount
	r.votes = make(map[uint64]bool)
	// 投了自己一票
	r.votes[r.id] = true
	r.voteCount++

	// 复位超时
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// 判断票数是否超过一半
	if r.voteCount >= len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id

	for _, v := range r.Prs {
		v.Match = 0
		v.Next = r.RaftLog.LastIndex() + 1

	}

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// TODO: ??? 为什么要来一条空日志(没有实际数据的空日志)
	// 就是为了通知一下，他的任期
	// NOTE: Leader should propose a noop entry on its term
	// 添加一条空日志并广播
	// 广播空日志给所有节点
	// 提交

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		return r.setpFollower(m)
	case StateCandidate:
		return r.setpCandidate(m)
	case StateLeader:
		return r.setpLeader(m)
	}

	return nil
}

func (r *Raft) setpFollower(m pb.Message) error {
	switch m.GetMsgType() {
	case pb.MessageType_MsgAppend:
	// 通知leader 发送心跳数据
	case pb.MessageType_MsgBeat:
		return nil
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	// 表示选举超时
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 广播发送投票请求
		r.bcastVoteRequest()

	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) setpCandidate(m pb.Message) error {
	switch m.GetMsgType() {
	case pb.MessageType_MsgAppend:
	// 通知leader 发送心跳数据
	case pb.MessageType_MsgBeat:
		return nil
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	// 表示选举超时
	case pb.MessageType_MsgHup:
		r.Term++
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
		})
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) setpLeader(m pb.Message) error {
	switch m.GetMsgType() {
	case pb.MessageType_MsgAppend:
	// 通知leader 发送心跳数据
	case pb.MessageType_MsgBeat:
		return nil
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	// 表示选举超时
	case pb.MessageType_MsgHup:
		r.Term++
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
		})
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(r.id)

	for id := range r.Prs {
		if id != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})

		}
	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
