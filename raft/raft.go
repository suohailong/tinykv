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

const (
	VOTE_REJECT  = true
	VOTE_APPROVE = false
)

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
	// 状态机当前已复制日志的最大索引
	Match uint64
	// 状态机当前将要复制的下一条日志的位置
	Next uint64
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

	// 当前赞成票
	voteCount int

	// 当前不赞成票
	denialCount int

	config *Config
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
		votes: make(map[uint64]bool, len(c.peers)),
		msgs:  make([]pb.Message, 0),
		Lead:  None,
		// 心跳超时时间
		heartbeatTimeout: c.HeartbeatTick,
		// 选举超时时间
		electionTimeout: c.ElectionTick,
		config:          c,
	}
	r.RaftLog.applied = c.Applied
	lastIndex := r.RaftLog.LastIndex()
	r.Prs[r.id] = &Progress{
		Match: lastIndex,
		Next:  lastIndex + 1,
	}

	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{
				Match: lastIndex,
				Next:  lastIndex + 1,
			}
		} else {
			r.Prs[id] = &Progress{
				Match: 0,
				Next:  lastIndex + 1,
			}
		}
	}
	return r
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// 2A
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
	r.denialCount = 0

	//TODO ???:这个值有疑问
	r.leadTransferee = 0
	// 复位超时
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	randomInt := rand.Intn(r.config.ElectionTick)
	r.electionTimeout = r.config.ElectionTick + randomInt
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.voteCount = 0
	r.denialCount = 0
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
	randomInt := rand.Intn(r.config.ElectionTick)
	r.electionTimeout = r.config.ElectionTick + randomInt

	// 判断票数是否超过一半
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id

	// 当领导者初次获得权力，必须初始化所有的跟随者的吓一跳日志位置为当前领导者最新日志的索引+1
	for _, v := range r.Prs {
		v.Match = 0
		v.Next = r.RaftLog.LastIndex() + 1

	}

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	randomInt := rand.Intn(r.config.ElectionTick)
	r.electionTimeout = r.config.ElectionTick + randomInt

	// 添加一条日志到本地
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	// 广播当前日志
	r.bcastAppend()
	// 提交
	r.commit()
}

func (r *Raft) appendEntries(entries ...*pb.Entry) {
	ents := make([]pb.Entry, 0)
	for _, e := range entries {
		// TODO: 这里先不管
		if e.EntryType == pb.EntryType_EntryConfChange {

		}
		ents = append(ents, *e)
	}
	r.RaftLog.appendEntries(ents...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//for id, v := range r.Prs {
	//	fmt.Printf("node: %d, progress: %+v, currentid: %v\n", id, v, r.id)
	//}
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
	// 跟随者接收到追加日志请求
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		//fmt.Printf("%d 收到%d append请求\n", r.id, m.From)
	// 跟随者接收到心跳请求
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	// 表示选举超时
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 广播发送投票请求
		r.bcastVoteRequest()
		//fmt.Printf("%d 邀请%v投票\n", r.id, r.Prs)
	// 跟随者接收到投票请求
	case pb.MessageType_MsgRequestVote:
		//fmt.Printf("%d 收到投票请求: 投给 %d\n", r.id, m.From)
		r.handleRequestVote(m)
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) setpCandidate(m pb.Message) error {
	// 候选人
	switch m.GetMsgType() {
	// 候选人选举超时
	case pb.MessageType_MsgHup:
		//TODO: 这里为什么还需要再次转换一下角色
		r.becomeCandidate()
		// 发送投票信息
		r.bcastVoteRequest()
	// 候选人接收到追加日志请求
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// 候选人接收到给其他候选人的投票请求
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// 候选人处理投票结果
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
		//fmt.Printf("%d 收到 %d 投票 : %v\n", r.id, m.From, !m.Reject)
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) setpLeader(m pb.Message) error {
	switch m.GetMsgType() {
	// 通知leader 发送心跳数据
	case pb.MessageType_MsgBeat:
		r.bcastHertBeat()
		// 发送心跳消息
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHertbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
		//fmt.Printf("%d 收到%d append resp: %v %v %v %v\n",
		//	r.id, m.From, m.LogTerm, m.Index, m.Commit, !m.Reject)
	//以下情况在leader失联后重新连入集群可能会发生
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		// 写入日志
		latestIndex := r.RaftLog.LastIndex()
		ents := make([]*pb.Entry, 0)
		for _, e := range m.Entries {
			ents = append(ents, &pb.Entry{
				EntryType: e.EntryType,
				Term:      r.Term,
				Index:     latestIndex + 1,
				Data:      e.Data,
			})
			latestIndex += 1
		}
		r.appendEntries(ents...)
		// 广播
		r.bcastAppend()
		r.commit()
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgTimeoutNow:
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) commit() {
	commitUpdated := false
	// 从上次提交到目前最新的日志之间找到一个比当前提交大的索引，同时又是大多数跟随者同意的索引, 提交这个索引
	for i := r.RaftLog.committed; i <= r.RaftLog.LastIndex(); i++ {
		if i <= r.RaftLog.committed {
			continue
		}
		matchCnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				matchCnt++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Prs)/2 && term == r.Term {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	// TODO
	if commitUpdated {
		r.bcastAppend()
	}
}

func (r *Raft) sendHertbeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// 拒绝它继续出任领导
		Reject: reject,
		Index:  r.RaftLog.LastIndex(),
	})
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// 拒绝它继续出任领导
		Reject: reject,
		Index:  r.RaftLog.LastIndex(),
	})
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	// 对端的当前日志索引位置
	peerIndex := r.Prs[to].Next - 1
	if lastIndex < peerIndex {
		return true
	}

	// TODO: 这里没有做完
	peerTerm, err := r.RaftLog.Term(peerIndex)
	if err != nil {
		return false
	}

	entries := r.RaftLog.Entries(peerIndex+1, lastIndex+1)
	sendEntries := make([]*pb.Entry, 0)
	for _, entry := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		})
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: peerTerm,
		Index:   peerIndex,
		Entries: sendEntries,
		// TODO: 这块不理解
		Commit: r.RaftLog.committed,
	})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// 发送投票响应
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		//TODO ??
		Term:   r.Term,
		Reject: reject,
	})
}

// 处理心跳响应
func (r *Raft) handleHertbeatResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.From, None)
		return
	}
	// term 相同或者比消息发送方大, 比较日志并同步
	logIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(logIndex)
	if logTerm > m.LogTerm || (logTerm == m.LogTerm && logIndex > m.Index) {
		// 同步日志
		r.sendAppend(m.From)
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) bcastHertBeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

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

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None)
		// TODO: 这里为什么要设置成对端的id, 这里就直接给对端投票了？
		r.Vote = m.From
		return
	}
	// 只能是候选人才能收到投票回复信息
	r.votes[m.From] = !m.Reject
	if !m.Reject {
		r.voteCount++
	} else {
		r.denialCount++
	}
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.denialCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// 处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	latestIndex := r.RaftLog.LastIndex()
	latestTerm, _ := r.RaftLog.Term(latestIndex)

	if r.Term > m.GetTerm() {
		r.sendRequestVoteResponse(m.From, VOTE_REJECT)
		return
	}

	// 这里日志任期比消息中的日志任期小或者任期相同
	if latestTerm > m.GetLogTerm() || (latestTerm == m.GetLogTerm() && latestIndex > m.GetIndex()) {
		// 如果日志比需要投票的机器新, 但是任期没有发送投票消息的任期大
		if r.Term < m.GetTerm() {
			// 先成为跟随者
			r.becomeFollower(m.GetTerm(), None)
		}
		// 投反对票
		r.sendRequestVoteResponse(m.From, VOTE_REJECT)
		return
	}

	// 到这里表示，当前节点的任期不比消息的大，同时日志也不比消息的新
	if r.Term < m.GetTerm() {
		r.becomeFollower(m.GetTerm(), None)
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, VOTE_APPROVE)
		return
	}

	if r.Vote == m.From {
		// 当已经投过票给对方，但是又收到对方的投票请求，说明对方可能没收到回复需要再回复
		r.sendRequestVoteResponse(m.From, VOTE_APPROVE)
		return
	}

	// TODO: 这里为什么能是fllower在日志小于对方的时候才能投赞成票，为什么candidate不行
	// 任期相同比较日志, 只适用于fllower
	if r.State == StateFollower &&
		r.Vote == None &&
		(latestTerm < m.Term || (latestTerm == m.Term && latestIndex <= m.GetIndex())) {
		r.sendRequestVoteResponse(m.From, VOTE_APPROVE)
		return
	}

	// 重置超时时间
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, VOTE_REJECT)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject == VOTE_APPROVE {
		// 更新进度到prs
		r.Prs[m.From].Match = m.GetIndex()
		r.Prs[m.From].Next = m.GetIndex() + 1
	} else {
		// 日志不一致
		if r.Prs[m.From].Next > 0 {
			// next递减
			r.Prs[m.From].Next -= 1
			//重试
			r.sendAppend(m.From)
		}
	}
	r.commit()
	//TODO
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 当前任期大于领导者任期返回假
	if r.Term > m.Term {
		r.sendAppendResponse(m.From, VOTE_REJECT)
		return
	}
	// 如果是候选人状态，接受到append日志则转变成跟随者
	r.becomeFollower(m.Term, m.From)

	// index 指的preindex term指的是preterm
	term, err := r.RaftLog.Term(m.GetIndex())
	if err != nil || term != m.LogTerm {
		// 日志条目的任期或索引不匹配
		r.sendAppendResponse(m.From, VOTE_REJECT)
		return
	}

	// 处理冲突追加日志
	if len(m.Entries) > 0 {
		appendStart := 0
		for i, entry := range m.Entries {
			if entry.GetIndex() > r.RaftLog.LastIndex() {
				appendStart = i
				break
			}
			term, _ := r.RaftLog.Term(entry.GetIndex())
			if term != entry.GetTerm() {
				r.RaftLog.RemoveAfter(entry.GetIndex())
				break
			}
			appendStart = i
		}
		if m.Entries[appendStart].GetIndex() > r.RaftLog.LastIndex() {
			for i := appendStart; i < len(m.Entries); i++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
			}
		}
	}

	// 如果leader commit > current commit 将当前commit替换成leadercommit和上一条新日志索引的较小值
	if r.RaftLog.committed < m.Commit {
		lastNewEntry := m.Index
		if len(m.Entries) > 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].GetIndex()
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}
	r.sendAppendResponse(m.From, VOTE_APPROVE)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if r.Term > m.Term {
		r.sendHertbeatResponse(m.From, VOTE_REJECT)
		return
	}
	r.becomeFollower(m.GetTerm(), m.GetFrom())
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHertbeatResponse(m.From, VOTE_REJECT)
		return
	}

	// TODO: ????
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	// 赞同继续担任领导
	r.sendHertbeatResponse(m.From, VOTE_APPROVE)
}

// TODO: 本期不实现
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
