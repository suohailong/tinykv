package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) processNormalRequest(en eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	// FIXME: 这里为什么要取第0个元素, 其余元素不处理吗
	req := msg.Requests[0]
	// 1. 检查key是否属于当前region
	key := d.getRequestKey(req)
	if key != nil {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			d.handleProposal(en, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
	}
	// 2. apply delete 和 put
	if req.CmdType == raft_cmdpb.CmdType_Delete {
		wb.DeleteCF(req.Delete.GetCf(), req.Delete.GetKey())
	}
	if req.CmdType == raft_cmdpb.CmdType_Put {
		wb.SetCF(req.Put.GetCf(), req.Put.Key, req.Put.Value)
	}
	// 3. 处理proposals
	// fmt.Println("apply", msg)
	d.handleProposal(en, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.peerStorage.applyState.AppliedIndex = en.GetIndex()
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)
			value, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.GetKey())
			if err != nil {
				log.Errorf("engine_util GetCF err: %v, CF: [%v], key:[%v]", err, req.Get.Cf, string(req.Get.GetKey()))
				value = nil
			}
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: value,
					},
				},
			}
			wb = &engine_util.WriteBatch{}
		case raft_cmdpb.CmdType_Put:
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				},
			}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				},
			}
		case raft_cmdpb.CmdType_Snap:
			// log.Warnf("%s get snap, region: %v", d.Tag, d.Region())
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			d.peerStorage.applyState.AppliedIndex = en.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.ctx.engine.Kv)
			wb = &engine_util.WriteBatch{}
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{
						Region: d.Region(),
					},
				},
			}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			// log.Warnf("%s get snap complete, region: %v", d.Tag, d.Region())
		}
		p.cb.Done(resp)

	})

	return wb
}

func (d *peerMsgHandler) processAdminRequest(en eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		applystate := d.peerStorage.applyState
		if req.CompactLog.CompactIndex >= applystate.TruncatedState.Index {
			applystate.TruncatedState.Index = req.CompactLog.CompactIndex
			applystate.TruncatedState.Term = req.CompactLog.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId), applystate)
		}
	case raft_cmdpb.AdminCmdType_Split:
		// log.Infof("apply split cmd, msg: %v", msg)
		leftRegion := d.Region()
		// 检查RegionEpoch是否改变
		if err, ok := util.CheckRegionEpoch(msg, leftRegion, true).(*util.ErrEpochNotMatch); ok {
			d.handleProposal(en, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
		// 检查splitkey是否在当前region中
		if err := util.CheckKeyInRegion(req.Split.SplitKey, leftRegion); err != nil {
			d.handleProposal(en, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}

		// 1. 克隆出一个新的region, 并初始化
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		// 5. 持久化 leftRegion 和 rightRegion 的信息。
		storeMeta.regionRanges.Delete(&regionItem{region: leftRegion})
		leftRegion.RegionEpoch.Version++
		newPeers := make([]*metapb.Peer, 0)
		for i, peer := range leftRegion.Peers {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      req.Split.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
		}
		rightRegion := &metapb.Region{
			Id:       req.Split.NewRegionId,
			StartKey: req.Split.SplitKey,
			EndKey:   leftRegion.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: newPeers,
		}
		storeMeta.regions[rightRegion.Id] = rightRegion
		leftRegion.EndKey = req.Split.SplitKey
		// storeMeta.regions[leftRegion.Id] = leftRegion
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
		storeMeta.Unlock()
		meta.WriteRegionState(wb, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(wb, rightRegion, rspb.PeerState_Normal)
		// 清除region size
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		// 6. 通过create peer创建新的peer并注册到router
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newPeer)
		// 启动当前peer
		d.ctx.router.send(rightRegion.Id, message.NewMsg(message.MsgTypeStart, nil))
		// 回复客户端提议请求
		d.handleProposal(en, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split: &raft_cmdpb.SplitResponse{
						Regions: []*metapb.Region{leftRegion, rightRegion},
					},
				},
			})
		})
		//7 快速刷新 scheduler 那里的 region 缓存
		// if d.IsLeader() {
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)
		// }
		// log.Infof("finish split region %v, newRegion: %v", leftRegion, rightRegion)
	}
	return wb
}

// 应用节点变更日志
func (d *peerMsgHandler) applyNodeConfChange(en eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	confChange := &eraftpb.ConfChange{}
	if err := confChange.Unmarshal(en.Data); err != nil {
		panic(err)
	}
	raftCmdRequest := &raft_cmdpb.RaftCmdRequest{}
	err := raftCmdRequest.Unmarshal(confChange.Context)
	if err != nil {
		panic(err)
	}
	req := raftCmdRequest.AdminRequest
	region := d.Region()
	// fmt.Println("apply change perr: ", req)

	// 检查regionEpoch 是否匹配
	if err, ok := util.CheckRegionEpoch(raftCmdRequest, region, true).(*util.ErrEpochNotMatch); ok {
		d.handleProposal(en, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return wb
	}

	// change the RegionLocalState, including RegionEpoch and Peers in Region
	// Call ApplyConfChange() of raft.RawNode
	pendingIndex := -1
	for index, peer := range region.Peers {
		if confChange.NodeId == peer.Id {
			pendingIndex = index
			break
		}
	}

	switch req.ChangePeer.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if pendingIndex == -1 {
			peer := req.ChangePeer.Peer
			// 添加节点到region.Peers
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			// 持久化region到kvdb中, rspb.PeerState_Normal 指的是正常提供服务
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			// 添加缓存
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if confChange.NodeId == d.Meta.Id {
			// 如果peer正是当前节点则d.destroyPeer()
			d.destroyPeer()
			return wb
		}
		if pendingIndex != -1 {
			// 删除region.Peers中的peer
			region.Peers = append(region.Peers[:pendingIndex], region.Peers[pendingIndex+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			// 持久化region到kvdb中, rspb.PeerState_Normal 指的是正常提供服务
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			// 清除缓存
			d.removePeerCache(confChange.NodeId)
		}
	}
	// d.RaftGroup.ApplyConfChange()
	d.RaftGroup.ApplyConfChange(*confChange)

	// 回复客户端提议请求
	d.handleProposal(en, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			},
		})
	})
	log.Warnf("confchannge notify scheduler flush cache")
	d.notifyHeartbeatScheduler(region, d.peer)
	// if d.IsLeader() {
	// 	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	// }
	return wb
}

func (d *peerMsgHandler) handleProposal(entry eraftpb.Entry, handler func(*proposal)) {
	// 如果提议不为空
	if len(d.proposals) > 0 {
		// 这里解析成request,就是为了判断request的类型
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term != entry.Term {
				// 如果日志的任期不匹配,报错
				p.cb.Done(ErrRespStaleCommand(entry.Term))
			} else {
				handler(p)
			}
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) processApply(en eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	if en.EntryType == eraftpb.EntryType_EntryConfChange {
		// apply 节点变更的日志必须在这里处理， 因为entry中存储的并不是adminRequest
		return d.applyNodeConfChange(en, wb)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	// 这里解析成request,就是为了判断request的类型
	err := msg.Unmarshal(en.Data)
	if err != nil {
		panic(err)
	}
	if len(msg.Requests) > 0 {
		// log.Infof("apply normal cmd, msg: %v", msg)
		wb := d.processNormalRequest(en, msg, wb)
		// log.Infof("finish apply normal cmd, msg: %v", msg)
		return wb
	}
	if msg.AdminRequest != nil {
		return d.processAdminRequest(en, msg, wb)
	}

	return wb

}

// 这里代表raftstore处理完消息，完成raft状态机转换之后并回复raft消息
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.peer.RaftGroup.HasReady() {
		// 1. 获取raft日志及状态持久化到peerStorage
		ready := d.peer.RaftGroup.Ready()
		// raft日志和raftLocalState保存在raftDB中
		applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			log.Errorf("save ready state failed: %v", err)
			panic(err)
		}
		// 2.处理storeMeta
		if applySnapResult != nil && !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			d.peerStorage.SetRegion(applySnapResult.Region)
			//storeMeta 保存当前node上所有的region信息
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[applySnapResult.Region.GetId()] = applySnapResult.Region
			storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			storeMeta.Unlock()
		}

		// 3.发送消息给其他peer
		d.Send(d.ctx.trans, ready.Messages)
		// 4.committedEntries是已经提交的但是还未被apply的日志，这里apply日志
		if len(ready.CommittedEntries) > 0 {
			wb := &engine_util.WriteBatch{}
			for _, en := range ready.CommittedEntries {
				// 还原message中的request
				wb = d.processApply(en, wb)
				if d.stopped {
					return
				}
			}
			//更新apply索引
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			// RaftApplyState 保存到peerStorage中的kvDB中
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			// kv值也保存到KvDB中
			wb.WriteToDB(d.peerStorage.Engines.Kv)

			// 启动logGc任务
			d.ScheduleCompactLog(d.peerStorage.truncatedIndex())

		}
		// 5. 更新raft状态机
		d.peer.RaftGroup.Advance(ready)
	}
}

// 这里代表收到并发送给raftstore消息
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		// 处理raft命令
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		// 提交也分裂提案
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) getRequestKey(request *raft_cmdpb.Request) []byte {
	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		return request.Get.Key
	case raft_cmdpb.CmdType_Delete:
		return request.Delete.Key
	case raft_cmdpb.CmdType_Put:
		return request.Put.Key
	}
	return nil
}
func (d *peerMsgHandler) proposalNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 1. 判断key是否存在于当前的region中
	if key := d.getRequestKey(msg.Requests[0]); key != nil {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	// 序列化消息
	msgBytes, err := msg.Marshal()
	if err != nil {
		// 这里为什么要求程序崩溃
		panic(err)
	}
	// 记录命令回调
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
	// 插入raft日志
	err = d.peer.RaftGroup.Propose(msgBytes)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
}

func (d *peerMsgHandler) proposalAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	// 回收已经应用的日志
	case raft_cmdpb.AdminCmdType_CompactLog:
		msgBytes, err := msg.Marshal()
		if err != nil {
			// 这里为什么要求程序崩溃
			panic(err)
		}
		d.peer.RaftGroup.Propose(msgBytes)
	// 切换领导者
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.peer.RaftGroup.TransferLeader(req.GetTransferLeader().GetPeer().GetId())
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	// 更改节点配置
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// 只有更改节点配置的这条日志被应用了之后才能开始下次更改
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			return
		}
		// 保存提议请求，用于异步回调给客户端
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		msgBytes, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.peer.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: req.GetChangePeer().GetChangeType(),
			NodeId:     req.GetChangePeer().GetPeer().GetId(),
			Context:    msgBytes, // raft请求
		})
	// region 分裂
	case raft_cmdpb.AdminCmdType_Split:
		// log.Infof("start to split proposal, current regions: %v", len(d.ctx.storeMeta.regions))
		// for k, v := range d.ctx.storeMeta.regions {
		// 	log.Infof("region: %d, startKey: %s, endKey: %s", k, string(v.StartKey), string(v.EndKey))
		// }

		// 检查当前regionEpoch是否已经改变
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		// 检查splitKey 是否在当前region中
		err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		// fmt.Printf("proposal split msg: %v, index: %d, term: %d \n", msg, d.nextProposalIndex(), d.Term())
		// 缓存提议请求
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})

		msgBytes, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.peer.RaftGroup.Propose(msgBytes)
	}
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// fmt.Println("proposal: ", msg)
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest == nil {
		// 处理正常的读写命令
		d.proposalNormalRequest(msg, cb)
	} else {
		//处理管理命令
		d.proposalAdminRequest(msg, cb)
	}

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		// 定时检查是否需要进行region分裂
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
