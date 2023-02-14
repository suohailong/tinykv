// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// in which the Scheduler scans stores and determines whether there is an imbalance and which region it should move.
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// the Scheduler will select all suitable stores
	storeInfos := cluster.GetStores()
	if len(storeInfos) <= 0 {
		log.Warnf("There is no store in the cluster ")
		return nil
	}
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range storeInfos {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	// Then sort them according to their region size
	// 按照size从大到小排序
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	// 寻找一个合适的region的策略
	// 1. 找一个pending(磁盘过载)的region
	// 2. 找一个follower region
	// 3. 找一个leader region
	// 4. 找到了就移动， 找不到则开启下一轮寻找，知道尝试完所有的region
	var moveFrom *core.RegionInfo
	var moveFromStore *core.StoreInfo
	for _, target := range suitableStores {
		cluster.GetPendingRegionsWithLock(target.GetID(), func(rc core.RegionsContainer) {
			moveFrom = rc.RandomRegion(nil, nil)
		})
		if moveFrom != nil {
			moveFromStore = target
			break
		}
		cluster.GetFollowersWithLock(target.GetID(), func(rc core.RegionsContainer) {
			moveFrom = rc.RandomRegion(nil, nil)
		})
		if moveFrom != nil {
			moveFromStore = target
			break
		}
		cluster.GetLeadersWithLock(target.GetID(), func(rc core.RegionsContainer) {
			moveFrom = rc.RandomRegion(nil, nil)
		})
		if moveFrom != nil {
			moveFromStore = target
			break
		}
	}
	if moveFrom == nil {
		log.Warn("suitable region not found")
		return nil
	}
	// 当前region副本数小于集群最大副本数量直接放弃本次操作
	if len(moveFrom.GetStoreIds()) < cluster.GetMaxReplicas() {
		log.Warnf("<region %d> not has enough replicas", moveFrom.GetID())
		return nil
	}

	var moveToStore *core.StoreInfo
	for i := len(suitableStores) - 1; i > 0; i-- {
		store := suitableStores[i]
		fmt.Println("region size", store.GetRegionSize())
		// 目标store不能在原来的region里
		if _, ok := moveFrom.GetStoreIds()[store.GetID()]; !ok {
			moveToStore = store
			break
		}
	}

	if moveToStore == nil {
		log.Warn("target store not found")
		return nil
	}
	if moveFromStore.GetRegionSize()-moveToStore.GetRegionSize() < 2*moveFrom.GetApproximateSize() {
		log.Warn("suitable store regionSize - target regionsize < 2 * suitable.ApproximateSize")
		return nil
	}
	peer, err := cluster.AllocPeer(moveToStore.GetID())
	if err != nil {
		log.Errorf("<store %d> alloc new peer failed && err=%+v", moveToStore.GetID(), err)
		return nil
	}
	// log.Infof("operator.CreateMovePeerOperator move region: %d from store: %d, to store: %d", moveFrom.GetID(), movefro)
	op, err := operator.CreateMovePeerOperator(
		fmt.Sprintf(
			"<region %d> move from <store %d> to <store %d>",
			moveFrom.GetID(),
			moveFromStore.GetID(),
			moveToStore.GetID(),
		),
		cluster,
		moveFrom,
		operator.OpBalance,
		moveFromStore.GetID(),
		moveToStore.GetID(),
		peer.GetId(),
	)
	if err != nil {
		log.Errorf("create move peer operator failed && err=%+v", err)
		return nil
	}
	return op
}
