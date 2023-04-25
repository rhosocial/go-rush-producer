package node

import (
	"context"
	"math"
	"sync"

	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolMaster struct {
	Node                   *NodeInfo.NodeInfo
	WorkerCancelFunc       context.CancelCauseFunc
	WorkerCancelFuncRWLock sync.RWMutex
	Retry                  uint8
}

func (pm *PoolMaster) IsWorking() bool {
	return pm.WorkerCancelFunc != nil
}

func (pm *PoolMaster) Accept(master *NodeInfo.NodeInfo) {
	pm.Node = master
}

// RetryUp 尝试次数递增。
func (pm *PoolMaster) RetryUp() uint8 {
	if pm.Retry == math.MaxUint8 { // 如果已经达到最大值，则不再增大。
		return pm.Retry
	}
	pm.Retry += 1
	return pm.Retry
}

// RetryDown 尝试次数递减。
func (pm *PoolMaster) RetryDown() uint8 {
	if pm.Retry == 0 { // 如果已经达到最小值，则不再减小。
		return 0
	}
	pm.Retry -= 1
	return pm.Retry
}

// RetryClear 尝试次数清空。
func (pm *PoolMaster) RetryClear() {
	pm.Retry = 0
}
