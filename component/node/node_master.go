package node

import (
	"context"
	"math"
	"sync"

	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

// PoolMaster 节点池主节点身份。
type PoolMaster struct {
	Node                   *NodeInfo.NodeInfo      // 节点
	WorkerCancelFunc       context.CancelCauseFunc // 主节点身份协程取消句柄。
	WorkerCancelFuncRWLock sync.RWMutex            // 操作主节点身份协程取消句柄锁
	Retry                  uint8                   // 重试次数
	RetryRWLock            sync.RWMutex            // 操作重试次数锁。
}

// IsWorking 主节点身份协程是否在工作中。
func (pm *PoolMaster) IsWorking() bool {
	return pm.WorkerCancelFunc != nil
}

// Accept 接受新的主节点。
func (pm *PoolMaster) Accept(master *NodeInfo.NodeInfo) {
	pm.Clear()
	pm.Node = master
}

// Clear 清空节点和重试次数。
func (pm *PoolMaster) Clear() {
	pm.Node = nil
	pm.Retry = 0
}

// RetryUp 尝试次数递增。
func (pm *PoolMaster) RetryUp() uint8 {
	pm.RetryRWLock.Lock()
	defer pm.RetryRWLock.Unlock()
	if pm.Retry == math.MaxUint8 { // 如果已经达到最大值，则不再增大。
		return pm.Retry
	}
	pm.Retry += 1
	return pm.Retry
}

// RetryDown 尝试次数递减。
func (pm *PoolMaster) RetryDown() uint8 {
	pm.RetryRWLock.Lock()
	defer pm.RetryRWLock.Unlock()
	if pm.Retry == 0 { // 如果已经达到最小值，则不再减小。
		return 0
	}
	pm.Retry -= 1
	return pm.Retry
}

// RetryClear 尝试次数清空。
func (pm *PoolMaster) RetryClear() {
	pm.RetryRWLock.Lock()
	defer pm.RetryRWLock.Unlock()
	pm.Retry = 0
}
