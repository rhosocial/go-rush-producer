package node

import (
	"context"
	"log"
	"math"
	"sync"

	"github.com/rhosocial/go-rush-producer/models"
	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolSlaves struct {
	NodesRWLock sync.RWMutex
	Nodes       map[uint64]NodeInfo.NodeInfo
	NodesRetry  map[uint64]uint8
	NextTurn    uint32

	WorkerCancelFunc       context.CancelCauseFunc
	WorkerCancelFuncRWLock sync.RWMutex

	DetectInactiveCallback func(id uint64, retry uint8)
}

// ---- Turn ---- //

func (ps *PoolSlaves) Count() int {
	return len(ps.Nodes)
}

func (ps *PoolSlaves) incTurn() {
	ps.NextTurn += 1
}

func (ps *PoolSlaves) GetTurn() uint32 {
	defer ps.incTurn()
	return ps.NextTurn
}

// GetTurnCandidate 获取候选接替顺序的节点ID。如果没有候选，则返回0。
func (ps *PoolSlaves) GetTurnCandidate() uint64 {
	ps.NodesRWLock.RLock()
	defer ps.NodesRWLock.RUnlock()
	turn := uint32(math.MaxUint32)
	target := uint64(0)
	for i, v := range ps.Nodes {
		if turn >= v.Turn {
			turn = v.Turn
			target = i
		}
	}
	return target
}

// ---- Turn ---- //

// ---- Worker ---- //

func (ps *PoolSlaves) IsWorking() bool {
	return ps.WorkerCancelFunc != nil
}

// ---- Worker ---- //

func (ps *PoolSlaves) Get(id uint64) *NodeInfo.NodeInfo {
	node, exist := ps.Nodes[id]
	if !exist {
		return nil
	}
	return &node
}

// RetryUpAll 所有节点重试次数加 1。
func (ps *PoolSlaves) RetryUpAll() {
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	for i := range ps.Nodes {
		ps.NodesRetry[i] += 1
	}
}

// RetryUp 尝试次数递增。
func (ps *PoolSlaves) RetryUp(id uint64) uint8 {
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	if ps.NodesRetry[id] == math.MaxUint8 { // 如果已经达到最大值，则不再增大。
		return ps.NodesRetry[id]
	}
	ps.NodesRetry[id] += 1
	return ps.NodesRetry[id]
}

// RetryDown 尝试次数递减。
func (ps *PoolSlaves) RetryDown(id uint64) uint8 {
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	if ps.NodesRetry[id] == 0 { // 如果已经达到最小值，则不再减小。
		return ps.NodesRetry[id]
	}
	ps.NodesRetry[id] -= 1
	return ps.NodesRetry[id]
}

// RetryClear 尝试次数清空。
func (ps *PoolSlaves) RetryClear(id uint64) {
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	ps.NodesRetry[id] = 0
}

// GetRetry 获取重试次数。
func (ps *PoolSlaves) GetRetry(id uint64) uint8 {
	ps.NodesRWLock.RLock()
	defer ps.NodesRWLock.RUnlock()
	return ps.NodesRetry[id]
}

// RetryUpAllAndRemoveIfRetriedOut 重试次数调升，且在重试达到“不活跃上限”或“移除上限”后报告“不活跃”或“移除”。
//
// 注意 limitInactive 须比 limitRemoved 小，否则可能产生意想不到的后果。
//
// 返回被删除的节点ID数组指针。
func (ps *PoolSlaves) RetryUpAllAndRemoveIfRetriedOut(limitInactive uint8, limitRemoved uint8) *[]uint64 {
	if limitInactive >= limitRemoved {
		log.Printf("Warning! the limit of inactive(%d) is greater than or equal to limit of removed(%d).\n", limitInactive, limitRemoved)
	}
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	removed := make([]uint64, 0)
	for i := range ps.Nodes {
		ps.NodesRetry[i] += 1
		node := ps.Get(i)
		if ps.NodesRetry[i] >= limitInactive && ps.NodesRetry[i] < limitRemoved && node != nil && ps.DetectInactiveCallback != nil {
			go ps.DetectInactiveCallback(i, ps.NodesRetry[i])
		}
		if ps.NodesRetry[i] >= limitRemoved {
			if node != nil {
				_, err := node.RemoveSelf()
				if err != nil {
					log.Println(err)
				}
			}
			delete(ps.Nodes, i)
			removed = append(removed, i)
		}
	}
	return &removed
}

// GetRegisteredNodeInfos 获取所有从节点信息。
func (ps *PoolSlaves) GetRegisteredNodeInfos() *map[uint64]*models.RegisteredNodeInfo {
	ps.NodesRWLock.RLock()
	defer ps.NodesRWLock.RUnlock()

	slaves := make(map[uint64]*models.RegisteredNodeInfo)
	for i, v := range ps.Nodes {
		slaves[i] = v.ToRegisteredNodeInfo()
		slaves[i].Retry = ps.NodesRetry[i]
	}
	return &slaves
}

// AddSlaveNode 添加从节点信息。
func (ps *PoolSlaves) AddSlaveNode(slave NodeInfo.NodeInfo) bool {
	//if slave == nil {
	//	return false
	//}
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	ps.Nodes[slave.ID] = slave
	return true
}

// Refresh 刷新节点。
// 刷新后会重新确定下一个顺序。
func (ps *PoolSlaves) Refresh(nodes *[]NodeInfo.NodeInfo) {
	result := make(map[uint64]NodeInfo.NodeInfo)
	turnMax := uint32(0)
	for _, node := range *nodes {
		if true { // TODO: 判断节点是否有效。
			result[node.ID] = node
			if node.Turn > turnMax {
				turnMax = node.Turn
			}
		}
	}
	// TODO: 此处应该加锁，但加锁后会出现死锁，待排查。
	//ps.NodesRWLock.Lock()
	//defer ps.NodesRWLock.Unlock()
	ps.Nodes = result
	ps.NextTurn = turnMax + 1
	ps.NodesRetry = make(map[uint64]uint8)
}

// Check 检查从节点是否有效。检查通过则返回节点信息 models.NodeInfo。
//
// 1. 若节点不存在，则报 ErrNodeMasterDoesNotHaveSpecifiedSlave。
//
// 2. 检查 models.FreshNodeInfo 是否与本节点维护一致。若不一致，则报 ErrNodeSlaveFreshNodeInfoInvalid。
func (ps *PoolSlaves) Check(id uint64, fresh *models.FreshNodeInfo) (*NodeInfo.NodeInfo, error) {
	// 检查指定ID是否存在，如果不是，则报错。
	// slave, exist := n.Slaves[id]
	slave := ps.Get(id)
	//if slave == nil {
	//	return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	//}
	// 再检查 FreshNodeInfo 是否相同。
	origin := models.FreshNodeInfo{
		Name:        slave.Name,
		NodeVersion: slave.NodeVersion,
		Host:        slave.Host,
		Port:        slave.Port,
	}
	if origin.IsEqual(fresh) {
		return slave, nil
	}
	return nil, ErrNodeSlaveFreshNodeInfoInvalid
}

func (ps *PoolSlaves) CheckIfExists(fresh *models.FreshNodeInfo) *NodeInfo.NodeInfo {
	for id := range ps.Nodes {
		if slave, err := ps.Check(id, fresh); err == nil {
			return slave
		}
	}
	return nil
}
