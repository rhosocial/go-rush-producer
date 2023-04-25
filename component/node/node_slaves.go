package node

import (
	"context"
	"math"
	"sync"

	base "github.com/rhosocial/go-rush-producer/models"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolSlaves struct {
	NodesRWLock sync.RWMutex
	Nodes       map[uint64]*models.NodeInfo
	NodesRetry  map[uint64]uint8
	NextTurn    uint

	WorkerCancelFunc       context.CancelCauseFunc
	WorkerCancelFuncRWLock sync.RWMutex
}

func (ps *PoolSlaves) Count() int {
	return len(ps.Nodes)
}

func (ps *PoolSlaves) incTurn() {
	ps.NextTurn += 1
}

func (ps *PoolSlaves) GetTurn() uint {
	defer ps.incTurn()
	return ps.NextTurn
}

// ---- Worker ---- //

func (ps *PoolSlaves) IsWorking() bool {
	return ps.WorkerCancelFunc != nil
}

// ---- Worker ---- //

func (ps *PoolSlaves) Get(id uint64) *models.NodeInfo {
	if ps.NodesRetry == nil {
		ps.NodesRetry = make(map[uint64]uint8)
	}
	return ps.Nodes[id]
}

// RetryUp 尝试次数递增。
func (ps *PoolSlaves) RetryUp(id uint64) uint8 {
	retry := ps.GetRetry(id)
	if retry == math.MaxUint8 { // 如果已经达到最大值，则不再增大。
		return retry
	}
	ps.NodesRetry[id] += 1
	return ps.NodesRetry[id]
}

// RetryDown 尝试次数递减。
func (ps *PoolSlaves) RetryDown(id uint64) uint8 {
	retry := ps.GetRetry(id)
	if retry == 0 { // 如果已经达到最小值，则不再减小。
		return retry
	}
	ps.NodesRetry[id] -= 1
	return ps.NodesRetry[id]
}

// RetryClear 尝试次数清空。
func (ps *PoolSlaves) RetryClear(id uint64) {
	if ps.NodesRetry == nil {
		ps.NodesRetry = make(map[uint64]uint8)
	}
	ps.NodesRetry[id] = 0
}

// GetRetry 获取重试次数。
func (ps *PoolSlaves) GetRetry(id uint64) uint8 {
	if ps.NodesRetry == nil {
		ps.NodesRetry = make(map[uint64]uint8)
	}
	ps.RetryClear(id)
	return 0
}

func (ps *PoolSlaves) GetRegisteredNodeInfos() *map[uint64]*base.RegisteredNodeInfo {
	ps.NodesRWLock.RLock()
	defer ps.NodesRWLock.RUnlock()

	slaves := make(map[uint64]*base.RegisteredNodeInfo)
	for i, v := range ps.Nodes {
		slaves[i] = models.InitRegisteredWithModel(v)
	}
	return &slaves
}

// AddSlaveNode 添加从节点信息。
func (ps *PoolSlaves) AddSlaveNode(slave *models.NodeInfo) bool {
	if slave == nil {
		return false
	}
	ps.NodesRWLock.Lock()
	defer ps.NodesRWLock.Unlock()
	ps.Nodes[slave.ID] = slave
	return true
}

// Refresh 刷新节点。
// 刷新后会重新确定下一个顺序。
func (ps *PoolSlaves) Refresh(nodes *[]models.NodeInfo) {
	result := make(map[uint64]*models.NodeInfo)
	turnMax := uint(0)
	for _, node := range *nodes {
		if true { // TODO: 判断节点是否有效。
			result[node.ID] = &node
			if node.Turn > turnMax {
				turnMax = node.Turn
			}
		}
	}
	ps.Nodes = result
	ps.NextTurn = turnMax + 1
}

// Check 检查从节点是否有效。检查通过则返回节点信息 models.NodeInfo。
//
// 1. 若节点不存在，则报 ErrNodeMasterDoesNotHaveSpecifiedSlave。
//
// 2. 检查 models.FreshNodeInfo 是否与本节点维护一致。若不一致，则报 ErrNodeSlaveFreshNodeInfoInvalid。
func (ps *PoolSlaves) Check(id uint64, fresh *base.FreshNodeInfo) (*models.NodeInfo, error) {
	// 检查指定ID是否存在，如果不是，则报错。
	// slave, exist := n.Slaves[id]
	slave := ps.Get(id)
	if slave == nil {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	// 再检查 FreshNodeInfo 是否相同。
	origin := base.FreshNodeInfo{
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

func (ps *PoolSlaves) CheckIfExists(fresh *base.FreshNodeInfo) *models.NodeInfo {
	for id, _ := range ps.Nodes {
		if slave, err := ps.Check(id, fresh); err == nil {
			return slave
		}
	}
	return nil
}
