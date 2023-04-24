package node

import (
	"context"
	"errors"
	"sync"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolSlaves struct {
	Nodes                  map[uint64]*models.NodeInfo
	NodesRWMutex           sync.RWMutex
	WorkerCancelFunc       context.CancelCauseFunc
	WorkerCancelFuncRWLock sync.RWMutex
}

// ---- Worker ---- //

func (ps *PoolSlaves) Start(ctx context.Context, self *PoolSelf, master *PoolMaster) {
	ps.WorkerCancelFuncRWLock.Lock()
	defer ps.WorkerCancelFuncRWLock.Unlock()
	if ps.IsWorking() {
		return
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	ps.WorkerCancelFunc = cancel
	go ps.worker(ctxChild, WorkerSlaveIntervals{
		Base: 1000,
	}, self, master)
}

func (ps *PoolSlaves) Stop() {
	ps.WorkerCancelFuncRWLock.Lock()
	defer ps.WorkerCancelFuncRWLock.Unlock()
	ps.WorkerCancelFunc(errors.New("stop"))
	ps.WorkerCancelFunc = nil
}

func (ps *PoolSlaves) IsWorking() bool {
	return ps.WorkerCancelFunc != nil
}

// ---- Worker ---- //

func (ps *PoolSlaves) Get(id uint64) *models.NodeInfo {
	return ps.Nodes[id]
}

func (ps *PoolSlaves) GetRegisteredNodeInfos() *map[uint64]*models.RegisteredNodeInfo {
	ps.NodesRWMutex.RLock()
	defer ps.NodesRWMutex.RUnlock()

	slaves := make(map[uint64]*models.RegisteredNodeInfo)
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
	ps.NodesRWMutex.Lock()
	defer ps.NodesRWMutex.Unlock()
	ps.Nodes[slave.ID] = slave
	return true
}

// Refresh 刷新节点。
func (ps *PoolSlaves) Refresh(nodes *[]models.NodeInfo) {
	result := make(map[uint64]*models.NodeInfo)
	for _, node := range *nodes {
		if true { // TODO: 判断节点是否有效。
			result[node.ID] = &node
		}
	}
	ps.Nodes = result
}

// Check 检查从节点是否有效。检查通过则返回节点信息 models.NodeInfo。
//
// 1. 若节点不存在，则报 ErrNodeMasterDoesNotHaveSpecifiedSlave。
//
// 2. 检查 models.FreshNodeInfo 是否与本节点维护一致。若不一致，则报 ErrNodeSlaveFreshNodeInfoInvalid。
func (ps *PoolSlaves) Check(id uint64, fresh *models.FreshNodeInfo) (*models.NodeInfo, error) {
	// 检查指定ID是否存在，如果不是，则报错。
	// slave, exist := n.Slaves[id]
	slave := ps.Get(id)
	if slave == nil {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
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
