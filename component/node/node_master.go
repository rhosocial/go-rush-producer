package node

import (
	"context"
	"errors"
	"sync"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolMaster struct {
	Node                   *models.NodeInfo
	WorkerCancelFunc       context.CancelCauseFunc
	WorkerCancelFuncRWLock sync.RWMutex
}

func (pm *PoolMaster) Start(ctx context.Context) {
	pm.WorkerCancelFuncRWLock.Lock()
	defer pm.WorkerCancelFuncRWLock.Unlock()
	if pm.IsWorking() {
		return
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	pm.WorkerCancelFunc = cancel
	go pm.worker(ctxChild, WorkerMasterIntervals{
		Base: 1000,
	})
}

func (pm *PoolMaster) Stop() {
	pm.WorkerCancelFuncRWLock.Lock()
	defer pm.WorkerCancelFuncRWLock.Unlock()
	pm.WorkerCancelFunc(errors.New("stop"))
	pm.WorkerCancelFunc = nil
}

func (pm *PoolMaster) IsWorking() bool {
	return pm.WorkerCancelFunc != nil
}

func (pm *PoolMaster) Accept(master *models.NodeInfo) {
	pm.Node = master
}
