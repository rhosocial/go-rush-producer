package node

import (
	"context"
	"log"
	"time"
)

type WorkerSlaveIntervals struct {
	Base uint16 `json:"base"`
}

// worker 以"从节点"身份执行。
func (ps *PoolSlaves) worker(ctx context.Context, interval WorkerSlaveIntervals, nodes *Pool, process func(nodes *Pool)) {
	log.Println("Worker Slave is working...")
	if nodes.Self.Node == nil {
		return
	}
	if nodes.Master.Node == nil {
		return
	}
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println(context.Cause(ctx))
			return
		default:
			process(nodes)
		}
	}
}

func workerSlaveCheckMaster(nodes *Pool) {
	err := nodes.CheckMaster(nodes.Master.Node)
	if err == nil {
		nodes.Master.RetryClear()
	} else {
		nodes.Master.RetryUp()
		log.Println(err)
	}
	// TODO: <参数点> 从节点检查主节点最大重试次数。
	if nodes.Master.Retry >= 3 {
		// TODO: 重试次数过多，尝试主动接替。
		err := nodes.TrySupersede()
		if err != nil {
			// 表示已经有其它主节点，刷新主节点。
			fresh, err := nodes.DiscoverMasterNode(false)
			if err != nil {
				return
			}
			nodes.Master.Accept(fresh)
			nodes.Master.RetryClear()
			return
		}
		nodes.Supersede(nodes.Master.Node.ToRegisteredNodeInfo())
	}
}

type WorkerMasterIntervals struct {
	Base uint16 `json:"base"`
}

// worker 以"主节点"身份执行。
func (pm *PoolMaster) worker(ctx context.Context, interval WorkerMasterIntervals, nodes *Pool, process func(nodes *Pool)) {
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println(context.Cause(ctx))
			return
		default:
			process(nodes)
		}
	}
}

func workerMasterCheckSlaves(nodes *Pool) {
	log.Println("Worker Master is working...")
	nodes.Slaves.RetryUpAll()
}
