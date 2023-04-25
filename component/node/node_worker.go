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
	log.Println("working...")
	nodes.Slaves.RetryUpAll()
}
