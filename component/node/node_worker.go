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

// workerMaster 主节点任务。
//
// 1. 调增所有子节点重试次数。
//
// 2. 报告自己活跃。
//
// TODO: 3. 检查自己是否处于异常状况，即自己上次报告活跃是否远超阈值，同时刷新自己的数据表信息。
func workerMaster(nodes *Pool) {
	log.Println("Worker Master is working...")
	go nodes.Slaves.RetryUpAllAndRemoveIfRetriedOut(2, 3) // 1. 调增所有子节点重试次数。超过重试次数上限则直接删除，并不通知对方。TODO: <参数点> 超限次数，最小不应低于3。
	if nodes.Self.AliveUpAndClearIf(10) == 9 {            // 2. 报告自己活跃。 TODO: <参数点> 报告活跃间隔。
		nodes.Self.Node.LogReportActive()
	}
	// TODO: 3. 检查自己是否处于异常状况，即自己上次报告活跃是否远超阈值，同时刷新自己的数据表信息。
}
