package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

type WorkerSlaveIntervals struct {
	Base uint16 `json:"base"`
}

// worker 以"从节点"身份执行。
func (ps *PoolSlaves) worker(ctx context.Context, interval WorkerSlaveIntervals, nodes *Pool, process func(ctx context.Context, nodes *Pool)) {
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
			log.Println("Worker Slave stopped, due to", context.Cause(ctx))
			return
		default:
			process(ctx, nodes)
		}
	}
}

func workerSlaveCheckMaster(ctx context.Context, nodes *Pool) {
	err, resp := nodes.CheckMaster(nodes.Master.Node)
	if err == nil {
		nodes.Master.RetryClear()
	} else {
		nodes.Master.RetryUp()
		log.Println(err, nodes.Master.Retry)
	}
	// 检查自己是否存在。
	if resp != nil {
		var respContent RequestMasterStatusResponse
		var body = make([]byte, resp.ContentLength)
		if _, err := resp.Body.Read(body); err != nil && err != io.EOF {
			log.Println(ErrNodeRequestResponseError)
		}
		err = json.Unmarshal(body, &respContent)
		if err != nil {
			log.Println("Worker Slave:", err)
		}
		if !respContent.Data.Attended {
			// 如果发现自己不存在，则直接停机。
			nodes.Stop(ctx, ErrNodeSlaveInvalid)
		}
	}
	// TODO: <参数点> 从节点检查主节点最大重试次数。
	if nodes.Master.Retry >= 3 {
		go func(master *NodeInfo.NodeInfo) {
			_, err := nodes.Self.Node.LogReportExistedNodeSlaveReportMasterInactive(master)
			if err != nil {
				log.Println(err)
			}
		}(nodes.Master.Node)
		// TODO: 重试次数过多，尝试主动接替。
		log.Println("retried out, try to supersede:")
		err := nodes.TrySupersede()
		if err != nil {
			// 表示已经有其它主节点，刷新主节点。
			log.Println(err)
			fresh, err := nodes.DiscoverMasterNode(false)
			if err != nil {
				return
			}
			nodes.AcceptMaster(fresh)
			return
		}
		nodes.Supersede(nodes.Master.Node.ToRegisteredNodeInfo())
	}
}

type WorkerMasterIntervals struct {
	Base uint16 `json:"base"`
}

// worker 以"主节点"身份执行。
func (pm *PoolMaster) worker(ctx context.Context, interval WorkerMasterIntervals, nodes *Pool, process func(ctx context.Context, nodes *Pool)) {
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println("Worker Master stopped, due to", context.Cause(ctx))
			return
		default:
			process(ctx, nodes)
		}
	}
}

var intervalCheckSelf = 0
var ErrNodeMasterRecordIsNotValid = errors.New("the record of master is not valid")

// workerMaster 主节点任务。
//
// 1. 调增所有子节点重试次数。
//
// 2. 报告自己活跃。
//
// 3. 每十秒检查一次数据表自己的信息是否与自己相等。
func workerMaster(ctx context.Context, nodes *Pool) {
	log.Println("Worker Master is working...")
	go nodes.Slaves.RetryUpAllAndRemoveIfRetriedOut(2, 3) // 1. 调增所有子节点重试次数。超过重试次数上限则直接删除，并不通知对方。TODO: <参数点> 超限次数，最小不应低于3。
	if nodes.Self.AliveUpAndClearIf(10) == 9 {            // 2. 报告自己活跃。 TODO: <参数点> 报告活跃间隔。
		if _, err := nodes.Self.Node.LogReportActive(); err != nil {
			log.Println(err)
		}
	}
	// 每十秒检查一次数据表自己的信息是否与自己相等。
	intervalCheckSelf++
	if intervalCheckSelf%10 == 0 {
		intervalCheckSelf = 0
		if !nodes.Self.CheckSelf() {
			nodes.stopMaster(ctx, ErrNodeMasterRecordIsNotValid)
		}
	}
}
