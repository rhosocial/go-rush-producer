package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/rhosocial/go-rush-producer/component"
	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

type WorkerSlaveIntervals struct {
	Base uint16 `json:"base"`
}

func (n *Pool) AttachWorkerSlaveWorkerCallbacks(fn func(ctx context.Context, nodes *Pool)) {
	n.Self.workerSlaveCallbacksRWLock.Lock()
	defer n.Self.workerSlaveCallbacksRWLock.Unlock()
	n.Self.workerSlaveCallbacks = append(n.Self.workerSlaveCallbacks, fn)
}

// worker 以"从节点"身份执行。
func (ps *PoolSlaves) worker(ctx context.Context, interval WorkerSlaveIntervals, nodes *Pool) {
	if (*component.GlobalEnv).RunningMode == component.RunningModeDebug {
		logPrintln("Worker Slave is working...")
	}
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
			logPrintln("Worker Slave stopped, due to", context.Cause(ctx))
			return
		default:
			if !workerSlaveCheckMaster(ctx, nodes) {
				continue
			}
			fns := nodes.Self.workerSlaveCallbacks
			for _, fn := range fns {
				fn(ctx, nodes)
			}
		}
	}
}

func workerSlaveCheckMaster(ctx context.Context, nodes *Pool) bool {
	resp, err := nodes.CheckMaster(nodes.Master.Node)
	if err != nil {
		nodes.Master.RetryUp()
		logPrintln(err, nodes.Master.Retry)
	}
	//if err == nil {
	//	nodes.Master.RetryClear()
	//} else {
	//	nodes.Master.RetryUp()
	//	logPrintln(err, nodes.Master.Retry)
	//}
	// 检查自己是否存在。
	if resp != nil {
		var respContent RequestMasterStatusResponse
		var body = make([]byte, resp.ContentLength)
		if _, err := resp.Body.Read(body); err != nil && err != io.EOF {
			logPrintln(ErrNodeRequestResponseError)
		}
		err = json.Unmarshal(body, &respContent)
		if err != nil {
			logPrintln("Worker Slave:", err)
		}
		if !respContent.Data.Attended {
			// 如果发现自己不存在，则尝试重新加入。
			nodes.Stop(ErrNodeSlaveInvalid)
			self := NodeInfo.NewNodeInfo("GO-RUSH-PRODUCER", "0.0.1", *(*(*component.GlobalEnv).Net).ListenPort, 1)
			Nodes = NewNodePool(self)
			err := nodes.Start(context.Background(), IdentitySlave)
			if err != nil {
				logPrintln(err)
			}
		}
		if respContent.Data.IsMasterWorking {
			// 主节点正在工作，更新重试计数。
			nodes.Master.RetryClear()
		} else {
			logPrintln(ErrNodeMasterWorkerStopped.Error())
			nodes.Master.RetryUp()
		}
	}
	// TODO: <参数点> 从节点检查主节点最大重试次数。
	if nodes.Master.Retry >= 3 {
		go func(master *NodeInfo.NodeInfo) {
			_, err := nodes.Self.Node.LogReportExistedNodeSlaveReportMasterInactive(master)
			if err != nil {
				logPrintln(err)
			}
		}(nodes.Master.Node)
		// TODO: 重试次数过多，尝试主动接替。
		logPrintln("retried out, try to supersede:")
		err := nodes.TrySupersede()
		if err != nil {
			// 表示已经有其它主节点，刷新主节点。
			logPrintln(err)
			fresh, err := nodes.DiscoverMasterNode(false)
			if err != nil {
				return true
			}
			nodes.AcceptMaster(fresh)
			return true
		}
		nodes.Supersede(nodes.Master.Node.ToRegisteredNodeInfo())
		return false
	}
	return true
}

type WorkerMasterIntervals struct {
	Base uint16 `json:"base"`
}

func (n *Pool) AttachWorkerMasterWorkerCallbacks(fn func(ctx context.Context, nodes *Pool)) {
	n.Self.workerMasterCallbacksRWLock.Lock()
	defer n.Self.workerMasterCallbacksRWLock.Unlock()
	n.Self.workerMasterCallbacks = append(n.Self.workerMasterCallbacks, fn)
}

// worker 以"主节点"身份执行。
func (pm *PoolMaster) worker(ctx context.Context, interval WorkerMasterIntervals, nodes *Pool) {
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			logPrintln("Worker Master stopped, due to", context.Cause(ctx))
			return
		default:
			workerMaster(ctx, nodes)
			fns := nodes.Self.workerMasterCallbacks
			for _, fn := range fns {
				fn(ctx, nodes)
			}
		}
	}
}

var intervalCheckSelf = 0
var intervalCheckSelfRWMutex sync.RWMutex
var ErrNodeMasterRecordIsNotValid = errors.New("the record of master is not valid")

// workerMaster 主节点任务。
//
// 1. 调增所有子节点重试次数。
//
// 2. 报告自己活跃。
//
// 3. 每十秒检查一次数据表自己的信息是否与自己相等。
func workerMaster(ctx context.Context, nodes *Pool) {
	if (*component.GlobalEnv).RunningMode == component.RunningModeDebug {
		logPrintln("Worker Master is working...")
	}
	go nodes.Slaves.RetryUpAllAndRemoveIfRetriedOut(3, 4) // 1. 调增所有子节点重试次数。超过重试次数上限则直接删除，并不通知对方。TODO: <参数点> 超限次数，最小不应低于3。
	go func() {
		if nodes.Self.AliveUpAndClearIf(10) == 9 { // 2. 报告自己活跃。 TODO: <参数点> 报告活跃间隔。
			if _, err := nodes.Self.Node.LogReportActive(); err != nil {
				logPrintln(err)
			}
		}
		// 每十秒检查一次
		// 1. 数据表自己的信息是否与自己相等；
		// 2. 是否有失效节点记录。
		intervalCheckSelfRWMutex.Lock()
		defer intervalCheckSelfRWMutex.Unlock()
		intervalCheckSelf++
		if intervalCheckSelf%10 == 0 {
			intervalCheckSelf = 0
			if !nodes.Self.CheckSelf() {
				err := nodes.stopMaster(ErrNodeMasterRecordIsNotValid)
				if err != nil {
					logPrintln(err)
				}
			}
		}
	}()
}
