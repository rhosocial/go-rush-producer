package node

import (
	"context"
	"log"
	"time"
)

type WorkerSlaveIntervals struct {
	Base uint16 `json:"base"`
}

// workerSlave 以"从节点"身份执行。
func (n *Pool) workerSlave(ctx context.Context, interval WorkerSlaveIntervals) {
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println(context.Cause(ctx))
			return
		default:
			err := n.CheckMaster(n.Master)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

type WorkerMasterIntervals struct {
	Base uint16 `json:"base"`
}

func (n *Pool) workerMaster(ctx context.Context, interval WorkerMasterIntervals) {
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println(context.Cause(ctx))
			return
		default:
			//process(ctx)
		}
	}
}
