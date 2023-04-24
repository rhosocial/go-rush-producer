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
func (ps *PoolSlaves) worker(ctx context.Context, interval WorkerSlaveIntervals, self *PoolSelf, master *PoolMaster) {
	if self == nil {
		return
	}
	if master == nil {
		return
	}
	for {
		time.Sleep(time.Duration(interval.Base) * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println(context.Cause(ctx))
			return
		default:
			err := self.CheckMaster(master.Node)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

type WorkerMasterIntervals struct {
	Base uint16 `json:"base"`
}

func (pm *PoolMaster) worker(ctx context.Context, interval WorkerMasterIntervals) {
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
