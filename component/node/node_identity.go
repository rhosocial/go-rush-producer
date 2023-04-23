package node

import (
	"errors"
	"log"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

const (
	IdentityNotDetermined = 0
	IdentityMaster        = 1
	IdentitySlave         = 2
	IdentityAll           = IdentityMaster | IdentitySlave
)

func (n *Pool) SwitchIdentityMasterOn() {
	n.Identity = n.Identity | IdentityMaster
}

func (n *Pool) SwitchIdentityMasterOff() {
	n.Identity = n.Identity &^ IdentityMaster
}

func (n *Pool) SwitchIdentitySlaveOn() {
	n.Identity = n.Identity | IdentitySlave
}

func (n *Pool) SwitchIdentitySlaveOff() {
	n.Identity = n.Identity &^ IdentitySlave
}

func (n *Pool) IsIdentityMaster() bool {
	return n.Identity&IdentityMaster > 0
}

func (n *Pool) IsIdentitySlave() bool {
	return n.Identity&IdentitySlave > 0
}

func (n *Pool) IsIdentityNotDetermined() bool {
	return n.Identity == IdentityNotDetermined
}

func (n *Pool) startMaster() error {
	return nil
}

func (n *Pool) startSlave() error {
	return nil
}

// Start 启动工作流程。
//
// 注意，调用此方法前，需自行保证：
// 1. 端口能够成功绑定，否则会产生不可预知的后果。
// 2. n.Self 已准备好。
// 3. 若指定为从节点模式，则 n.Master 也应当准备好。
func (n *Pool) Start(identity int) error {
	if identity == IdentityMaster {
		// 指定为 Master。
		// 发现主节点。
		master, err := n.DiscoverMasterNode(false)
		// 如果主节点已存在，则尝试连接。
		// 如果能正常连接，则报异常并退出。
		// 如果不能正常连接，则检查数据库存活。
		// 如果存活，则退出。如果并不存活。则尝试接替。
		log.Println(master, err)
	} else if identity == IdentitySlave {
		// 指定为 Slave，失败则退出。
		master, err := n.DiscoverMasterNode(true)
		if errors.Is(err, models.ErrNodeLevelAlreadyHighest) {
			return err
		}
		log.Println(master)
	} else if identity == IdentityAll {
		// 不指定具体身份：
		// 1. 发现主，若主存在，则尝试加入。
		//   1.1. 若网络失败，则访问数据库检查是否有报告存活日志。若有，则维持当前身份。超过最大次数则退出；若没有，则尝试接替。
		//        接替流程：删除之前的异常记录，并将其移入 node_info_legacy；再转入条件2.
		//   1.2. 若网络成功，但返回错误或拒绝，报告主节点问题后退出。
		// 2. 未发现主，则自己设为主。

		master, err := n.DiscoverMasterNode(false)
		log.Println(master, err)
		if errors.Is(err, models.ErrNodeSuperiorNotExist) {
			// Switch identity to master.
			n.CommitSelfAsMasterNode()
		} else if errors.Is(err, ErrNodeMasterIsSelf) {
			// I am already a master node.
			// Switch identity to master.
			n.Self = master
			n.Master = n.Self
			n.SwitchIdentityMasterOn()
		} else if errors.Is(err, ErrNodeMasterExisted) {
			// A valid master node with the same socket already exists. Exiting.
			log.Fatalln(err)
		} else if errors.Is(err, ErrNodeMasterInvalid) {
			log.Fatalln(err)
		} else if errors.Is(err, ErrNodeRequestInvalid) {
			log.Fatalln(err)
		} else if err == nil {
			// Regard `masterNode` as my master:
			n.AcceptMaster(master)
			// Notify master to add this node as its slave:
			_, err := n.NotifyMasterToAddSelfAsSlave()
			if err != nil {
				log.Fatalln(err)
			}
		} else if err != nil {
			log.Fatalln(err)
		}
	}
	return nil
}

// Stop 退出流程。
//
// 1. 若自己是 Master，则通知所有从节点停机或选择一个从节点并通知其接替自己。
// 2. 若自己是 Slave，则通知主节点自己停机。
// 3. 若身份未定，不做任何动作。
func (n *Pool) Stop() {
	if n.IsIdentityNotDetermined() {
		return
	}
	if n.IsIdentityMaster() {
		// 通知所有从节点停机或选择一个从节点并通知其接替自己。
		// TODO: 通知从节点接替以及其它从节点切换主节点
		n.NotifySlaveToTakeoverSelf()
		n.NotifyAllSlavesToSwitchSuperior(uint64(0))
	}
	if n.IsIdentitySlave() {
		// 通知主节点自己停机。
		n.NotifyMasterToRemoveSelf()
	}
}
