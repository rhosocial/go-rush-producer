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

var ErrNodeLevelAlreadyHighest = errors.New("I am already the highest level")

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

// DiscoverMasterNode 发现主节点。返回发现的节点信息指针。
//
// 调用前，Pool.Self 必须已经设置 models.NodeInfo 的 Level 值。上级即为 Pool.Self.Level - 1，且不指定具体上级 ID。
//
// 1. 如果 Level 已经为 0，则没有更高级，报 ErrNodeLevelAlreadyHighest。
//
// 2. 查阅数据库。如果查找不到记录，则报 models.ErrNodeSuperiorNotExist。如果数据库出错，则据实报错。
//
// 3. 如果存在主节点数据，则尝试检查主节点。参见 CheckMaster。
func (n *Pool) DiscoverMasterNode(specifySuperior bool) (*models.NodeInfo, error) {
	log.Println("Discover master...")
	if n.Self.Level == 0 {
		return nil, ErrNodeLevelAlreadyHighest
	}
	if node, err := n.Self.GetSuperiorNode(specifySuperior); err == nil {
		log.Print("Discovered master: ", node.Log())
		err = n.CheckMaster(node)
		return node, err
	} else {
		log.Println("Error(s) reported when discovering master record: ", err)
		return nil, err
	}
}

func (n *Pool) startMaster(master *models.NodeInfo, err error) error {
	if errors.Is(err, ErrNodeLevelAlreadyHighest) {
		// 什么也不做
	} else if errors.Is(err, models.ErrNodeSuperiorNotExist) {
		// 主节点不存在，将自己作为主节点。需要更新数据库。
		n.CommitSelfAsMasterNode()
	} else if errors.Is(err, models.ErrNodeDatabaseError) {
		// 数据库出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterInvalid) {
		// 主节点信息出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterIsSelf) {
		// 主节点是自己，将自己作为主节点。但不更新数据库。
	} else if errors.Is(err, ErrNodeRequestInvalid) {
		// 构造请求出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterExisted) {
		// 主节点已存在，直接退出。
		return err
	} else if err != nil {
		log.Println(err)
		return err
	}
	n.Self = master
	n.Master = n.Self
	n.SwitchIdentityMasterOn()
	return nil
}

// startSlave 将自己作为 master 的从节点。
func (n *Pool) startSlave(master *models.NodeInfo, err error) error {
	if errors.Is(err, ErrNodeLevelAlreadyHighest) {
		// 已经是最高级，不存在上级主节点。
		return err
	} else if errors.Is(err, models.ErrNodeSuperiorNotExist) {
		// 主节点不存在，直接退出。
		return err
	} else if errors.Is(err, models.ErrNodeDatabaseError) {
		// 数据库出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterInvalid) {
		// 主节点信息出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterIsSelf) {
		// 主节点是自己，将自己作为主节点。但不更新数据库。
		return err
	} else if errors.Is(err, ErrNodeRequestInvalid) {
		// 构造请求出错，直接退出。
		return err
	} else if errors.Is(err, ErrNodeMasterExisted) {
		// 主节点已存在，直接退出。
		return err
	} else if err != nil {
		log.Println(err)
		return err
	}
	// 未出错，则接受主节点，并通知其将自己加入。
	n.SwitchIdentitySlaveOn()
	n.AcceptMaster(master)
	_, err = n.NotifyMasterToAddSelfAsSlave()
	if err != nil {
		log.Fatalln(err)
	}
	return nil
}

func (n *Pool) stopMaster() error {
	n.SwitchIdentityMasterOff()
	return nil
}

func (n *Pool) stopSlave() error {
	n.SwitchIdentitySlaveOff()
	return nil
}

// Start 启动工作流程。
//
// 注意，调用此方法前，需自行保证：
// 1. 端口能够成功绑定，否则会产生不可预知的后果。
// 2. n.Self 已准备好。
// 3. 若指定为从节点模式，则 n.Master 也应当准备好。
func (n *Pool) Start(identity int) error {
	master, err := n.DiscoverMasterNode(false)
	if identity == IdentityMaster { // 指定为 Master。
		// 发现主节点。
		// 如果主节点已存在，则尝试连接。
		// 如果能正常连接，则报异常并退出。
		// 如果不能正常连接，则检查数据库存活。
		// 如果存活，则退出。如果并不存活。则尝试接替。
		log.Println(master, err)
		return n.startMaster(master, err)
	} else if identity == IdentitySlave {
		// 指定为 Slave，失败则退出。
		log.Println(master, err)
		// 未出错时启动从节点模式。
		return n.startSlave(master, err)
	} else if identity == IdentityAll {
		// 不指定具体身份：
		// 1. 先按从节点发现主节点。若主节点存在，则尝试加入。
		//   1.1. 若网络失败，则访问数据库检查是否有报告存活日志。若有，则维持当前身份。超过最大次数则退出；若没有，则尝试接替。
		//        接替流程：删除之前的异常记录，并将其移入 node_info_legacy；再转入条件2.
		//   1.2. 若网络成功，但返回错误或拒绝，报告主节点问题后退出。
		// 2. 若未发现主节点，则自己设为主。
		log.Println(master, err)

		if errors.Is(err, ErrNodeLevelAlreadyHighest) {
			// 已经是最高级，不存在上级主节点。认为自己是主节点。
			return n.startMaster(master, err)
		} else if errors.Is(err, models.ErrNodeSuperiorNotExist) {
			// 主节点不存在，设置自己为主节点。
			return n.startMaster(n.Self, models.ErrNodeSuperiorNotExist)
		} else if errors.Is(err, models.ErrNodeDatabaseError) {
			// 数据库出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeMasterInvalid) {
			// 主节点信息出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeMasterIsSelf) {
			// 主节点是自己，将自己作为主节点。但不更新数据库。
			return n.startMaster(master, nil)
		} else if errors.Is(err, ErrNodeRequestInvalid) {
			// 构造请求出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeMasterExisted) {
			// 主节点已存在，设置自己为从节点。
			return n.startSlave(master, nil)
		} else if err != nil {
			log.Println(err)
			return err
		}
		// 未出错时启动主节点模式。
		return n.startSlave(master, nil)
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
