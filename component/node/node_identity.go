package node

import (
	"context"
	"errors"

	"github.com/rhosocial/go-rush-producer/component"
	base "github.com/rhosocial/go-rush-producer/models"
	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
	"gorm.io/gorm"
)

const (
	IdentityNotDetermined = 0
	IdentityMaster        = 1
	IdentitySlave         = 2
	IdentityAll           = IdentityMaster | IdentitySlave
)

var ErrNodeLevelAlreadyHighest = errors.New("it is already the highest level")

func (n *Pool) AttachIdentitySwitchedMasterOnCallbacks(fn func()) {
	n.Self.identitySwitchedMasterOnCallbacksRWLock.Lock()
	defer n.Self.identitySwitchedMasterOnCallbacksRWLock.Unlock()
	n.Self.identitySwitchedMasterOnCallbacks = append(n.Self.identitySwitchedMasterOnCallbacks, fn)
}

func (n *Pool) AttachIdentitySwitchedMasterOffCallbacks(fn func()) {
	n.Self.identitySwitchedMasterOffCallbacksRWLock.Lock()
	defer n.Self.identitySwitchedMasterOffCallbacksRWLock.Unlock()
	n.Self.identitySwitchedMasterOffCallbacks = append(n.Self.identitySwitchedMasterOffCallbacks, fn)
}

func (n *Pool) AttachIdentitySwitchedSlaveOnCallbacks(fn func()) {
	n.Self.identitySwitchedSlaveOnCallbacksRWLock.Lock()
	defer n.Self.identitySwitchedSlaveOnCallbacksRWLock.Unlock()
	n.Self.identitySwitchedSlaveOnCallbacks = append(n.Self.identitySwitchedSlaveOnCallbacks, fn)
}

func (n *Pool) AttachIdentitySwitchedSlaveOffCallbacks(fn func()) {
	n.Self.identitySwitchedSlaveOffCallbacksRWLock.Lock()
	defer n.Self.identitySwitchedSlaveOffCallbacksRWLock.Unlock()
	n.Self.identitySwitchedSlaveOffCallbacks = append(n.Self.identitySwitchedSlaveOffCallbacks, fn)
}

func (n *Pool) SwitchIdentityMasterOn() {
	logPrintln("Identity switched MASTER: ON")
	n.Self.Identity = n.Self.Identity | IdentityMaster
	fns := n.Self.identitySwitchedMasterOnCallbacks
	for _, fn := range fns {
		fn()
	}
}

func (n *Pool) SwitchIdentityMasterOff() {
	logPrintln("Identity switched MASTER: OFF")
	n.Self.Identity = n.Self.Identity &^ IdentityMaster
	fns := n.Self.identitySwitchedMasterOffCallbacks
	for _, fn := range fns {
		fn()
	}
}

func (n *Pool) SwitchIdentitySlaveOn() {
	logPrintln("Identity switched SLAVE: ON")
	n.Self.Identity = n.Self.Identity | IdentitySlave
	fns := n.Self.identitySwitchedSlaveOnCallbacks
	for _, fn := range fns {
		fn()
	}
}

func (n *Pool) SwitchIdentitySlaveOff() {
	logPrintln("Identity switched SLAVE: OFF")
	n.Self.Identity = n.Self.Identity &^ IdentitySlave
	fns := n.Self.identitySwitchedSlaveOffCallbacks
	for _, fn := range fns {
		fn()
	}
}

func (n *Pool) IsIdentityMaster() bool {
	return n.Self.Identity&IdentityMaster > 0
}

func (n *Pool) IsIdentitySlave() bool {
	return n.Self.Identity&IdentitySlave > 0
}

func (n *Pool) IsIdentityNotDetermined() bool {
	return n.Self.Identity == IdentityNotDetermined
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
func (n *Pool) DiscoverMasterNode(specifySuperior bool) (*NodeInfo.NodeInfo, error) {
	logPrintln("Discover master...")
	if n.Self.Node.Level == 0 {
		return nil, ErrNodeLevelAlreadyHighest
	}
	node, err := n.Self.Node.GetSuperiorNode(specifySuperior)
	if err == nil {
		logPrint("Discovered master: ", node.Log())
		_, err = n.CheckMaster(node)
		return node, err
	}
	logPrintln("Error(s) reported when discovering master record: ", err)
	return nil, err
}

// startMaster 以"主节点"身份启动。
//
// master 为指定的"主节点"。
// 若主节点为空，且原因也为空，则指定自己为主节点。
func (n *Pool) startMaster(ctx context.Context, master *NodeInfo.NodeInfo, cause error) error {
	isMasterFresh := false
	if errors.Is(cause, ErrNodeLevelAlreadyHighest) {
		// 什么也不做
	} else if errors.Is(cause, NodeInfo.ErrNodeSuperiorNotExist) || errors.Is(cause, ErrNodeRequestResponseError) {
		// 主节点不存在，将自己作为主节点。需要更新数据库。
		// 发现相同套接字的其它节点。
		if master == nil {
			master = n.Self.Node
		} else {
			node, err := master.GetNodeBySocket()
			// logPrintln(node, err)
			if err != gorm.ErrRecordNotFound {
				// 若发现其它相同套接字节点，则应尝试通信。如果能获取节点状态，则应退出。
				err := n.CheckNodeStatus(node)
				if err != nil {
					logPrintln(err)
					return ErrNodeMasterExisted
				}
			}
			// 若数据库信息不存在，则直接接替。
		}
		if !n.CommitSelfAsMasterNode() {
			return NodeInfo.ErrNodeDatabaseError
		}
		isMasterFresh = true
	} else if errors.Is(cause, NodeInfo.ErrNodeDatabaseError) {
		// 数据库出错，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeMasterInvalid) {
		// 主节点信息出错，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeMasterIsSelf) {
		// 主节点是自己，将自己作为主节点。但不更新数据库。
	} else if errors.Is(cause, ErrNodeRequestInvalid) {
		// 构造请求出错，直接退出。
		return cause
		// } else if errors.Is(cause, ErrNodeRequestResponseError) {
		// 删除失效信息。
	} else if errors.Is(cause, ErrNodeMasterExisted) {
		// 主节点已存在，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeExistedMasterWithdrawn) {
		// TODO: 刷新已存在节点，排除自己。
		nodes, err := master.GetAllSlaveNodes()
		if err != nil {
			return err
		}
		n.Slaves.Refresh(nodes)
	} else if cause != nil { // 此判断必须放在最后作为兜底。
		logPrintln(cause)
		return cause
	}
	if master == nil {
		if cause == nil {
			// 若主节点为空，且原因也为空，则指定自己为主节点。
			if !n.CommitSelfAsMasterNode() {
				return NodeInfo.ErrNodeDatabaseError
			}
			isMasterFresh = true
		}
	} else {
		// 若主节点由内容，则视为指定 master。
		n.Self.Node = master
	}
	// 主节点身份不变。
	// n.Master.Node = nil
	n.SwitchIdentityMasterOn()
	if isMasterFresh {
		if _, err := n.Self.Node.LogReportFreshMasterJoined(); err != nil {
			logPrintln(err)
		}
	}
	n.StartMasterWorker(ctx)
	return nil
}

// startSlave 将自己作为 master 的从节点。
func (n *Pool) startSlave(ctx context.Context, master *NodeInfo.NodeInfo, cause error) error {
	if errors.Is(cause, ErrNodeLevelAlreadyHighest) {
		// 已经是最高级，不存在上级主节点。
		return cause
	} else if errors.Is(cause, NodeInfo.ErrNodeSuperiorNotExist) {
		// 主节点不存在，直接退出。
		return cause
	} else if errors.Is(cause, NodeInfo.ErrNodeDatabaseError) {
		// 数据库出错，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeMasterInvalid) {
		// 主节点信息出错，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeMasterIsSelf) {
		// 主节点是自己，将自己作为主节点。但不更新数据库。
		return cause
	} else if errors.Is(cause, ErrNodeRequestInvalid) {
		// 构造请求出错，直接退出。
		return cause
	} else if errors.Is(cause, ErrNodeMasterExisted) {
		// 主节点已存在，直接退出。
		return cause
	} else if cause != nil {
		logPrintln(cause)
		return cause
	}
	// 未出错，则接受主节点，并通知其将自己加入。
	n.SwitchIdentitySlaveOn()
	n.AcceptMaster(master)
	_, cause = n.NotifyMasterToAddSelfAsSlave()
	if cause != nil {
		logFatalln(cause)
	}

	n.StartSlaveWorker(ctx)

	return nil
}

// stopMaster 停止主节点。
func (n *Pool) stopMaster(cause error) error {
	logPrintln("Worker Master stopping, due to", cause)
	n.StopMasterWorker(cause)
	n.SwitchIdentityMasterOff()
	// 通知所有从节点停机或选择一个从节点并通知其接替自己。
	// 通知从节点接替以及其它从节点切换主节点
	candidateID := n.Slaves.GetTurnCandidate()
	if errors.Is(cause, ErrNodeMasterRecordIsNotValid) {
		// 数据不一致直接停机，不通知交接和切换。
		// n.Master.Clear()
	} else if candidateID == 0 { // 没有候选接替节点，删除自己。
		_, err := n.Self.Node.RemoveSelf()
		if err != nil {
			logPrintln("Failed to stop self:", err)
		}
	} else {
		err := n.Handover(candidateID)
		if err != nil {
			logPrintln(err)
			return err
		}
		if _, err := n.NotifyAllSlavesToSwitchSuperior(candidateID); err != nil {
			logPrintln(err)
		}
		if _, err := n.NotifySlaveToTakeoverSelf(candidateID); err != nil {
			logPrintln(err)
		}
	}
	if _, err := n.Self.Node.LogReportExistedMasterWithdrawn(); err != nil {
		logPrintln(err)
	}
	return nil
}

// stopSlave 停止从节点。
func (n *Pool) stopSlave(cause error) error {
	logPrintln("Worker Slave stopping, due to", cause)
	n.StopSlaveWorker(cause)
	n.SwitchIdentitySlaveOff()
	// 通知主节点自己停机。
	if errors.Is(cause, ErrNodeTakeoverMaster) { // 什么也不做。
	} else { // 其它原因停机需要通知主节点删除自己。忽略错误。
		if _, err := n.NotifyMasterToRemoveSelf(); err != nil {
			logPrintln(err)
		}
	}
	return nil
}

// Start 启动工作流程。
//
// ctx 启动当前动作的上下文。建议调用该方法的 ctx 为调用上下文的子上下文。
//
// identity 身份。1-只接受主节点，2-只接受从节点，3-先尝试主，已经有主时作为其从。其它数字不起作用。
//
// 注意，调用此方法前，需自行保证：
// 1. 端口能够成功绑定，否则会产生不可预知的后果。
// 2. n.Self 已准备好。
func (n *Pool) Start(ctx context.Context, identity int) error {
	master, err := n.DiscoverMasterNode(false)
	if identity == IdentityMaster { // 指定为 Master。
		// 发现主节点。
		// 如果主节点已存在，则尝试连接。
		// 如果能正常连接，则报异常并退出。
		// 如果不能正常连接，则检查数据库存活。
		// 如果存活，则退出。如果并不存活。则尝试接替。
		if (*component.GlobalEnv).RunningMode == component.RunningModeDebug {
			logPrintln(master, err)
		}
		if err == nil {
			// 发现主节点，并工作正常。直接退出。
			return ErrNodeMasterExisted
		}
		return n.startMaster(ctx, master, err)
	} else if identity == IdentitySlave {
		// 指定为 Slave，失败则退出。
		if (*component.GlobalEnv).RunningMode == component.RunningModeDebug {
			logPrintln(master, err)
		}
		// 未出错时启动从节点模式。
		return n.startSlave(ctx, master, err)
	} else if identity == IdentityAll {
		// 不指定具体身份：
		// 1. 先按从节点发现主节点。若主节点存在，则尝试加入。
		//   1.1. 若网络失败，则访问数据库检查是否有报告存活日志。若有，则维持当前身份。超过最大次数则退出；若没有，则尝试接替。
		//        接替流程：删除之前的异常记录，并将其移入 node_info_legacy；再转入条件2.
		//   1.2. 若网络成功，但返回错误或拒绝，报告主节点问题后退出。
		// 2. 若未发现主节点，则自己设为主。
		if (*component.GlobalEnv).RunningMode == component.RunningModeDebug {
			logPrintln(master, err)
		}

		if errors.Is(err, ErrNodeLevelAlreadyHighest) {
			// 已经是最高级，不存在上级主节点。认为自己是主节点。
			return n.startMaster(ctx, master, err)
		} else if errors.Is(err, NodeInfo.ErrNodeSuperiorNotExist) {
			// 主节点不存在，设置自己为主节点。
			return n.startMaster(ctx, n.Self.Node, NodeInfo.ErrNodeSuperiorNotExist)
		} else if errors.Is(err, NodeInfo.ErrNodeDatabaseError) {
			// 数据库出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeMasterInvalid) {
			// 主节点信息出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeMasterIsSelf) {
			// 主节点是自己，将自己作为主节点。但不更新数据库。
			return n.startMaster(ctx, master, nil)
		} else if errors.Is(err, ErrNodeRequestInvalid) {
			// 构造请求出错，直接退出。
			return err
		} else if errors.Is(err, ErrNodeRequestResponseError) || errors.Is(err, ErrNodeMasterValidButRefused) {
			// 请求响应失败，将自己作为主。将异常节点删除。
			if _, err := master.RemoveSelf(); err != nil {
				logPrintln(err)
			}
			return n.startMaster(ctx, n.Self.Node, ErrNodeRequestResponseError)
		} else if errors.Is(err, ErrNodeMasterExisted) {
			// 主节点已存在，设置自己为从节点。
			return n.startSlave(ctx, master, nil)
		} else if err != nil {
			logPrintln(err)
			return err
		}
		// 未出错时启动主节点模式。
		return n.startSlave(ctx, master, nil)
	}
	return nil
}

// Stop 退出流程。
//
// ctx 启动时的上下文。
//
// cause 停止原因。
//
// 1. 若自己是 Master，则通知所有从节点停机或选择一个从节点并通知其接替自己。
// 2. 若自己是 Slave，则通知主节点自己停机。
// 3. 若身份未定，不做任何动作。
func (n *Pool) Stop(cause error) {
	if n.IsIdentityNotDetermined() {
		return
	}
	if n.IsIdentityMaster() {
		err := n.stopMaster(cause)
		if err != nil {
			logPrintln(err)
		}
	}
	if n.IsIdentitySlave() {
		err := n.stopSlave(cause)
		if err != nil {
			logPrintln(err)
		}
	}
}

// TrySupersede 尝试数据库更新。若更新成功，则表示自己已经成功抢占为主节点。若报任何异常，均表示没有抢占成功，需要重新查找主节点。
func (n *Pool) TrySupersede() error {
	err := n.Self.Node.SupersedeMasterNode(n.Master.Node)
	if err != nil {
		return err
	}
	return nil
}

// Supersede 从节点接替主节点。
func (n *Pool) Supersede(master *base.RegisteredNodeInfo) {
	if master == nil {
		return
	}
	// 此时已删除，无法返回节点，只能相信传入的 master。
	real, err := NodeInfo.GetNodeInfo(master.ID)
	if err != gorm.ErrRecordNotFound {
		// 如果还存在，则不能取代。
		logPrintln(real.Log())
		return
	}
	// 刷新自己，已经是 master 。
	if err := n.Self.Node.Refresh(); err != nil {
		logPrintln(err)
		return
	}
	// 刷新成功，停止从节点身份；清除主节点信息。
	err = n.stopSlave(ErrNodeTakeoverMaster)
	if err != nil {
		logPrintln(err)
		return
	}
	n.Master.Clear()
	// 启动主节点身份。
	err = n.startMaster(context.Background(), n.Self.Node, ErrNodeExistedMasterWithdrawn)
	if err != nil {
		logPrintln(err)
		return
	}
	// 此时从节点为空，需要刷新。
	n.RefreshSlavesNodeInfo()
}

// Handover 向 candidate 交接主节点身份。
func (n *Pool) Handover(candidate uint64) error {
	//if n.Master.Node == nil {
	//	return ErrNodeMasterInvalid
	//}
	// 交接时，候选节点必须存在。若不存在，将报错。
	node := n.Slaves.Get(candidate)
	if node == nil {
		return ErrNodeSlaveInvalid
	}
	//logPrintln("Handover: database preparing...")
	// 若交接主节点报错，则认为已有其它节点。
	err := n.Self.Node.HandoverMasterNode(node)
	if err != nil {
		logPrintln("Handover error(s) reported:", err)
		return err
	}
	//info, err := NodeInfo.GetNodeInfo(candidate)
	//if err != nil {
	//	return err
	//} else {
	//	logPrintln(info.Log())
	//}
	//logPrintln("Handover: database finished.")
	// 此时已认为交接成功。
	return nil
}

// SwitchSuperior 切换主节点。master 为新的主节点登记信息。
func (n *Pool) SwitchSuperior(master *base.RegisteredNodeInfo) error {
	// 更新 master 节点：
	node, err := NodeInfo.GetNodeInfo(master.ID)
	if err != nil {
		return ErrNodeMasterInvalid
	}
	n.AcceptMaster(node)
	// 检查 master 节点。
	if _, err := n.CheckMaster(n.Master.Node); err != nil {
		return err
	}
	return nil
}
