package node

import (
	"context"
	"errors"
	"net"

	"github.com/rhosocial/go-rush-producer/component"
	"github.com/rhosocial/go-rush-producer/models"
	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
	"gorm.io/gorm"
)

type Pool struct {
	Self   PoolSelf
	Master PoolMaster
	Slaves PoolSlaves
}

var Nodes *Pool

var ErrNetworkUnavailable = errors.New("cannot find available network interface(s)")

func ExternalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, infa := range interfaces {
		if infa.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if infa.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := infa.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			ip := getIPFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, ErrNetworkUnavailable
}

// 获取ip
func getIPFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	if ip.IsLoopback() {
		logPrintln("loopback address found: ", ip)
		return nil
	}
	ipv4 := ip.To4()
	//ipv6 := ip.To16()
	if ipv4 == nil {
		return nil // not an ipv4 address
	}
	return ipv4
}

func (n *Pool) RefreshSelfSocket() error {
	host, err := ExternalIP()
	if err != nil && (*component.GlobalEnv).Localhost == false {
		return err
	}
	if host != nil {
		n.Self.Node.Host = host.String()
	} else {
		n.Self.Node.Host = "127.0.0.1"
	}
	return nil
}

func NewNodePool(self *NodeInfo.NodeInfo) *Pool {
	var nodes = Pool{
		// Identity: IdentityNotDetermined,
		// Master:   &NodeInfo.NodeInfo{},
		// Self: self,
		// Slaves:   make(map[uint64]NodeInfo.NodeInfo),
		Self: PoolSelf{
			Identity: IdentityNotDetermined,
			Node:     self,
		},
		Master: PoolMaster{},
		Slaves: PoolSlaves{NodesRetry: make(map[uint64]uint8)},
	}
	nodes.Slaves.DetectInactiveCallback = nodes.DetectSlaveNodeInactiveCallback
	err := nodes.RefreshSelfSocket()
	if err != nil {
		logFatalln(err)
		return nil
	}
	return &nodes
}

var ErrNodeSlaveFreshNodeInfoInvalid = errors.New("invalid slave fresh node info")

func (n *Pool) CommitSelfAsMasterNode() bool {
	n.Self.Upgrade()
	_, err := n.Self.Node.CommitSelfAsMasterNode()
	if err == nil {
		return true
	}
	logPrintln(err)
	return false
}

// AcceptSlave 接受从节点。
func (n *Pool) AcceptSlave(node *models.FreshNodeInfo) (*NodeInfo.NodeInfo, error) {
	logPrintln(node.Log())
	n.Slaves.NodesRWLock.Lock()
	defer n.Slaves.NodesRWLock.Unlock()
	// 检查 n.Slaves 是否存在该节点。
	// 如果存在，则直接返回。
	n.RefreshSlavesNodeInfo()
	if slave := n.Slaves.CheckIfExists(node); slave != nil {
		logPrintln("The specified slave node record already exists.")
		return slave, nil
	}
	// 如果不存在，则加入该节点为从节点。
	slave := NodeInfo.NodeInfo{
		Name:        node.Name,
		NodeVersion: node.NodeVersion,
		Host:        node.Host,
		Port:        node.Port,
		Turn:        n.Slaves.GetTurn(),
	}
	// 需要判断数据库中是否存在相同套接字的条目。
	existed, err := slave.GetNodeBySocket()
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		// 如有，则要尝试与其通信。若通信成功，则拒绝接入。
		err = n.CheckNodeStatus(existed)
		if errors.Is(err, ErrNodeExisted) {
			return nil, err
		}
	}

	// 需要判断数据库中是否存在该条目。
	_, err = n.Self.Node.AddSlaveNode(&slave)
	if err != nil {
		return nil, err
	}
	n.Slaves.Nodes[slave.ID] = slave
	if _, err := n.Self.Node.LogReportFreshSlaveJoined(&slave); err != nil {
		logPrintln(err)
	}
	return &slave, nil
}

// AcceptMaster 接受主节点。
func (n *Pool) AcceptMaster(master *NodeInfo.NodeInfo) {
	n.Master.Accept(master)
	if err := n.Self.Node.Refresh(); err != nil {
		logPrintln(err)
	}
	n.RefreshSlavesNodeInfo()
}

// RemoveSlave 删除指定节点。删除前要校验客户端提供的信息。若未报错，则视为删除成功。
//
// 1. 检查节点是否有效。检查流程参见 Slaves.Check。
//
// 2. 调用 Self 模型的删除从节点信息。删除成功后，将其从 Slaves 删除。
func (n *Pool) RemoveSlave(id uint64, fresh *models.FreshNodeInfo) (bool, error) {
	logPrintf("Remove Slave: %d\n", id)
	n.Slaves.NodesRWLock.Lock()
	defer n.Slaves.NodesRWLock.Unlock()
	slave, err := n.Slaves.Check(id, fresh)
	if err != nil {
		return false, err
	}
	if _, err := n.Self.Node.RemoveSlaveNode(slave); err != nil {
		return false, err
	}
	delete(n.Slaves.Nodes, id)
	if _, err := n.Self.Node.LogReportExistedSlaveWithdrawn(slave); err != nil {
		logPrintln(err)
	}
	return true, nil
}

// RefreshSlavesStatus 刷新从节点状态。
func (n *Pool) RefreshSlavesStatus() ([]uint64, []uint64) {
	remaining := make([]uint64, 0)
	removed := make([]uint64, 0)
	n.Slaves.NodesRWLock.Lock()
	defer n.Slaves.NodesRWLock.Unlock()
	for i, slave := range n.Slaves.Nodes {
		if _, err := n.GetSlaveStatus(i); err != nil {
			if _, err := n.Self.Node.RemoveSlaveNode(&slave); err != nil {
				logPrintln(err)
			}
			delete(n.Slaves.Nodes, i)
			removed = append(removed, i)
		} else {
			remaining = append(remaining, i)
		}
	}
	return remaining, removed
}

// RefreshSlavesNodeInfo 刷新从节点信息。
func (n *Pool) RefreshSlavesNodeInfo() {
	nodes, err := n.Self.Node.GetAllSlaveNodes()
	if err != nil {
		return
	}
	n.Slaves.Refresh(nodes)
}

// ---- Worker ---- //

// StartMasterWorker 启动主节点身份工作协程。
func (n *Pool) StartMasterWorker(ctx context.Context) {
	n.Master.WorkerCancelFuncRWLock.Lock()
	defer n.Master.WorkerCancelFuncRWLock.Unlock()
	if n.Master.IsWorking() {
		return
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	n.Master.WorkerCancelFunc = cancel
	go n.Master.worker(ctxChild, WorkerMasterIntervals{
		Base: 1000,
	}, n)
}

var ErrNodeSystemSignalStopped = errors.New("received a system signal to stop")

// StopMasterWorker 停止主节点身份工作协程。
func (n *Pool) StopMasterWorker(cause error) {
	n.Master.WorkerCancelFuncRWLock.Lock()
	defer n.Master.WorkerCancelFuncRWLock.Unlock()
	n.Master.WorkerCancelFunc(cause)
	n.Master.WorkerCancelFunc = nil
}

// StartSlaveWorker 启动从节点身份工作协程。
func (n *Pool) StartSlaveWorker(ctx context.Context) {
	n.Slaves.WorkerCancelFuncRWLock.Lock()
	defer n.Slaves.WorkerCancelFuncRWLock.Unlock()
	if n.Slaves.IsWorking() {
		// 已经启动了
		return
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	n.Slaves.WorkerCancelFunc = cancel
	//offset := math.Pow(2.0, float64(n.Self.Node.Turn)) * 100
	//if offset > 60000 {
	//	offset = 60000
	//}
	//log.Printf("worker interval:%f\n", offset)
	go n.Slaves.worker(ctxChild, WorkerSlaveIntervals{
		Base: 1000,
	}, n)
}

// StopSlaveWorker 停止从节点身份工作协程。
func (n *Pool) StopSlaveWorker(cause error) {
	n.Slaves.WorkerCancelFuncRWLock.Lock()
	defer n.Slaves.WorkerCancelFuncRWLock.Unlock()
	n.Slaves.WorkerCancelFunc(cause)
	n.Slaves.WorkerCancelFunc = nil
}

// ---- Worker ---- //

// ---- Callback ---- //

func (n *Pool) DetectSlaveNodeInactiveCallback(id uint64, retry uint8) {
	if _, err := n.Self.Node.LogReportExistedNodeMasterDetectedSlaveInactive(id, retry); err != nil {
		logPrintln(err)
	}
}

// ---- Callback ---- //
