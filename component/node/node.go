package node

import (
	"errors"
	"log"
	"net"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

type Pool struct {
	// Identity uint8
	// Master                       *models.NodeInfo
	// Self *models.NodeInfo
	// SlavesRWMutex                sync.RWMutex
	// Slaves                       map[uint64]models.NodeInfo
	// WorkerMasterCancelFunc       context.CancelCauseFunc
	// WorkerMasterCancelFuncRWLock sync.RWMutex
	// WorkerSlaveCancelFunc        context.CancelCauseFunc
	// WorkerSlaveCancelFuncRWLock  sync.RWMutex
	Self   PoolSelf
	Master PoolMaster
	Slaves PoolSlaves
}

var Nodes *Pool

var ErrNetworkUnavailable = errors.New("network unavailable")

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
			ip := getIpFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip, nil
		}
	}
	return nil, ErrNetworkUnavailable
}

// 获取ip
func getIpFromAddr(addr net.Addr) net.IP {
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
	ip = ip.To4()
	if ip == nil {
		return nil // not an ipv4 address
	}
	return ip
}

func (n *Pool) RefreshSelfSocket() error {
	host, err := ExternalIP()
	if err != nil {
		return err
	}
	n.Self.Node.Host = host.String()
	return nil
}

func NewNodePool(self *models.NodeInfo) *Pool {
	var nodes = Pool{
		// Identity: IdentityNotDetermined,
		// Master:   &models.NodeInfo{},
		// Self: self,
		// Slaves:   make(map[uint64]models.NodeInfo),
		Self: PoolSelf{
			Identity: IdentityNotDetermined,
			Node:     self,
		},
		Master: PoolMaster{},
		Slaves: PoolSlaves{},
	}
	nodes.RefreshSelfSocket()
	return &nodes
}

var ErrNodeSlaveFreshNodeInfoInvalid = errors.New("invalid slave fresh node info")

func (n *Pool) CommitSelfAsMasterNode() (bool, error) {
	n.Self.Downgrade()
	if _, err := n.Self.Node.CommitSelfAsMasterNode(); err == nil {
		n.Master.Node = n.Self.Node
		return true, nil
	} else {
		return false, nil
	}
}

func (n *Pool) AcceptMaster(node *models.NodeInfo) {
	n.Master.Node = node
	n.Self.SetLevel(n.Master.Node.Level + 1)
}

func (n *Pool) CheckSlaveNodeIfExists(node *models.FreshNodeInfo) *models.NodeInfo {
	for id, _ := range n.Slaves.Nodes {
		if slave, err := n.Slaves.Check(id, node); err == nil {
			return slave
		}
	}
	return nil
}

func (n *Pool) AcceptSlave(node *models.FreshNodeInfo) (*models.NodeInfo, error) {
	log.Println(node.Log())
	n.Slaves.NodesRWMutex.Lock()
	defer n.Slaves.NodesRWMutex.Unlock()
	// 检查 n.Slaves 是否存在该节点。
	// 如果存在，则直接返回。
	n.RefreshSlavesNodeInfo()
	if slave := n.CheckSlaveNodeIfExists(node); slave != nil {
		log.Println("The specified slave node record already exists.")
		return slave, nil
	}
	// 如果不存在，则加入该节点为从节点。
	slave := models.NodeInfo{
		Name:        node.Name,
		NodeVersion: node.NodeVersion,
		Host:        node.Host,
		Port:        node.Port,
	}
	// 需要判断数据库中是否存在该条目。
	_, err := n.Self.Node.AddSlaveNode(&slave)
	if err != nil {
		return nil, err
	}
	n.Slaves.Nodes[slave.ID] = &slave
	n.Self.Node.LogReportFreshSlaveJoined(&slave)
	return &slave, nil
}

// RemoveSlave 删除指定节点。删除前要校验客户端提供的信息。若未报错，则视为删除成功。
//
// 1. 检查节点是否有效。检查流程参见 Slaves.Check。
//
// 2. 调用 Self 模型的删除从节点信息。删除成功后，将其从 Slaves 删除。
func (n *Pool) RemoveSlave(id uint64, fresh *models.FreshNodeInfo) (bool, error) {
	log.Printf("Remove Slave: %d\n", id)
	n.Slaves.NodesRWMutex.Lock()
	defer n.Slaves.NodesRWMutex.Unlock()
	slave, err := n.Slaves.Check(id, fresh)
	if err != nil {
		return false, err
	}
	if _, err := n.Self.Node.RemoveSlaveNode(slave); err != nil {
		return false, err
	}
	delete(n.Slaves.Nodes, id)
	n.Self.Node.LogReportExistedSlaveWithdrawn(slave)
	return true, nil
}

func (n *Pool) RefreshSlavesStatus() ([]uint64, []uint64) {
	remaining := make([]uint64, 0)
	removed := make([]uint64, 0)
	n.Slaves.NodesRWMutex.Lock()
	defer n.Slaves.NodesRWMutex.Unlock()
	for i, slave := range n.Slaves.Nodes {
		if _, err := n.GetSlaveStatus(i); err != nil {
			n.Self.Node.RemoveSlaveNode(slave)
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
