package node

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/rhosocial/go-rush-common/component/response"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

type Pool struct {
	Identity      uint8
	Master        *models.NodeInfo
	Self          *models.NodeInfo
	SlavesRWMutex sync.RWMutex
	Slaves        map[uint64]models.NodeInfo
}

var Nodes *Pool

func (n *Pool) GetRegisteredSlaveNodeInfos() *map[uint64]*models.RegisteredNodeInfo {
	n.SlavesRWMutex.RLock()
	defer n.SlavesRWMutex.RUnlock()

	slaves := make(map[uint64]*models.RegisteredNodeInfo)
	for i, v := range n.Slaves {
		slaves[i] = models.InitRegisteredWithModel(&v)
	}
	return &slaves
}

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
	n.Self.Host = host.String()
	return nil
}

func NewNodePool(self *models.NodeInfo) *Pool {
	var nodes = Pool{
		Identity: IdentityNotDetermined,
		Master:   &models.NodeInfo{},
		Self:     self,
		Slaves:   make(map[uint64]models.NodeInfo),
	}
	nodes.RefreshSelfSocket()
	return &nodes
}

var ErrNodeSlaveFreshNodeInfoInvalid = errors.New("invalid slave fresh node info")

// CheckSlave 检查从节点是否有效。检查通过则返回节点信息 models.NodeInfo。
//
// 1. 若节点不存在，则报 ErrNodeMasterDoesNotHaveSpecifiedSlave。
//
// 2. 检查 FreshNodeInfo 是否与本节点维护一致。若不一致，则报 ErrNodeSlaveFreshNodeInfoInvalid。
func (n *Pool) CheckSlave(id uint64, fresh *models.FreshNodeInfo) (*models.NodeInfo, error) {
	// 检查指定ID是否存在，如果不是，则报错。
	slave, exist := n.Slaves[id]
	if !exist {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	// 再检查 FreshNodeInfo 是否相同。
	origin := models.FreshNodeInfo{
		Name:        slave.Name,
		NodeVersion: slave.NodeVersion,
		Host:        slave.Host,
		Port:        slave.Port,
	}
	if origin.IsEqual(fresh) {
		return &slave, nil
	}
	return nil, ErrNodeSlaveFreshNodeInfoInvalid
}

func (n *Pool) CommitSelfAsMasterNode() (bool, error) {
	n.Self.Level = n.Self.Level - 1
	if _, err := n.Self.CommitSelfAsMasterNode(); err == nil {
		n.Master = n.Self
		n.Identity = n.Identity | IdentityMaster
		return true, nil
	}
	return false, nil
}

var ErrNodeMasterInvalid = errors.New("master node invalid")
var ErrNodeMasterValidButRefused = errors.New("master is valid but refuse to communicate")

var ErrNodeMasterIsSelf = errors.New("master node is self")
var ErrNodeMasterExisted = errors.New("a valid master node with the same socket already exists")

// CheckMaster 检查主节点有效性。如果有效，则返回 nil。
// 如果指定主节点不存在，则报 ErrNodeMasterInvalid。
//
// 尝试连接主节点。如果返回 ErrNodeRequestInvalid，则视为请求异常。
//
// 判断 master 的套接字是否与自己相同。
//
// 1. 如果相同，则认为是自己。
// 如果连接未报错，则表明已经存在对应节点，报 ErrNodeMasterExisted；
// 如果连接报错，则将 master 作为异常失效信息，报 ErrNodeMasterIsSelf。
//
// 2. 如果不同，则认为主节点是另一个进程。
// 如果连接报错，则报 ErrNodeMasterInvalid。
// 如果连接返回状态码不是 http.StatusOK，则同样视为报错。
func (n *Pool) CheckMaster(master *models.NodeInfo) error {
	if master == nil {
		log.Println("Master not specified")
		return ErrNodeMasterInvalid
	}
	log.Printf("Checking Master [ID: %d - %s]...\n", master.ID, master.Socket())
	resp, err := n.SendRequestMasterStatus()
	if err != nil {
		log.Println(err)
		return ErrNodeRequestInvalid
	}
	if n.Self.IsSocketEqual(master) {
		if err == nil {
			return ErrNodeMasterExisted
		}
		return ErrNodeMasterIsSelf
	}
	if resp.StatusCode != http.StatusOK {
		var body = make([]byte, resp.ContentLength)
		resp.Body.Read(body)
		log.Println(string(body))
		return ErrNodeMasterValidButRefused
	}
	return nil
}

func (n *Pool) AcceptMaster(node *models.NodeInfo) {
	n.Master = node
	n.Self.Level = n.Master.Level + 1
	n.SwitchIdentitySlaveOn()
}

func (n *Pool) CheckSlaveNodeIfExists(node *models.FreshNodeInfo) *models.NodeInfo {
	for id, _ := range n.Slaves {
		if slave, err := n.CheckSlave(id, node); err == nil {
			return slave
		}
	}
	return nil
}

func (n *Pool) AcceptSlave(node *models.FreshNodeInfo) (*models.NodeInfo, error) {
	log.Println(node.Log())
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
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
	_, err := n.Self.AddSlaveNode(&slave)
	if err != nil {
		return nil, err
	}
	n.Slaves[slave.ID] = slave
	return &slave, nil
}

// RemoveSlave 删除指定节点。删除前要校验客户端提供的信息。若未报错，则视为删除成功。
//
// 1. 检查节点是否有效。检查流程参见 CheckSlave。
//
// 2. 调用 Self 模型的删除从节点信息。删除成功后，将其从 Slaves 删除。
func (n *Pool) RemoveSlave(id uint64, fresh *models.FreshNodeInfo) (bool, error) {
	log.Printf("Remove Slave: %d\n", id)
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
	slave, err := n.CheckSlave(id, fresh)
	if err != nil {
		return false, err
	}
	if _, err := n.Self.RemoveSlaveNode(slave); err != nil {
		return false, err
	}
	delete(n.Slaves, id)
	return true, nil
}

func (n *Pool) RefreshSlavesStatus() ([]uint64, []uint64) {
	remaining := make([]uint64, 0)
	removed := make([]uint64, 0)
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
	for i, slave := range n.Slaves {
		if _, err := n.GetSlaveStatus(i); err != nil {
			n.Self.RemoveSlaveNode(&slave)
			delete(n.Slaves, i)
			removed = append(removed, i)
		} else {
			remaining = append(remaining, i)
		}
	}
	return remaining, removed
}

// GetSlaveStatus 当前节点（主节点）获取其从节点状态。
func (n *Pool) GetSlaveStatus(id uint64) (bool, error) {
	resp, err := n.SendRequestSlaveStatus(id)
	if err != nil {
		return false, err
	}
	var body = make([]byte, resp.ContentLength)
	_, err = resp.Body.Read(body)
	if err != io.EOF && err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(string(body))
	}
	return true, nil
}

type NotifyMasterToAddSelfAsSlaveResponseData struct {
	ID          uint64 `json:"id"`
	Name        string `json:"name"`
	NodeVersion string `json:"node_version"`
	Host        string `json:"host"`
	Port        uint16 `json:"port"`
}

type NotifyMasterToAddSelfAsSlaveResponse = response.Generic[NotifyMasterToAddSelfAsSlaveResponseData, any]

// NotifyMasterToAddSelfAsSlave 当前节点（从节点）通知主节点添加自己为其从节点。
func (n *Pool) NotifyMasterToAddSelfAsSlave() (bool, error) {
	resp, err := n.SendRequestMasterToAddSelfAsSlave()
	if err != nil {
		return false, err
	}
	var body = make([]byte, resp.ContentLength)
	_, err = resp.Body.Read(body)
	if err != io.EOF && err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(string(body))
	}
	respData := NotifyMasterToAddSelfAsSlaveResponse{}
	err = json.Unmarshal(body, &respData)
	if err != nil {
		return false, err
	}
	// 校验成功，将返回的ID作为自己的ID。
	self, err := models.GetNodeInfo(respData.Data.ID)
	n.Self = self
	return true, nil
}

// NotifyMasterToRemoveSelf 当前节点（从节点）通知主节点删除自己。
func (n *Pool) NotifyMasterToRemoveSelf() (bool, error) {
	resp, err := n.SendRequestMasterToRemoveSelf()
	if err != nil {
		return false, ErrNodeRequestInvalid
	}
	var body = make([]byte, resp.ContentLength)
	_, err = resp.Body.Read(body)
	if err != io.EOF && err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(string(body))
	}
	return true, nil
}

// NotifySlaveToTakeoverSelf 当前节点（主节点）通知从节点接替自己。
func (n *Pool) NotifySlaveToTakeoverSelf() (bool, error) {
	return true, nil
}

func (n *Pool) NotifyAllSlavesToSwitchSuperior(succeedID uint64) (bool, error) {
	return true, nil
}

func (n *Pool) NotifySlaveToSwitchSuperior() (bool, error) {
	return true, nil
}

func (n *Pool) CheckSlavesStatus() {
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
}

// RefreshSlavesNodeInfo 刷新从节点信息。
func (n *Pool) RefreshSlavesNodeInfo() {
	nodes, err := n.Self.GetAllSlaveNodes()
	if err != nil {
		return
	}
	result := make(map[uint64]models.NodeInfo)
	for _, node := range *nodes {
		if true { // 访问节点是否有效。
			result[node.ID] = node
		}
	}
	n.Slaves = result
}
