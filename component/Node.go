package component

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

const (
	NodeIdentityNotDetermined = 0
	NodeIdentityMaster        = 1
	NodeIdentitySlave         = 2
	NodeIdentityAll           = NodeIdentityMaster | NodeIdentitySlave

	NodeRequestStatus             = 0x00000001
	NodeRequestMasterStatus       = 0x00010001
	NodeRequestMasterNotifyAdd    = 0x00010011
	NodeRequestMasterNotifyModify = 0x00010012
	NodeRequestMasterNotifyDelete = 0x00010013
	NodeRequestSlaveStatus        = 0x00020001
	NodeRequestSlaveNotify        = 0x00020011

	NodeRequestMethodStatus             = http.MethodGet
	NodeRequestMethodMasterStatus       = http.MethodGet
	NodeRequestMethodMasterNotifyAdd    = http.MethodPut
	NodeRequestMethodMasterNotifyDelete = http.MethodDelete
	NodeRequestMethodSlaveStatus        = http.MethodGet

	NodeRequestURLFormatStatus             = "http://%s/server"
	NodeRequestURLFormatMasterStatus       = "http://%s/server/master"
	NodeRequestURLFormatMasterNotifyAdd    = "http://%s/server/master/notify"
	NodeRequestURLFormatMasterNotifyModify = "http://%s/server/master/notify"
	NodeRequestURLFormatMasterNotifyDelete = "http://%s/server/master/notify"
	NodeRequestURLFormatSlaveStatus        = "http://%s/server/slave"
	NodeRequestURLFormatSlaveNotify        = "http://%s/server/slave/notify"

	NodeRequestHeaderXAuthorizationTokenKey   = "X-Authorization-Token"
	NodeRequestHeaderXAuthorizationTokenValue = "$2a$04$jajGD06BJd.KmTM7pgCRzeFSIMWLAUbTCOQPNJRDMnMltPZp3tK1y"
)

type NodePool struct {
	Identity      uint8
	Master        *models.NodeInfo
	Self          *models.NodeInfo
	SlavesRWMutex sync.RWMutex
	Slaves        map[uint64]models.NodeInfo
}

var Nodes *NodePool

func (n *NodePool) IsIdentityMaster() bool {
	return n.Identity&NodeIdentityMaster > 0
}

func (n *NodePool) IsIdentitySlave() bool {
	return n.Identity&NodeIdentitySlave > 0
}

func (n *NodePool) IsIdentityNotDetermined() bool {
	return n.Identity == NodeIdentityNotDetermined
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

func (n *NodePool) RefreshSelfSocket() error {
	host, err := ExternalIP()
	if err != nil {
		return err
	}
	n.Self.Host = host.String()
	return nil
}

func NewNodePool(self *models.NodeInfo) *NodePool {
	var nodes = NodePool{
		Identity: NodeIdentityNotDetermined,
		Master:   &models.NodeInfo{},
		Self:     self,
		Slaves:   make(map[uint64]models.NodeInfo),
	}
	nodes.RefreshSelfSocket()
	return &nodes
}

// FreshNodeInfo 新节点信息。
type FreshNodeInfo struct {
	Name        string `form:"name" json:"name" binding:"required"`
	NodeVersion string `form:"node_version" json:"node_version" binding:"required"`
	Host        string `form:"host" json:"string" binding:"required"`
	Port        uint16 `form:"port" json:"port" binding:"required"`
}

func (n *FreshNodeInfo) Encode() string {
	params := make(url.Values)
	params.Add("name", n.Name)
	params.Add("node_version", n.NodeVersion)
	params.Add("host", n.Host)
	params.Add("port", strconv.Itoa(int(n.Port)))
	return params.Encode()
}

// CheckSlave 检查从节点是否有效。
func (n *NodePool) CheckSlave(id uint64, fresh *FreshNodeInfo) (*models.NodeInfo, error) {
	// 检查指定ID是否存在，如果不是，则报错。
	slave, exist := n.Slaves[id]
	if !exist {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	// 再检查 FreshNodeInfo 是否相同。
	if slave.Name == fresh.Name && slave.NodeVersion == fresh.NodeVersion && slave.Host == fresh.Host && slave.Port == fresh.Port {
		return &slave, nil
	}
	return nil, ErrNodeSlaveSocketInvalid
}

// DiscoverMasterNode 发现主节点。返回发现的节点信息指针。
// 调用前，NodePool.Self 必须已经设置 models.NodeInfo 的 Level 值。上级即为 NodePool.Self.Level - 1，且不指定具体上级 ID。
// 如果 Level 已经为 0，则没有更高级，报 models.ErrNodeLevelAlreadyHighest。
// 如果查找不到最高级，则报 models.ErrNodeSuperiorNotExist。
func (n *NodePool) DiscoverMasterNode() (*models.NodeInfo, error) {
	node, err := n.Self.GetSuperiorNode(false)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (n *NodePool) SwitchIdentityMasterOn() {
	n.Identity = n.Identity | NodeIdentityMaster
}

func (n *NodePool) SwitchIdentityMasterOff() {
	n.Identity = n.Identity &^ NodeIdentityMaster
}

func (n *NodePool) SwitchIdentitySlaveOn() {
	n.Identity = n.Identity | NodeIdentitySlave
}

func (n *NodePool) SwitchIdentitySlaveOff() {
	n.Identity = n.Identity &^ NodeIdentitySlave
}

func (n *NodePool) CommitSelfAsMasterNode() (bool, error) {
	n.Self.Level = n.Self.Level - 1
	if _, err := n.Self.CommitSelfAsMasterNode(); err == nil {
		n.Master = n.Self
		n.Identity = n.Identity | NodeIdentityMaster
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
// 尝试连接主节点。判断 master 的套接字是否与自己相同。
//
// 1. 如果相同，则认为是自己。
// 如果连接未报错，则表明已经存在对应节点，报 ErrNodeMasterExisted；
// 如果连接报错，则将 master 作为异常失效信息，报 ErrNodeMasterIsSelf。
//
// 2. 如果不同，则认为主节点是另一个进程。
// 如果连接报错，则报 ErrNodeMasterInvalid。
// 如果连接返回状态码不是 http.StatusOK，则同样视为报错。
func (n *NodePool) CheckMaster(master *models.NodeInfo) error {
	if master == nil {
		return ErrNodeMasterInvalid
	}
	resp, err := n.SendRequestMasterStatus()
	if n.Self.IsSocketEqual(master) {
		// 如果与主节点 Socket 一致，那自己就是 Master。
		// 但此时仍要检查对应 Socket 是否依然有效。
		// 如果连接未报错，则认为主节点已存在。
		if err == nil {
			return ErrNodeMasterExisted
		}
		// 如果连接报错，则将本节点视为 Master。
		return ErrNodeMasterIsSelf
	}
	if err != nil {
		return ErrNodeMasterInvalid
	}
	if resp.StatusCode != http.StatusOK {
		var body = make([]byte, resp.ContentLength)
		resp.Body.Read(body)
		log.Println(string(body))
		return ErrNodeMasterValidButRefused
	}
	return nil
}

func (n *NodePool) AcceptMaster(node *models.NodeInfo) {
	n.Master = node
	n.Self.Level = n.Master.Level + 1
	n.SwitchIdentitySlaveOn()
}

func (n *NodePool) AcceptSlave(node *models.NodeInfo) (bool, error) {
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
	_, err := n.Self.AddSlaveNode(node)
	if err != nil {
		return false, nil
	}
	n.Slaves[node.ID] = *node
	return true, nil
}

var ErrNodeSlaveSocketInvalid = errors.New("invalid slave socket")

func (n *NodePool) RemoveSlave(id uint64, fresh *FreshNodeInfo) (bool, error) {
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

func (n *NodePool) RefreshSlavesStatus() ([]uint64, []uint64) {
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
func (n *NodePool) GetSlaveStatus(id uint64) (bool, error) {
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

// NotifyMasterToAddSelfAsSlave 当前节点（从节点）通知主节点添加自己为其从节点。
func (n *NodePool) NotifyMasterToAddSelfAsSlave() (bool, error) {
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
	return true, nil
}

// NotifyMasterToRemoveSelf 当前节点（从节点）通知主节点删除自己。
func (n *NodePool) NotifyMasterToRemoveSelf() (bool, error) {
	resp, err := n.SendRequestMasterToRemoveSelf()
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

func (n *NodePool) CheckSlavesStatus() {
	n.SlavesRWMutex.Lock()
	defer n.SlavesRWMutex.Unlock()
}

func (n *NodePool) RefreshSlaveNodes() {
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

// SendRequestMasterStatus 向"主节点-状态"发送请求。
// 如果已经是最高级，则报 models.ErrNodeLevelAlreadyHighest。
// 如果构建请求出错，则据实返回，此时第一个返回值为空。
// 请求构建成功，则发送请求，超时固定设为 1 秒。并返回响应和对应的错误。
func (n *NodePool) SendRequestMasterStatus() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	URL := fmt.Sprintf(NodeRequestURLFormatMasterStatus, n.Master.Socket())
	req, err := http.NewRequest(NodeRequestMethodMasterStatus, URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(NodeRequestHeaderXAuthorizationTokenKey, NodeRequestHeaderXAuthorizationTokenValue)
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *NodePool) CheckResponseMasterStatus(response *http.Response, err error) {

}

func (n *NodePool) SendRequestMasterToAddSelfAsSlave() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	URL := fmt.Sprintf(NodeRequestURLFormatMasterNotifyAdd, n.Master.Socket())
	self := FreshNodeInfo{
		Host:        n.Self.Host,
		Port:        n.Self.Port,
		Name:        n.Self.Name,
		NodeVersion: n.Self.NodeVersion,
	}
	var body = strings.NewReader(self.Encode())
	req, err := http.NewRequest(NodeRequestMethodMasterNotifyAdd, URL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add(NodeRequestHeaderXAuthorizationTokenKey, NodeRequestHeaderXAuthorizationTokenValue)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *NodePool) CheckResponseMasterNotifyAdd(response *http.Response, err error) {

}

func (n *NodePool) SendRequestMasterToRemoveSelf() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	URL := fmt.Sprintf(NodeRequestURLFormatMasterNotifyDelete, n.Master.Socket())
	req, err := http.NewRequest(NodeRequestMethodMasterNotifyDelete, URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(NodeRequestHeaderXAuthorizationTokenKey, NodeRequestHeaderXAuthorizationTokenValue)
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *NodePool) CheckResponseMasterNotifyDelete(response *http.Response, err error) {

}

var ErrNodeMasterDoesNotHaveSpecifiedSlave = errors.New("the specified slave node does not exist on the current master node")

// SendRequestSlaveStatus 发送请求：获取指定ID从节点状态。
func (n *NodePool) SendRequestSlaveStatus(id uint64) (*http.Response, error) {
	slave, exist := n.Slaves[id]
	if !exist {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	URL := fmt.Sprintf(NodeRequestURLFormatSlaveStatus, slave.Socket())
	req, err := http.NewRequest(NodeRequestMethodSlaveStatus, URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(NodeRequestHeaderXAuthorizationTokenKey, NodeRequestHeaderXAuthorizationTokenValue)
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *NodePool) SendRequest() {

}
