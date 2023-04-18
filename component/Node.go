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
	"time"

	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

const (
	NodeIdentityNotDetermined = 0
	NodeIdentityMaster        = 1
	NodeIdentitySlave         = 2

	NodeRequestStatus             = 0x00000001
	NodeRequestMasterStatus       = 0x00010001
	NodeRequestMasterNotifyAdd    = 0x00010011
	NodeRequestMasterNotifyModify = 0x00010012
	NodeRequestMasterNotifyDelete = 0x00010013
	NodeRequestSlaveStatus        = 0x00020001
	NodeRequestSlaveNotify        = 0x00020011

	NodeRequestMethodStatus          = http.MethodGet
	NodeRequestMethodMasterStatus    = http.MethodGet
	NodeRequestMethodMasterNotifyAdd = http.MethodPut
	NodeRequestMethodSlaveStatus     = http.MethodGet

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
	Identity uint8
	Master   *models.NodeInfo
	Self     *models.NodeInfo
	Nodes    map[uint64]models.NodeInfo
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
		Nodes:    make(map[uint64]models.NodeInfo),
	}
	nodes.RefreshSelfSocket()
	return &nodes
}

func (n *NodePool) DiscoverMasterNode() (*models.NodeInfo, error) {
	node, err := n.Self.GetSuperiorNode(false)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (n *NodePool) SwitchIdentityToMaster() {
	n.Master = n.Self
	n.Identity = n.Identity | NodeIdentityMaster
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

func (n *NodePool) CheckMaster(master *models.NodeInfo) error {
	if master == nil {
		return ErrNodeMasterInvalid
	}
	if n.Self.IsSocketEqual(master) {
		// 如果与主节点 Socket 一致，那自己就是 Master。
		// 但此时仍要检查对应 Socket 是否依然有效。
		_, err := n.SendRequestMasterStatus()
		// 如果连接未报错，则认为主节点已存在。
		if err == nil {
			return ErrNodeMasterExisted
		}
		// 如果连接报错，则将本节点视为 Master。
		return ErrNodeMasterIsSelf
	}
	resp, err := n.SendRequestMasterStatus()
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
	n.Identity = n.Identity | NodeIdentitySlave
}

func (n *NodePool) AcceptSlave(node *models.NodeInfo) (bool, error) {
	return n.Self.AddSlaveNode(node)
}

func (n *NodePool) NotifyMasterToAddSlave() (bool, error) {
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
	n.Nodes = result
}

func (n *NodePool) NodeSelectMaster(node *models.NodeInfo) {
	if n.Identity == NodeIdentityNotDetermined {

	}
	masterNodes, err := node.GetPeerActiveNodes()
	if len(*masterNodes) > 0 {
		if err == nil {
			// 成功拿到主节点信息。将本节点设为其从节点。
			n.Identity = NodeIdentitySlave
		} else if err == models.ErrNodeNumberOfMasterNodeExceedsLowerLimit {
			// 缺主节点。选自己为主。

		} else if err == models.ErrNodeNumberOfMasterNodeExceedsUpperLimit {
			// 主节点过多。等待多余主节点自己退出。稍后再试。
		}
	}
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

type FreshNodeInfo struct {
	Name        string `json:"name"`
	NodeVersion string `json:"node_version"`
	Host        string `json:"string"`
	Port        uint16 `json:"port"`
}

func (n *NodePool) SendRequestMasterToAddSelfAsSlave() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	URL := fmt.Sprintf(NodeRequestURLFormatMasterNotifyAdd, n.Master.Socket())
	params := make(url.Values)
	params.Add("name", n.Self.Name)
	params.Add("node_version", n.Self.NodeVersion)
	params.Add("host", n.Self.Host)
	params.Add("port", strconv.Itoa(int(n.Self.Port)))
	var body = strings.NewReader(params.Encode())
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

func (n *NodePool) SendRequest() {

}
