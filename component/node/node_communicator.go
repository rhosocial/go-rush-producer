package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rhosocial/go-rush-common/component/response"
	"github.com/rhosocial/go-rush-producer/models"
	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

const (
	RequestStatus             = 0x00000001
	RequestMasterStatus       = 0x00010001
	RequestMasterNotifyAdd    = 0x00010011
	RequestMasterNotifyModify = 0x00010012
	RequestMasterNotifyDelete = 0x00010013
	RequestSlaveStatus        = 0x00020001
	RequestSlaveNotify        = 0x00020011

	RequestMethodStatus                    = http.MethodGet
	RequestMethodMasterStatus              = http.MethodGet
	RequestMethodMasterNotifyAdd           = http.MethodPut
	RequestMethodMasterNotifyDelete        = http.MethodDelete
	RequestMethodSlaveStatus               = http.MethodGet
	RequestMethodSlaveNotifyTakeover       = http.MethodPost
	RequestMethodSlaveNotifySwitchSuperior = http.MethodPost

	RequestURLFormatStatus                    = "http://%s/server"
	RequestURLFormatMasterStatus              = "http://%s/server/master"
	RequestURLFormatMasterNotifyAdd           = "http://%s/server/master/notify"
	RequestURLFormatMasterNotifyModify        = "http://%s/server/master/notify"
	RequestURLFormatMasterNotifyDelete        = "http://%s/server/master/notify"
	RequestURLFormatSlaveStatus               = "http://%s/server/slave"
	RequestURLFormatSlaveNotifyTakeover       = "http://%s/server/slave/notify/takeover"
	RequestURLFormatSlaveNotifySwitchSuperior = "http://%s/server/slave/notify/switch_superior"

	RequestHeaderXAuthorizationTokenKey   = "X-Authorization-Token"
	RequestHeaderXAuthorizationTokenValue = "$2a$04$jajGD06BJd.KmTM7pgCRzeFSIMWLAUbTCOQPNJRDMnMltPZp3tK1y"
	RequestHeaderXNodeIDKey               = "X-Node-ID"
)

// ------ MasterStatus ------ //

// SendRequestMasterStatus 向"主节点-状态"发送请求。
// 如果已经是最高级，则报 ErrNodeLevelAlreadyHighest。
// 如果构建请求出错，则据实返回，此时第一个返回值为空。
// 请求构建成功，则发送请求，超时固定设为 1 秒。并返回响应和对应的错误。
func (n *Pool) SendRequestMasterStatus(master *NodeInfo.NodeInfo) (*http.Response, error) {
	if master == nil {
		return nil, ErrNodeLevelAlreadyHighest
	}
	req, err := n.PrepareNodeRequest(RequestMethodMasterStatus, RequestURLFormatMasterStatus, master.Socket(), nil, "")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// RequestMasterStatusResponseData 从节点请求主节点状态响应体的数据部分。
type RequestMasterStatusResponseData struct {
	Host       string `json:"host,omitempty"`        // 主节点自己的套接字。
	ClientIP   string `json:"client_ip,omitempty"`   // 请求从节点的客户端IP地址。
	RemoteAddr string `json:"remote_addr,omitempty"` // 请求从节点的远程地址（套接字）。
}

// RequestMasterStatusResponseExtension 从节点请求主节点状态响应体的扩展部分。
type RequestMasterStatusResponseExtension struct {
	Slaves *map[uint64]*models.RegisteredNodeInfo `json:"slaves,omitempty"` // 已登记从节点信息。
}

// RequestMasterStatusResponse 从节点请求主节点状态响应体。
type RequestMasterStatusResponse = response.Generic[RequestMasterStatusResponseData, RequestMasterStatusResponseExtension]

// ------ MasterStatus ------ //

// ------ MasterNotifyAdd ------ //

// SendRequestMasterToAddSelfAsSlave 发送请求通知主节点添加自己为从节点。
func (n *Pool) SendRequestMasterToAddSelfAsSlave() (*http.Response, error) {
	if n.Master.Node == nil {
		return nil, ErrNodeLevelAlreadyHighest
	}
	self := models.FreshNodeInfo{
		Host:        n.Self.Node.Host,
		Port:        n.Self.Node.Port,
		Name:        n.Self.Node.Name,
		NodeVersion: n.Self.Node.NodeVersion,
	}
	var body = strings.NewReader(self.Encode())
	req, err := n.PrepareNodeRequest(RequestMethodMasterNotifyAdd, RequestURLFormatMasterNotifyAdd, n.Master.Node.Socket(), body, "application/x-www-form-urlencoded")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *Pool) CheckResponseMasterNotifyAdd(response *http.Response, err error) {

}

// ------ MasterNotifyAdd ------ //

// ------ MasterNotifyRemove ------ //

func (n *Pool) SendRequestMasterToRemoveSelf() (*http.Response, error) {
	if n.Master.Node == nil {
		return nil, ErrNodeLevelAlreadyHighest
	}
	fresh := models.FreshNodeInfo{
		Host:        n.Self.Node.Host,
		Port:        n.Self.Node.Port,
		Name:        n.Self.Node.Name,
		NodeVersion: n.Self.Node.NodeVersion,
	}
	query := fmt.Sprintf("?id=%d&%s", n.Self.Node.ID, fresh.Encode())
	req, err := n.PrepareNodeRequest(RequestMethodMasterNotifyDelete, RequestURLFormatMasterNotifyDelete+query, n.Master.Node.Socket(), nil, "")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	req.Header.Add(RequestHeaderXAuthorizationTokenKey, RequestHeaderXAuthorizationTokenValue)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *Pool) CheckResponseMasterNotifyDelete(response *http.Response, err error) {

}

// ------ MasterNotifyRemove ------ //

var ErrNodeMasterDoesNotHaveSpecifiedSlave = errors.New("the specified slave node does not exist on the current master node")
var ErrNodeRequestInvalid = errors.New("invalid node request")
var ErrNodeRequestResponseError = errors.New("node request response error")
var ErrNodeExistedMasterWithdrawn = errors.New("existed master withdrawn")

// ------ SlaveGetStatus ------ //

// SendRequestSlaveStatus 发送请求：获取指定ID从节点状态。
func (n *Pool) SendRequestSlaveStatus(id uint64) (*http.Response, error) {
	slave := n.Slaves.Get(id)
	if slave == nil {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	req, err := n.PrepareNodeRequest(RequestMethodSlaveStatus, RequestURLFormatSlaveStatus, slave.Socket(), nil, "")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// ------ SlaveGetStatus ------ //

// ------ GetStatus ------ //

func (n *Pool) CheckNodeStatus(node *NodeInfo.NodeInfo) error {
	resp, err := n.SendRequestStatus(node)
	log.Println(node, err)
	if resp != nil && resp.StatusCode == http.StatusOK {
		// 请求正常，应当退出。
		return ErrNodeExisted
	}
	inactive, err := n.Self.Node.LogReportExistedNodeMasterReportSlaveInactive(node)
	log.Println(inactive, err)
	self, err := node.RemoveSelf()
	log.Println(self, err)
	return err
}

func (n *Pool) SendRequestStatus(node *NodeInfo.NodeInfo) (*http.Response, error) {
	req, err := n.PrepareNodeRequest(RequestMethodSlaveStatus, RequestURLFormatStatus, node.Socket(), nil, "")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// ------ GetStatus ------ //

// ------ SlaveNotifyMasterToSwitchSuperior ------ //

func (n *Pool) SendRequestSlaveNotifyMasterToSwitchSuperior(node *NodeInfo.NodeInfo, master *NodeInfo.NodeInfo) (*http.Response, error) {
	if master == nil {
		return nil, ErrNodeMasterInvalid
	}
	var body = strings.NewReader(master.ToRegisteredNodeInfo().Encode())
	req, err := n.PrepareNodeRequest(RequestMethodSlaveNotifySwitchSuperior, RequestURLFormatSlaveNotifySwitchSuperior, node.Socket(), body, "application/x-www-form-urlencoded")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// ------ SlaveNotifyMasterToSwitchSuperior ------ //

var ErrNodeSlaveNodeInvalid = errors.New("the specified slave node is invalid")

// ------ SlaveNotifyMasterToTakeover ------ //

func (n *Pool) SendRequestSlaveNotifyMasterToTakeover(node *NodeInfo.NodeInfo) (*http.Response, error) {
	if node == nil {
		return nil, ErrNodeSlaveNodeInvalid
	}
	var body = strings.NewReader(n.Self.Node.ToRegisteredNodeInfo().Encode())
	req, err := n.PrepareNodeRequest(RequestMethodSlaveNotifyTakeover, RequestURLFormatSlaveNotifyTakeover, node.Socket(), body, "application/x-www-form-urlencoded")
	if err != nil {
		log.Println(err)
		return nil, ErrNodeRequestResponseError
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// ------ SlaveNotifyMasterToTakeover ------ //

// PrepareNodeRequest 准备节点间通信请求。
// 准备请求过程中产生错误将如实返回。
// 建议用法：调用该函数获取到错误时，不向上继续反馈，而统一报 ErrNodeRequestInvalid 错误。并出错原因记录到日志。
func (n *Pool) PrepareNodeRequest(method string, urlFormat string, socket string, body io.Reader, contentType string) (*http.Request, error) {
	URL := fmt.Sprintf(urlFormat, socket)
	req, err := http.NewRequest(method, URL, body)
	req.Header.Add(RequestHeaderXAuthorizationTokenKey, RequestHeaderXAuthorizationTokenValue)
	if n != nil && n.Self.Node.ID != 0 {
		req.Header.Add(RequestHeaderXNodeIDKey, strconv.FormatUint(n.Self.Node.ID, 10))
	}
	if len(contentType) > 0 {
		req.Header.Add("Content-Type", contentType)
	}
	if err != nil {
		log.Printf("[Prepare Request][method:%s][url%s][error:%s]\n", method, URL, err.Error())
	}
	return req, err
}

func ParseNodeRequestResponse[T1 interface{}, T2 interface{}](resp *http.Response) (bool, error) {
	response.UnmarshalResponseBodyBaseWithDataAndExtension[T1, T2](resp)

	return true, nil
}

// ---- TODO 待确认下述代码用途 ---- //

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

// NotifyMasterToAddSelfAsSlaveResponseData 通知主节点添加自己为从节点 HTTP 响应体格式。
type NotifyMasterToAddSelfAsSlaveResponseData struct {
	ID          uint64 `json:"id"`           // 新登记的从节点的ID
	Name        string `json:"name"`         // 新登记的从节点的名称
	NodeVersion string `json:"node_version"` // 新登记的从节点的版本。
	Host        string `json:"host"`         // 新登记的从节点的域（IP地址）。
	Port        uint16 `json:"port"`         // 新登记的从节点的端口。
	Turn        uint   `json:"turn"`         // 新登记的从节点的接替顺序。
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
	self, err := NodeInfo.GetNodeInfo(respData.Data.ID)
	n.Self.Node = self
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
func (n *Pool) NotifySlaveToTakeoverSelf(candidateID uint64) (bool, error) {
	if n.Slaves.Count() == 0 {
		log.Println("no slave nodes")
		return true, nil
	} // 如果没有从节点，则不必通知。
	log.Printf("Notify slave[%d] to take over\n", candidateID)
	n.Slaves.NodesRWLock.Lock()
	defer n.Slaves.NodesRWLock.Unlock()
	var candidate *NodeInfo.NodeInfo
	for i, v := range n.Slaves.Nodes {
		if i == candidateID {
			candidate = v
			break
		}
	}

	resp, err := n.SendRequestSlaveNotifyMasterToTakeover(candidate)
	if err != nil {
		return false, err
	}
	if resp != nil {
		log.Println(resp.StatusCode, resp.Body)
	}
	return true, nil
}

func (n *Pool) NotifyAllSlavesToSwitchSuperior(candidateID uint64) (bool, error) {
	n.Slaves.NodesRWLock.Lock()
	defer n.Slaves.NodesRWLock.Unlock()
	if n.Slaves.Count() <= 1 {
		log.Println("no other slaves to be notified to switch superior")
		return true, nil
	}
	var candidate *NodeInfo.NodeInfo
	for i, v := range n.Slaves.Nodes {
		if i == candidateID {
			candidate = v
			break
		}
	}
	for i, v := range n.Slaves.Nodes {
		if i != candidateID {
			go n.NotifySlaveToSwitchSuperior(v, candidate)
		}
	}
	return true, nil
}

func (n *Pool) NotifySlaveToSwitchSuperior(slave *NodeInfo.NodeInfo, candidate *NodeInfo.NodeInfo) (bool, error) {
	if slave == nil {
		return false, ErrNodeSlaveNodeInvalid
	}
	if candidate == nil {
		return false, ErrNodeMasterInvalid
	}
	log.Printf("Notify slave[%d] to switch superior[%d]\n", slave.SuperiorID, candidate.ID)
	resp, err := n.SendRequestSlaveNotifyMasterToSwitchSuperior(slave, candidate) // 不关心响应。
	if err != nil {
		return false, err
	}
	if resp != nil {
		log.Println(resp.StatusCode, resp.Body)
	}
	return true, nil
}
