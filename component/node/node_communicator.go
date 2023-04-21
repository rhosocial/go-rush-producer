package node

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rhosocial/go-rush-common/component/response"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

const (
	RequestStatus             = 0x00000001
	RequestMasterStatus       = 0x00010001
	RequestMasterNotifyAdd    = 0x00010011
	RequestMasterNotifyModify = 0x00010012
	RequestMasterNotifyDelete = 0x00010013
	RequestSlaveStatus        = 0x00020001
	RequestSlaveNotify        = 0x00020011

	RequestMethodStatus             = http.MethodGet
	RequestMethodMasterStatus       = http.MethodGet
	RequestMethodMasterNotifyAdd    = http.MethodPut
	RequestMethodMasterNotifyDelete = http.MethodDelete
	RequestMethodSlaveStatus        = http.MethodGet

	RequestURLFormatStatus             = "http://%s/server"
	RequestURLFormatMasterStatus       = "http://%s/server/master"
	RequestURLFormatMasterNotifyAdd    = "http://%s/server/master/notify"
	RequestURLFormatMasterNotifyModify = "http://%s/server/master/notify"
	RequestURLFormatMasterNotifyDelete = "http://%s/server/master/notify"
	RequestURLFormatSlaveStatus        = "http://%s/server/slave"
	RequestURLFormatSlaveNotify        = "http://%s/server/slave/notify"

	RequestHeaderXAuthorizationTokenKey   = "X-Authorization-Token"
	RequestHeaderXAuthorizationTokenValue = "$2a$04$jajGD06BJd.KmTM7pgCRzeFSIMWLAUbTCOQPNJRDMnMltPZp3tK1y"
)

// DiscoverMasterNode 发现主节点。返回发现的节点信息指针。
// 调用前，Pool.Self 必须已经设置 models.NodeInfo 的 Level 值。上级即为 Pool.Self.Level - 1，且不指定具体上级 ID。
// 如果 Level 已经为 0，则没有更高级，报 models.ErrNodeLevelAlreadyHighest。
// 如果查找不到最高级，则报 models.ErrNodeSuperiorNotExist。
func (n *Pool) DiscoverMasterNode() (*models.NodeInfo, error) {
	node, err := n.Self.GetSuperiorNode(false)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// ------ MasterStatus ------ //

// SendRequestMasterStatus 向"主节点-状态"发送请求。
// 如果已经是最高级，则报 models.ErrNodeLevelAlreadyHighest。
// 如果构建请求出错，则据实返回，此时第一个返回值为空。
// 请求构建成功，则发送请求，超时固定设为 1 秒。并返回响应和对应的错误。
func (n *Pool) SendRequestMasterStatus() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	req, err := PrepareNodeRequest(RequestMethodMasterStatus, RequestURLFormatMasterStatus, n.Master.Socket(), nil, "")
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

type RequestMasterStatusResponseData struct {
	Host       string `json:"host,omitempty"`
	ClientIP   string `json:"client_ip,omitempty"`
	RemoteAddr string `json:"remote_addr,omitempty"`
}

type RequestMasterStatusResponseExtension struct {
	Slaves *map[uint64]*models.RegisteredNodeInfo `json:"slaves,omitempty"`
}

type RequestMasterStatusResponse = response.Generic[RequestMasterStatusResponseData, RequestMasterStatusResponseExtension]

// ------ MasterStatus ------ //

// ------ MasterNotifyAdd ------ //

// SendRequestMasterToAddSelfAsSlave 发送请求通知主节点添加自己为从节点。
func (n *Pool) SendRequestMasterToAddSelfAsSlave() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	self := models.FreshNodeInfo{
		Host:        n.Self.Host,
		Port:        n.Self.Port,
		Name:        n.Self.Name,
		NodeVersion: n.Self.NodeVersion,
	}
	var body = strings.NewReader(self.Encode())
	req, err := PrepareNodeRequest(RequestMethodMasterNotifyAdd, RequestURLFormatMasterNotifyAdd, n.Master.Socket(), body, "application/x-www-form-urlencoded")
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *Pool) CheckResponseMasterNotifyAdd(response *http.Response, err error) {

}

func (n *Pool) SendRequestMasterToRemoveSelf() (*http.Response, error) {
	if n.Master == nil {
		return nil, models.ErrNodeLevelAlreadyHighest
	}
	fresh := models.FreshNodeInfo{
		Host:        n.Self.Host,
		Port:        n.Self.Port,
		Name:        n.Self.Name,
		NodeVersion: n.Self.NodeVersion,
	}
	query := fmt.Sprintf("?id=%d&%s", n.Self.ID, fresh.Encode())
	req, err := PrepareNodeRequest(RequestMethodMasterNotifyDelete, RequestURLFormatMasterNotifyDelete+query, n.Master.Socket(), nil, "")
	if err != nil {
		return nil, ErrNodeRequestInvalid
	}
	req.Header.Add(RequestHeaderXAuthorizationTokenKey, RequestHeaderXAuthorizationTokenValue)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *Pool) CheckResponseMasterNotifyDelete(response *http.Response, err error) {

}

var ErrNodeMasterDoesNotHaveSpecifiedSlave = errors.New("the specified slave node does not exist on the current master node")
var ErrNodeRequestInvalid = errors.New("invalid node request")

// SendRequestSlaveStatus 发送请求：获取指定ID从节点状态。
func (n *Pool) SendRequestSlaveStatus(id uint64) (*http.Response, error) {
	slave, exist := n.Slaves[id]
	if !exist {
		return nil, ErrNodeMasterDoesNotHaveSpecifiedSlave
	}
	req, err := PrepareNodeRequest(RequestMethodSlaveStatus, RequestURLFormatSlaveStatus, slave.Socket(), nil, "")
	if err != nil {
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func PrepareNodeRequest(method string, urlFormat string, socket string, body io.Reader, contentType string) (*http.Request, error) {
	URL := fmt.Sprintf(urlFormat, socket)
	req, err := http.NewRequest(method, URL, body)
	req.Header.Add(RequestHeaderXAuthorizationTokenKey, RequestHeaderXAuthorizationTokenValue)
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
