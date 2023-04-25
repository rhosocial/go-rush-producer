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

type NotifyMasterToAddSelfAsSlaveResponseData struct {
	ID          uint64 `json:"id"`
	Name        string `json:"name"`
	NodeVersion string `json:"node_version"`
	Host        string `json:"host"`
	Port        uint16 `json:"port"`
	Turn        uint   `json:"turn"`
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
func (n *Pool) NotifySlaveToTakeoverSelf() (bool, error) {
	if n.Slaves.Count() == 0 {
		return true, nil
	} // 如果没有从节点，则不必通知。

	return true, nil
}

func (n *Pool) NotifyAllSlavesToSwitchSuperior(succeedID uint64) (bool, error) {
	return true, nil
}

func (n *Pool) NotifySlaveToSwitchSuperior() (bool, error) {
	return true, nil
}
