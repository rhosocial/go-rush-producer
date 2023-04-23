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

// ------ MasterStatus ------ //

// SendRequestMasterStatus 向"主节点-状态"发送请求。
// 如果已经是最高级，则报 models.ErrNodeLevelAlreadyHighest。
// 如果构建请求出错，则据实返回，此时第一个返回值为空。
// 请求构建成功，则发送请求，超时固定设为 1 秒。并返回响应和对应的错误。
func SendRequestMasterStatus(master *models.NodeInfo) (*http.Response, error) {
	if master == nil {
		return nil, ErrNodeLevelAlreadyHighest
	}
	req, err := PrepareNodeRequest(RequestMethodMasterStatus, RequestURLFormatMasterStatus, master.Socket(), nil, "")
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
	if n.Master == nil {
		return nil, ErrNodeLevelAlreadyHighest
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
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

func (n *Pool) CheckResponseMasterNotifyAdd(response *http.Response, err error) {

}

func (n *Pool) SendRequestMasterToRemoveSelf() (*http.Response, error) {
	if n.Master == nil {
		return nil, ErrNodeLevelAlreadyHighest
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
		log.Println(err)
		return nil, ErrNodeRequestInvalid
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	return resp, err
}

// PrepareNodeRequest 准备节点间通信请求。
// 准备请求过程中产生错误将如实返回。
// 建议用法：调用该函数获取到错误时，不向上继续反馈，而统一报 ErrNodeRequestInvalid 错误。并出错原因记录到日志。
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
