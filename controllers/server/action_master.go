package controllerServer

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-producer/component/node"
	base "github.com/rhosocial/go-rush-producer/models"
)

// ActionSlaveGetMasterStatus 从节点发起获取主节点（自己）状态的请求。
// 应当返回请求节点的 r.Request.Host、r.ClientIP() 和 r.Request.RemoteAddr 供远程节点校验。
func (c *ControllerServer) ActionSlaveGetMasterStatus(r *gin.Context) {
	// TODO: 从请求的 Header 中解出 X-Node-ID，并校验。
	if nodeID, err := strconv.ParseUint(r.GetHeader("X-Node-ID"), 10, 64); err == nil {
		// 解析 nodeID 成功，则更新其重试次数。
		node.Nodes.Slaves.RetryClear(nodeID)
	}
	log.Println(r.GetHeader("X-Node-ID"))
	r.JSON(http.StatusOK, c.NewResponseGeneric(
		r, 0, "success", node.RequestMasterStatusResponseData{
			Host:       r.Request.Host,
			ClientIP:   r.ClientIP(),
			RemoteAddr: r.Request.RemoteAddr,
		}, node.RequestMasterStatusResponseExtension{
			Slaves: node.Nodes.Slaves.GetRegisteredNodeInfos(),
		},
	))
}

// ActionSlaveNotifyMasterAddSelf 从节点通知主节点（自己）添加其为从节点。
// 从节点应当发来 models.FreshNodeInfo 信息。
// TODO: 校验从节点发来的 models.FreshNodeInfo 信息。
//
// 方法必须为 PUT，Header 的 Content-Type 必须为 application/json。
//
// 参数必须包含：
//
// 1. port: 请求加入从节点的端口号。
//
// 2. host: 请求加入从节点的域（IP地址）。可以是IPv4或IPv6。该信息仅供判断是否与本机收到的客户端IP一致，并不会登记为实际 host 。
//
// 3. name: 请求加入从节点的名称。
//
// 4. node_version: 请求加入从节点的版本号。
//
// 当接受了从节点等级请求后，响应码为 200 OK。响应体为 JSON 字符串，格式和说明参见 node.NotifyMasterToAddSelfAsSlaveResponseData。
// 若请求有误，则返回具体错误信息。
func (c *ControllerServer) ActionSlaveNotifyMasterAddSelf(r *gin.Context) {
	port, err := strconv.ParseUint(r.PostForm("port"), 10, 16)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, err.Error(), nil, nil))
		return
	}
	host := r.PostForm("host")
	fresh := base.FreshNodeInfo{
		Name:        r.PostForm("name"),
		NodeVersion: r.PostForm("node_version"),
		Host:        r.ClientIP(),
		Port:        uint16(port),
	}
	slave, err := node.Nodes.AcceptSlave(&fresh)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusInternalServerError, c.NewResponseGeneric(r, 1, "failed to accept slave", err.Error(), nil))
		return
	}
	respData := node.NotifyMasterToAddSelfAsSlaveResponseData{
		ID:          slave.ID,
		Name:        slave.Name,
		NodeVersion: slave.NodeVersion,
		Host:        slave.Host,
		Port:        slave.Port,
		Turn:        slave.Turn,
	}
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", respData, host == r.ClientIP()))
}

// ActionSlaveNotifyMasterModifySelf 从节点通知主节点（自己）修改自身信息。
// TODO:可以修改的项待定。
func (c *ControllerServer) ActionSlaveNotifyMasterModifySelf(r *gin.Context) {
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}

// ActionSlaveNotifyMasterRemoveSelf 从节点通知主节点（自己）退出。
//
// 方法必须为 DELETE。
//
// 参数必须包含：
//
// 1. id: 请求退出从节点的ID。
//
// 2. port: 请求退出从节点的端口号。
//
// 3. name: 请求退出从节点的名称。
//
// 4. node_version: 请求退出从节点的版本。
//
// 以上四个参数必须与实际一直才能删除。
func (c *ControllerServer) ActionSlaveNotifyMasterRemoveSelf(r *gin.Context) {
	// 校验客户端信息
	// 请求ID和Socket是否对应。如果不是，则返回禁止。
	slaveID, err := strconv.ParseUint(r.Query("id"), 10, 64)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, "failed to parse `id`", err.Error(), nil))
		return
	}
	port, err := strconv.ParseUint(r.Query("port"), 10, 16)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, "failed to parse `port`", err.Error(), nil))
		return
	}
	fresh := base.FreshNodeInfo{
		Host:        r.ClientIP(),
		Port:        uint16(port),
		Name:        r.Query("name"),
		NodeVersion: r.Query("node_version"),
	}
	if _, err := node.Nodes.RemoveSlave(slaveID, &fresh); err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusInternalServerError, c.NewResponseGeneric(r, 1, "failed to remove slave", err.Error(), nil))
		return
	}
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}
