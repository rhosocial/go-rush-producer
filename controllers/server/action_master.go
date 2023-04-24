package controllerServer

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-producer/component/node"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
)

// ActionSlaveGetMasterStatus 从节点发起获取主节点（自己）状态的请求。
// 应当返回请求节点的 r.Request.Host、r.ClientIP() 和 r.Request.RemoteAddr 供远程节点校验。
func (c *ControllerServer) ActionSlaveGetMasterStatus(r *gin.Context) {
	// TODO: 从请求的 Header 中解出 X-Node-ID，并校验。
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
// 从节点应当发来 component.FreshNodeInfo 信息。
// TODO: 校验从节点发来的 models.FreshNodeInfo 信息。
func (c *ControllerServer) ActionSlaveNotifyMasterAddSelf(r *gin.Context) {
	port, err := strconv.ParseUint(r.PostForm("port"), 10, 16)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, err.Error(), nil, nil))
		return
	}
	host := r.PostForm("host")
	fresh := models.FreshNodeInfo{
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
	}
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", respData, host == r.ClientIP()))
}

// ActionSlaveNotifyMasterModifySelf 从节点通知主节点（自己）修改自身信息。
// TODO:可以修改的项待定。
func (c *ControllerServer) ActionSlaveNotifyMasterModifySelf(r *gin.Context) {
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}

// ActionSlaveNotifyMasterRemoveSelf 从节点通知主节点（自己）退出。
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
	fresh := models.FreshNodeInfo{
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
