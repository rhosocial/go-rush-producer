package controllerServer

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-producer/component"
	"github.com/rhosocial/go-rush-producer/models/node_info"
)

type ControllerServer struct {
	commonComponent.GenericController
}

func (c *ControllerServer) RegisterActions(r *gin.Engine) {
	controller := r.Group("/server")
	{
		controllerMaster := controller.Group("/master")
		{
			controllerMaster.GET("", c.ActionSlaveGetMasterStatus)
			controllerSlaveNotifyMaster := controllerMaster.Group("/notify")
			{
				controllerSlaveNotifyMaster.PUT("", c.ActionSlaveNotifyMasterAddSelf)
				controllerSlaveNotifyMaster.PATCH("", c.ActionSlaveNotifyMasterModifySelf)
				controllerSlaveNotifyMaster.DELETE("", c.ActionSlaveNotifyMasterRemoveSelf)
			}
		}
		controllerSlave := controller.Group("/slave")
		{
			controllerSlave.GET("", c.ActionMasterGetSlaveStatus)
			controllerMasterNotifySlave := controllerSlave.Group("/notify")
			{
				controllerMasterNotifySlave.POST("", c.ActionMasterNotifySlave)
			}
		}
		controller.GET("", c.ActionStatus)
	}
}

func (c *ControllerServer) ActionStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

type ActionSlaveGetMasterStatusResponseData struct {
	Host       string `json:"host"`
	ClientIP   string `json:"client_ip"`
	RemoteAddr string `json:"remote_addr"`
}

// ActionSlaveGetMasterStatus 从节点发起获取主节点（自己）状态的请求。
// 应当返回请求节点的 r.Request.Host、r.ClientIP() 和 r.Request.RemoteAddr 供远程节点校验。
func (c *ControllerServer) ActionSlaveGetMasterStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(
		r, 0, "success", ActionSlaveGetMasterStatusResponseData{
			Host:       r.Request.Host,
			ClientIP:   r.ClientIP(),
			RemoteAddr: r.Request.RemoteAddr,
		}, nil,
	))
}

// ActionSlaveNotifyMasterAddSelf 从节点通知主节点（自己）添加其为从节点。
// 从节点应当发来 component.FreshNodeInfo 信息。
// TODO: 校验从节点发来的 models.FreshNodeInfo 信息。
func (c *ControllerServer) ActionSlaveNotifyMasterAddSelf(r *gin.Context) {
	port, err := strconv.ParseUint(r.PostForm("port"), 10, 16)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, err.Error(), nil, nil))
		return
	}
	host := r.PostForm("host")
	fresh := models.FreshNodeInfo{
		Name:        r.PostForm("name"),
		NodeVersion: r.PostForm("node_version"),
		Host:        r.ClientIP(),
		Port:        uint16(port),
	}
	slave, err := component.Nodes.AcceptSlave(&fresh)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusInternalServerError, commonComponent.NewGenericResponse(r, 1, "failed to accept slave", err.Error(), nil))
		return
	}
	respData := component.NotifyMasterToAddSelfAsSlaveResponseData{
		ID:          slave.ID,
		Name:        slave.Name,
		NodeVersion: slave.NodeVersion,
		Host:        slave.Host,
		Port:        slave.Port,
	}
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", respData, host == r.ClientIP()))
}

// ActionSlaveNotifyMasterModifySelf 从节点通知主节点（自己）修改自身信息。
// TODO:可以修改的项待定。
func (c *ControllerServer) ActionSlaveNotifyMasterModifySelf(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

// ActionSlaveNotifyMasterRemoveSelf 从节点通知主节点（自己）退出。
func (c *ControllerServer) ActionSlaveNotifyMasterRemoveSelf(r *gin.Context) {
	// 校验客户端信息
	// 请求ID和Socket是否对应。如果不是，则返回禁止。
	slaveID, err := strconv.ParseUint(r.Query("id"), 10, 64)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, "failed to parse `id`", err.Error(), nil))
		return
	}
	port, err := strconv.ParseUint(r.Query("port"), 10, 16)
	if err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, "failed to parse `port`", err.Error(), nil))
		return
	}
	fresh := models.FreshNodeInfo{
		Host:        r.ClientIP(),
		Port:        uint16(port),
		Name:        r.Query("name"),
		NodeVersion: r.Query("node_version"),
	}
	if _, err := component.Nodes.RemoveSlave(slaveID, &fresh); err != nil {
		r.Error(err)
		r.AbortWithStatusJSON(http.StatusInternalServerError, commonComponent.NewGenericResponse(r, 1, "failed to remove slave", err.Error(), nil))
		return
	}
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

type ActionMasterGetSlaveStatusResponseData struct {
	Remaining []uint64 `json:"remaining"`
	Removed   []uint64 `json:"removed"`
}

// ActionMasterGetSlaveStatus 当前节点（从节点）收到主节点获取本节点（从节点）状态请求。（仅对等网络有效）
func (c *ControllerServer) ActionMasterGetSlaveStatus(r *gin.Context) {
	remaining, removed := component.Nodes.RefreshSlavesStatus()
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", ActionMasterGetSlaveStatusResponseData{remaining, removed}, nil))
}

// ActionMasterNotifySlave 当前节点（主节点）通知从节点。（通知内容待定，仅对等网络有效）
func (c *ControllerServer) ActionMasterNotifySlave(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}
