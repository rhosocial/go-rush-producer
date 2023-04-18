package controllerServer

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-producer/component"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
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

type ResponseDataSlaveGetMasterStatus struct {
	Host       string `json:"host"`
	ClientIP   string `json:"client_ip"`
	RemoteAddr string `json:"remote_addr"`
}

func (c *ControllerServer) ActionSlaveGetMasterStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(
		r, 0, "success", ResponseDataSlaveGetMasterStatus{
			Host:       r.Request.Host,
			ClientIP:   r.ClientIP(),
			RemoteAddr: r.Request.RemoteAddr,
		}, nil,
	))
}

// ActionSlaveNotifyMasterAddSelf 从节点通知主节点（自己）添加其为从节点。
func (c *ControllerServer) ActionSlaveNotifyMasterAddSelf(r *gin.Context) {
	port, err := strconv.ParseUint(r.PostForm("port"), 10, 16)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, err.Error(), nil, nil))
		return
	}
	host := r.PostForm("host")
	slave := models.NodeInfo{
		Name:        r.PostForm("name"),
		NodeVersion: r.PostForm("node_version"),
		Host:        r.ClientIP(),
		Port:        uint16(port),
	}
	component.Nodes.AcceptSlave(&slave)
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", host == r.ClientIP(), nil))
}

// ActionSlaveNotifyMasterModifySelf 从节点通知主节点（自己）修改自身信息。
// 可以修改的项包括
func (c *ControllerServer) ActionSlaveNotifyMasterModifySelf(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

// ActionSlaveNotifyMasterRemoveSelf 从节点通知主节点（自己）退出。
func (c *ControllerServer) ActionSlaveNotifyMasterRemoveSelf(r *gin.Context) {
	// 校验客户端信息
	// 请求ID和Socket是否对应。如果不是，则返回禁止。
	slaveID, err := strconv.ParseUint(r.Param("id"), 10, 64)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, err.Error(), nil, nil))
		return
	}
	port, err := strconv.ParseUint(r.Param("port"), 10, 16)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(r, 1, err.Error(), nil, nil))
		return
	}
	if _, err := component.Nodes.RemoveSlave(slaveID, r.ClientIP(), uint16(port)); err != nil {
		r.AbortWithStatusJSON(http.StatusForbidden, commonComponent.NewGenericResponse(r, 1, err.Error(), nil, nil))
		return
	}
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

func (c *ControllerServer) ActionMasterGetSlaveStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

func (c *ControllerServer) ActionMasterNotifySlave(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}
