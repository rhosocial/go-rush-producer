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

type ActionSlaveNotifyMasterAddSelfRequestBody struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

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

func (c *ControllerServer) ActionSlaveNotifyMasterModifySelf(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

func (c *ControllerServer) ActionSlaveNotifyMasterRemoveSelf(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

func (c *ControllerServer) ActionMasterGetSlaveStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}

func (c *ControllerServer) ActionMasterNotifySlave(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
}
