package controllerServer

import (
	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"net/http"
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
	RemoteIP   string `json:"remote_ip"`
	RemoteAddr string `json:"remote_addr"`
}

func (c *ControllerServer) ActionSlaveGetMasterStatus(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(
		r, 0, "success", ResponseDataSlaveGetMasterStatus{
			Host:       r.Request.Host,
			RemoteIP:   r.RemoteIP(),
			RemoteAddr: r.Request.RemoteAddr,
		}, nil,
	))
}

func (c *ControllerServer) ActionSlaveNotifyMasterAddSelf(r *gin.Context) {
	r.JSON(http.StatusOK, commonComponent.NewGenericResponse(r, 0, "success", nil, nil))
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
