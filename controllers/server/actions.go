package controllerServer

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-common/component/controller"
)

type ControllerServer struct {
	controller.GenericController
}

func (c *ControllerServer) RegisterActions(r *gin.Engine) {
	// 服务器组
	controller := r.Group("/server")
	{
		// 主节点
		controllerMaster := controller.Group("/master")
		{
			// 从节点获取从节点信息
			controllerMaster.GET("", c.ActionSlaveGetMasterStatus)
			// 从节点通知主节点
			controllerSlaveNotifyMaster := controllerMaster.Group("/notify")
			{
				// 新增
				controllerSlaveNotifyMaster.PUT("", c.ActionSlaveNotifyMasterAddSelf)
				// 修改
				controllerSlaveNotifyMaster.PATCH("", c.ActionSlaveNotifyMasterModifySelf)
				// 删除
				controllerSlaveNotifyMaster.DELETE("", c.ActionSlaveNotifyMasterRemoveSelf)
			}
		}
		// 从节点
		controllerSlave := controller.Group("/slave")
		{
			// 主节点获取从节点信息
			controllerSlave.GET("", c.ActionMasterGetSlaveStatus)
			// 主节点通知从节点
			controllerMasterNotifySlave := controllerSlave.Group("/notify")
			{
				// 接替自己
				controllerMasterNotifySlave.POST("/takeover", c.ActionMasterNotifySlaveToTakeover)
				// 主节点切换
				controllerMasterNotifySlave.POST("/switch_superior", c.ActionMasterNotifySlaveToSwitchSuperior)
			}
		}
		// 服务器状态。用于未知节点获取当前节点信息。
		controller.GET("", c.ActionStatus)
	}
}

// ActionStatus 服务器状态。仅用于未知节点获取当前节点信息。
func (c *ControllerServer) ActionStatus(r *gin.Context) {
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}
