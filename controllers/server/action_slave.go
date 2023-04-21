package controllerServer

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-producer/component/node"
)

type ActionMasterGetSlaveStatusResponseData struct {
	Remaining []uint64 `json:"remaining"`
	Removed   []uint64 `json:"removed"`
}

// ActionMasterGetSlaveStatus 当前节点（从节点）收到主节点获取本节点（从节点）状态请求。（仅对等网络有效）
func (c *ControllerServer) ActionMasterGetSlaveStatus(r *gin.Context) {
	remaining, removed := node.Nodes.RefreshSlavesStatus()
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", ActionMasterGetSlaveStatusResponseData{remaining, removed}, nil))
}

// ActionMasterNotifySlaveToTakeover 当前节点（从节点）收到主节点发起接替自己主节点身份请求。（仅对等网络有效）
func (c *ControllerServer) ActionMasterNotifySlaveToTakeover(r *gin.Context) {

	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}

// ActionMasterNotifySlaveToSwitchSuperior 当前节点（从节点）收到主节点发起向另一节点切换主节点身份请求。（仅对等网络有效）
func (c *ControllerServer) ActionMasterNotifySlaveToSwitchSuperior(r *gin.Context) {

}
