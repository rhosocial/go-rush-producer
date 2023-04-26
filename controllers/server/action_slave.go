package controllerServer

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/rhosocial/go-rush-producer/component/node"
	base "github.com/rhosocial/go-rush-producer/models"
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
	// 1.
	var existed base.RegisteredNodeInfo
	if err := r.ShouldBindWith(&existed, binding.FormPost); err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, "failed to bind post body", err.Error(), nil))
		return
	}
	if id, err := strconv.ParseUint(r.GetHeader("X-Node-ID"), 10, 64); err != nil && id != existed.ID {
		r.AbortWithStatusJSON(http.StatusForbidden, c.NewResponseGeneric(r, 1, "invalid master node id", nil, nil))
	}
	node.Nodes.Supersede(&existed)
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}

// ActionMasterNotifySlaveToSwitchSuperior 当前节点（从节点）收到主节点发起向另一节点切换主节点身份请求。（仅对等网络有效）
func (c *ControllerServer) ActionMasterNotifySlaveToSwitchSuperior(r *gin.Context) {
	var superseded base.RegisteredNodeInfo
	if err := r.ShouldBind(&superseded); err != nil {
		r.AbortWithStatusJSON(http.StatusBadRequest, c.NewResponseGeneric(r, 1, "failed to bind post body", err.Error(), nil))
		return
	}
	// 1. 将超时容忍时限加长。原有时长为 m，加长后为 m + n。
	// 2. 在 m 时询问新 master。
	// 3. 若新 master 准备好，且有自己。恢复原有容忍时长 n。
	// 4. 若新 master 未准备好，等待 1 次。若再次未准备好。尝试接替。
	err := node.Nodes.SwitchSuperior(&superseded)
	if err != nil {
		r.AbortWithStatusJSON(http.StatusInternalServerError, c.NewResponseGeneric(r, 1, "failed to switch superior", err.Error(), nil))
		return
	}
	r.JSON(http.StatusOK, c.NewResponseGeneric(r, 0, "success", nil, nil))
}
