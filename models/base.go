package models

import (
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var NodeInfoDB *gorm.DB

// FreshNodeInfo 新节点信息。
type FreshNodeInfo struct {
	Name        string `form:"name" json:"name" binding:"required"`
	NodeVersion string `form:"node_version" json:"node_version" binding:"required"`
	Host        string `form:"host" json:"host" binding:"required"`
	Port        uint16 `form:"port" json:"port" binding:"required"`
}

// RegisteredNodeInfo 已登记节点信息。
type RegisteredNodeInfo struct {
	FreshNodeInfo
	ID         uint64 `form:"id" json:"id"`
	Level      uint8  `form:"level" json:"level"`
	SuperiorID uint64 `form:"superior_id" json:"superior_id"`
	Turn       uint   `form:"turn" json:"turn"`
	IsActive   uint8  `form:"is_active" json:"is_active"`
	Retry      uint8  `form:"retry" json:"retry"`
}

func Socket(host string, port uint16) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("host = ? AND port = ?", host, port)
	}
}

func (n *FreshNodeInfo) Encode() string {
	params := make(url.Values)
	params.Add("name", n.Name)
	params.Add("node_version", n.NodeVersion)
	params.Add("host", n.Host)
	params.Add("port", strconv.Itoa(int(n.Port)))
	return params.Encode()
}

func (n *FreshNodeInfo) Log() string {
	return fmt.Sprintf("Fresh Node: %39s:%-5d | %s @ %s", n.Host, n.Port, n.Name, n.NodeVersion)
}

func (n *FreshNodeInfo) IsEqual(target *FreshNodeInfo) bool {
	if n != nil && target == nil || n == nil && target != nil {
		if gin.Mode() == gin.DebugMode {
			log.Println("One of the two is nil and the other is not.")
		}
		return false
	}
	if gin.Mode() == gin.DebugMode {
		log.Println("Origin: ", n.Log())
		log.Println("Target: ", target.Log())
	}
	return n.Name == target.Name && n.NodeVersion == target.NodeVersion && n.Host == target.Host && n.Port == target.Port
}

// Log 输出信息。
// TODO: 待补充 RegisteredNodeInfo 的 IsActive 字段友好输出。
func (n *RegisteredNodeInfo) Log() string {
	return fmt.Sprintf("Regst Node: %39s:%-5d | %s @ %s | Superior: %10d | Level: %3d | Turn: %3d | Active: %d\n",
		n.Host, n.Port, n.Name, n.NodeVersion,
		n.SuperiorID, n.Level, n.Turn, n.IsActive,
	)
}

func (n *RegisteredNodeInfo) Encode() string {
	params := make(url.Values)
	params.Add("name", n.Name)
	params.Add("node_version", n.NodeVersion)
	params.Add("host", n.Host)
	params.Add("port", strconv.Itoa(int(n.Port)))
	params.Add("id", strconv.FormatUint(n.ID, 10))
	params.Add("level", strconv.Itoa(int(n.Level)))
	params.Add("superior_id", strconv.FormatUint(n.SuperiorID, 10))
	params.Add("turn", strconv.FormatUint(uint64(n.Turn), 10))
	params.Add("is_active", strconv.Itoa(int(n.IsActive)))
	return params.Encode()
}
