package models

import (
	"time"

	"gorm.io/plugin/optimisticlock"
)

const (
	FieldIsActiveActive                  = 0
	FieldIsActiveSuperiorDetectExited    = 1
	FieldIsActiveSubordinateDetectExited = 2
)

type NodeInfo struct {
	ID          uint64                 `gorm:"column:id;primaryKey;<-:false" json:"id"`
	Name        string                 `gorm:"column:name;default:''" json:"name"`
	NodeVersion string                 `gorm:"column:node_version;default:'';<-:create" json:"node_version"`
	Host        string                 `gorm:"column:host;<-:create" json:"Host"`
	Port        uint16                 `gorm:"column:port;<-:create" json:"Port"`
	Level       uint8                  `gorm:"column:level;<-:create" json:"Level"`
	SuperiorID  uint64                 `gorm:"column:superior_id;<-create" json:"superior_id"`
	Order       uint                   `gorm:"column:order;<-:create" json:"order"`
	IsActive    uint8                  `gorm:"column:is_active;default:0" json:"is_active"`
	CreatedAt   time.Time              `gorm:"column:created_at;autoCreateTime:milli" json:"created_at"`
	UpdatedAt   time.Time              `gorm:"column:updated_at;autoUpdateTime:milli" json:"updated_at"`
	Version     optimisticlock.Version `gorm:"column:version;default:0" json:"version"`
}

func (m *NodeInfo) TableName() string {
	return "node_info"
}

// FreshNodeInfo 新节点信息。
type FreshNodeInfo struct {
	Name        string `form:"name" json:"name" binding:"required"`
	NodeVersion string `form:"node_version" json:"node_version" binding:"required"`
	Host        string `form:"host" json:"string" binding:"required"`
	Port        uint16 `form:"port" json:"port" binding:"required"`
}

type RegisteredNodeInfo struct {
	FreshNodeInfo
	ID         uint64 `json:"id" binding:"required"`
	Level      uint8  `json:"level" binding:"required"`
	SuperiorID uint64 `json:"superior_id" binding:"required"`
	Order      uint   `json:"order" binding:"required"`
	IsActive   uint8  `json:"is_active" binding:"required"`
}
