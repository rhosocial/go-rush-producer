package models

import (
	"time"

	models "github.com/rhosocial/go-rush-producer/models/node_info_legacy"
	"gorm.io/gorm"
	"gorm.io/plugin/optimisticlock"
)

const (
	FieldIsActiveActive                  = 0
	FieldIsActiveSuperiorDetectExited    = 1
	FieldIsActiveSubordinateDetectExited = 2
)

type NodeInfo struct {
	ID          uint64                 `gorm:"column:id;primaryKey;autoIncrement;<-:false" json:"id"`
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

// AfterDelete NodeInfo 删除后将最后一刻数据移入 models.NodeInfoLegacy 中。
// TODO: 1. 插入操作出错是否报错。若报错，则会阻断删除。若不报错，则会掩盖 models.NodeInfoLegacy 异常。
func (m *NodeInfo) AfterDelete(tx *gorm.DB) (err error) {
	legacy := models.NodeInfoLegacy{
		ID:          m.ID,
		Name:        m.Name,
		NodeVersion: m.NodeVersion,
		Host:        m.Host,
		Port:        m.Port,
		Level:       m.Level,
		SuperiorID:  m.SuperiorID,
		Order:       m.Order,
		IsActive:    m.IsActive,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
		Version:     m.Version,
	}
	if tx := tx.Create(&legacy); tx.Error != nil {
		return tx.Error // TODO: 1. 此处报错若不想阻塞删除，则应当改为 return nil。
	}
	return nil
}

// FreshNodeInfo 新节点信息。
type FreshNodeInfo struct {
	Name        string `form:"name" json:"name" binding:"required"`
	NodeVersion string `form:"node_version" json:"node_version" binding:"required"`
	Host        string `form:"host" json:"string" binding:"required"`
	Port        uint16 `form:"port" json:"port" binding:"required"`
}

// RegisteredNodeInfo 已登记节点信息。
type RegisteredNodeInfo struct {
	FreshNodeInfo
	ID         uint64 `json:"id" binding:"required"`
	Level      uint8  `json:"level" binding:"required"`
	SuperiorID uint64 `json:"superior_id" binding:"required"`
	Order      uint   `json:"order" binding:"required"`
	IsActive   uint8  `json:"is_active" binding:"required"`
}
