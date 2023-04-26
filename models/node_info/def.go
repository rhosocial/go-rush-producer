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
	Level       uint8                  `gorm:"column:level" json:"Level"`
	SuperiorID  uint64                 `gorm:"column:superior_id" json:"superior_id"`
	Turn        uint                   `gorm:"column:turn" json:"turn"`
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
		Turn:        m.Turn,
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

func (m *NodeInfo) Subordinate() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("level = ?", m.Level+1).Where("superior_id = ?", m.ID)
	}
}

func (m *NodeInfo) ActiveSubordinate() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return m.Subordinate()(db).Where("is_active = ?", FieldIsActiveActive)
	}
}

func (m *NodeInfo) Superior() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("level = ?", m.Level-1).Where("id = ?", m.SuperiorID)
	}
}
