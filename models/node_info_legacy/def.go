package models

import (
	"time"

	"gorm.io/plugin/optimisticlock"
)

type NodeInfoLegacy struct {
	ID          uint64                 `gorm:"column:id;primaryKey;<-:create" json:"id"`
	Name        string                 `gorm:"column:name;<-:create" json:"name"`
	NodeVersion string                 `gorm:"column:node_version;<-:create" json:"node_version"`
	Host        string                 `gorm:"column:host;<-:create" json:"Host"`
	Port        uint16                 `gorm:"column:port;<-:create" json:"Port"`
	Level       uint8                  `gorm:"column:level;<-:create" json:"Level"`
	SuperiorID  uint64                 `gorm:"column:superior_id;<-create" json:"superior_id"`
	Turn        uint                   `gorm:"column:turn;<-:create" json:"order"`
	IsActive    uint8                  `gorm:"column:is_active;<-:create" json:"is_active"`
	CreatedAt   time.Time              `gorm:"column:created_at;<-:create" json:"created_at"`
	UpdatedAt   time.Time              `gorm:"column:updated_at;<-:create" json:"updated_at"`
	Version     optimisticlock.Version `gorm:"column:version;<-:create" json:"version"`
}

func (m *NodeInfoLegacy) TableName() string {
	return "node_info_legacy"
}
