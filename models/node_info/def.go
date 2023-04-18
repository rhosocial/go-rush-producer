package models

import "time"

const (
	FieldIsActiveActive               = 0
	FieldIsActiveReportExited         = 1
	FieldIsActiveSuperiorDetectExited = 2

	FieldIsDeleteFalse = 0
	FieldIsDeleteTrue  = 1
)

type NodeInfo struct {
	ID          uint64    `gorm:"column:id;primaryKey;<-:false" json:"id"`
	Name        string    `gorm:"column:name;default:''" json:"name"`
	NodeVersion string    `gorm:"column:node_version;default:'';<-:create" json:"node_version"`
	Host        string    `gorm:"column:host;<-:create" json:"Host"`
	Port        uint16    `gorm:"column:port;<-:create" json:"Port"`
	Level       uint8     `gorm:"column:level;<-:create" json:"Level"`
	SuperiorID  uint64    `gorm:"column:superior_id;<-create" json:"superior_id"`
	Order       uint      `gorm:"column:order;<-:create" json:"order"`
	IsActive    uint8     `gorm:"column:is_active;default:0" json:"is_active"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime:milli" json:"created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at;autoUpdateTime:milli" json:"updated_at"`
	Version     uint64    `gorm:"column:version;default:0" json:"version"`
	IsDelete    bool      `gorm:"column:is_delete;default:0" json:"is_delete"`
}

func (m *NodeInfo) TableName() string {
	return "node_info"
}
