package models

import (
	"time"

	"gorm.io/plugin/optimisticlock"
)

const (
	NodeLogTypeReportActive                         = 0 // 节点报告自己活跃
	NodeLogTypeFreshNodeMasterJoined                = 1 // 新主节点加入
	NodeLogTypeExistedNodeMasterWithdrawn           = 2 // 旧主节点主动退出
	NodeLogTypeFreshNodeSlaveJoined                 = 3 // 新从节点加入
	NodeLogTypeExistedNodeSlaveWithdrawn            = 4 // 旧从节点主动退出
	NodeLogTypeExistedNodeSlaveReportMasterInactive = 5 // 旧从节点报告主节点不活跃
	NodeLogTypeExistedNodeMasterReportSlaveInactive = 6 // 旧主节点报告从节点不活跃
)

type NodeLog struct {
	ID           uint64                 `gorm:"column:id;primaryKey;autoIncrement;<-:false" json:"id"`
	NodeID       uint64                 `gorm:"column:node_id;<-:create" json:"node_id"`
	Type         uint8                  `gorm:"column:type;<-:create" json:"type"`
	TargetNodeID uint64                 `gorm:"column:target_node_id;default:0;<-:create" json:"target_node_id"`
	CreatedAt    time.Time              `gorm:"column:created_at;autoCreateTime:milli" json:"created_at"`
	UpdatedAt    time.Time              `gorm:"column:updated_at;autoUpdateTime:milli" json:"updated_at"`
	Version      optimisticlock.Version `gorm:"column:version;default:0" json:"version"`
}

func (m *NodeLog) TableName() string {
	return "node_log"
}
