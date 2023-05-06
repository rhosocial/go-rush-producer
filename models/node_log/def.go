package models

import (
	"time"

	"gorm.io/plugin/optimisticlock"
)

const (
	NodeLogTypeReportActive                         = 0 // The node reports itself as active
	NodeLogTypeFreshNodeMasterJoined                = 1 // The fresh master node report joined
	NodeLogTypeExistedNodeMasterWithdrawn           = 2 // The existed master node report withdrawn
	NodeLogTypeFreshNodeSlaveJoined                 = 3 // The fresh slave node report joined
	NodeLogTypeExistedNodeSlaveWithdrawn            = 4 // The existed slave node report withdrawn
	NodeLogTypeExistedNodeSlaveReportMasterInactive = 5 // The existed slave report master as inactive
	NodeLogTypeExistedNodeMasterReportSlaveInactive = 6 // The existed master report slave as inactive
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
