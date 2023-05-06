package models

import (
	"time"

	models "github.com/rhosocial/go-rush-producer/models/node_info_legacy"
	NodeLog "github.com/rhosocial/go-rush-producer/models/node_log"
	"gorm.io/gorm"
	"gorm.io/plugin/optimisticlock"
)

type NodeInfo struct {
	ID          uint64                 `gorm:"column:id;primaryKey;autoIncrement;<-:false" json:"id"`
	Name        string                 `gorm:"column:name;default:''" json:"name"`
	NodeVersion string                 `gorm:"column:node_version;default:'';<-:create" json:"node_version"`
	Host        string                 `gorm:"column:host;<-:create" json:"Host"`
	Port        uint16                 `gorm:"column:port;<-:create" json:"Port"`
	Level       uint8                  `gorm:"column:level" json:"Level"`
	SuperiorID  uint64                 `gorm:"column:superior_id" json:"superior_id"`
	Turn        uint32                 `gorm:"column:turn" json:"turn"`
	CreatedAt   time.Time              `gorm:"column:created_at;autoCreateTime:milli" json:"created_at"`
	UpdatedAt   time.Time              `gorm:"column:updated_at;autoUpdateTime:milli" json:"updated_at"`
	Version     optimisticlock.Version `gorm:"column:version;default:0" json:"version"`
}

// TableName 数据表名。
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
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
		Version:     m.Version,
	}
	if tx := tx.Create(&legacy); tx.Error != nil {
		return tx.Error // TODO: 1. 此处报错若不想阻塞删除，则应当改为 return nil。
	}
	return nil
}

// Subordinate 附加当前节点的下级条件。
func (m *NodeInfo) Subordinate() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("level = ?", m.Level+1).Where("superior_id = ?", m.ID)
	}
}

// Superior 附加当前节点的上级条件。
func (m *NodeInfo) Superior() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("level = ?", m.Level-1).Where("id = ?", m.SuperiorID)
	}
}

// LogActiveLatest 附加当前节点报告活跃日志，按最后更新时间倒序排序。
func (m *NodeInfo) LogActiveLatest() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("node_id = ?", m.ID).Where("type = ?", NodeLog.NodeLogTypeReportActive).Order("updated_at desc")
	}
}

func (m *NodeInfo) LogSlaveReportMasterInactiveLatest(targetID uint64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("node_id = ?", m.ID).Where("type = ?", NodeLog.NodeLogTypeExistedNodeSlaveReportMasterInactive).Where("target_node_id = ?", targetID).Order("updated_at desc")
	}
}

func (m *NodeInfo) LogMasterReportSlaveInactiveLatest(targetID uint64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("node_id = ?", m.ID).Where("type = ?", NodeLog.NodeLogTypeExistedNodeMasterReportSlaveInactive).Where("target_node_id = ?", targetID).Order("updated_at desc")
	}
}

func (m *NodeInfo) ScopeSocket() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("host = ? AND port = ?", m.Host, m.Port)
	}
}
