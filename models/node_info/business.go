package models

import (
	"errors"
	"fmt"
	"log"

	base "github.com/rhosocial/go-rush-producer/models"
	models "github.com/rhosocial/go-rush-producer/models/node_log"
	"gorm.io/gorm"
)

const (
	MinimumNumberOfActiveMasterNodes = 1
)

func NewNodeInfo(name string, port uint16, level uint8) *NodeInfo {
	var node = NodeInfo{
		Name:        name,
		NodeVersion: "0.0.1",
		Port:        port,
		Level:       level,
	}
	return &node
}

func (m *NodeInfo) IsSocketEqual(target *NodeInfo) bool {
	return m.Host == target.Host && m.Port == target.Port
}

var ErrNodeNumberOfMasterNodeExceedsUpperLimit = errors.New("the number of active master nodes exceeds the upper limit")
var ErrNodeNumberOfMasterNodeExceedsLowerLimit = errors.New("the number of active master nodes exceeds the lower limit")

func (m *NodeInfo) GetPeerActiveNodes() (*[]NodeInfo, error) {
	var nodes []NodeInfo
	var condition = map[string]interface{}{
		"is_active": FieldIsActiveActive,
		"level":     m.Level,
	}
	tx := base.NodeInfoDB.Where(condition).Find(&nodes)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &nodes, nil
}

func (m *NodeInfo) Socket() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func (m *NodeInfo) Log() string {
	return fmt.Sprintf("[GO-RUSH] %10d | %39s:%-5d | Superior: %10d | Level: %3d | Turn: %3d | Active: %d\n",
		m.ID,
		m.Host, m.Port,
		m.SuperiorID, m.Level, m.Turn, m.IsActive,
	)
}

var ErrNodeSuperiorNotExist = errors.New("superior node not exist")
var ErrNodeDatabaseError = errors.New("node database error") // TODO: 具体错误信息待完善。

// GetSuperiorNode 获得当前级别的上级节点。如果要指定上级，则 specifySuperior = true。
// 如果为发现上级阶段，则不指定上级。如果为检查上级，则需要指定。
// 如果已经是最高级，则报 ErrNodeLevelAlreadyHighest。
// 如果查询数据库不存在上级节点，则报 ErrNodeSuperiorNotExist。其它数据库错误则报 ErrNodeDatabaseError。
func (m *NodeInfo) GetSuperiorNode(specifySuperior bool) (*NodeInfo, error) {
	var node NodeInfo
	var condition = map[string]interface{}{
		"is_active": FieldIsActiveActive,
		"level":     m.Level - 1,
	}
	if specifySuperior {
		condition["id"] = m.SuperiorID
	}
	if tx := base.NodeInfoDB.Where(condition).First(&node); tx.Error == gorm.ErrRecordNotFound {
		return nil, ErrNodeSuperiorNotExist
	} else if tx.Error != nil {
		log.Println(tx.Error)
		return nil, ErrNodeDatabaseError
	}
	return &node, nil
}

func (m *NodeInfo) GetAllSlaveNodes() (*[]NodeInfo, error) {
	var slaveNodes []NodeInfo
	var conditionActiveSlave = map[string]interface{}{
		"Level":       m.Level + 1,
		"superior_id": m.ID,
	}
	if tx := base.NodeInfoDB.Where(conditionActiveSlave).Find(&slaveNodes); tx.Error != nil {
		return nil, tx.Error
	}
	return &slaveNodes, nil
}

func GetNodeInfo(id uint64) (*NodeInfo, error) {
	var record NodeInfo
	if tx := base.NodeInfoDB.Take(&record, id); tx.Error != nil {
		return nil, tx.Error
	}
	return &record, nil
}

func (m *NodeInfo) GetAllActiveSlaveNodes() (*[]NodeInfo, error) {
	nodes, err := m.GetAllSlaveNodes()
	if err != nil {
		return nil, err
	}
	results := make([]NodeInfo, 0)
	for _, n := range *nodes {
		if n.IsActive != 0 {
			continue
		}
		results = append(results, n)
	}
	return &results, nil
}

// AddSlaveNode 添加从节点信息到数据库。
// 从节点的上级节点为当前节点。
// 从节点的 Level 为当前节点 + 1。
// 从节点的 Turn 为当前所有节点最大 Turn + 1。如果没有从节点，则默认为 1。
func (m *NodeInfo) AddSlaveNode(n *NodeInfo) (bool, error) {
	n.SuperiorID = m.ID
	n.Level = m.Level + 1
	if tx := base.NodeInfoDB.Create(n); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func (m *NodeInfo) CommitSelfAsMasterNode() (bool, error) {
	if tx := base.NodeInfoDB.Create(m); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func (m *NodeInfo) GetNodeBySocket() (*NodeInfo, error) {
	var node NodeInfo
	if tx := base.NodeInfoDB.Scopes(base.Socket(m.Host, m.Port)).Take(&node); tx.Error != nil {
		return nil, tx.Error
	}
	return &node, nil
}

var ErrMasterNodeIsNotSuperior = errors.New("the specified master node is not my superior")
var ErrSlaveNodeIsNotSubordinate = errors.New("the specified slave node is not my subordinate")

func (m *NodeInfo) IsSuperior(master *NodeInfo) bool {
	return m != nil && m.Level > 0 && master != nil && m.Level == master.Level+1 && m.SuperiorID == master.ID
}

func (m *NodeInfo) IsSubordinate(slave *NodeInfo) bool {
	return m != nil && slave != nil && slave.Level > 1 && m.Level+1 == slave.Level && slave.SuperiorID == m.ID
}

// SupersedeMasterNode 主节点异常时从节点尝试接替。
//
// 此方法涉及到一系列数据库操作，需要在事务中进行。其中某次数据库操作报错，所有之前的操作都将会滚。
//
// 步骤如下：
//
// 1. 查询 master 对应的 ID、Host、Port、Level 是否与数据表内一致。如果不一致，则报错。
//
// 2. 删除 master 记录。
//
// 3. 修改自己的记录：level -=1，m.SuperiorID = master.SuperiorID，m.Turn = master.Turn。
//
// 4. 修改其它节点的 SuperiorID 为自己。
func (m *NodeInfo) SupersedeMasterNode(master *NodeInfo) error {
	return base.NodeInfoDB.Transaction(func(tx *gorm.DB) error {
		// 1. 判断提供的 master 是否与数据库对应，以及是否为我的上级。
		var realMaster NodeInfo
		tx = tx.Where(base.Socket(master.Host, master.Port)).Where("level = ?", master.Level).Take(&realMaster, master.ID)
		if tx.Error != nil {
			return tx.Error
		}
		if !m.IsSuperior(&realMaster) {
			return ErrMasterNodeIsNotSuperior
		}
		// 2. 记录上级ID和接替顺序，然后删除。
		superiorID := realMaster.SuperiorID
		turn := realMaster.Turn
		tx = tx.Delete(realMaster)
		if tx.Error != nil {
			return tx.Error
		}
		// 3. 将自己的级别提升，并尝试保存。
		m.Level -= 1
		m.SuperiorID = superiorID
		m.Turn = turn
		tx = tx.Save(m)
		if tx.Error != nil {
			return tx.Error
		}
		// 4. 修改其它节点的上级ID为自己。
		tx = tx.Model(&NodeInfo{}).Where("superior_id = ?", superiorID).Update("superior_id", m.ID)
		if tx.Error != nil {
			return tx.Error
		}
		return nil
	})
}

// HandoverMasterNode 主节点主动通知候选节点接替自己。
//
// 此方法涉及到一系列数据库操作，需要在事务中进行。其中某次数据库操作报错，所有之前的操作都将会滚。
//
// 步骤如下：
//
// 1. 查询 candidate 对应的 ID、Host、Port、Level 是否与数据表内一致。如果不一致，则报错。
//
// 2. 删除 master 记录。
//
// 3. 修改 candidate 的记录：level -=1，candidate.SuperiorID = master.SuperiorID，candidate.Turn = master.Turn。
//
// 4. 修改其它节点的 SuperiorID 为自己。
func (m *NodeInfo) HandoverMasterNode(candidate *NodeInfo) error {
	return base.NodeInfoDB.Transaction(func(tx *gorm.DB) error {
		// 1. 判断提供的 candidate 是否与数据库对应，以及是否为我的下级。
		var realSlave NodeInfo
		tx = tx.Where(base.Socket(candidate.Host, candidate.Port)).Where("level = ?", candidate.Level).Take(&realSlave, candidate.ID)
		if tx.Error != nil {
			return tx.Error
		}
		if !m.IsSubordinate(candidate) {
			return ErrSlaveNodeIsNotSubordinate
		}
		// 2. 记录自己的ID和接替顺序，然后删除。
		superiorID := m.SuperiorID
		turn := m.Turn
		tx = tx.Delete(m)
		if tx.Error != nil {
			return tx.Error
		}
		// 3. 将候选的级别提升，并尝试保存。
		realSlave.Level -= 1
		realSlave.SuperiorID = superiorID
		realSlave.Turn = turn
		tx = tx.Save(realSlave)
		if tx.Error != nil {
			return tx.Error
		}
		// 4. 修改其它节点的上级ID为自己。
		tx = tx.Model(&NodeInfo{}).Where("superior_id = ?", superiorID).Update("superior_id", realSlave.ID)
		if tx.Error != nil {
			return tx.Error
		}
		return nil
	})
}

// ErrModelInvalid 表示删除出错。
// TODO: 此为暂定名。
var ErrModelInvalid = errors.New("slave not invalid")

func (m *NodeInfo) RemoveSlaveNode(slave *NodeInfo) (bool, error) {
	if slave.Level != m.Level+1 || slave.SuperiorID != m.ID {
		return false, ErrModelInvalid
	}
	if tx := base.NodeInfoDB.Delete(&slave); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

// RemoveSelf 删除自己。
func (m *NodeInfo) RemoveSelf() (bool, error) {
	if tx := base.NodeInfoDB.Delete(m); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func InitRegisteredWithModel(n *NodeInfo) *base.RegisteredNodeInfo {
	if n == nil {
		return nil
	}
	var registered = base.RegisteredNodeInfo{
		FreshNodeInfo: base.FreshNodeInfo{
			Name:        n.Name,
			NodeVersion: n.NodeVersion,
			Host:        n.Host,
			Port:        n.Port,
		},
		ID:         n.ID,
		Level:      n.Level,
		SuperiorID: n.SuperiorID,
		Turn:       n.Turn,
		IsActive:   n.IsActive,
	}
	return &registered
}

// ---- Log ---- //

func (m *NodeInfo) LogReportActive() (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeReportActive, 0).Record()
}

func (m *NodeInfo) LogReportFreshSlaveJoined(fresh *NodeInfo) (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeFreshNodeSlaveJoined, fresh.ID).Record()
}

func (m *NodeInfo) LogReportExistedSlaveWithdrawn(existed *NodeInfo) (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeExistedNodeSlaveWithdrawn, existed.ID).Record()
}

func (m *NodeInfo) LogReportFreshMasterJoined() (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeFreshNodeMasterJoined, 0).Record()
}

func (m *NodeInfo) LogReportExistedMasterWithdrawn() (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeExistedNodeMasterWithdrawn, 0).Record()
}

func (m *NodeInfo) LogReportExistedNodeSlaveReportMasterInactive(master *NodeInfo) (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeExistedNodeSlaveReportMasterInactive, master.ID).Record()
}

func (m *NodeInfo) LogReportExistedNodeMasterReportSlaveInactive(slave *NodeInfo) (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeExistedNodeMasterReportSlaveInactive, slave.ID).Record()
}

func (m *NodeInfo) NewNodeLog(logType uint8, target uint64) *models.NodeLog {
	log := models.NodeLog{
		NodeID:       m.ID,
		Type:         logType,
		TargetNodeID: target,
	}
	return &log
}

// ---- Log ---- //

func (m *NodeInfo) ToFreshNodeInfo() *base.FreshNodeInfo {
	if m == nil {
		return nil
	}
	var fresh = base.FreshNodeInfo{
		Name:        m.Name,
		NodeVersion: m.NodeVersion,
		Host:        m.Host,
		Port:        m.Port,
	}
	return &fresh
}

func (m *NodeInfo) ToRegisteredNodeInfo() *base.RegisteredNodeInfo {
	if m == nil {
		return nil
	}
	var registered = base.RegisteredNodeInfo{
		FreshNodeInfo: *m.ToFreshNodeInfo(),
		ID:            m.ID,
		Level:         m.Level,
		SuperiorID:    m.SuperiorID,
		Turn:          m.Turn,
		IsActive:      m.IsActive,
		Retry:         0,
	}
	return &registered
}
