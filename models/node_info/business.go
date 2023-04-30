package models

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

	base "github.com/rhosocial/go-rush-producer/models"
	models "github.com/rhosocial/go-rush-producer/models/node_log"
	"gorm.io/gorm"
)

// NewNodeInfo creates a new NodeInfo instance.
func NewNodeInfo(name string, port uint16, level uint8) *NodeInfo {
	var node = NodeInfo{
		Name:        name,
		NodeVersion: "0.0.1",
		Port:        port,
		Level:       level,
	}
	return &node
}

// IsSocketEqual determine whether the sockets of two nodes are equal.
func (m *NodeInfo) IsSocketEqual(target *NodeInfo) bool {
	return m.Host == target.Host && m.Port == target.Port
}

// GetPeerActiveNodes get all nodes at the same level as me.
// If not found, an empty array is returned without error.
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
	ip := net.ParseIP(m.Host)
	if ip != nil && strings.Contains(m.Host, ":") { // IPv6
		return fmt.Sprintf("[%s]:%d", m.Host, m.Port)
	}
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func (m *NodeInfo) Log() string {
	return fmt.Sprintf("[GO-RUSH] %10d [Version:%d] | %39s:%-5d | Superior: %10d | Level: %3d | Turn: %3d | Active: %d\n",
		m.ID, m.Version.Int64,
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

// GetAllSlaveNodes 获取当前节点的所有从节点。
func (m *NodeInfo) GetAllSlaveNodes() (*[]NodeInfo, error) {
	var slaveNodes []NodeInfo
	if tx := base.NodeInfoDB.Scopes(m.Subordinate()).Find(&slaveNodes); tx.Error != nil {
		return nil, tx.Error
	}
	return &slaveNodes, nil
}

// GetNodeInfo 根据指定ID获取NodeInfo记录。若指定ID的记录不存在，则报 gorm.ErrRecordNotFound。
func GetNodeInfo(id uint64) (*NodeInfo, error) {
	var record NodeInfo
	if tx := base.NodeInfoDB.Take(&record, id); tx.Error != nil {
		log.Println(tx.Error)
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
	return m != nil && slave != nil && slave.Level >= 1 && m.Level == slave.Level-1 && slave.SuperiorID == m.ID
}

// SupersedeMasterNode 主节点异常时从节点尝试接替。
//
// 此方法涉及到一系列数据库操作，需要在事务中进行。其中某次数据库操作报错，所有之前的操作都将会滚。
//
// 步骤如下：
//
// 1. 查询 master 对应的 ID、Host、Port、Level 是否与数据表内一致。如果不一致，则报错。
//
// 2. 记录 master 的ID、SuperiorID和turn，然后删除 master 记录
//
// 3. 修改自己的记录：level -=1，m.SuperiorID = master.SuperiorID，m.Turn = master.Turn。
//
// 4. 修改其它节点的 SuperiorID 为自己。
func (m *NodeInfo) SupersedeMasterNode(master *NodeInfo) error {
	return base.NodeInfoDB.Transaction(func(tx *gorm.DB) error {
		// 1. 判断提供的 master 是否与数据库对应，以及是否为我的上级。
		var realMaster NodeInfo
		if err := tx.Scopes(base.Socket(master.Host, master.Port)).Where("level = ?", master.Level).Take(&realMaster, master.ID).Error; err != nil {
			return err
		}
		if !m.IsSuperior(&realMaster) {
			log.Println(m)
			log.Println(realMaster)
			return ErrMasterNodeIsNotSuperior
		}
		// 2. 记录上级ID和接替顺序，然后删除。
		prevID := realMaster.ID
		superiorID := realMaster.SuperiorID
		turn := realMaster.Turn
		if err := tx.Delete(&realMaster).Error; err != nil {
			return err
		}
		// 3. 将自己的级别提升，并尝试保存。
		m.Level -= 1
		m.SuperiorID = superiorID
		m.Turn = turn
		if err := tx.Save(m).Error; err != nil {
			return tx.Error
		}
		// 4. 修改其它节点的上级ID为自己。
		if err := tx.Model(&NodeInfo{}).Where("superior_id = ?", prevID).Update("superior_id", m.ID).Error; err != nil {
			return err
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
// 1. 查询 candidate 对应的 ID、Host、Port、Level 是否与数据表内一致。如果不一致，则报 gorm.ErrRecordNotFound。如果不是自己的直接下属，则报 ErrSlaveNodeIsNotSubordinate。
//
// 2. 删除 master 记录。如果查询记录已不存在，则不会报错。
//
// 3. 修改 candidate 的记录：level -=1，candidate.SuperiorID = master.SuperiorID，candidate.Turn = master.Turn。
//
// 4. 修改其它节点的 SuperiorID 为自己。
func (m *NodeInfo) HandoverMasterNode(candidate *NodeInfo) error {
	return base.NodeInfoDB.Transaction(func(tx *gorm.DB) error {
		// 1. 判断提供的 candidate 是否与数据库对应，以及是否为我的下级。
		var realSlave NodeInfo
		if err := tx.Scopes(base.Socket(candidate.Host, candidate.Port)).Where("level = ?", candidate.Level).Take(&realSlave, candidate.ID).Error; err != nil {
			return err
		}
		if !m.IsSubordinate(candidate) {
			log.Printf("Master: [%d], Candidate: [%d]\n", m.ID, candidate.ID)
			return ErrSlaveNodeIsNotSubordinate
		}
		// 2. 记录自己的ID和接替顺序，然后删除。删除不存在的记录不会报错。
		prevID := m.ID
		superiorID := m.SuperiorID
		turn := m.Turn
		if err := tx.Delete(m).Error; err != nil {
			return err
		}
		// 3. 将候选的级别提升，并尝试保存。保存出错，则视为已经有其它主节点接替。
		//stmt := tx.Session(&gorm.Session{
		//	DryRun: true,
		//}).Model(&realSlave).Updates(map[string]interface{}{
		//	"level":       realSlave.Level - 1,
		//	"turn":        turn,
		//	"superior_id": superiorID,
		//}).Statement
		//log.Println(stmt.SQL.String())
		//log.Println(stmt.Vars)
		if err := tx.Model(&realSlave).Updates(map[string]interface{}{
			"level":       realSlave.Level - 1,
			"turn":        turn,
			"superior_id": superiorID,
		}).Error; err != nil {
			return err
		}
		//log.Println(realSlave.Log())
		// 4. 修改其它节点的上级ID为自己。
		//stmt := tx.Session(&gorm.Session{
		//	DryRun: true,
		//}).Model(&NodeInfo{}).Where("superior_id = ?", superiorID).Where("level = ?", realSlave.Level+1).Update("superior_id", realSlave.ID).Statement
		//log.Println(stmt.SQL.String())
		//log.Println(stmt.Vars)
		//var r NodeInfo
		//if err := tx.Take(&r, realSlave.ID).Error; err != nil {
		//	log.Println(err)
		//} else {
		//	log.Println(r.Log())
		//}
		if err := tx.Model(&NodeInfo{}).Where("superior_id = ?", prevID).Where("level = ?", realSlave.Level+1).Update("superior_id", realSlave.ID).Error; err != nil {
			return err
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

// ---- Log ---- //

func (m *NodeInfo) LogReportActive() (int64, error) {
	log, err := m.GetLogActiveLatest()
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return m.NewNodeLog(models.NodeLogTypeReportActive, 0).Record()
	}
	return log.VersionUp()
}

func (m *NodeInfo) LogReportExistedNodeMasterDetectedSlaveInactive(id uint64, retry uint8) (int64, error) {
	return m.NewNodeLog(models.NodeLogTypeExistedNodeMasterReportSlaveInactive, id).Record()
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
	log, err := m.GetLogSlaveReportMasterInactive(master.ID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return m.NewNodeLog(models.NodeLogTypeExistedNodeSlaveReportMasterInactive, master.ID).Record()
	}
	return log.VersionUp()
}

func (m *NodeInfo) LogReportExistedNodeMasterReportSlaveInactive(slave *NodeInfo) (int64, error) {
	log, err := m.GetLogMasterReportSlaveInactive(slave.ID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return m.NewNodeLog(models.NodeLogTypeExistedNodeMasterReportSlaveInactive, slave.ID).Record()
	}
	return log.VersionUp()
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

func (m *NodeInfo) Refresh() error {
	if err := base.NodeInfoDB.Take(m, m.ID).Error; err != nil {
		return err
	}
	return nil
}

func (m *NodeInfo) GetLogActiveLatest() (*models.NodeLog, error) {
	var log models.NodeLog
	if tx := base.NodeInfoDB.Scopes(m.LogActiveLatest()).First(&log); tx.Error != nil {
		return nil, tx.Error
	}
	return &log, nil
}

func (m *NodeInfo) GetLogSlaveReportMasterInactive(targetID uint64) (*models.NodeLog, error) {
	var log models.NodeLog
	if tx := base.NodeInfoDB.Scopes(m.LogSlaveReportMasterInactiveLatest(targetID)).First(&log); tx.Error != nil {
		return nil, tx.Error
	}
	return &log, nil
}

func (m *NodeInfo) GetLogMasterReportSlaveInactive(targetID uint64) (*models.NodeLog, error) {
	var log models.NodeLog
	if tx := base.NodeInfoDB.Scopes(m.LogMasterReportSlaveInactiveLatest(targetID)).First(&log); tx.Error != nil {
		return nil, tx.Error
	}
	return &log, nil
}
