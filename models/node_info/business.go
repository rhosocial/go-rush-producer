package models

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"gorm.io/gorm"
)

var NodeInfoDB *gorm.DB

const (
	MinimumNumberOfActiveMasterNodes = 1
)

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
	tx := NodeInfoDB.Where(condition).Find(&nodes)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &nodes, nil
}

func (m *NodeInfo) IsMaster() bool {
	nodes, err := m.GetPeerActiveNodes()
	if err != nil {
		return false
	}
	return (*nodes)[0].Order == m.Order
}

func (m *NodeInfo) Socket() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func (m *NodeInfo) Log() string {
	return fmt.Sprintf("[GO-RUSH] %10d | %39s:%-5d | Superior: %10d | Level: %3d | Order: %3d | Active: %d\n",
		m.ID,
		m.Host, m.Port,
		m.SuperiorID, m.Level, m.Order, m.IsActive,
	)
}

var ErrNodeLevelAlreadyHighest = errors.New("it's already the highest level")
var ErrNodeSuperiorNotExist = errors.New("superior node not exist")

// GetSuperiorNode 获得当前级别的上级节点。如果要指定上级，则 specifySuperior = true。
// 如果为发现上级阶段，则不指定上级。如果为检查上级，则需要指定。
// 如果已经是最高级，则报 ErrNodeLevelAlreadyHighest。
// 如果查询数据库不存在上级节点，则报 ErrNodeSuperiorNotExist。其它数据库错误据实返回。
func (m *NodeInfo) GetSuperiorNode(specifySuperior bool) (*NodeInfo, error) {
	if m.Level == 0 {
		return nil, ErrNodeLevelAlreadyHighest
	}
	var node NodeInfo
	var condition = map[string]interface{}{
		"is_active": FieldIsActiveActive,
		"level":     m.Level - 1,
	}
	if specifySuperior {
		condition["id"] = m.SuperiorID
	}
	if tx := NodeInfoDB.Where(condition).First(&node); tx.Error == gorm.ErrRecordNotFound {
		return nil, ErrNodeSuperiorNotExist
	} else if tx.Error != nil {
		return nil, tx.Error
	}
	return &node, nil
}

func (m *NodeInfo) GetAllSlaveNodes() (*[]NodeInfo, error) {
	var slaveNodes []NodeInfo
	var conditionActiveSlave = map[string]interface{}{
		"Level":       m.Level + 1,
		"superior_id": m.ID,
	}
	tx := NodeInfoDB.Where(conditionActiveSlave).Find(&slaveNodes)
	if tx.Error != nil {
		return nil, tx.Error
	}
	return &slaveNodes, nil
}

func GetNodeInfo(id uint64) (*NodeInfo, error) {
	var record NodeInfo
	if tx := NodeInfoDB.Take(&record, id); tx.Error != nil {
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
// 从节点的 Order 为当前所有节点最大 Order + 1。如果没有从节点，则默认为 1。
func (m *NodeInfo) AddSlaveNode(node *NodeInfo) (bool, error) {
	node.SuperiorID = m.ID
	node.Level = m.Level + 1
	node.Order = 0
	if tx := NodeInfoDB.Create(node); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func (m *NodeInfo) CommitSelfAsMasterNode() (bool, error) {
	if tx := NodeInfoDB.Create(m); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func (m *NodeInfo) TakeoverMasterNode(master *NodeInfo) (bool, error) {
	if m.Level == 0 {
		return false, ErrNodeLevelAlreadyHighest
	}
	m.Level = master.Level
	m.Order = master.Order
	m.IsActive = FieldIsActiveActive
	condition := map[string]interface{}{
		"id":      master.ID,
		"version": m.Version,
	}
	if tx := NodeInfoDB.Model(m).Where(condition).Updates(map[string]interface{}{
		"level":     m.Level,
		"order":     m.Order,
		"is_active": m.IsActive,
	}); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

// ErrModelInvalid 表示删除出错。
// TODO: 此为暂定名。
var ErrModelInvalid = errors.New("slave not invalid")

func (m *NodeInfo) RemoveSlaveNode(slave *NodeInfo) (bool, error) {
	if slave.Level != m.Level+1 || slave.SuperiorID != m.ID {
		return false, ErrModelInvalid
	}
	if tx := NodeInfoDB.Delete(&NodeInfo{}, slave.ID); tx.Error != nil {
		return false, tx.Error
	}
	return true, nil
}

func (n *FreshNodeInfo) Encode() string {
	params := make(url.Values)
	params.Add("name", n.Name)
	params.Add("node_version", n.NodeVersion)
	params.Add("host", n.Host)
	params.Add("port", strconv.Itoa(int(n.Port)))
	return params.Encode()
}

func (n *FreshNodeInfo) Log() string {
	return fmt.Sprintf("Fresh Node: %39s:%-5d | %s @ %s", n.Host, n.Port, n.Name, n.NodeVersion)
}

func (n *FreshNodeInfo) IsEqual(target *FreshNodeInfo) bool {
	if n != nil && target == nil || n == nil && target != nil {
		return false
	}
	log.Println("Origin: ", n.Log())
	log.Println("Target: ", target.Log())
	return n.Name == target.Name && n.NodeVersion == target.NodeVersion && n.Host == target.Host && n.Port == target.Port
}

// Log 输出信息。
// TODO: 待补充 RegisteredNodeInfo 的 IsActive 字段友好输出。
func (n *RegisteredNodeInfo) Log() string {
	return fmt.Sprintf("Regst Node: %39s:%-5d | %s @ %s | Superior: %10d | Level: %3d | Order: %3d | Active: %d\n",
		n.Host, n.Port, n.Name, n.NodeVersion,
		n.SuperiorID, n.Level, n.Order, n.IsActive,
	)
}

func (n *RegisteredNodeInfo) Encode() string {
	params := make(url.Values)
	params.Add("name", n.Name)
	params.Add("node_version", n.NodeVersion)
	params.Add("host", n.Host)
	params.Add("port", strconv.Itoa(int(n.Port)))
	params.Add("id", strconv.FormatUint(n.ID, 10))
	params.Add("level", strconv.Itoa(int(n.Level)))
	params.Add("superior_id", strconv.FormatUint(n.SuperiorID, 10))
	params.Add("order", strconv.FormatUint(uint64(n.Order), 10))
	params.Add("is_active", strconv.Itoa(int(n.IsActive)))
	return params.Encode()
}

func InitRegisteredWithModel(node *NodeInfo) *RegisteredNodeInfo {
	if node == nil {
		return nil
	}
	var registered = RegisteredNodeInfo{
		FreshNodeInfo: FreshNodeInfo{
			Name:        node.Name,
			NodeVersion: node.NodeVersion,
			Host:        node.Host,
			Port:        node.Port,
		},
		ID:         node.ID,
		Level:      node.Level,
		SuperiorID: node.SuperiorID,
		Order:      node.Order,
		IsActive:   node.IsActive,
	}
	return &registered
}
