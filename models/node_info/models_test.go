package models

import (
	"testing"

	mysqlConfig "github.com/rhosocial/go-rush-common/component/mysql"
	"github.com/rhosocial/go-rush-producer/models"
	NodeInfoLegacy "github.com/rhosocial/go-rush-producer/models/node_info_legacy"
	NodeLog "github.com/rhosocial/go-rush-producer/models/node_log"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestFreshNodeInfo_Encode(t *testing.T) {
	t.Run("Empty content", func(t *testing.T) {
		node := models.FreshNodeInfo{}
		assert.Equal(t, "host=&name=&node_version=&port=0", node.Encode())
	})
	t.Run("Random content", func(t *testing.T) {
		node := models.FreshNodeInfo{
			Host:        "192.168.0.1",
			Port:        uint16(38081),
			NodeVersion: "1.0.0",
			Name:        "GO-RUSH-PRODUCER",
		}
		assert.Equal(t, "host=192.168.0.1&name=GO-RUSH-PRODUCER&node_version=1.0.0&port=38081", node.Encode())
	})
}

func TestFreshNodeInfo_IsEqual(t *testing.T) {
	t.Run("nil origin and target", func(t *testing.T) {
		var origin *models.FreshNodeInfo
		var target *models.FreshNodeInfo
		assert.True(t, origin.IsEqual(target))
	})
	t.Run("one is nil and the other is not", func(t *testing.T) {
		origin := new(models.FreshNodeInfo)
		var target *models.FreshNodeInfo
		origin.Host = "192.168.0.1"
		origin.Port = uint16(38081)
		origin.Name = "GO-RUSH-PRODUCER"
		origin.NodeVersion = "1.0.0"
		assert.False(t, origin.IsEqual(target))
	})
	t.Run("origin is equal to target", func(t *testing.T) {
		origin := new(models.FreshNodeInfo)
		origin.Host = "192.168.0.1"
		origin.Port = uint16(38081)
		origin.Name = "GO-RUSH-PRODUCER"
		origin.NodeVersion = "1.0.0"
		target := new(models.FreshNodeInfo)
		target.Host = "192.168.0.1"
		target.Port = uint16(38081)
		target.Name = "GO-RUSH-PRODUCER"
		target.NodeVersion = "1.0.0"
		assert.True(t, origin.IsEqual(target))
	})
}

func TestNewNodeInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		node := NewNodeInfo("node_name_test_case", "1.0.0-test", 38081, 1)
		assert.Equal(t, "node_name_test_case", node.Name)
		assert.Equal(t, "1.0.0-test", node.NodeVersion)
		assert.Equal(t, uint16(38081), node.Port)
		assert.Equal(t, uint8(1), node.Level)
	})
}

func TestNodeInfo_Socket(t *testing.T) {
	node := NewNodeInfo("node_name_test_case", "1.0.0-test", 38081, 1)
	t.Run("empty host", func(t *testing.T) {
		assert.Equal(t, ":38081", node.Socket())
	})
	t.Run("localhost", func(t *testing.T) {
		node.Host = "127.0.0.1"
		assert.Equal(t, "127.0.0.1:38081", node.Socket())
	})
	t.Run("internal", func(t *testing.T) {
		node.Host = "192.168.0.1"
		assert.Equal(t, "192.168.0.1:38081", node.Socket())
	})

	nodeIPv6 := NewNodeInfo("node_name_test_case", "1.0.0-test", 38081, 1)
	nodeIPv6.Host = "::1"
	t.Run("IPv6 loopback", func(t *testing.T) {
		assert.Equal(t, "[::1]:38081", nodeIPv6.Socket())
	})
}

func TestNodeInfo_IsSocketEqual(t *testing.T) {
	node := NewNodeInfo("node_name_test_case", "1.0.0-test", 38081, 1)
	node.Host = "127.0.0.1"
	targetLoopback1 := NewNodeInfo("node_name_test_case_1", "1.0.0-test", 38081, 1)
	targetLoopback1.Host = "127.0.0.1"
	targetLoopback2 := NewNodeInfo("node_name_test_case_2", "1.0.0-test", 38082, 1)
	targetLoopback2.Host = "127.0.0.1"
	targetLoopback3 := NewNodeInfo("node_name_test_case_2", "1.0.0-test", 38082, 1)
	targetLoopback3.Host = "127.0.0.2"

	t.Run("loopback equal 1", func(t *testing.T) {
		assert.True(t, node.IsSocketEqual(targetLoopback1))
	})
	t.Run("loopback equal 2", func(t *testing.T) {
		assert.True(t, targetLoopback2.IsSocketEqual(targetLoopback3))
	})
	t.Run("loopback unequal 1", func(t *testing.T) {
		assert.False(t, node.IsSocketEqual(targetLoopback2))
	})
	t.Run("loopback unequal 2", func(t *testing.T) {
		assert.False(t, targetLoopback1.IsSocketEqual(targetLoopback3))
	})

	target3 := NewNodeInfo("node_name_test_case_3", "1.0.0-test", 38082, 1)
	target3.Host = "192.168.0.1"
	target4 := NewNodeInfo("node_name_test_case_4", "1.0.0-test", 38082, 1)
	target4.Host = "192.168.0.2"

	t.Run("internal unequal", func(t *testing.T) {
		assert.False(t, target3.IsSocketEqual(target4))
	})

	t.Run("nil node info", func(t *testing.T) {
		var nodeNil *NodeInfo
		assert.False(t, nodeNil.IsSocketEqual(node))
		assert.False(t, node.IsSocketEqual(nodeNil))
	})

	nodeIPv6 := NewNodeInfo("node_name_test_case", "1.0.0-test", 38081, 1)
	nodeIPv6.Host = "::1"

	t.Run("loopback equal 3", func(t *testing.T) {
		assert.True(t, nodeIPv6.IsSocketEqual(node))
		assert.True(t, nodeIPv6.IsSocketEqual(targetLoopback1))
	})
}

func teardownNodeInfo(t *testing.T) {
	if models.NodeInfoDB != nil {
		if err := models.NodeInfoDB.Rollback().Error; err != nil {
			t.Fatalf(err.Error())
		}
	}
}

func setupGorm(t *testing.T) {
	var config = mysqlConfig.EnvMySQLServer{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "12345678",
		DB:       "go-rush-producer",
		Charset:  "utf8mb4",
		Location: "Local",
	}
	db, err := gorm.Open(mysql.Open(config.GetDSN()), &gorm.Config{})
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	models.NodeInfoDB = db.Begin()
	//if err := models.NodeInfoDB.SavePoint("origin").Error; err != nil {
	//	t.Fatalf(err.Error())
	//	return
	//}
}

var root *NodeInfo
var sub1 *NodeInfo
var sub2 *NodeInfo
var subN *NodeInfo

func prepareNodeInfo(t *testing.T) {
	tx := models.NodeInfoDB

	// root
	root = NewNodeInfo("root", "1.0.0", 38081, 0)
	root.Host = "127.0.0.1"
	if err := tx.Create(&root).Error; err != nil {
		t.Fatalf(err.Error())
		return
	}
	assert.Greater(t, root.ID, uint64(0))

	// sub1 is subordinate of root
	sub1 = NewNodeInfo("sub1", "1.0.0", 38082, 1)
	sub1.Host = "127.0.0.1"
	sub1.SuperiorID = root.ID
	sub1.Turn = 1
	if err := tx.Create(&sub1).Error; err != nil {
		t.Fatalf(err.Error())
		return
	}
	assert.Greater(t, sub1.ID, root.ID)

	// sub2 is subordinate of root
	sub2 = NewNodeInfo("sub2", "1.0.0", 38083, 1)
	sub2.Host = "127.0.0.1"
	sub2.SuperiorID = root.ID
	sub2.Turn = sub1.Turn + 1
	if err := tx.Create(&sub2).Error; err != nil {
		t.Fatalf(err.Error())
	}
	assert.Greater(t, sub2.ID, sub1.ID)

	// subN is not subordinate of root
	subN = NewNodeInfo("subN", "1.0.0", 38084, 1)
	subN.Host = "127.0.0.1"
	subN.SuperiorID = 0
	subN.Turn = sub1.Turn + 1
	if err := tx.Create(&subN).Error; err != nil {
		t.Fatalf(err.Error())
		return
	}
	assert.Greater(t, subN.ID, sub1.ID)
}

func TestNodeInfo_GetAllSlaveNodes(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	t.Run("normal case", func(t *testing.T) {
		nodes, err := root.GetAllSlaveNodes()
		if err != nil {
			t.Fatalf(err.Error())
			return
		}
		assert.NotNil(t, nodes)
		assert.Len(t, *nodes, 2)
		assert.Equal(t, "sub1", (*nodes)[0].Name)
		assert.Equal(t, "sub2", (*nodes)[1].Name)
	})
}

func TestNodeInfo_GetSuperiorNode(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	t.Run("root is the superior of sub1", func(t *testing.T) {
		node, err := sub1.GetSuperiorNode(true)
		if err != nil {
			t.Fatalf(err.Error())
			return
		}
		assert.NotNil(t, node)
		assert.Equal(t, root.Name, node.Name)
		assert.Equal(t, root.ID, node.ID)
		assert.Equal(t, root.ID, sub1.SuperiorID)
	})
	t.Run("root is not the superior of subN", func(t *testing.T) {
		_, err := subN.GetSuperiorNode(true)
		assert.ErrorIs(t, ErrNodeSuperiorNotExist, err)
	})
}

func TestNodeInfo_IsSubordinate(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	t.Run("sub1 is the subordinate of root", func(t *testing.T) {
		assert.True(t, root.IsSubordinate(sub1))
	})
	t.Run("sub2 is the subordinate of root", func(t *testing.T) {
		assert.True(t, root.IsSubordinate(sub2))
	})
	t.Run("subN is not he subordinate of root", func(t *testing.T) {
		assert.False(t, root.IsSubordinate(subN))
	})
}

func TestNodeInfo_IsSuperior(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	t.Run("root is the superior of sub1", func(t *testing.T) {
		assert.True(t, sub1.IsSuperior(root))
	})
	t.Run("root is not the superior of subN", func(t *testing.T) {
		assert.False(t, subN.IsSuperior(root))
	})
}

func TestNodeInfo_RemoveSelf(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	tx := models.NodeInfoDB

	t.Run("normal case", func(t *testing.T) {
		var legacy NodeInfoLegacy.NodeInfoLegacy
		assert.ErrorIs(t, tx.Model(&NodeInfoLegacy.NodeInfoLegacy{}).Where("id = ?", sub1.ID).Take(&legacy).Error, gorm.ErrRecordNotFound)

		result, err := sub1.RemoveSelf()
		assert.True(t, result)
		assert.Nil(t, err)

		tx.Model(&NodeInfoLegacy.NodeInfoLegacy{}).Where("id = ?", sub1.ID).Take(&legacy)
		assert.Equal(t, sub1.ID, legacy.ID)
	})
}

func TestNodeInfo_LogReportActive(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	tx := models.NodeInfoDB

	t.Run("normal case", func(t *testing.T) {
		var log NodeLog.NodeLog
		assert.ErrorIs(t, tx.Model(&NodeLog.NodeLog{}).Where("node_id = ?", sub1.ID).Take(&log).Error, gorm.ErrRecordNotFound)

		active, err := sub1.LogReportActive()
		assert.Equal(t, int64(1), active)
		assert.Nil(t, err)

		tx.Model(&NodeLog.NodeLog{}).Where("node_id = ?", sub1.ID).Take(&log)
		assert.Equal(t, int64(1), log.Version.Int64)

		active, err = sub1.LogReportActive()
		tx.Model(&NodeLog.NodeLog{}).Where("node_id = ?", sub1.ID).Take(&log)
		assert.Equal(t, int64(2), log.Version.Int64)
	})
}

func TestNodeInfo_RemoveSlaveNode(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	tx := models.NodeInfoDB

	t.Run("normal case", func(t *testing.T) {
		var legacy NodeInfoLegacy.NodeInfoLegacy
		assert.ErrorIs(t, tx.Model(&NodeInfoLegacy.NodeInfoLegacy{}).Where("id = ?", sub1.ID).Take(&legacy).Error, gorm.ErrRecordNotFound)

		result, err := root.RemoveSlaveNode(sub1)
		assert.True(t, result)
		assert.Nil(t, err)

		tx.Model(&NodeInfoLegacy.NodeInfoLegacy{}).Where("id = ?", sub1.ID).Take(&legacy)
		assert.Equal(t, sub1.ID, legacy.ID)
	})
}

func TestNodeInfo_Refresh(t *testing.T) {
	setupGorm(t)
	prepareNodeInfo(t)
	defer teardownNodeInfo(t)

	tx := models.NodeInfoDB

	t.Run("change the name of sub1", func(t *testing.T) {
		newName := sub1.Name + sub1.NodeVersion
		if err := tx.Model(&NodeInfo{}).Where("id = ?", sub1.ID).Update("name", newName).Error; err != nil {
			t.Fatalf(err.Error())
			return
		}

		assert.NotEqual(t, newName, sub1.Name)
		sub1.Refresh()
		assert.Equal(t, newName, sub1.Name)
	})
}
