package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreshNodeInfo_Encode(t *testing.T) {
	t.Run("Empty content", func(t *testing.T) {
		node := FreshNodeInfo{}
		assert.Equal(t, "host=&name=&node_version=&port=0", node.Encode())
	})
	t.Run("Random content", func(t *testing.T) {
		node := FreshNodeInfo{
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
		var origin *FreshNodeInfo
		var target *FreshNodeInfo
		assert.True(t, origin.IsEqual(target))
	})
	t.Run("one is nil and the other is not", func(t *testing.T) {
		origin := new(FreshNodeInfo)
		var target *FreshNodeInfo
		origin.Host = "192.168.0.1"
		origin.Port = uint16(38081)
		origin.Name = "GO-RUSH-PRODUCER"
		origin.NodeVersion = "1.0.0"
		assert.False(t, origin.IsEqual(target))
	})
	t.Run("origin is equal to target", func(t *testing.T) {
		origin := new(FreshNodeInfo)
		origin.Host = "192.168.0.1"
		origin.Port = uint16(38081)
		origin.Name = "GO-RUSH-PRODUCER"
		origin.NodeVersion = "1.0.0"
		target := new(FreshNodeInfo)
		target.Host = "192.168.0.1"
		target.Port = uint16(38081)
		target.Name = "GO-RUSH-PRODUCER"
		target.NodeVersion = "1.0.0"
		assert.True(t, origin.IsEqual(target))
	})
}
