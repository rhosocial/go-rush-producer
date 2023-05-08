package models

import (
	"time"

	"github.com/rhosocial/go-rush-producer/models"
)

func (m *NodeLog) Record() (int64, error) {
	tx := models.NodeInfoDB.Create(m)
	if tx.Error == nil {
		return tx.RowsAffected, nil
	}
	return 0, tx.Error
}

func (m *NodeLog) VersionUp() (int64, error) {
	tx := models.NodeInfoDB.Model(m).Update("updated_at", time.Now())
	if tx.Error == nil {
		return tx.RowsAffected, nil
	}
	return 0, tx.Error
}
