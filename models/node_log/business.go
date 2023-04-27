package models

import (
	"time"

	base "github.com/rhosocial/go-rush-producer/models"
)

func (m *NodeLog) Record() (int64, error) {
	if tx := base.NodeInfoDB.Create(m); tx.Error == nil {
		return tx.RowsAffected, nil
	} else {
		return 0, tx.Error
	}
}

func (m *NodeLog) Updated() (int64, error) {
	if tx := base.NodeInfoDB.Model(m).Update("updated_at", time.Now()); tx.Error == nil {
		return tx.RowsAffected, nil
	} else {
		return 0, tx.Error
	}
}
