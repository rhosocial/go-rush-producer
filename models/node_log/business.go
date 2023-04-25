package models

import base "github.com/rhosocial/go-rush-producer/models"

func (m *NodeLog) Record() (int64, error) {
	if tx := base.NodeInfoDB.Create(m); tx.Error == nil {
		return tx.RowsAffected, nil
	} else {
		return 0, tx.Error
	}
}
