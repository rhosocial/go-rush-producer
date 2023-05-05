package models

import (
	"context"
	"time"

	base "github.com/rhosocial/go-rush-producer/models"
)

func (m *NodeLog) Record(ctx context.Context) (int64, error) {
	if tx := base.NodeInfoDB.WithContext(ctx).Create(m); tx.Error == nil {
		return tx.RowsAffected, nil
	} else {
		return 0, tx.Error
	}
}

func (m *NodeLog) VersionUp(ctx context.Context) (int64, error) {
	if tx := base.NodeInfoDB.WithContext(ctx).Model(m).Update("updated_at", time.Now()); tx.Error == nil {
		return tx.RowsAffected, nil
	} else {
		return 0, tx.Error
	}
}
