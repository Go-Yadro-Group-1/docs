package repository

import (
	"context"
	"fmt"

	"github.com/Go-Yadro-Group-1/db"
)

type AnalyticsRepository struct {
	db *db.DB
}

func NewAnalyticsRepository(db *db.DB) *AnalyticsRepository {
	return &AnalyticsRepository{db: db}
}

type AnalyticsStorage interface {
	OpenTaskTimeStorage
	TaskStateTimeStorage
	ComplexityTaskTimeStorage
	TaskPriorityCountStorage
	ActivityByTaskStorage
	DeleteAllByProject(ctx context.Context, projectID int) error
}

func (r *AnalyticsRepository) DeleteAllByProject(ctx context.Context, projectID int) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	tables := []string{
		"analytics.open_task_time",
		"analytics.task_state_time",
		"analytics.complexity_task_time",
		"analytics.task_priority_count",
		"analytics.activity_by_task",
	}

	for _, table := range tables {
		query := `DELETE FROM ` + table + ` WHERE id_project = $1`
		_, err := tx.ExecContext(ctx, query, projectID)
		if err != nil {
			return fmt.Errorf("failed to delete from %s: %w", table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
