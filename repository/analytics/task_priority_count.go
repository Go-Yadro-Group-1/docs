package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type TaskPriorityCountData struct {
	ProjectID    int       `json:"project_id"`
	CreationTime time.Time `json:"creation_time"`
	State        string    `json:"state"`
	Data         any       `json:"data"`
}

type TaskPriorityCountStorage interface {
	SaveTaskPriorityCount(ctx context.Context, projectID int, state string, data any) error
	GetTaskPriorityCount(ctx context.Context, projectID int, state string) ([]TaskPriorityCountData, error)
}

func (r *AnalyticsRepository) SaveTaskPriorityCount(ctx context.Context, projectID int, state string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	query := `
		INSERT INTO analytics.task_priority_count (id_project, creation_time, state, data)
		VALUES ($1, $2, $3, $4)
	`

	_, err = r.db.ExecContext(ctx, query, projectID, time.Now(), state, jsonData)
	if err != nil {
		return fmt.Errorf("failed to save task priority count: %w", err)
	}

	return nil
}

func (r *AnalyticsRepository) GetTaskPriorityCount(ctx context.Context, projectID int, state string) ([]TaskPriorityCountData, error) {
	query := `
		SELECT id_project, creation_time, state, data
		FROM analytics.task_priority_count
		WHERE id_project = $1 AND state = $2
		ORDER BY creation_time DESC
	`

	rows, err := r.db.QueryContext(ctx, query, projectID, state)
	if err != nil {
		return nil, fmt.Errorf("failed to get task priority count: %w", err)
	}
	defer rows.Close()

	var results []TaskPriorityCountData
	for rows.Next() {
		var item TaskPriorityCountData
		var jsonData []byte
		if err := rows.Scan(&item.ProjectID, &item.CreationTime, &item.State, &jsonData); err != nil {
			return nil, fmt.Errorf("failed to scan task priority count: %w", err)
		}

		item.Data = jsonData
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task priority count: %w", err)
	}

	return results, nil
}
