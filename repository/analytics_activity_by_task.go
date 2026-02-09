package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type ActivityByTaskData struct {
	ProjectID    int       `json:"project_id"`
	CreationTime time.Time `json:"creation_time"`
	State        string    `json:"state"`
	Data         any       `json:"data"`
}

type ActivityByTaskStorage interface {
	SaveActivityByTask(ctx context.Context, projectID int, state string, data any) error
	GetActivityByTask(ctx context.Context, projectID int, state string) ([]ActivityByTaskData, error)
}

func (r *AnalyticsRepository) SaveActivityByTask(ctx context.Context, projectID int, state string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	query := `
		INSERT INTO analytics.activity_by_task (id_project, creation_time, state, data)
		VALUES ($1, $2, $3, $4)
	`

	_, err = r.db.ExecContext(ctx, query, projectID, time.Now(), state, jsonData)
	if err != nil {
		return fmt.Errorf("failed to save activity by task: %w", err)
	}

	return nil
}

func (r *AnalyticsRepository) GetActivityByTask(ctx context.Context, projectID int, state string) ([]ActivityByTaskData, error) {
	query := `
		SELECT id_project, creation_time, state, data
		FROM analytics.activity_by_task
		WHERE id_project = $1 AND state = $2
		ORDER BY creation_time DESC
	`

	rows, err := r.db.QueryContext(ctx, query, projectID, state)
	if err != nil {
		return nil, fmt.Errorf("failed to get activity by task: %w", err)
	}
	defer rows.Close()

	var results []ActivityByTaskData
	for rows.Next() {
		var item ActivityByTaskData
		var jsonData []byte
		if err := rows.Scan(&item.ProjectID, &item.CreationTime, &item.State, &jsonData); err != nil {
			return nil, fmt.Errorf("failed to scan activity by task: %w", err)
		}

		item.Data = jsonData
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating activity by task: %w", err)
	}

	return results, nil
}
