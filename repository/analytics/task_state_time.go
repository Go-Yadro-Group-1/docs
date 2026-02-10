package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type TaskStateTimeData struct {
	ProjectID    int       `json:"project_id"`
	CreationTime time.Time `json:"creation_time"`
	Data         any       `json:"data"`
	State        string    `json:"state"`
}

type TaskStateTimeStorage interface {
	SaveTaskStateTime(ctx context.Context, projectID int, state string, data any) error
	GetTaskStateTime(ctx context.Context, projectID int, state string) ([]TaskStateTimeData, error)
}

func (r *AnalyticsRepository) SaveTaskStateTime(ctx context.Context, projectID int, state string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	query := `
		INSERT INTO analytics.task_state_time (id_project, creation_time, state, data)
		VALUES ($1, $2, $3, $4)
	`

	_, err = r.db.ExecContext(ctx, query, projectID, time.Now(), state, jsonData)
	if err != nil {
		return fmt.Errorf("failed to save task state time: %w", err)
	}

	return nil
}

func (r *AnalyticsRepository) GetTaskStateTime(ctx context.Context, projectID int, state string) ([]TaskStateTimeData, error) {
	query := `
		SELECT id_project, creation_time, data, state
		FROM analytics.task_state_time
		WHERE id_project = $1 AND state = $2
		ORDER BY creation_time DESC
	`

	rows, err := r.db.QueryContext(ctx, query, projectID, state)
	if err != nil {
		return nil, fmt.Errorf("failed to get task state time: %w", err)
	}
	defer rows.Close()

	var results []TaskStateTimeData
	for rows.Next() {
		var item TaskStateTimeData
		var jsonData []byte
		if err := rows.Scan(&item.ProjectID, &item.CreationTime, &jsonData, &item.State); err != nil {
			return nil, fmt.Errorf("failed to scan task state time: %w", err)
		}

		item.Data = jsonData
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task state time: %w", err)
	}

	return results, nil
}
