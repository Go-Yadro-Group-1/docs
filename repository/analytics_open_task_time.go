package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type OpenTaskTimeData struct {
	ProjectID    int       `json:"project_id"`
	CreationTime time.Time `json:"creation_time"`
	Data         any       `json:"data"`
}

type OpenTaskTimeStorage interface {
	SaveOpenTaskTime(ctx context.Context, projectID int, data any) error
	GetOpenTaskTime(ctx context.Context, projectID int) ([]OpenTaskTimeData, error)
}

func (r *AnalyticsRepository) SaveOpenTaskTime(ctx context.Context, projectID int, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	query := `
		INSERT INTO analytics.open_task_time (id_project, creation_time, data)
		VALUES ($1, $2, $3)
	`

	_, err = r.db.ExecContext(ctx, query, projectID, time.Now(), jsonData)
	if err != nil {
		return fmt.Errorf("failed to save open task time: %w", err)
	}

	return nil
}

func (r *AnalyticsRepository) GetOpenTaskTime(ctx context.Context, projectID int) ([]OpenTaskTimeData, error) {
	query := `
		SELECT id_project, creation_time, data
		FROM analytics.open_task_time
		WHERE id_project = $1
		ORDER BY creation_time DESC
	`

	rows, err := r.db.QueryContext(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get open task time: %w", err)
	}
	defer rows.Close()

	var results []OpenTaskTimeData
	for rows.Next() {
		var item OpenTaskTimeData
		var jsonData []byte
		if err := rows.Scan(&item.ProjectID, &item.CreationTime, &jsonData); err != nil {
			return nil, fmt.Errorf("failed to scan open task time: %w", err)
		}

		item.Data = jsonData
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating open task time: %w", err)
	}

	return results, nil
}
