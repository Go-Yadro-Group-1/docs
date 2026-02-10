package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type ComplexityTaskTimeData struct {
	ProjectID    int       `json:"project_id"`
	CreationTime time.Time `json:"creation_time"`
	Data         any       `json:"data"`
}

type ComplexityTaskTimeStorage interface {
	SaveComplexityTaskTime(ctx context.Context, projectID int, data any) error
	GetComplexityTaskTime(ctx context.Context, projectID int) ([]ComplexityTaskTimeData, error)
}

func (r *AnalyticsRepository) SaveComplexityTaskTime(ctx context.Context, projectID int, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	query := `
		INSERT INTO analytics.complexity_task_time (id_project, creation_time, data)
		VALUES ($1, $2, $3)
	`

	_, err = r.db.ExecContext(ctx, query, projectID, time.Now(), jsonData)
	if err != nil {
		return fmt.Errorf("failed to save complexity task time: %w", err)
	}

	return nil
}

func (r *AnalyticsRepository) GetComplexityTaskTime(ctx context.Context, projectID int) ([]ComplexityTaskTimeData, error) {
	query := `
		SELECT id_project, creation_time, data
		FROM analytics.complexity_task_time
		WHERE id_project = $1
		ORDER BY creation_time DESC
	`

	rows, err := r.db.QueryContext(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get complexity task time: %w", err)
	}
	defer rows.Close()

	var results []ComplexityTaskTimeData
	for rows.Next() {
		var item ComplexityTaskTimeData
		var jsonData []byte
		if err := rows.Scan(&item.ProjectID, &item.CreationTime, &jsonData); err != nil {
			return nil, fmt.Errorf("failed to scan complexity task time: %w", err)
		}

		item.Data = jsonData
		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating complexity task time: %w", err)
	}

	return results, nil
}
