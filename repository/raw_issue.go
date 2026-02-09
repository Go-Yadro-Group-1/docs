package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Go-Yadro-Group-1/db"
)

type RawIssue struct {
	ID          int            `json:"id"`
	ProjectID   int            `json:"project_id"`
	AuthorID    int            `json:"author_id"`
	AssigneeID  sql.NullInt64  `json:"assignee_id"`
	Key         string         `json:"key"`
	Summary     sql.NullString `json:"summary"`
	Description sql.NullString `json:"description"`
	Type        sql.NullString `json:"type"`
	Priority    sql.NullString `json:"priority"`
	Status      sql.NullString `json:"status"`
	CreatedTime sql.NullTime   `json:"created_time"`
	ClosedTime  sql.NullTime   `json:"closed_time"`
	UpdatedTime sql.NullTime   `json:"updated_time"`
	TimeSpent   sql.NullInt64  `json:"time_spent"`
}

type RawIssueRepository struct {
	db *db.DB
}

func NewRawIssueRepository(db *db.DB) *RawIssueRepository {
	return &RawIssueRepository{db: db}
}

func (r *RawIssueRepository) GetAll(ctx context.Context) ([]RawIssue, error) {
	query := `
        SELECT id, project_id, author_id, assignee_id, key, summary, 
               description, type, priority, status, created_time, 
               closed_time, updated_time, time_spent
        FROM raw.issue
        ORDER BY id
    `

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get issues: %w", err)
	}
	defer rows.Close()

	var issues []RawIssue
	for rows.Next() {
		var i RawIssue
		if err := rows.Scan(
			&i.ID, &i.ProjectID, &i.AuthorID, &i.AssigneeID, &i.Key,
			&i.Summary, &i.Description, &i.Type, &i.Priority, &i.Status,
			&i.CreatedTime, &i.ClosedTime, &i.UpdatedTime, &i.TimeSpent,
		); err != nil {
			return nil, fmt.Errorf("failed to scan issue: %w", err)
		}
		issues = append(issues, i)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating issues: %w", err)
	}

	return issues, nil
}

func (r *RawIssueRepository) GetByID(ctx context.Context, id int) (*RawIssue, error) {
	query := `
        SELECT id, project_id, author_id, assignee_id, key, summary, 
               description, type, priority, status, created_time, 
               closed_time, updated_time, time_spent
        FROM raw.issue 
        WHERE id = $1
    `

	var i RawIssue
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&i.ID, &i.ProjectID, &i.AuthorID, &i.AssigneeID, &i.Key,
		&i.Summary, &i.Description, &i.Type, &i.Priority, &i.Status,
		&i.CreatedTime, &i.ClosedTime, &i.UpdatedTime, &i.TimeSpent,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get issue by ID: %w", err)
	}

	return &i, nil
}

func (r *RawIssueRepository) GetByProjectID(ctx context.Context, projectID int) ([]RawIssue, error) {
	query := `
        SELECT id, project_id, author_id, assignee_id, key, summary, 
               description, type, priority, status, created_time, 
               closed_time, updated_time, time_spent
        FROM raw.issue 
        WHERE project_id = $1
        ORDER BY created_time DESC
    `

	rows, err := r.db.QueryContext(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get issues by project: %w", err)
	}
	defer rows.Close()

	var issues []RawIssue
	for rows.Next() {
		var i RawIssue
		if err := rows.Scan(
			&i.ID, &i.ProjectID, &i.AuthorID, &i.AssigneeID, &i.Key,
			&i.Summary, &i.Description, &i.Type, &i.Priority, &i.Status,
			&i.CreatedTime, &i.ClosedTime, &i.UpdatedTime, &i.TimeSpent,
		); err != nil {
			return nil, fmt.Errorf("failed to scan issue: %w", err)
		}
		issues = append(issues, i)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating issues: %w", err)
	}

	return issues, nil
}

func (r *RawIssueRepository) Create(ctx context.Context, issue *RawIssue) error {
	query := `
        INSERT INTO raw.issue 
            (id, project_id, author_id, assignee_id, key, summary, 
             description, type, priority, status, created_time, 
             closed_time, updated_time, time_spent)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    `

	_, err := r.db.ExecContext(ctx, query,
		issue.ID, issue.ProjectID, issue.AuthorID, issue.AssigneeID,
		issue.Key, issue.Summary, issue.Description, issue.Type,
		issue.Priority, issue.Status, issue.CreatedTime, issue.ClosedTime,
		issue.UpdatedTime, issue.TimeSpent,
	)

	if err != nil {
		return fmt.Errorf("failed to create issue: %w", err)
	}

	return nil
}

func (r *RawIssueRepository) Upsert(ctx context.Context, issue *RawIssue) error {
	query := `
        INSERT INTO raw.issue 
            (id, project_id, author_id, assignee_id, key, summary, 
             description, type, priority, status, created_time, 
             closed_time, updated_time, time_spent)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (id) DO UPDATE SET
            project_id = EXCLUDED.project_id,
            author_id = EXCLUDED.author_id,
            assignee_id = EXCLUDED.assignee_id,
            key = EXCLUDED.key,
            summary = EXCLUDED.summary,
            description = EXCLUDED.description,
            type = EXCLUDED.type,
            priority = EXCLUDED.priority,
            status = EXCLUDED.status,
            created_time = EXCLUDED.created_time,
            closed_time = EXCLUDED.closed_time,
            updated_time = EXCLUDED.updated_time,
            time_spent = EXCLUDED.time_spent
    `

	_, err := r.db.ExecContext(ctx, query,
		issue.ID, issue.ProjectID, issue.AuthorID, issue.AssigneeID,
		issue.Key, issue.Summary, issue.Description, issue.Type,
		issue.Priority, issue.Status, issue.CreatedTime, issue.ClosedTime,
		issue.UpdatedTime, issue.TimeSpent,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert issue: %w", err)
	}

	return nil
}

func (r *RawIssueRepository) Delete(ctx context.Context, id int) error {
	query := `DELETE FROM raw.issue WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete issue: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *RawIssueRepository) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM raw.issue`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count issues: %w", err)
	}
	return count, nil
}

func (r *RawIssueRepository) CountByProject(ctx context.Context, projectID int) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM raw.issue WHERE project_id = $1`, projectID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count issues by project: %w", err)
	}
	return count, nil
}

func (r *RawIssueRepository) GetOpenCountByProject(ctx context.Context, projectID int) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `
        SELECT COUNT(*) 
        FROM raw.issue 
        WHERE project_id = $1 
        AND status IS DISTINCT FROM 'Closed' 
        AND status IS DISTINCT FROM 'Resolved'
    `, projectID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count open issues: %w", err)
	}
	return count, nil
}

func (r *RawIssueRepository) GetClosedCountByProject(ctx context.Context, projectID int) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `
        SELECT COUNT(*) 
        FROM raw.issue 
        WHERE project_id = $1 
        AND (status = 'Closed' OR status = 'Resolved')
    `, projectID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count closed issues: %w", err)
	}
	return count, nil
}

func (r *RawIssueRepository) GetAverageTimeByProject(ctx context.Context, projectID int) (float64, error) {
	var avgHours float64
	err := r.db.QueryRowContext(ctx, `
        SELECT AVG(EXTRACT(EPOCH FROM (closed_time - created_time)) / 3600)
        FROM raw.issue 
        WHERE project_id = $1 
        AND closed_time IS NOT NULL
        AND created_time IS NOT NULL
    `, projectID).Scan(&avgHours)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate average time: %w", err)
	}
	return avgHours, nil
}
