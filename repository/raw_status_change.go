package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Go-Yadro-Group-1/db"
)

type StatusChange struct {
	IssueID    int       `json:"issue_id"`
	AuthorID   int       `json:"author_id"`
	ChangeTime time.Time `json:"change_time"`
	FromStatus string    `json:"from_status"`
	ToStatus   string    `json:"to_status"`
}

type StatusChangeRepository struct {
	db *db.DB
}

func NewStatusChangeRepository(db *db.DB) *StatusChangeRepository {
	return &StatusChangeRepository{db: db}
}

func (r *StatusChangeRepository) GetByIssueID(ctx context.Context, issueID int) ([]StatusChange, error) {
	query := `
        SELECT issue_id, author_id, change_time, from_status, to_status
        FROM raw.status_changes
        WHERE issue_id = $1
        ORDER BY change_time ASC
    `

	rows, err := r.db.QueryContext(ctx, query, issueID)
	if err != nil {
		return nil, fmt.Errorf("failed to get status changes: %w", err)
	}
	defer rows.Close()

	var changes []StatusChange
	for rows.Next() {
		var c StatusChange
		if err := rows.Scan(&c.IssueID, &c.AuthorID, &c.ChangeTime, &c.FromStatus, &c.ToStatus); err != nil {
			return nil, fmt.Errorf("failed to scan status change: %w", err)
		}
		changes = append(changes, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating status changes: %w", err)
	}

	return changes, nil
}

func (r *StatusChangeRepository) GetByProjectID(ctx context.Context, projectID int) ([]StatusChange, error) {
	query := `
        SELECT sc.issue_id, sc.author_id, sc.change_time, sc.from_status, sc.to_status
        FROM raw.status_changes sc
        JOIN raw.issue i ON sc.issue_id = i.id
        WHERE i.project_id = $1
        ORDER BY sc.change_time ASC
    `

	rows, err := r.db.QueryContext(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get status changes by project: %w", err)
	}
	defer rows.Close()

	var changes []StatusChange
	for rows.Next() {
		var c StatusChange
		if err := rows.Scan(&c.IssueID, &c.AuthorID, &c.ChangeTime, &c.FromStatus, &c.ToStatus); err != nil {
			return nil, fmt.Errorf("failed to scan status change: %w", err)
		}
		changes = append(changes, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating status changes: %w", err)
	}

	return changes, nil
}

func (r *StatusChangeRepository) Create(ctx context.Context, change *StatusChange) error {
	query := `
        INSERT INTO raw.status_changes 
            (issue_id, author_id, change_time, from_status, to_status)
        VALUES ($1, $2, $3, $4, $5)
    `

	_, err := r.db.ExecContext(ctx, query,
		change.IssueID, change.AuthorID, change.ChangeTime,
		change.FromStatus, change.ToStatus,
	)

	if err != nil {
		return fmt.Errorf("failed to create status change: %w", err)
	}

	return nil
}

func (r *StatusChangeRepository) BulkInsert(ctx context.Context, changes []StatusChange) error {
	if len(changes) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
        INSERT INTO raw.status_changes 
            (issue_id, author_id, change_time, from_status, to_status)
        VALUES ($1, $2, $3, $4, $5)
    `

	for _, change := range changes {
		_, err := tx.ExecContext(ctx, query,
			change.IssueID, change.AuthorID, change.ChangeTime,
			change.FromStatus, change.ToStatus,
		)
		if err != nil {
			return fmt.Errorf("failed to insert status change: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *StatusChangeRepository) DeleteByIssueID(ctx context.Context, issueID int) error {
	query := `DELETE FROM raw.status_changes WHERE issue_id = $1`

	result, err := r.db.ExecContext(ctx, query, issueID)
	if err != nil {
		return fmt.Errorf("failed to delete status changes: %w", err)
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
