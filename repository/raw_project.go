package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Go-Yadro-Group-1/db"
)

type RawProject struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

type RawProjectRepository struct {
	db *db.DB
}

func NewRawProjectRepository(db *db.DB) *RawProjectRepository {
	return &RawProjectRepository{db: db}
}

func (r *RawProjectRepository) GetAll(ctx context.Context) ([]RawProject, error) {
	query := `SELECT id, title FROM raw.project ORDER BY id`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get projects: %w", err)
	}
	defer rows.Close()

	var projects []RawProject
	for rows.Next() {
		var p RawProject
		if err := rows.Scan(&p.ID, &p.Title); err != nil {
			return nil, fmt.Errorf("failed to scan project: %w", err)
		}
		projects = append(projects, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating projects: %w", err)
	}

	return projects, nil
}

func (r *RawProjectRepository) GetByID(ctx context.Context, id int) (*RawProject, error) {
	query := `SELECT id, title FROM raw.project WHERE id = $1`

	var p RawProject
	err := r.db.QueryRowContext(ctx, query, id).Scan(&p.ID, &p.Title)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get project by ID: %w", err)
	}

	return &p, nil
}

func (r *RawProjectRepository) Create(ctx context.Context, project *RawProject) error {
	query := `INSERT INTO raw.project (id, title) VALUES ($1, $2)`

	_, err := r.db.ExecContext(ctx, query, project.ID, project.Title)
	if err != nil {
		return fmt.Errorf("failed to create project: %w", err)
	}

	return nil
}

func (r *RawProjectRepository) Upsert(ctx context.Context, project *RawProject) error {
	query := `
        INSERT INTO raw.project (id, title)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title
    `

	_, err := r.db.ExecContext(ctx, query, project.ID, project.Title)
	if err != nil {
		return fmt.Errorf("failed to upsert project: %w", err)
	}

	return nil
}

func (r *RawProjectRepository) Delete(ctx context.Context, id int) error {
	query := `DELETE FROM raw.project WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete project: %w", err)
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

func (r *RawProjectRepository) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM raw.project`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count projects: %w", err)
	}
	return count, nil
}
