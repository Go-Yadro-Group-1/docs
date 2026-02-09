package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Go-Yadro-Group-1/db"
)

type RawAuthor struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type RawAuthorRepository struct {
	db *db.DB
}

func NewRawAuthorRepository(db *db.DB) *RawAuthorRepository {
	return &RawAuthorRepository{db: db}
}

func (r *RawAuthorRepository) GetByID(ctx context.Context, id int) (*RawAuthor, error) {
	query := `SELECT id, name FROM raw.author WHERE id = $1`

	var a RawAuthor
	err := r.db.QueryRowContext(ctx, query, id).Scan(&a.ID, &a.Name)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get author by ID: %w", err)
	}

	return &a, nil
}

func (r *RawAuthorRepository) GetOrCreate(ctx context.Context, id int, name string) (*RawAuthor, error) {
	query := `
        INSERT INTO raw.author (id, name)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name
        RETURNING id, name
    `

	var a RawAuthor
	err := r.db.QueryRowContext(ctx, query, id, name).Scan(&a.ID, &a.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create author: %w", err)
	}

	return &a, nil
}

func (r *RawAuthorRepository) Upsert(ctx context.Context, author *RawAuthor) error {
	query := `
        INSERT INTO raw.author (id, name)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name
    `

	_, err := r.db.ExecContext(ctx, query, author.ID, author.Name)
	if err != nil {
		return fmt.Errorf("failed to upsert author: %w", err)
	}

	return nil
}
