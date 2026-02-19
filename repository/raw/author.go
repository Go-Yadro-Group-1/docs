package raw

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Go-Yadro-Group-1/db"
)

var (
	ErrAuthorNotFound  = errors.New("author not found")
	ErrAuthorEmptyName = errors.New("author name cannot be empty")
	ErrAuthorEmptyID   = errors.New("author id cannot be zero")
)

type AuthorRepository interface {
	Create(ctx context.Context, author *RawAuthor) error
	GetByID(ctx context.Context, id int) (*RawAuthor, error)
	GetByName(ctx context.Context, name string) (*RawAuthor, error)
	List(ctx context.Context, filter *AuthorFilter) ([]*RawAuthor, error)
	Update(ctx context.Context, author *RawAuthor) error
	Delete(ctx context.Context, id int) error
}

type RawAuthor struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type AuthorFilter struct {
	Limit  int
	Offset int
	Search *string
}

type RawAuthorRepository struct {
	db *db.DB
}

func NewRawAuthorRepository(db *db.DB) *RawAuthorRepository {
	return &RawAuthorRepository{db: db}
}

func (r *RawAuthorRepository) Create(ctx context.Context, author *RawAuthor) error {
	if author == nil || author.Name == "" {
		return ErrAuthorEmptyName
	}
	if author.ID == 0 {
		return ErrAuthorEmptyID
	}

	query := `INSERT INTO raw.author (id, name) VALUES ($1, $2)`

	_, err := r.db.ExecContext(ctx, query, author.ID, author.Name)
	if err != nil {
		return fmt.Errorf("repository: create author: %w", err)
	}

	return nil
}

func (r *RawAuthorRepository) GetByID(ctx context.Context, id int) (*RawAuthor, error) {
	query := `SELECT id, name FROM raw.author WHERE id = $1`

	var author RawAuthor
	err := r.db.QueryRowContext(ctx, query, id).Scan(&author.ID, &author.Name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrAuthorNotFound
		}
		return nil, fmt.Errorf("repository: get author by id: %w", err)
	}

	return &author, nil
}

func (r *RawAuthorRepository) GetByName(ctx context.Context, name string) (*RawAuthor, error) {
	query := `SELECT id, name FROM raw.author WHERE name = $1`

	var author RawAuthor
	err := r.db.QueryRowContext(ctx, query, name).Scan(&author.ID, &author.Name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrAuthorNotFound
		}
		return nil, fmt.Errorf("repository: get author by name: %w", err)
	}

	return &author, nil
}

func (r *RawAuthorRepository) List(ctx context.Context, filter *AuthorFilter) ([]*RawAuthor, error) {
	f := AuthorFilter{Limit: 100, Offset: 0}
	if filter != nil {
		if filter.Limit > 0 {
			f.Limit = filter.Limit
		}
		f.Offset = filter.Offset
		f.Search = filter.Search
	}

	query := `SELECT id, name FROM raw.author WHERE 1=1`
	args := []interface{}{}
	argID := 1

	if f.Search != nil && *f.Search != "" {
		query += fmt.Sprintf(" AND name ILIKE $%d", argID)
		args = append(args, "%"+*f.Search+"%")
		argID++
	}

	query += fmt.Sprintf(" ORDER BY name LIMIT $%d OFFSET $%d", argID, argID+1)
	args = append(args, f.Limit, f.Offset)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("repository: list raw.author: %w", err)
	}
	defer rows.Close()

	var authors []*RawAuthor
	for rows.Next() {
		var a RawAuthor
		if err := rows.Scan(&a.ID, &a.Name); err != nil {
			return nil, fmt.Errorf("repository: scan author: %w", err)
		}
		authors = append(authors, &a)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: rows iteration: %w", err)
	}

	return authors, nil
}

func (r *RawAuthorRepository) Update(ctx context.Context, author *RawAuthor) error {
	if author == nil || author.ID == 0 || author.Name == "" {
		return fmt.Errorf("invalid author data for update")
	}

	query := `UPDATE raw.author SET name = $1 WHERE id = $2`

	result, err := r.db.ExecContext(ctx, query, author.Name, author.ID)
	if err != nil {
		return fmt.Errorf("repository: update author: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("repository: check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrAuthorNotFound
	}

	return nil
}

func (r *RawAuthorRepository) Delete(ctx context.Context, id int) error {
	query := `DELETE FROM raw.author WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("repository: delete author: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("repository: check rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrAuthorNotFound
	}

	return nil
}
