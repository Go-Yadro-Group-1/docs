package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Go-Yadro-Group-1/config"
	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

func Connect(cfg *config.DBConfig) (*DB, error) {
	dsn := cfg.GetDSN()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	fmt.Println("Connected to PostgreSQL")
	return &DB{db}, nil
}

func (db *DB) Close() error {
	if db.DB != nil {
		return db.DB.Close()
	}
	return nil
}
