package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Go-Yadro-Group-1/config"
	"github.com/Go-Yadro-Group-1/db"
	"github.com/Go-Yadro-Group-1/repository/raw"
)

// generateAuthorID — простая генерация уникального ID
// В продакшене используйте: snowflake, uuid, или sequence в БД
func generateAuthorID() int {
	return int(time.Now().UnixNano() % 1_000_000_000)
}

func main() {
	// === 1. Загрузка конфигурации ===
	cfg, err := config.LoadDBConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	log.Printf("loaded config: host=%s port=%s dbname=%s", cfg.Host, cfg.Port, cfg.Name)

	// === 2. Подключение к базе данных ===
	dbConn, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	log.Println("✓ Connected to PostgreSQL")

	// === 3. Инициализация репозитория ===
	authorRepo := raw.NewRawAuthorRepository(dbConn)
	ctx := context.Background()

	// === 4. Демонстрация CRUD операций ===
	runDemo(ctx, authorRepo)

	// === 5. Graceful shutdown ===
	waitForShutdown(dbConn)
}

func runDemo(ctx context.Context, repo raw.AuthorRepository) {
	log.Println("\n=== 📝 Creating author ===")

	// Генерируем ID вручную (так как в схеме id INT, не SERIAL)
	authorID := generateAuthorID()
	newAuthor := &raw.RawAuthor{
		ID:   authorID,
		Name: "Лев Толстой",
	}

	if err := repo.Create(ctx, newAuthor); err != nil {
		log.Printf("❌ create error: %v", err)
	} else {
		log.Printf("✅ created author with ID: %d", newAuthor.ID)
	}

	log.Println("\n=== 🔍 Getting author by ID ===")
	author, err := repo.GetByID(ctx, authorID)
	if err != nil {
		log.Printf("❌ get error: %v", err)
	} else {
		log.Printf("✅ found author: ID=%d, Name=%q", author.ID, author.Name)
	}

	log.Println("\n=== 🔍 Getting author by name ===")
	authorByName, err := repo.GetByName(ctx, "Лев Толстой")
	if err != nil {
		log.Printf("❌ get by name error: %v", err)
	} else {
		log.Printf("✅ found by name: ID=%d, Name=%q", authorByName.ID, authorByName.Name)
	}

	log.Println("\n=== 📋 Listing authors ===")
	filter := &raw.AuthorFilter{
		Limit:  10,
		Offset: 0,
		Search: nil,
	}
	authors, err := repo.List(ctx, filter)
	if err != nil {
		log.Printf("❌ list error: %v", err)
	} else {
		log.Printf("✅ found %d authors:", len(authors))
		for i, a := range authors {
			log.Printf("   %d. ID=%d, Name=%q", i+1, a.ID, a.Name)
		}
	}

	log.Println("\n=== 🔍 Listing authors with search ===")
	searchTerm := "Толстой"
	filterWithSearch := &raw.AuthorFilter{
		Limit:  10,
		Offset: 0,
		Search: &searchTerm,
	}
	authorsFiltered, err := repo.List(ctx, filterWithSearch)
	if err != nil {
		log.Printf("❌ list with search error: %v", err)
	} else {
		log.Printf("✅ found %d authors matching %q:", len(authorsFiltered), searchTerm)
		for i, a := range authorsFiltered {
			log.Printf("   %d. ID=%d, Name=%q", i+1, a.ID, a.Name)
		}
	}

	log.Println("\n=== ✏️ Updating author ===")
	newAuthor.Name = "Лев Николаевич Толстой"
	if err := repo.Update(ctx, newAuthor); err != nil {
		log.Printf("❌ update error: %v", err)
	} else {
		log.Printf("✅ author %d updated", newAuthor.ID)

		// Проверяем, что обновление применилось
		updated, err := repo.GetByID(ctx, newAuthor.ID)
		if err == nil {
			log.Printf("   verified: Name=%q", updated.Name)
		}
	}

	log.Println("\n=== ❌ Trying to create duplicate ID ===")
	duplicate := &raw.RawAuthor{
		ID:   authorID, // тот же ID
		Name: "Дубликат",
	}
	if err := repo.Create(ctx, duplicate); err != nil {
		log.Printf("✅ expected error on duplicate ID: %v", err)
	} else {
		log.Println("⚠️ warning: duplicate ID was not rejected")
	}

	log.Println("\n=== 🗑️ Deleting author ===")
	if err := repo.Delete(ctx, authorID); err != nil {
		log.Printf("❌ delete error: %v", err)
	} else {
		log.Printf("✅ author %d deleted", authorID)
	}

	log.Println("\n=== 🔍 Verifying deletion ===")
	_, err = repo.GetByID(ctx, authorID)
	if err != nil {
		log.Printf("✅ confirmed: author not found after delete: %v", err)
	} else {
		log.Println("⚠️ warning: author still exists after delete")
	}
}

func waitForShutdown(dbConn *db.DB) {
	log.Println("\n=== 🛑 Waiting for shutdown signal ===")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Ждём сигнал
	sig := <-sigCh
	log.Printf("received signal: %v", sig)

	// Создаём контекст с таймаутом для graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Закрываем соединение с БД
	if err := dbConn.Close(); err != nil {
		log.Printf("⚠️ error closing database connection: %v", err)
	} else {
		log.Println("✅ database connection closed")
	}

	// Ждём завершения контекста или таймаута
	<-ctx.Done()
	log.Println("👋 shutdown complete")
}
