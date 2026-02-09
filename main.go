package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/Go-Yadro-Group-1/config"
	"github.com/Go-Yadro-Group-1/db"
	"github.com/Go-Yadro-Group-1/repository"
)

func main() {
	log.Println("=== Database Connection Testing ===")

	cfg, err := config.LoadDBConfig()
	if err != nil {
		log.Fatalf("Configuration loading error: %v", err)
	}

	database, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	defer database.Close()
	log.Println("Database connection established")

	projectRepo := repository.NewRawProjectRepository(database)
	authorRepo := repository.NewRawAuthorRepository(database)
	issueRepo := repository.NewRawIssueRepository(database)
	statusChangeRepo := repository.NewStatusChangeRepository(database)
	analyticsRepo := repository.NewAnalyticsRepository(database)

	ctx := context.Background()

	log.Println("\n=== Test 1: Working with projects (raw.project) ===")
	testProjects(ctx, projectRepo)

	log.Println("\n=== Test 2: Working with authors (raw.author) ===")
	testAuthors(ctx, authorRepo)

	log.Println("\n=== Test 3: Working with issues (raw.issue) ===")
	testIssues(ctx, issueRepo, projectRepo, authorRepo)

	log.Println("\n=== Test 4: Working with status changes (raw.status_changes) ===")
	testStatusChanges(ctx, statusChangeRepo)

	log.Println("\n=== Test 5: Working with analytics ===")
	testAnalytics(ctx, analyticsRepo)

	log.Println("\n All tests completed successfully!")
}

func testProjects(ctx context.Context, repo *repository.RawProjectRepository) {
	project := &repository.RawProject{
		ID:    9999,
		Title: "Test Project",
	}
	if err := repo.Upsert(ctx, project); err != nil {
		log.Printf("Project creation/update error: %v", err)
	} else {
		log.Println("Project created/updated")
	}

	p, err := repo.GetByID(ctx, 9999)
	if err != nil {
		log.Printf("Project retrieval error: %v", err)
	} else if p != nil {
		log.Printf("Project found: ID=%d, Title=\"%s\"", p.ID, p.Title)
	}

	projects, err := repo.GetAll(ctx)
	if err != nil {
		log.Printf("Project list retrieval error: %v", err)
	} else {
		log.Printf("Total projects in DB: %d", len(projects))
		if len(projects) > 0 {
			log.Printf("   First project: ID=%d, Title=\"%s\"", projects[0].ID, projects[0].Title)
		}
	}

	count, err := repo.Count(ctx)
	if err != nil {
		log.Printf("Project count error: %v", err)
	} else {
		log.Printf("Total project count: %d", count)
	}
}

func testAuthors(ctx context.Context, repo *repository.RawAuthorRepository) {
	author, err := repo.GetOrCreate(ctx, 8888, "Test Author")
	if err != nil {
		log.Printf("Author creation error: %v", err)
	} else {
		log.Printf("Author created/retrieved: ID=%d, Name=\"%s\"", author.ID, author.Name)
	}

	a, err := repo.GetByID(ctx, 8888)
	if err != nil {
		log.Printf("Author retrieval error: %v", err)
	} else if a != nil {
		log.Printf("Author found: ID=%d, Name=\"%s\"", a.ID, a.Name)
	}
}

func testIssues(ctx context.Context, repo *repository.RawIssueRepository, projectRepo *repository.RawProjectRepository, authorRepo *repository.RawAuthorRepository) {
	_ = projectRepo.Upsert(ctx, &repository.RawProject{ID: 9999, Title: "Test Project"})
	_, _ = authorRepo.GetOrCreate(ctx, 8888, "Test Author")

	issue := &repository.RawIssue{
		ID:          7777,
		ProjectID:   9999,
		AuthorID:    8888,
		AssigneeID:  sql.NullInt64{Int64: 8888, Valid: true},
		Key:         "TEST-7777",
		Summary:     sql.NullString{String: "Test issue summary", Valid: true},
		Description: sql.NullString{String: "Test description", Valid: true},
		Type:        sql.NullString{String: "Bug", Valid: true},
		Priority:    sql.NullString{String: "Medium", Valid: true},
		Status:      sql.NullString{String: "Open", Valid: true},
		CreatedTime: sql.NullTime{Time: time.Now(), Valid: true},
		UpdatedTime: sql.NullTime{Time: time.Now(), Valid: true},
		TimeSpent:   sql.NullInt64{Int64: 120, Valid: true},
	}

	if err := repo.Upsert(ctx, issue); err != nil {
		log.Printf("Issue creation/update error: %v", err)
	} else {
		log.Println("Issue created/updated")
	}

	i, err := repo.GetByID(ctx, 7777)
	if err != nil {
		log.Printf("Issue retrieval error: %v", err)
	} else if i != nil {
		status := "NULL"
		if i.Status.Valid {
			status = i.Status.String
		}
		log.Printf("Issue found: ID=%d, Key=\"%s\", Status=\"%s\"", i.ID, i.Key, status)
	}

	issues, err := repo.GetByProjectID(ctx, 9999)
	if err != nil {
		log.Printf("Project issues retrieval error: %v", err)
	} else {
		log.Printf("Issues in project 9999: %d", len(issues))
	}

	openCount, _ := repo.GetOpenCountByProject(ctx, 9999)
	closedCount, _ := repo.GetClosedCountByProject(ctx, 9999)
	log.Printf("Project 9999 statistics: open=%d, closed=%d", openCount, closedCount)
}

func testStatusChanges(ctx context.Context, repo *repository.StatusChangeRepository) {
	change := &repository.StatusChange{
		IssueID:    7777,
		AuthorID:   8888,
		ChangeTime: time.Now(),
		FromStatus: "Open",
		ToStatus:   "In Progress",
	}

	if err := repo.Create(ctx, change); err != nil {
		log.Printf("Status change creation error: %v", err)
	} else {
		log.Println("Status change created")
	}

	changes, err := repo.GetByIssueID(ctx, 7777)
	if err != nil {
		log.Printf("Status changes retrieval error: %v", err)
	} else {
		log.Printf("Status changes for issue 7777: %d", len(changes))
	}
}

func testAnalytics(ctx context.Context, repo *repository.AnalyticsRepository) {
	testData := map[string]interface{}{
		"bins":   []int{10, 20, 30},
		"values": []int{5, 15, 25},
	}

	if err := repo.SaveOpenTaskTime(ctx, 9999, testData); err != nil {
		log.Printf("Error saving open_task_time: %v", err)
	} else {
		log.Println("Data saved to open_task_time")
	}

	if err := repo.SaveTaskStateTime(ctx, 9999, "Open", testData); err != nil {
		log.Printf("Error saving task_state_time: %v", err)
	} else {
		log.Println("Data saved to task_state_time")
	}

	if err := repo.SaveComplexityTaskTime(ctx, 9999, testData); err != nil {
		log.Printf("Error saving complexity_task_time: %v", err)
	} else {
		log.Println("Data saved to complexity_task_time")
	}

	if err := repo.SaveTaskPriorityCount(ctx, 9999, "Open", testData); err != nil {
		log.Printf("Error saving task_priority_count: %v", err)
	} else {
		log.Println("Data saved to task_priority_count")
	}

	if err := repo.SaveActivityByTask(ctx, 9999, "Open", testData); err != nil {
		log.Printf("Error saving activity_by_task: %v", err)
	} else {
		log.Println("Data saved to activity_by_task")
	}

	openTasks, err := repo.GetOpenTaskTime(ctx, 9999)
	if err != nil {
		log.Printf("Error retrieving open_task_time: %v", err)
	} else {
		log.Printf("Records in open_task_time for project 9999: %d", len(openTasks))
	}
}
