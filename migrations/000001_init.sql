-- Сначала удаляем зависимую схему analytics с каскадным удалением
DROP SCHEMA IF EXISTS analytics CASCADE;

-- Затем удаляем основную схему raw
DROP SCHEMA IF EXISTS raw CASCADE;

-- Создаём схемы
CREATE SCHEMA raw;
CREATE SCHEMA analytics;

-- Таблицы в схеме raw
CREATE TABLE raw.project (
    id INT PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE raw.author (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE raw.issue (
    id INT PRIMARY KEY,
    project_id INT NOT NULL REFERENCES raw.project(id),
    author_id INT NOT NULL REFERENCES raw.author(id),
    assignee_id INT REFERENCES raw.author(id),
    key TEXT NOT NULL,
    summary TEXT,
    description TEXT,
    type TEXT,
    priority TEXT,
    status TEXT,
    created_time TIMESTAMP,
    closed_time TIMESTAMP,
    updated_time TIMESTAMP,
    time_spent INT
);

CREATE TABLE raw.status_changes (
    issue_id INT NOT NULL REFERENCES raw.issue(id),
    author_id INT NOT NULL REFERENCES raw.author(id),
    change_time TIMESTAMP NOT NULL,
    from_status TEXT,
    to_status TEXT
);

-- Индексы для производительности
CREATE INDEX idx_issue_project_id ON raw.issue(project_id);
CREATE INDEX idx_issue_author_id ON raw.issue(author_id);
CREATE INDEX idx_issue_assignee_id ON raw.issue(assignee_id);
CREATE INDEX idx_issue_key ON raw.issue(key);
CREATE INDEX idx_issue_created_time ON raw.issue(created_time);
CREATE INDEX idx_status_changes_issue_id ON raw.status_changes(issue_id);
CREATE INDEX idx_status_changes_change_time ON raw.status_changes(change_time);

-- Таблицы в схеме analytics
CREATE TABLE analytics.open_task_time (
    id_project INT NOT NULL REFERENCES raw.project(id),
    creation_time TIMESTAMP NOT NULL,
    data JSONB NOT NULL
);

CREATE TABLE analytics.task_state_time (
    id_project INT NOT NULL REFERENCES raw.project(id),
    creation_time TIMESTAMP NOT NULL,
    data JSONB NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE analytics.complexity_task_time (
    id_project INT NOT NULL REFERENCES raw.project(id),
    creation_time TIMESTAMP NOT NULL,
    data JSONB NOT NULL
);

CREATE TABLE analytics.task_priority_count (
    id_project INT NOT NULL REFERENCES raw.project(id),
    creation_time TIMESTAMP NOT NULL,
    state TEXT NOT NULL,
    data JSONB NOT NULL
);

CREATE TABLE analytics.activity_by_task (
    id_project INT NOT NULL REFERENCES raw.project(id),
    creation_time TIMESTAMP NOT NULL,
    state TEXT NOT NULL,
    data JSONB NOT NULL
);

-- Индексы для аналитических таблиц
CREATE INDEX idx_open_task_time_project ON analytics.open_task_time(id_project);
CREATE INDEX idx_open_task_time_creation ON analytics.open_task_time(creation_time);
CREATE INDEX idx_task_state_time_project ON analytics.task_state_time(id_project);
CREATE INDEX idx_task_state_time_state ON analytics.task_state_time(state);
CREATE INDEX idx_complexity_task_time_project ON analytics.complexity_task_time(id_project);
CREATE INDEX idx_task_priority_count_project ON analytics.task_priority_count(id_project);
CREATE INDEX idx_task_priority_count_state ON analytics.task_priority_count(state);
CREATE INDEX idx_activity_by_task_project ON analytics.activity_by_task(id_project);
CREATE INDEX idx_activity_by_task_state ON analytics.activity_by_task(state);