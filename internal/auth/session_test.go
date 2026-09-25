package auth

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
)

func TestExpireSessionsRemovesElapsedAndKeepsLive(t *testing.T) {
	ctx := context.Background()
	_, queries, user := openSessionDB(t)
	now := time.Now().Unix()
	expired := []byte("expired-session")
	live := []byte("live-session")
	if _, err := queries.CreateSession(ctx, user.ID, expired, now-1000, now-1, "", ""); err != nil {
		t.Fatalf("expired session: %v", err)
	}
	if _, err := queries.CreateSession(ctx, user.ID, live, now, now+3600, "", ""); err != nil {
		t.Fatalf("live session: %v", err)
	}

	if _, err := (&Service{Q: queries}).ExpireSessions(ctx); err != nil {
		t.Fatalf("expire: %v", err)
	}

	if _, err := queries.GetSessionByTokenHash(ctx, expired); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired session err = %v, want gone", err)
	}
	if _, err := queries.GetSessionByTokenHash(ctx, live); err != nil {
		t.Fatalf("live session: %v", err)
	}
}

func openSessionDB(t *testing.T) (*sql.DB, *db.Queries, db.User) {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	queries := db.New(pool)
	user, err := queries.CreateUser(context.Background(), db.CreateUserParams{
		Username: "athlete", Email: "athlete@example.com", DisplayName: "Athlete", CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	return pool, queries, user
}
