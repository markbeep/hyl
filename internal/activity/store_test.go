package activity

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/media"
)

// newStoreForTest opens a migrated temporary database with one user, which the
// ingest tests own without touching the filesystem beyond the temp dir.
func newStoreForTest(t *testing.T) (*Store, *db.Queries, int64) {
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
		Username: "ingestuser", Email: "ingest@example.com", DisplayName: "ingest",
		CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	return NewStore(pool, zap.NewNop()), queries, user.ID
}

// ingestedFIT builds a payload our FIT reader accepts, so the test exercises the
// real parse-and-store path.
func ingestedFIT(t *testing.T, start time.Time, distance float64) []byte {
	t.Helper()
	stored := db.Activity{
		Sport: SportRide, StartedAt: start.Unix(), ElapsedTimeS: 600, MovingTimeS: 600, DistanceM: distance,
	}
	var points []Point
	for i := range 60 {
		lat := 52.0 + float64(i)*0.0002
		lon := 5.0
		dist := distance * float64(i) / 60
		speed := distance / 600
		points = append(points, Point{
			Seq: int64(i), T: start.Add(time.Duration(i*10) * time.Second).Unix(), ElapsedS: int64(i * 10),
			Lat: &lat, Lon: &lon, DistM: &dist, Spd: &speed,
		})
	}
	var buffer bytes.Buffer
	if err := WriteFIT(&buffer, stored, points); err != nil {
		t.Fatalf("WriteFIT: %v", err)
	}
	return buffer.Bytes()
}

// TestIngestCallsExportQueueOnce pins the contract every ingestion path relies
// on: the hook fires exactly once, for the stored activity, and not for a
// duplicate the guards refused.
func TestTruncateStringKeepsValidUTF8(t *testing.T) {
	// The 120-byte title limit lands inside this emoji when it follows 119 ASCII
	// bytes, which used to store the rune's leading bytes and corrupt the value.
	title := strings.Repeat("a", 119) + "🚴 more"
	cut := truncateString(title, 120)
	if !utf8.ValidString(cut) {
		t.Fatalf("truncateString produced invalid UTF-8: %q", cut)
	}
	if len(cut) > 120 {
		t.Fatalf("truncated value is %d bytes, want at most 120", len(cut))
	}
	if !strings.HasPrefix(title, cut) {
		t.Fatalf("truncated value %q is not a prefix of the input", cut)
	}

	// A value that fits is returned untouched.
	if got := truncateString("short", 120); got != "short" {
		t.Fatalf("short value changed: %q", got)
	}
	// An all-ASCII value is cut exactly at the limit.
	if got := truncateString(strings.Repeat("b", 200), 120); len(got) != 120 {
		t.Fatalf("ascii cut to %d bytes, want 120", len(got))
	}
}

func TestOrderChronologicallySortsSamplesAndStart(t *testing.T) {
	base := time.Date(2026, 3, 1, 7, 0, 0, 0, time.UTC)
	// A GPX can hold several track segments, and they are not guaranteed to be
	// in order; the second sample here precedes the first.
	parsed := ParsedActivity{
		StartedAt: base.Add(20 * time.Second),
		Samples: []Sample{
			{T: base.Add(20 * time.Second)},
			{T: base},
			{T: base.Add(10 * time.Second)},
		},
	}
	got := orderChronologically(parsed)
	for i := 1; i < len(got.Samples); i++ {
		if got.Samples[i].T.Before(got.Samples[i-1].T) {
			t.Fatalf("samples are not ascending at %d: %v then %v", i, got.Samples[i-1].T, got.Samples[i].T)
		}
	}
	if !got.StartedAt.Equal(base) {
		t.Fatalf("start = %v, want the earliest sample %v", got.StartedAt, base)
	}
	points := SamplePoints(got.StartedAt, got.Samples)
	for _, point := range points {
		if point.ElapsedS < 0 {
			t.Fatalf("negative elapsed time %d at seq %d", point.ElapsedS, point.Seq)
		}
	}
}

func TestIngestCallsExportQueueOnce(t *testing.T) {
	store, _, userID := newStoreForTest(t)
	ctx := context.Background()
	start := time.Date(2026, 4, 1, 7, 30, 0, 0, time.UTC)
	payload := ingestedFIT(t, start, 30000)

	var calls []db.Activity
	store.ExportQueue = func(_ context.Context, a db.Activity) error {
		calls = append(calls, a)
		return nil
	}

	stored, err := store.Ingest(ctx, userID, bytes.NewReader(payload), IngestOptions{})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("hook called %d times, want 1", len(calls))
	}
	if calls[0].ID != stored.ID || calls[0].UserID != userID {
		t.Fatalf("hook saw %+v, want activity %d of user %d", calls[0], stored.ID, userID)
	}

	// The second upload is a duplicate: it is refused before the insert, so the
	// hook must not fire a second time.
	if _, err := store.Ingest(ctx, userID, bytes.NewReader(payload), IngestOptions{}); err == nil {
		t.Fatal("a duplicate ingest succeeded")
	}
	if len(calls) != 1 {
		t.Fatalf("hook called %d times after a duplicate, want 1", len(calls))
	}
}

// TestIngestSurvivesExportQueueProblems proves an automatic export can never
// break an upload: no hook and a failing hook both still store the activity.
func TestIngestSurvivesExportQueueProblems(t *testing.T) {
	store, queries, userID := newStoreForTest(t)
	ctx := context.Background()
	start := time.Date(2026, 4, 2, 7, 30, 0, 0, time.UTC)

	if _, err := store.Ingest(ctx, userID, bytes.NewReader(ingestedFIT(t, start, 5000)), IngestOptions{}); err != nil {
		t.Fatalf("Ingest with a nil hook: %v", err)
	}

	store.ExportQueue = func(context.Context, db.Activity) error {
		return errors.New("strava is unreachable")
	}
	stored, err := store.Ingest(ctx, userID, bytes.NewReader(ingestedFIT(t, start.Add(time.Hour), 6000)), IngestOptions{})
	if err != nil {
		t.Fatalf("Ingest with a failing hook: %v", err)
	}
	if _, err := queries.GetActivity(ctx, stored.ID); err != nil {
		t.Fatalf("the activity was not stored: %v", err)
	}
}

func TestDeleteOwnedActivityPreventsReimport(t *testing.T) {
	store, queries, userID := newStoreForTest(t)
	ctx := context.Background()
	payload := ingestedFIT(t, time.Date(2026, 5, 1, 7, 0, 0, 0, time.UTC), 5000)
	stored, err := store.Ingest(ctx, userID, bytes.NewReader(payload), IngestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	photo, err := queries.CreateMedia(ctx, db.CreateMediaParams{
		UserID: userID, ActivityID: &stored.ID, Kind: "activity", CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cleaned []int64
	store.RemovePhotos = func(_ context.Context, photos []db.Medium) error {
		if _, err := queries.GetActivity(ctx, stored.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("callback ran before commit: %v", err)
		}
		for _, p := range photos {
			cleaned = append(cleaned, p.ID)
		}
		return nil
	}
	if err := store.Delete(ctx, userID, stored.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(cleaned) != 1 || cleaned[0] != photo.ID {
		t.Fatalf("cleaned media IDs = %v, want [%d]", cleaned, photo.ID)
	}
	if _, err := queries.GetActivity(ctx, stored.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted activity still exists: %v", err)
	}
	_, err = store.Ingest(ctx, userID, bytes.NewReader(payload), IngestOptions{})
	var duplicate *DuplicateError
	if !errors.As(err, &duplicate) || duplicate.ActivityID != 0 {
		t.Fatalf("reimport error = %v, want deleted duplicate", err)
	}
}

func TestDeleteRemovesCommittedPhotoFiles(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(ingestedFIT(t,
		time.Date(2026, 5, 6, 7, 0, 0, 0, time.UTC), 5000)), IngestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	photo, err := queries.CreateMedia(ctx, db.CreateMediaParams{
		UserID: ownerID, ActivityID: &stored.ID, Kind: media.KindActivity, CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := media.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.RemovePhotos = media.NewHandlers(svc, store.Pool, store.Log).RemovePhotos
	for _, variant := range []string{media.VariantFull, media.VariantThumb} {
		path := svc.Path(media.KindActivity, photo.ID, variant)
		if err := os.WriteFile(path, []byte("photo"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Delete(ctx, ownerID, stored.ID); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{media.VariantFull, media.VariantThumb} {
		if _, err := os.Stat(svc.Path(media.KindActivity, photo.ID, variant)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("photo %s survived delete: %v", variant, err)
		}
	}
}

func TestDeleteRejectsMissingAndForeignActivities(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(ingestedFIT(t,
		time.Date(2026, 5, 2, 7, 0, 0, 0, time.UTC), 5000)), IngestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: "other", Email: "other@example.com", DisplayName: "Other",
		CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	store.RemovePhotos = func(context.Context, []db.Medium) error {
		called = true
		return nil
	}
	var response *apperr.Error
	if err := store.Delete(ctx, other.ID, stored.ID); !errors.As(err, &response) || response.Code != apperr.CodeForbidden {
		t.Fatalf("foreign delete error = %v, want forbidden", err)
	}
	if called {
		t.Fatal("photo cleanup ran for foreign activity")
	}
	if _, err := queries.GetActivity(ctx, stored.ID); err != nil {
		t.Fatalf("foreign activity removed: %v", err)
	}
	if _, err := queries.GetTombstone(ctx, ownerID, stored.DedupeHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign delete wrote tombstone: %v", err)
	}
	if err := store.Delete(ctx, ownerID, stored.ID+999); !errors.As(err, &response) || response.Code != apperr.CodeNotFound {
		t.Fatalf("missing delete error = %v, want not-found", err)
	}
}

func TestFailedDeleteKeepsActivityAndPhotos(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	payload := ingestedFIT(t, time.Date(2026, 5, 3, 7, 0, 0, 0, time.UTC), 5000)
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(payload), IngestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	photo, err := queries.CreateMedia(ctx, db.CreateMediaParams{
		UserID: ownerID, ActivityID: &stored.ID, Kind: "activity", CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	store.RemovePhotos = func(context.Context, []db.Medium) error {
		called = true
		return nil
	}
	if _, err := store.Pool.ExecContext(ctx, `CREATE TRIGGER prevent_delete BEFORE DELETE ON activities BEGIN SELECT RAISE(ABORT, 'delete failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, ownerID, stored.ID); err == nil {
		t.Fatal("delete succeeded despite failed row removal")
	}
	if called {
		t.Fatal("photo cleanup ran after rollback")
	}
	if _, err := queries.GetActivity(ctx, stored.ID); err != nil {
		t.Fatalf("activity missing after rollback: %v", err)
	}
	if _, err := queries.GetMedia(ctx, photo.ID); err != nil {
		t.Fatalf("photo row missing after rollback: %v", err)
	}
	if _, err := queries.GetTombstone(ctx, ownerID, stored.DedupeHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rollback left tombstone: %v", err)
	}
}

func TestDeleteSurvivesPhotoCleanupProblems(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	for i, callback := range []func(context.Context, []db.Medium) error{
		nil,
		func(context.Context, []db.Medium) error { return errors.New("disk unavailable") },
	} {
		stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(ingestedFIT(t,
			time.Date(2026, 5, 4+i, 7, 0, 0, 0, time.UTC), 5000)), IngestOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := queries.CreateMedia(ctx, db.CreateMediaParams{
			UserID: ownerID, ActivityID: &stored.ID, Kind: "activity", CreatedAt: 1,
		}); err != nil {
			t.Fatal(err)
		}
		store.RemovePhotos = callback
		if err := store.Delete(ctx, ownerID, stored.ID); err != nil {
			t.Fatalf("Delete with callback %d: %v", i, err)
		}
		if _, err := queries.GetActivity(ctx, stored.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("activity survived callback %d: %v", i, err)
		}
	}
}

func TestDeleteProviderActivityRemovesMatchingActivityAndPhotos(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	payload := ingestedFIT(t, time.Date(2026, 7, 1, 7, 0, 0, 0, time.UTC), 5000)
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(payload), IngestOptions{
		Source: "intervals_oauth", SourceRef: "provider-42",
	})
	if err != nil {
		t.Fatal(err)
	}
	photo, err := queries.CreateMedia(ctx, db.CreateMediaParams{
		UserID: ownerID, ActivityID: &stored.ID, Kind: media.KindActivity, CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := media.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.RemovePhotos = media.NewHandlers(svc, store.Pool, store.Log).RemovePhotos
	for _, variant := range []string{media.VariantFull, media.VariantThumb} {
		if err := os.WriteFile(svc.Path(media.KindActivity, photo.ID, variant), []byte("photo"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteProviderActivity(ctx, ownerID, "intervals_oauth", "provider-42"); err != nil {
		t.Fatalf("DeleteProviderActivity: %v", err)
	}
	if err := store.DeleteProviderActivity(ctx, ownerID, "intervals_oauth", "provider-42"); err != nil {
		t.Fatalf("repeated provider deletion: %v", err)
	}
	if _, err := queries.GetActivity(ctx, stored.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("provider activity survived: %v", err)
	}
	for _, variant := range []string{media.VariantFull, media.VariantThumb} {
		if _, err := os.Stat(svc.Path(media.KindActivity, photo.ID, variant)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("provider photo %s survived: %v", variant, err)
		}
	}
	_, err = store.Ingest(ctx, ownerID, bytes.NewReader(payload), IngestOptions{
		Source: "intervals_oauth", SourceRef: "provider-42",
	})
	var duplicate *DuplicateError
	if !errors.As(err, &duplicate) || duplicate.ActivityID != 0 {
		t.Fatalf("provider reimport error = %v, want deleted duplicate", err)
	}
}

func TestDeleteProviderActivityIgnoresUnknownIdentity(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(ingestedFIT(t,
		time.Date(2026, 7, 2, 7, 0, 0, 0, time.UTC), 5000)), IngestOptions{
		Source: "intervals_oauth", SourceRef: "provider-42",
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	store.RemovePhotos = func(context.Context, []db.Medium) error {
		called = true
		return nil
	}
	for _, identity := range []struct {
		ownerID int64
		source  string
		ref     string
	}{
		{ownerID, "", "provider-42"},
		{ownerID, "intervals_oauth", ""},
		{ownerID, "intervals_oauth", "unknown"},
		{ownerID, "intervals_apikey", "provider-42"},
		{ownerID + 999, "intervals_oauth", "provider-42"},
	} {
		if err := store.DeleteProviderActivity(ctx, identity.ownerID, identity.source, identity.ref); err != nil {
			t.Fatalf("unknown identity %+v: %v", identity, err)
		}
	}
	if _, err := queries.GetActivity(ctx, stored.ID); err != nil {
		t.Fatalf("unknown provider identity removed activity: %v", err)
	}
	if _, err := queries.GetTombstone(ctx, ownerID, stored.DedupeHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown provider identity wrote tombstone: %v", err)
	}
	if called {
		t.Fatal("photo cleanup ran for unknown provider identity")
	}
}

func TestDeleteProviderActivityRollsBackOnFailure(t *testing.T) {
	store, queries, ownerID := newStoreForTest(t)
	ctx := context.Background()
	stored, err := store.Ingest(ctx, ownerID, bytes.NewReader(ingestedFIT(t,
		time.Date(2026, 7, 3, 7, 0, 0, 0, time.UTC), 5000)), IngestOptions{
		Source: "intervals_apikey", SourceRef: "provider-99",
	})
	if err != nil {
		t.Fatal(err)
	}
	photo, err := queries.CreateMedia(ctx, db.CreateMediaParams{
		UserID: ownerID, ActivityID: &stored.ID, Kind: media.KindActivity, CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := media.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.RemovePhotos = media.NewHandlers(svc, store.Pool, store.Log).RemovePhotos
	path := svc.Path(media.KindActivity, photo.ID, media.VariantFull)
	if err := os.WriteFile(path, []byte("photo"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool.ExecContext(ctx, `CREATE TRIGGER prevent_delete BEFORE DELETE ON activities BEGIN SELECT RAISE(ABORT, 'delete failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteProviderActivity(ctx, ownerID, "intervals_apikey", "provider-99"); err == nil {
		t.Fatal("provider delete succeeded despite failed row removal")
	}
	if _, err := queries.GetActivity(ctx, stored.ID); err != nil {
		t.Fatalf("activity missing after rollback: %v", err)
	}
	if _, err := queries.GetTombstone(ctx, ownerID, stored.DedupeHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rollback left tombstone: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("photo removed after rollback: %v", err)
	}
}
