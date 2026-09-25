package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
)

// disconnectHarness is an account with both providers, one import rule on each,
// and a Strava export in each status. The fake records the best-effort revoke.
type disconnectHarness struct {
	connections  *Connections
	queries      *db.Queries
	pool         *sql.DB
	user         db.User
	pending      db.Activity
	sent         db.Activity
	failed       db.Activity
	revokes      int
	revokeStatus int
	cipher       *secrets.Cipher
	tokenStatus  int
	tokenBody    string
}

func newDisconnectHarness(t *testing.T) *disconnectHarness {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "hyl.db")
	pool, err := db.Open(config.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cfg := config.Config{
		BaseURL: "http://localhost:8080", Version: "test",
		SecretKey:          "0123456789abcdef0123456789abcdef",
		StravaClientID:     "client-id",
		StravaClientSecret: "client-secret",
	}
	cipher, err := secrets.New(cfg.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(pool)

	user, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: "athlete", Email: "athlete@example.com", DisplayName: "Athlete", CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	access, err := cipher.EncryptString("intervals-token")
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := cipher.EncryptString("strava-refresh")
	if err != nil {
		t.Fatal(err)
	}
	athleteID := "42"
	for _, kind := range []string{KindIntervalsOAuth, KindIntervalsAPIKey, KindStravaOAuth} {
		token := access
		var rotated []byte
		if kind == KindStravaOAuth {
			token = refresh
			rotated = refresh
		}
		if _, err := queries.UpsertConnection(ctx, db.UpsertConnectionParams{
			UserID: user.ID, Kind: kind, ExternalAthleteID: &athleteID,
			AccessTokenCipher: token, RefreshTokenCipher: rotated,
			AutoExport: false, ExportMessage: "Imported from hyl", CreatedAt: 1, UpdatedAt: 1,
		}); err != nil {
			t.Fatalf("create %s: %v", kind, err)
		}
		if err := queries.UpsertImportRule(ctx, user.ID, kind, "ride", true, 1); err != nil {
			t.Fatalf("rule %s: %v", kind, err)
		}
	}

	pending := insertActivity(t, queries, user.ID, "pending-hash")
	sent := insertActivity(t, queries, user.ID, "sent-hash")
	failed := insertActivity(t, queries, user.ID, "error-hash")
	queueExport(t, queries, pending, exportStatusPending)
	queueExport(t, queries, sent, exportStatusSent)
	queueExport(t, queries, failed, exportStatusError)

	h := &disconnectHarness{
		queries: queries, pool: pool, user: user, cipher: cipher,
		pending: pending, sent: sent, failed: failed,
		revokeStatus: http.StatusNoContent,
	}
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/oauth/token") {
			w.Header().Set("Content-Type", "application/json")
			if h.tokenStatus != 0 {
				w.WriteHeader(h.tokenStatus)
			}
			if h.tokenBody != "" {
				_, _ = w.Write([]byte(h.tokenBody))
			}
			return
		}
		h.revokes++
		w.WriteHeader(h.revokeStatus)
	}))
	t.Cleanup(fake.Close)
	cfg.StravaOAuthBase = fake.URL
	h.connections = &Connections{
		Pool: pool, Q: queries, Cfg: cfg, Log: zap.NewNop(), Cipher: cipher,
		intervalsBase: fake.URL, httpClient: fake.Client(),
	}
	return h
}

func insertActivity(t *testing.T, queries *db.Queries, userID int64, hash string) db.Activity {
	t.Helper()
	row, err := queries.CreateActivity(context.Background(), db.CreateActivityParams{
		UserID: userID, Title: hash, Sport: "ride", StartedAt: 1,
		ElapsedTimeS: 60, MovingTimeS: 60, DistanceM: 100, HasGps: false,
		Visibility: "default", Source: "manual", DedupeHash: hash, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create activity: %v", err)
	}
	return row
}

func queueExport(t *testing.T, queries *db.Queries, activity db.Activity, status string) {
	t.Helper()
	ctx := context.Background()
	if _, err := queries.CreateExport(ctx, activity.ID, activity.UserID, exportTargetStrava, activity.DedupeHash, 1, 1); err != nil {
		t.Fatalf("queue export: %v", err)
	}
	if status == exportStatusPending {
		return
	}
	row, err := queries.GetExport(ctx, activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queries.UpdateExportStatus(ctx, status, nil, 1, nil, 1, row.ID); err != nil {
		t.Fatalf("set export status: %v", err)
	}
}

func TestDisconnectIntervalsOAuthLeavesPendingStravaExport(t *testing.T) {
	h := newDisconnectHarness(t)
	ctx := context.Background()

	if err := h.connections.Disconnect(ctx, h.user.ID, KindIntervalsOAuth); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsOAuth); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("intervals connection err = %v, want gone", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	rules, err := h.queries.ListImportRules(ctx, h.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stravaRule bool
	for _, rule := range rules {
		if rule.ConnectionKind == KindIntervalsOAuth {
			t.Fatalf("intervals import rule remained: %+v", rule)
		}
		if rule.ConnectionKind == KindStravaOAuth && rule.Sport == "ride" {
			stravaRule = true
		}
	}
	if !stravaRule {
		t.Fatal("strava import rule was removed")
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
	if h.revokes != 1 {
		t.Fatalf("provider revoke calls = %d, want 1", h.revokes)
	}
}

func TestDisconnectIntervalsAPIKeyLeavesStravaExports(t *testing.T) {
	h := newDisconnectHarness(t)
	ctx := context.Background()

	if err := h.connections.Disconnect(ctx, h.user.ID, KindIntervalsAPIKey); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsAPIKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("api key connection err = %v, want gone", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsOAuth); err != nil {
		t.Fatalf("intervals oauth connection: %v", err)
	}
	rules, err := h.queries.ListImportRules(ctx, h.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	var oauthRule, stravaRule bool
	for _, rule := range rules {
		if rule.ConnectionKind == KindIntervalsAPIKey {
			t.Fatalf("api key import rule remained: %+v", rule)
		}
		if rule.ConnectionKind == KindIntervalsOAuth {
			oauthRule = true
		}
		if rule.ConnectionKind == KindStravaOAuth {
			stravaRule = true
		}
	}
	if !oauthRule || !stravaRule {
		t.Fatalf("other import rules missing: %+v", rules)
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
	if h.revokes != 0 {
		t.Fatalf("api key disconnect called the provider %d times", h.revokes)
	}
}

func TestDisconnectStravaRemovesPendingExportsAndKeepsHistory(t *testing.T) {
	h := newDisconnectHarness(t)
	ctx := context.Background()

	if err := h.connections.Disconnect(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("strava connection err = %v, want gone", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsOAuth); err != nil {
		t.Fatalf("intervals connection: %v", err)
	}
	rules, err := h.queries.ListImportRules(ctx, h.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	var intervalsRule bool
	for _, rule := range rules {
		if rule.ConnectionKind == KindStravaOAuth {
			t.Fatalf("strava import rule remained: %+v", rule)
		}
		if rule.ConnectionKind == KindIntervalsOAuth {
			intervalsRule = true
		}
	}
	if !intervalsRule {
		t.Fatal("intervals import rule was removed")
	}
	if _, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pending export err = %v, want gone", err)
	}
	sent, err := h.queries.GetExport(ctx, h.sent.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("sent export: %v", err)
	}
	if sent.Status != exportStatusSent {
		t.Fatalf("sent status = %q", sent.Status)
	}
	failed, err := h.queries.GetExport(ctx, h.failed.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("errored export: %v", err)
	}
	if failed.Status != exportStatusError {
		t.Fatalf("errored status = %q", failed.Status)
	}
	if h.revokes != 1 {
		t.Fatalf("provider revoke calls = %d, want 1", h.revokes)
	}
}

func TestDisconnectKeepsLocalRemovalWhenRevokeFails(t *testing.T) {
	h := newDisconnectHarness(t)
	h.revokeStatus = http.StatusInternalServerError
	ctx := context.Background()

	if err := h.connections.Disconnect(ctx, h.user.ID, KindIntervalsOAuth); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsOAuth); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("intervals connection err = %v, want gone", err)
	}
	if h.revokes != 1 {
		t.Fatalf("provider revoke calls = %d, want 1", h.revokes)
	}
}

func TestConfirmedStravaDeauthorizationRemovesConnectionWithoutRevoking(t *testing.T) {
	h := newDisconnectHarness(t)
	core, logs := observer.New(zapcore.InfoLevel)
	h.connections.Log = zap.New(core)
	h.tokenStatus = http.StatusBadRequest
	h.tokenBody = `{"message":"Bad Request","errors":[{"resource":"RefreshToken","field":"refresh_token","code":"invalid"}]}`
	ctx := context.Background()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Deauthorize(ctx, conn); err != nil {
		t.Fatalf("deauthorize: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("strava connection err = %v, want gone", err)
	}
	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindIntervalsOAuth); err != nil {
		t.Fatalf("intervals connection: %v", err)
	}
	rules, err := h.queries.ListImportRules(ctx, h.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	var intervalsRule bool
	for _, rule := range rules {
		if rule.ConnectionKind == KindStravaOAuth {
			t.Fatalf("strava import rule remained: %+v", rule)
		}
		if rule.ConnectionKind == KindIntervalsOAuth {
			intervalsRule = true
		}
	}
	if !intervalsRule {
		t.Fatal("intervals import rule was removed")
	}
	if _, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pending export err = %v, want gone", err)
	}
	sent, err := h.queries.GetExport(ctx, h.sent.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("sent export: %v", err)
	}
	if sent.Status != exportStatusSent {
		t.Fatalf("sent status = %q", sent.Status)
	}
	failed, err := h.queries.GetExport(ctx, h.failed.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("errored export: %v", err)
	}
	if failed.Status != exportStatusError {
		t.Fatalf("errored status = %q", failed.Status)
	}
	if h.revokes != 0 {
		t.Fatalf("provider revoke calls = %d, want the deauthorization not to revoke again", h.revokes)
	}
	if logs.FilterMessage("strava connection removed after deauthorization").Len() != 1 {
		t.Fatal("want the confirmed removal logged")
	}
}

func TestForgedStravaDeauthorizationKeepsConnectionAndStoresRotation(t *testing.T) {
	h := newDisconnectHarness(t)
	core, logs := observer.New(zapcore.InfoLevel)
	h.connections.Log = zap.New(core)
	h.tokenBody = `{"access_token":"access-2","refresh_token":"refresh-2","expires_at":1893456000}`
	ctx := context.Background()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Deauthorize(ctx, conn); err != nil {
		t.Fatalf("deauthorize: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
	stored, err := h.cipher.DecryptString(mustConnection(t, h, ctx).RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if stored != "refresh-2" {
		t.Fatalf("stored refresh token = %q, want the rotated pair", stored)
	}
	if h.revokes != 0 {
		t.Fatalf("provider revoke calls = %d, want none", h.revokes)
	}
	if logs.FilterMessage("ignored an unconfirmed strava deauthorization").Len() != 1 {
		t.Fatal("want the ignored claim logged")
	}
}

func TestTransientStravaDeauthorizationChangesNothing(t *testing.T) {
	h := newDisconnectHarness(t)
	h.tokenStatus = http.StatusInternalServerError
	h.tokenBody = `{"message":"unavailable"}`
	ctx := context.Background()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Deauthorize(ctx, conn); err != nil {
		t.Fatalf("deauthorize: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
	stored, err := h.cipher.DecryptString(mustConnection(t, h, ctx).RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if stored != "strava-refresh" {
		t.Fatalf("stored refresh token = %q, want the original pair", stored)
	}
	if h.revokes != 0 {
		t.Fatalf("provider revoke calls = %d, want none", h.revokes)
	}
}

func TestMisconfiguredStravaClientDoesNotConfirmDeauthorization(t *testing.T) {
	h := newDisconnectHarness(t)
	h.tokenStatus = http.StatusBadRequest
	h.tokenBody = `{"message":"Bad Request","errors":[{"resource":"Application","field":"client_id","code":"invalid"}]}`
	ctx := context.Background()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Deauthorize(ctx, conn); err != nil {
		t.Fatalf("deauthorize: %v", err)
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	stored, err := h.cipher.DecryptString(mustConnection(t, h, ctx).RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if stored != "strava-refresh" {
		t.Fatalf("stored refresh token = %q, want the original pair", stored)
	}
}

func TestDeauthorizationStoreFailureReportsError(t *testing.T) {
	h := newDisconnectHarness(t)
	core, logs := observer.New(zapcore.ErrorLevel)
	h.connections.Log = zap.New(core)
	h.tokenBody = `{"access_token":"access-2","refresh_token":"refresh-2","expires_at":1893456000}`
	failTokenWrites(t, h.pool, 2)
	ctx := context.Background()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Deauthorize(ctx, conn); err == nil {
		t.Fatal("deauthorize succeeded after the token write kept failing")
	}
	if logs.FilterMessage("storing the rotated strava tokens failed").Len() != 1 {
		t.Fatalf("error logs = %d, want the failed store logged", logs.Len())
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
	stored, err := h.cipher.DecryptString(mustConnection(t, h, ctx).RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if stored != "strava-refresh" {
		t.Fatalf("stored refresh token = %q, want the unstored rotation not treated as current", stored)
	}
}

func mustConnection(t *testing.T, h *disconnectHarness, ctx context.Context) db.Connection {
	t.Helper()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestDisconnectTransactionFailureRemovesNothing(t *testing.T) {
	h := newDisconnectHarness(t)
	ctx := context.Background()

	// Abort the connection delete. Earlier deletes in the same transaction must
	// roll back with it; a delete that already committed would remain.
	if _, err := h.pool.Exec(`CREATE TRIGGER fail_connection_delete
		BEFORE DELETE ON connections
		BEGIN
			SELECT RAISE(ROLLBACK, 'connection delete failed');
		END`); err != nil {
		t.Fatal(err)
	}

	if err := h.connections.Disconnect(ctx, h.user.ID, KindStravaOAuth); err == nil {
		t.Fatal("disconnect succeeded after the connection delete failed")
	}

	if _, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth); err != nil {
		t.Fatalf("strava connection: %v", err)
	}
	rules, err := h.queries.ListImportRules(ctx, h.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	var stravaRule bool
	for _, rule := range rules {
		if rule.ConnectionKind == KindStravaOAuth {
			stravaRule = true
		}
	}
	if !stravaRule {
		t.Fatal("strava import rule was removed")
	}
	row, err := h.queries.GetExport(ctx, h.pending.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("pending export: %v", err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("pending export status = %q", row.Status)
	}
}

func TestRefreshStoresRotatedPair(t *testing.T) {
	h := newRefreshHarness(t, tokenPairHandler(t, "access-2", "refresh-2"))
	ctx := context.Background()
	before, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	updated, err := h.connections.Refresh(ctx, before, time.Now())
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	stored := storedRefresh(t, h, ctx)
	if stored != "refresh-2" {
		t.Fatalf("stored refresh token = %q, want the rotated pair", stored)
	}
	got, err := h.cipher.DecryptString(updated.RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if got != "refresh-2" {
		t.Fatalf("returned refresh token = %q, want the stored pair", got)
	}
}

func TestRefreshRetriesFailedTokenWrite(t *testing.T) {
	h := newRefreshHarness(t, tokenPairHandler(t, "access-2", "refresh-2"))
	failTokenWrites(t, h.pool, 1)
	ctx := context.Background()
	before, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := h.connections.Refresh(ctx, before, time.Now()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if stored := storedRefresh(t, h, ctx); stored != "refresh-2" {
		t.Fatalf("stored refresh token = %q, want the rotated pair after retry", stored)
	}
}

func TestRefreshStoreFailureReturnsError(t *testing.T) {
	h := newRefreshHarness(t, tokenPairHandler(t, "access-2", "refresh-2"))
	failTokenWrites(t, h.pool, 2)
	ctx := context.Background()
	before, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	oldExpiry := before.TokenExpiresAt

	updated, err := h.connections.Refresh(ctx, before, time.Now())
	if err == nil {
		t.Fatal("refresh succeeded after the token write kept failing")
	}
	if h.logs.FilterMessage("storing the rotated strava tokens failed").Len() != 1 {
		t.Fatalf("error logs = %d, want the failed store logged", h.logs.Len())
	}
	if stored := storedRefresh(t, h, ctx); stored != "refresh-1" {
		t.Fatalf("stored refresh token = %q, want the unstored rotation not treated as current", stored)
	}
	got, err := h.cipher.DecryptString(updated.RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if got != "refresh-1" {
		t.Fatalf("returned refresh token = %q, want the connection left unupdated", got)
	}
	after, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	if (oldExpiry == nil) != (after.TokenExpiresAt == nil) || (oldExpiry != nil && *oldExpiry != *after.TokenExpiresAt) {
		t.Fatalf("expiry changed from %v to %v", oldExpiry, after.TokenExpiresAt)
	}
}

type refreshHarness struct {
	connections *Connections
	queries     *db.Queries
	pool        *sql.DB
	cipher      *secrets.Cipher
	user        db.User
	logs        *observer.ObservedLogs
}

func newRefreshHarness(t *testing.T, handler http.HandlerFunc) *refreshHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	fake := httptest.NewServer(handler)
	t.Cleanup(fake.Close)

	cfg := config.Config{
		BaseURL: "http://localhost:8080", Version: "test",
		SecretKey:      "0123456789abcdef0123456789abcdef",
		StravaClientID: "client-id", StravaClientSecret: "client-secret",
		StravaOAuthBase: fake.URL,
	}
	cipher, err := secrets.New(cfg.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(pool)
	user, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: "athlete", Email: "athlete@example.com", DisplayName: "Athlete", CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	access, err := cipher.EncryptString("access-1")
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := cipher.EncryptString("refresh-1")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Unix()
	athleteID := "42"
	if _, err := queries.UpsertConnection(ctx, db.UpsertConnectionParams{
		UserID: user.ID, Kind: KindStravaOAuth, ExternalAthleteID: &athleteID,
		AccessTokenCipher: access, RefreshTokenCipher: refresh, TokenExpiresAt: &expires,
		AutoExport: false, ExportMessage: "Imported from hyl", CreatedAt: 1, UpdatedAt: 1,
	}); err != nil {
		t.Fatalf("create connection: %v", err)
	}
	core, logs := observer.New(zapcore.ErrorLevel)
	return &refreshHarness{
		connections: &Connections{Pool: pool, Q: queries, Cfg: cfg, Log: zap.New(core), Cipher: cipher},
		queries:     queries, pool: pool, cipher: cipher, user: user, logs: logs,
	}
}

func tokenPairHandler(t *testing.T, access, refresh string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": access, "refresh_token": refresh,
			"expires_at": time.Now().Add(6 * time.Hour).Unix(),
		})
	}
}

func storedRefresh(t *testing.T, h *refreshHarness, ctx context.Context) string {
	t.Helper()
	conn, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := h.cipher.DecryptString(conn.RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	return refresh
}

func failTokenWrites(t *testing.T, pool *sql.DB, times int) {
	t.Helper()
	if _, err := pool.Exec(`CREATE TABLE token_write_fails (remaining INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(`INSERT INTO token_write_fails (remaining) VALUES (?)`, times); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(`CREATE TRIGGER fail_token_write
		BEFORE UPDATE OF access_token_cipher ON connections
		WHEN (SELECT remaining FROM token_write_fails) > 0
		BEGIN
			UPDATE token_write_fails SET remaining = remaining - 1;
			SELECT RAISE(FAIL, 'token write failed');
		END`); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshDoesNotTreatUnstoredPairAsCurrent(t *testing.T) {
	h := newRefreshHarness(t, tokenPairHandler(t, "access-2", "refresh-2"))
	ctx := context.Background()
	before, err := h.queries.GetConnection(ctx, h.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.ExecContext(ctx, `DELETE FROM connections WHERE id = ?`, before.ID); err != nil {
		t.Fatal(err)
	}

	updated, err := h.connections.Refresh(ctx, before, time.Now())
	if err == nil {
		t.Fatal("refresh succeeded without storing the rotated pair")
	}
	got, err := h.cipher.DecryptString(updated.RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if got != "refresh-1" {
		t.Fatalf("returned refresh token = %q, want the unstored pair not treated as current", got)
	}
}
