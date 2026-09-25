package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
	"go.uber.org/zap"
)

// fakeStrava records every call the exporter makes so the multipart contract
// and the token rotation can be asserted exactly. It stands in for the real
// service, which needs a paid account and a single-athlete app.
type fakeStrava struct {
	mu              sync.Mutex
	refreshRequests int
	uploads         []uploadRequest
	authorizations  []string
	pollStatuses    []string
}

type uploadRequest struct {
	Fields map[string]string
	File   string
	Size   int
	Header string
}

func (f *fakeStrava) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("token request: %v", err)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "refresh-1" {
			t.Errorf("unexpected token request: %v", r.Form)
		}
		f.mu.Lock()
		f.refreshRequests++
		f.mu.Unlock()
		writeJSON(t, w, map[string]any{
			"access_token":  "access-2",
			"refresh_token": "refresh-2",
			"expires_at":    time.Now().Add(6 * time.Hour).Unix(),
			"athlete":       map[string]any{"id": 999},
		})
	})
	mux.HandleFunc("/api/v3/uploads", func(w http.ResponseWriter, r *http.Request) {
		request := uploadRequest{Header: r.Header.Get("Authorization")}
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			t.Errorf("upload request: %v", err)
		}
		request.Fields = map[string]string{}
		for key := range r.MultipartForm.Value {
			request.Fields[key] = r.MultipartForm.Value[key][0]
		}
		if files := r.MultipartForm.File["file"]; len(files) == 1 {
			request.File = files[0].Filename
			request.Size = int(files[0].Size)
		}
		f.mu.Lock()
		f.uploads = append(f.uploads, request)
		f.mu.Unlock()
		writeJSON(t, w, map[string]any{"id": 5150, "status": "Your activity is being processed.", "activity_id": 0})
	})
	mux.HandleFunc("/api/v3/uploads/5150", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.pollStatuses = append(f.pollStatuses, r.Header.Get("Authorization"))
		f.mu.Unlock()
		writeJSON(t, w, map[string]any{"id": 5150, "status": "Your activity is ready.", "activity_id": 4242})
	})
	return httptest.NewServer(mux)
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Errorf("writing the response failed: %v", err)
	}
}

func configureExportRefresh(exporter *Exporter, baseURL, clientID, clientSecret string) {
	exporter.Cfg.StravaClientID = clientID
	exporter.Cfg.StravaClientSecret = clientSecret
	exporter.connections.Cfg = exporter.Cfg
	exporter.connections.tokenBase = baseURL
}

// TestExportSendsFitToStrava drives the whole outbound path against the fake:
// token refresh, FIT synthesis, multipart contract and status polling.
func TestExportSendsFitToStrava(t *testing.T) {
	server := (&fakeStrava{}).server(t)
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")
	harness.exporter.newClient = func(accessToken string) *StravaClient {
		return &StravaClient{APIBase: server.URL + "/api/v3", AccessToken: accessToken, HTTP: server.Client()}
	}

	harness.exporter.Drain(context.Background())

	row, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatalf("export row: %v", err)
	}
	if row.Status != exportStatusSent {
		t.Fatalf("status = %q (%v), want sent", row.Status, row.LastError)
	}
	if row.RemoteID == nil || *row.RemoteID != "4242" {
		t.Fatalf("remote id = %v, want 4242", row.RemoteID)
	}

	// The token was rotated and stored.
	conn, err := harness.queries.GetConnection(context.Background(), harness.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	access, err := harness.cipher.DecryptString(conn.AccessTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if access != "access-2" {
		t.Fatalf("stored access token = %q, want the refreshed one", access)
	}
	refresh, err := harness.cipher.DecryptString(conn.RefreshTokenCipher)
	if err != nil {
		t.Fatal(err)
	}
	if refresh != "refresh-2" {
		t.Fatalf("stored refresh token = %q, want the rotated one", refresh)
	}
	if conn.TokenExpiresAt == nil || *conn.TokenExpiresAt < time.Now().Unix() {
		t.Fatalf("token expiry was not updated: %v", conn.TokenExpiresAt)
	}
}

// TestExportMultipartContract pins the wire format Strava expects.
func TestExportMultipartContract(t *testing.T) {
	fake := &fakeStrava{}
	server := fake.server(t)
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")
	harness.exporter.newClient = func(accessToken string) *StravaClient {
		return &StravaClient{APIBase: server.URL + "/api/v3", AccessToken: accessToken, HTTP: server.Client()}
	}

	harness.exporter.Drain(context.Background())

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.refreshRequests != 1 {
		t.Fatalf("token refreshed %d times, want 1", fake.refreshRequests)
	}
	if len(fake.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(fake.uploads))
	}
	upload := fake.uploads[0]
	if upload.Fields["data_type"] != "fit" {
		t.Errorf("data_type = %q, want fit", upload.Fields["data_type"])
	}
	if upload.Fields["external_id"] != harness.activity.DedupeHash {
		t.Errorf("external_id = %q, want the dedupe hash", upload.Fields["external_id"])
	}
	if upload.Fields["trainer"] != "0" {
		t.Errorf("trainer = %q, want 0", upload.Fields["trainer"])
	}
	if upload.Fields["name"] != "Export me" {
		t.Errorf("name = %q, want the activity title", upload.Fields["name"])
	}
	if upload.File != fmt.Sprintf("hyl-%d.fit", harness.activity.ID) {
		t.Errorf("file name = %q", upload.File)
	}
	if upload.Size == 0 {
		t.Error("the FIT payload was empty")
	}
	if upload.Header != "Bearer access-2" {
		t.Errorf("authorization = %q, want the refreshed token", upload.Header)
	}
	if len(fake.pollStatuses) != 1 {
		t.Fatalf("polls = %d, want 1", len(fake.pollStatuses))
	}
}

// TestExportRetriesAndGivesUp checks the attempt accounting: a provider that
// always fails must stop after exportMaxAttempts instead of looping forever.
func TestExportRetriesAndGivesUp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/oauth/token") {
			writeJSON(t, w, map[string]any{
				"access_token": "access-2", "refresh_token": "refresh-2",
				"expires_at": time.Now().Add(time.Hour).Unix(), "athlete": map[string]any{"id": 999},
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "id", "secret")
	harness.exporter.newClient = func(accessToken string) *StravaClient {
		return &StravaClient{APIBase: server.URL + "/api/v3", AccessToken: accessToken, HTTP: server.Client()}
	}

	for range exportMaxAttempts {
		harness.exporter.Drain(context.Background())
	}
	row, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != exportStatusError {
		t.Fatalf("status = %q after %d attempts, want error", row.Status, row.Attempts)
	}

	// A finished row is never retried.
	before := row.Attempts
	harness.exporter.Drain(context.Background())
	after, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if after.Attempts != before {
		t.Fatalf("attempts grew from %d to %d for a finished export", before, after.Attempts)
	}
}

func TestStravaAuthorizeURL(t *testing.T) {
	cfg := config.Config{
		BaseURL: "https://hyl.example.org", StravaClientID: "1234",
	}
	url := StravaAuthorizeURL(cfg, "state-token")
	for _, want := range []string{
		"https://www.strava.com/oauth/authorize",
		"client_id=1234",
		"redirect_uri=https%3A%2F%2Fhyl.example.org%2Fapi%2Fconnections%2Fstrava%2Fcallback",
		"scope=activity%3Awrite",
		"state=state-token",
	} {
		if !strings.Contains(url, want) {
			t.Errorf("authorize URL %q is missing %q", url, want)
		}
	}
}

type exportHarness struct {
	exporter *Exporter
	queries  *db.Queries
	pool     *sql.DB
	cipher   *secrets.Cipher
	user     db.User
	activity db.Activity
}

// newExportHarness builds an export queue with one pending activity and a
// Strava connection whose token is about to expire.
func newExportHarness(t *testing.T) exportHarness {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cfg := config.Config{
		BaseURL: "http://localhost:8080", Version: "test",
		SecretKey: "0123456789abcdef0123456789abcdef",
	}
	cipher, err := secrets.New(cfg.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(pool)
	ctx := context.Background()

	user, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: "exporter", Email: "export@example.com", DisplayName: "export", CreatedAt: 1, UpdatedAt: 1,
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
	athleteID := "999"
	expires := time.Now().Unix() // already inside the refresh window
	if _, err := queries.UpsertConnection(ctx, db.UpsertConnectionParams{
		UserID: user.ID, Kind: KindStravaOAuth, ExternalAthleteID: &athleteID,
		AccessTokenCipher: access, RefreshTokenCipher: refresh, TokenExpiresAt: &expires,
		AutoExport: true, ExportMessage: "Imported from hyl", CreatedAt: 1, UpdatedAt: 1,
	}); err != nil {
		t.Fatalf("create connection: %v", err)
	}

	activityRow, err := queries.CreateActivity(ctx, db.CreateActivityParams{
		UserID: user.ID, Title: "Export me", Sport: "ride", StartedAt: time.Now().Add(-2 * time.Hour).Unix(),
		ElapsedTimeS: 600, MovingTimeS: 600, DistanceM: 20000, HasGps: true,
		Visibility: "default", Source: "manual", DedupeHash: "export-hash", CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create activity: %v", err)
	}
	for i := range 30 {
		timestamp := activityRow.StartedAt + int64(i*20)
		latitude := 52.0 + float64(i)*0.0002
		longitude := 5.0
		if _, err := pool.ExecContext(ctx,
			`INSERT INTO activity_points (activity_id, seq, t, elapsed_s, lat, lon) VALUES (?, ?, ?, ?, ?, ?)`,
			activityRow.ID, i, timestamp, i*20, latitude, longitude); err != nil {
			t.Fatalf("insert point: %v", err)
		}
	}

	exporter := NewExporter(pool, cfg, zap.NewNop(), cipher)
	exporter.sleep = func(context.Context, time.Duration) {} // no pacing in tests

	if _, err := QueueExport(ctx, queries, activityRow, exportTargetStrava); err != nil {
		t.Fatalf("queue export: %v", err)
	}
	return exportHarness{exporter: exporter, queries: queries, pool: pool, cipher: cipher, user: user, activity: activityRow}
}

// TestExportRateLimitDoesNotSpendAnAttempt is the regression test for exports
// being failed permanently by throttling: a 429 says nothing about the activity,
// and five throttled ticks used to mark the row terminally broken.
func TestExportRateLimitDoesNotSpendAnAttempt(t *testing.T) {
	var mu sync.Mutex
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/token" {
			writeJSON(t, w, map[string]any{
				"access_token": "access-2", "refresh_token": "refresh-2",
				"expires_at": time.Now().Add(6 * time.Hour).Unix(),
				"athlete":    map[string]any{"id": 999},
			})
			return
		}
		mu.Lock()
		uploads++
		mu.Unlock()
		// Strava reports throttle usage through headers rather than
		// Retry-After, which is exactly what used to be ignored.
		w.Header().Set("X-RateLimit-Limit", "100,1000")
		w.Header().Set("X-RateLimit-Usage", "100,1000")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"Rate Limit Exceeded"}`))
	}))
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")
	harness.exporter.newClient = func(accessToken string) *StravaClient {
		return &StravaClient{APIBase: server.URL + "/api/v3", AccessToken: accessToken, HTTP: server.Client()}
	}

	harness.exporter.Drain(context.Background())

	ctx := context.Background()
	row, err := harness.queries.GetExport(ctx, harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("status = %q, want pending after a rate limit", row.Status)
	}
	if row.Attempts != 0 {
		t.Fatalf("a rate limit spent %d attempts, want 0", row.Attempts)
	}
	if row.LastError == nil || *row.LastError == "" {
		t.Fatal("the rate limit was not recorded")
	}

	// The window is respected: a second pass does not hit Strava again.
	harness.exporter.Drain(ctx)
	mu.Lock()
	after := uploads
	mu.Unlock()
	if after != 1 {
		t.Fatalf("uploads = %d, want the second drain to be throttled", after)
	}
}

// TestExportCanBeRequeuedAfterAFailure pins the recovery path: a row that ended
// in error is the only one a re-queue may reset, so an upload Strava rejected
// can be retried once the cause is fixed.
func TestExportCanBeRequeuedAfterAFailure(t *testing.T) {
	harness := newExportHarness(t)
	ctx := context.Background()

	// A pending row keeps its place, which the handler reports as a conflict.
	queued, err := QueueExport(ctx, harness.queries, harness.activity, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if queued {
		t.Fatal("a pending export was queued a second time")
	}

	row, err := harness.queries.GetExport(ctx, harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	message := "strava rejected the activity"
	if _, err := harness.queries.UpdateExportStatus(ctx, exportStatusError, nil, exportMaxAttempts, &message, time.Now().Unix(), row.ID); err != nil {
		t.Fatal(err)
	}

	queued, err = QueueExport(ctx, harness.queries, harness.activity, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("a failed export could not be re-queued")
	}
	reset, err := harness.queries.GetExport(ctx, harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Status != exportStatusPending || reset.Attempts != 0 || reset.LastError != nil {
		t.Fatalf("re-queued row = %+v, want a clean pending row", reset)
	}
}

// TestExportSendsSportType pins the round-trip of the provider's own activity
// type: without it every ride flavour Strava knows collapses into a generic one.
func TestExportSendsSportType(t *testing.T) {
	fake := &fakeStrava{}
	server := fake.server(t)
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")
	harness.exporter.newClient = func(accessToken string) *StravaClient {
		return &StravaClient{APIBase: server.URL + "/api/v3", AccessToken: accessToken, HTTP: server.Client()}
	}
	ctx := context.Background()
	if _, err := harness.pool.ExecContext(ctx,
		`UPDATE activities SET external_sport = ? WHERE id = ?`, "GravelRide", harness.activity.ID); err != nil {
		t.Fatal(err)
	}

	harness.exporter.Drain(ctx)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(fake.uploads))
	}
	if got := fake.uploads[0].Fields["sport_type"]; got != "GravelRide" {
		t.Fatalf("sport_type = %q, want GravelRide", got)
	}
}

func TestExportStoreFailureReschedules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{
			"access_token": "access-2", "refresh_token": "refresh-2",
			"expires_at": time.Now().Add(6 * time.Hour).Unix(),
		})
	}))
	defer server.Close()

	harness := newExportHarness(t)
	failTokenWrites(t, harness.pool, 2)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")

	harness.exporter.Drain(context.Background())

	row, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("status = %q, want the export rescheduled", row.Status)
	}
	if row.LastError != nil && *row.LastError == "reauthorize" {
		t.Fatal("a failed token write marked the export as needing reauthorization")
	}
	conn, err := harness.queries.GetConnection(context.Background(), harness.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	if conn.LastError != nil && *conn.LastError == "reauthorize" {
		t.Fatal("a failed token write marked the connection as needing reauthorization")
	}
}

func TestExportMarksReauthorizeWhenStravaRejectsRefresh(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Bad Request","errors":[{"resource":"RefreshToken","field":"refresh_token","code":"invalid"}]}`))
	}))
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")

	harness.exporter.Drain(context.Background())

	row, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != exportStatusError || row.LastError == nil || *row.LastError != "reauthorize" {
		t.Fatalf("export = status %q error %v, want reauthorize", row.Status, row.LastError)
	}
	conn, err := harness.queries.GetConnection(context.Background(), harness.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	if conn.LastError == nil || *conn.LastError != "reauthorize" {
		t.Fatalf("connection last error = %v, want reauthorize", conn.LastError)
	}
}

func TestStravaRejectedCredential(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "invalid refresh token",
			err: &ProviderError{
				StatusCode: http.StatusBadRequest,
				Message:    `strava rejected the token request (400): {"message":"Bad Request","errors":[{"resource":"RefreshToken","field":"refresh_token","code":"invalid"}]}`,
			},
			want: true,
		},
		{
			name: "bad client id",
			err: &ProviderError{
				StatusCode: http.StatusBadRequest,
				Message:    `strava rejected the token request (400): {"message":"Bad Request","errors":[{"resource":"Application","field":"client_id","code":"invalid"}]}`,
			},
			want: false,
		},
		{
			name: "unauthorized",
			err:  &ProviderError{StatusCode: http.StatusUnauthorized, Message: "strava rejected the token request (401)"},
			want: true,
		},
		{
			name: "forbidden",
			err:  &ProviderError{StatusCode: http.StatusForbidden, Message: "strava rejected the token request (403)"},
			want: true,
		},
		{
			name: "server error",
			err:  &ProviderError{StatusCode: http.StatusInternalServerError, Message: "unavailable"},
			want: false,
		},
		{
			name: "plain error",
			err:  errors.New("network"),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := stravaRejectedCredential(test.err); got != test.want {
				t.Fatalf("stravaRejectedCredential() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestExportDoesNotParkWhenStravaRejectsClientCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Bad Request","errors":[{"resource":"Application","field":"client_id","code":"invalid"}]}`))
	}))
	defer server.Close()

	harness := newExportHarness(t)
	configureExportRefresh(harness.exporter, server.URL, "client-id", "client-secret")

	harness.exporter.Drain(context.Background())

	row, err := harness.queries.GetExport(context.Background(), harness.activity.ID, exportTargetStrava)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != exportStatusPending {
		t.Fatalf("status = %q, want pending after a configuration failure", row.Status)
	}
	if row.LastError != nil && *row.LastError == "reauthorize" {
		t.Fatal("a client credential failure marked the export as needing reauthorization")
	}
	conn, err := harness.queries.GetConnection(context.Background(), harness.user.ID, KindStravaOAuth)
	if err != nil {
		t.Fatal(err)
	}
	if conn.LastError != nil && *conn.LastError == "reauthorize" {
		t.Fatal("a client credential failure parked a healthy connection")
	}
}
