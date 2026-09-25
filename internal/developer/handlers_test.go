package developer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
)

// TestDeveloperSummaryUsesActivityPageTrim checks the developer read against
// the activity page: raw points stay complete, another account is missing, and
// the summary track is the page map, including a privacy zone and a hidden route.
func TestDeveloperSummaryUsesActivityPageTrim(t *testing.T) {
	activities, dev, queries, pool := newDeveloperHandlers(t)
	ctx := context.Background()
	owner := createDeveloperUser(t, ctx, queries, "owner", "zones")
	other := createDeveloperUser(t, ctx, queries, "other", "everyone")

	points := northRoute(101, 100)
	secret := createDeveloperActivity(t, ctx, queries, owner.ID, "only_me", "secret")
	insertDeveloperRoute(t, ctx, pool, secret.ID, points)
	if _, err := queries.CreatePrivacyZone(ctx, owner.ID, "home", points[0].lat, points[0].lon, 1000, 1); err != nil {
		t.Fatal(err)
	}

	page := callActivityGet(t, activities, owner, secret.ID)
	got := callDeveloperGet(t, dev, owner, secret.ID)
	if page.Code != http.StatusOK || got.Code != http.StatusOK {
		t.Fatalf("owner page %d, developer %d %s", page.Code, got.Code, got.Body.String())
	}
	var detail api.ActivityDetail
	var payload api.DeveloperActivity
	if err := json.Unmarshal(page.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Points) != len(points) || payload.Points[0].Lat == nil || *payload.Points[0].Lat != points[0].lat {
		t.Fatalf("raw points = %d starting %v, want %d starting %v", len(payload.Points), payload.Points[0].Lat, len(points), points[0].lat)
	}
	if !payload.MapAvailable || !slices.Equal(payload.Track, detail.Route) || payload.Track[0] <= 52.008 {
		t.Fatalf("summary track = %v available %v, want the activity-page route starting after the zone", payload.Track, payload.MapAvailable)
	}

	foreign := createDeveloperActivity(t, ctx, queries, other.ID, "everyone", "foreign")
	denied := callDeveloperGet(t, dev, owner, foreign.ID)
	missing := callDeveloperGet(t, dev, owner, foreign.ID+99)
	if denied.Code != http.StatusNotFound || missing.Code != http.StatusNotFound || denied.Body.String() != missing.Body.String() {
		t.Fatalf("other account %d %s, missing %d %s", denied.Code, denied.Body.String(), missing.Code, missing.Body.String())
	}

	hidden := createDeveloperActivity(t, ctx, queries, owner.ID, "only_me", "hidden")
	if _, err := queries.UpdateActivity(ctx, db.UpdateActivityParams{
		ID: hidden.ID, Title: hidden.Title, Sport: hidden.Sport, Visibility: "only_me",
		RouteHidden: true, UpdatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	insertDeveloperRoute(t, ctx, pool, hidden.ID, points)
	hiddenRec := callDeveloperGet(t, dev, owner, hidden.ID)
	if hiddenRec.Code != http.StatusOK {
		t.Fatalf("hidden route status %d %s", hiddenRec.Code, hiddenRec.Body.String())
	}
	var hiddenPayload api.DeveloperActivity
	if err := json.Unmarshal(hiddenRec.Body.Bytes(), &hiddenPayload); err != nil {
		t.Fatal(err)
	}
	if hiddenPayload.MapAvailable || len(hiddenPayload.Track) != 0 || len(hiddenPayload.Points) != len(points) {
		t.Fatalf("hidden summary available %v track %d points %d", hiddenPayload.MapAvailable, len(hiddenPayload.Track), len(hiddenPayload.Points))
	}
}

func newDeveloperHandlers(t *testing.T) (*activity.Handlers, *Handlers, *db.Queries, *sql.DB) {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatal(err)
	}
	activities := activity.NewHandlers(pool, activity.NewStore(pool, zap.NewNop()), nil, zap.NewNop())
	return activities, New(pool, zap.NewNop(), activities), db.New(pool), pool
}

func createDeveloperUser(t *testing.T, ctx context.Context, queries *db.Queries, username, trimScope string) db.User {
	t.Helper()
	hash := "x"
	user, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: username, Email: username + "@example.com", DisplayName: username,
		PasswordHash: &hash, EmailVerified: true, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	user, err = queries.UpdateUserPrivacy(ctx, db.UpdateUserPrivacyParams{
		ProfileVisibility: "everyone", ActivitiesVisibility: "only_me",
		FollowPolicy: "everyone", MentionPolicy: "followers",
		TrimScope: trimScope, TrimRadiusM: 200, UpdatedAt: 1, ID: user.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func createDeveloperActivity(t *testing.T, ctx context.Context, queries *db.Queries, ownerID int64, visibility, dedupe string) db.Activity {
	t.Helper()
	row, err := queries.CreateActivity(ctx, db.CreateActivityParams{
		UserID: ownerID, Title: dedupe, Sport: "ride", StartedAt: 1000,
		ElapsedTimeS: 600, MovingTimeS: 600, DistanceM: 10000, HasGps: true,
		Visibility: visibility, Source: "manual", DedupeHash: dedupe, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

type routePoint struct {
	seq       int64
	lat, lon  float64
	elapsed   int64
	distanceM float64
}

func northRoute(n int, stepM float64) []routePoint {
	const mPerDegLat = 110540.0
	points := make([]routePoint, 0, n)
	for i := range n {
		points = append(points, routePoint{
			seq: int64(i), lat: 52.0 + float64(i)*stepM/mPerDegLat, lon: 5,
			elapsed: int64(i), distanceM: float64(i) * stepM,
		})
	}
	return points
}

func insertDeveloperRoute(t *testing.T, ctx context.Context, pool *sql.DB, activityID int64, points []routePoint) {
	t.Helper()
	for _, point := range points {
		if _, err := pool.ExecContext(ctx,
			`INSERT INTO activity_points (activity_id, seq, t, elapsed_s, lat, lon, dist_m) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			activityID, point.seq, 1_700_000_000+point.seq, point.elapsed, point.lat, point.lon, point.distanceM); err != nil {
			t.Fatal(err)
		}
	}
}

func callDeveloperGet(t *testing.T, handlers *Handlers, user db.User, activityID int64) *httptest.ResponseRecorder {
	t.Helper()
	return callHandler(t, handlers.Get, user, activityID)
}

func callActivityGet(t *testing.T, handlers *activity.Handlers, user db.User, activityID int64) *httptest.ResponseRecorder {
	t.Helper()
	return callHandler(t, handlers.Get, user, activityID)
}

func callHandler(t *testing.T, handler echo.HandlerFunc, user db.User, activityID int64) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("id")
	c.SetParamValues(strconv.FormatInt(activityID, 10))
	reqctx.SetUser(c, &user)
	if err := handler(c); err != nil {
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, map[string]string{"message": appErr.Message})
		} else {
			t.Fatal(err)
		}
	}
	return rec
}
