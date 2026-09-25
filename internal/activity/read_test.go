package activity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
)

// TestReadForViewerVisibility is the activity-page allow matrix. The owner
// always opens the activity. An accepted follow is what grants followers
// access. Everyone else gets the same not-found as a missing id.
func TestReadForViewerVisibility(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()

	owner := createReadUser(t, ctx, queries, "owner", "followers")
	follower := createReadUser(t, ctx, queries, "follower", "followers")
	pending := createReadUser(t, ctx, queries, "pending", "followers")
	rejected := createReadUser(t, ctx, queries, "rejected", "followers")
	stranger := createReadUser(t, ctx, queries, "stranger", "followers")

	if err := queries.UpsertFollow(ctx, follower.ID, owner.ID, "accepted", 1); err != nil {
		t.Fatal(err)
	}
	if err := queries.UpsertFollow(ctx, pending.ID, owner.ID, "pending", 1); err != nil {
		t.Fatal(err)
	}
	if err := queries.UpsertFollow(ctx, rejected.ID, owner.ID, "rejected", 1); err != nil {
		t.Fatal(err)
	}

	viewers := []struct {
		name string
		id   int64
		kind string
	}{
		{"owner", owner.ID, "owner"},
		{"follower", follower.ID, "accepted"},
		{"pending", pending.ID, "other"},
		{"rejected", rejected.ID, "other"},
		{"stranger", stranger.ID, "other"},
		{"anonymous", 0, "other"},
	}

	for _, account := range []string{"everyone", "followers", "only_me"} {
		if _, err := queries.UpdateUserPrivacy(ctx, db.UpdateUserPrivacyParams{
			ProfileVisibility: "everyone", ActivitiesVisibility: account,
			FollowPolicy: "everyone", MentionPolicy: "followers",
			TrimScope: "all", TrimRadiusM: 200, UpdatedAt: 1, ID: owner.ID,
		}); err != nil {
			t.Fatal(err)
		}
		for _, override := range []string{"default", "everyone", "followers", "only_me"} {
			activity := createListActivity(t, ctx, queries, owner.ID, override, "", 1000, override, account+"-"+override)
			effective := override
			if override == "default" {
				effective = account
			}
			for _, viewer := range viewers {
				_, err := handlers.ForViewer(ctx, activity.ID, viewer.id)
				allowed := viewer.kind == "owner" || effective == "everyone" || (effective == "followers" && viewer.kind == "accepted")
				if allowed {
					if err != nil {
						t.Errorf("account %s override %s viewer %s: %v, want the activity", account, override, viewer.name, err)
					}
					continue
				}
				if !sameNotFound(err) {
					t.Errorf("account %s override %s viewer %s: %v, want not-found", account, override, viewer.name, err)
				}
			}
		}
	}

	_, missingErr := handlers.ForViewer(ctx, 999999, stranger.ID)
	secret := createListActivity(t, ctx, queries, owner.ID, "secret", "", 1000, "only_me", "secret")
	_, hiddenErr := handlers.ForViewer(ctx, secret.ID, stranger.ID)
	if !sameNotFound(missingErr) || !sameNotFound(hiddenErr) || missingErr.Error() != hiddenErr.Error() {
		t.Fatalf("missing %v and hidden %v must be the same not-found", missingErr, hiddenErr)
	}
}

// TestReadForViewerTrimsTheMap checks the map returned with the activity:
// start and end privacy, a privacy zone, and a route the owner marked hidden.
func TestReadForViewerTrimsTheMap(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	owner := createReadUser(t, ctx, queries, "owner", "everyone")
	stranger := createReadUser(t, ctx, queries, "stranger", "everyone")

	points := straightRoute(101, 100) // 10 km, one point every 100 m
	activity := createListActivity(t, ctx, queries, owner.ID, "trimmed", "", 1000, "everyone", "trim")
	insertRoute(t, ctx, handlers, activity.ID, points)

	// 200 m cut from each end of a northbound route, rounded to five decimals.
	const (
		wantStartLat = 52.0018
		wantEndLat   = 52.08867
	)
	opened, err := handlers.ForViewer(ctx, activity.ID, stranger.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !opened.MapAvailable || len(opened.Route) < 4 {
		t.Fatalf("map = available %v len %d, want a trimmed route", opened.MapAvailable, len(opened.Route))
	}
	if opened.Route[0] != wantStartLat || opened.Route[1] != 5 || opened.Route[len(opened.Route)-2] != wantEndLat {
		t.Fatalf("route ends = %v … %v, want start %v and end %v",
			opened.Route[:2], opened.Route[len(opened.Route)-2:], wantStartLat, wantEndLat)
	}
	ownerView, err := handlers.ForViewer(ctx, activity.ID, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ownerView.Route[0] != opened.Route[0] || ownerView.Route[len(ownerView.Route)-2] != opened.Route[len(opened.Route)-2] {
		t.Fatal("the owner map was trimmed differently from another viewer's")
	}

	hidden := createListActivity(t, ctx, queries, owner.ID, "hidden", "", 1000, "everyone", "hidden-route")
	if _, err := queries.UpdateActivity(ctx, db.UpdateActivityParams{
		ID: hidden.ID, Title: hidden.Title, Sport: hidden.Sport, Visibility: "everyone",
		RouteHidden: true, UpdatedAt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	insertRoute(t, ctx, handlers, hidden.ID, points)
	hiddenView, err := handlers.ForViewer(ctx, hidden.ID, stranger.ID)
	if err != nil {
		t.Fatal(err)
	}
	if hiddenView.MapAvailable || len(hiddenView.Route) != 0 || len(hiddenView.Track) != 0 {
		t.Fatalf("hidden route map = available %v route %v track %v", hiddenView.MapAvailable, hiddenView.Route, hiddenView.Track)
	}

	if _, err := queries.UpdateUserPrivacy(ctx, db.UpdateUserPrivacyParams{
		ProfileVisibility: "everyone", ActivitiesVisibility: "everyone",
		FollowPolicy: "everyone", MentionPolicy: "followers",
		TrimScope: "zones", TrimRadiusM: 200, UpdatedAt: 1, ID: owner.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queries.CreatePrivacyZone(ctx, owner.ID, "home", *points[0].Lat, *points[0].Lon, 1000, 1); err != nil {
		t.Fatal(err)
	}
	zoned := createListActivity(t, ctx, queries, owner.ID, "zoned", "", 1000, "everyone", "zoned")
	insertRoute(t, ctx, handlers, zoned.ID, points)
	zonedView, err := handlers.ForViewer(ctx, zoned.ID, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !zonedView.MapAvailable || len(zonedView.Route) < 2 || zonedView.Route[0] <= 52.008 {
		t.Fatalf("zone-trimmed start = %v, want the route to begin after the first kilometre", zonedView.Route)
	}
}

// TestActivityPageUsesTheRead checks the page: a hidden activity and a missing
// id are the same not-found, and the owner still receives the trimmed map.
func TestActivityPageUsesTheRead(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	owner := createReadUser(t, ctx, queries, "owner", "only_me")
	stranger := createReadUser(t, ctx, queries, "stranger", "everyone")
	activity := createListActivity(t, ctx, queries, owner.ID, "private", "", 1000, "default", "page")
	insertRoute(t, ctx, handlers, activity.ID, straightRoute(101, 100))

	hidden := callGet(t, handlers, stranger, activity.ID)
	missing := callGet(t, handlers, stranger, activity.ID+99)
	if hidden.Code != http.StatusNotFound || missing.Code != http.StatusNotFound || hidden.Body.String() != missing.Body.String() {
		t.Fatalf("hidden %d %s, missing %d %s", hidden.Code, hidden.Body.String(), missing.Code, missing.Body.String())
	}

	rec := callGet(t, handlers, owner, activity.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("owner status %d %s", rec.Code, rec.Body.String())
	}
	var detail api.ActivityDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if !detail.MapAvailable || len(detail.Route) < 4 || detail.Route[0] != 52.0018 {
		t.Fatalf("page route start = %v available %v, want 52.0018", detail.Route, detail.MapAvailable)
	}
	if !detail.CanEdit {
		t.Fatal("the owner lost edit permission")
	}
}

func createReadUser(t *testing.T, ctx context.Context, queries *db.Queries, username, activitiesVisibility string) db.User {
	t.Helper()
	user := createListUser(t, ctx, queries, username)
	if _, err := queries.UpdateUserPrivacy(ctx, db.UpdateUserPrivacyParams{
		ProfileVisibility: "everyone", ActivitiesVisibility: activitiesVisibility,
		FollowPolicy: "everyone", MentionPolicy: "followers",
		TrimScope: "all", TrimRadiusM: 200, UpdatedAt: 1, ID: user.ID,
	}); err != nil {
		t.Fatal(err)
	}
	user, err := queries.GetUserByID(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func insertRoute(t *testing.T, ctx context.Context, handlers *Handlers, activityID int64, points []Point) {
	t.Helper()
	for _, point := range points {
		if _, err := handlers.Pool.ExecContext(ctx,
			`INSERT INTO activity_points (activity_id, seq, t, elapsed_s, lat, lon, dist_m) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			activityID, point.Seq, point.T, point.ElapsedS, point.Lat, point.Lon, point.DistM); err != nil {
			t.Fatal(err)
		}
	}
}

func sameNotFound(err error) bool {
	var appErr *apperr.Error
	return errors.As(err, &appErr) && appErr.Status == http.StatusNotFound && appErr.Message == "no such activity"
}

func callGet(t *testing.T, handlers *Handlers, user db.User, activityID int64) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("id")
	c.SetParamValues(strconv.FormatInt(activityID, 10))
	reqctx.SetUser(c, &user)
	if err := handlers.Get(c); err != nil {
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, map[string]string{"message": appErr.Message})
		} else {
			t.Fatal(err)
		}
	}
	return rec
}
