package media_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/media"
	"github.com/markbeep/hyl/internal/reqctx"
)

// TestServePhotoFollowsTheViewerRead is the photo allow matrix: a viewer can
// fetch a photo exactly when ForViewer would open its activity, a hidden or
// missing activity is the same not-found as a missing file, and avatars stay
// public.
func TestServePhotoFollowsTheViewerRead(t *testing.T) {
	mediaHandlers, activities, queries := newServeHandlers(t)
	ctx := context.Background()

	owner := createServeUser(t, ctx, queries, "owner", "followers")
	follower := createServeUser(t, ctx, queries, "follower", "everyone")
	pending := createServeUser(t, ctx, queries, "pending", "everyone")
	stranger := createServeUser(t, ctx, queries, "stranger", "everyone")
	if err := queries.UpsertFollow(ctx, follower.ID, owner.ID, "accepted", 1); err != nil {
		t.Fatal(err)
	}
	if err := queries.UpsertFollow(ctx, pending.ID, owner.ID, "pending", 1); err != nil {
		t.Fatal(err)
	}

	everyone := createServeActivity(t, ctx, queries, owner.ID, "everyone", "everyone")
	followers := createServeActivity(t, ctx, queries, owner.ID, "followers", "followers")
	onlyMe := createServeActivity(t, ctx, queries, owner.ID, "only_me", "only-me")
	everyonePhoto := createServeFile(t, mediaHandlers, owner.ID, &everyone.ID, media.KindActivity)
	followersPhoto := createServeFile(t, mediaHandlers, owner.ID, &followers.ID, media.KindActivity)
	onlyMePhoto := createServeFile(t, mediaHandlers, owner.ID, &onlyMe.ID, media.KindActivity)
	avatar := createServeFile(t, mediaHandlers, owner.ID, nil, media.KindAvatar)

	viewers := []struct {
		name string
		user *db.User
	}{
		{"owner", &owner},
		{"follower", &follower},
		{"pending", &pending},
		{"stranger", &stranger},
		{"anonymous", nil},
	}
	photos := []struct {
		name     string
		id       int64
		activity int64
	}{
		{"everyone", everyonePhoto, everyone.ID},
		{"followers", followersPhoto, followers.ID},
		{"only_me", onlyMePhoto, onlyMe.ID},
	}

	for _, photo := range photos {
		for _, viewer := range viewers {
			viewerID := int64(0)
			if viewer.user != nil {
				viewerID = viewer.user.ID
			}
			_, readErr := activities.ForViewer(ctx, photo.activity, viewerID)
			rec := callServe(t, mediaHandlers, viewer.user, photo.id)
			if readErr == nil {
				if rec.Code != http.StatusOK {
					t.Errorf("%s photo viewer %s: status %d %s, want the file", photo.name, viewer.name, rec.Code, rec.Body.String())
				}
				continue
			}
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s photo viewer %s: status %d %s, want not-found to match the viewer read", photo.name, viewer.name, rec.Code, rec.Body.String())
			}
		}
	}

	hidden := callServe(t, mediaHandlers, &stranger, onlyMePhoto)
	missingPhoto := callServe(t, mediaHandlers, &stranger, onlyMePhoto+99)
	if hidden.Code != http.StatusNotFound || missingPhoto.Code != http.StatusNotFound || hidden.Body.String() != missingPhoto.Body.String() {
		t.Fatalf("hidden activity photo %d %s, missing photo %d %s", hidden.Code, hidden.Body.String(), missingPhoto.Code, missingPhoto.Body.String())
	}

	public := callServe(t, mediaHandlers, &stranger, avatar)
	anonAvatar := callServe(t, mediaHandlers, nil, avatar)
	if public.Code != http.StatusOK || anonAvatar.Code != http.StatusOK {
		t.Fatalf("avatar stranger %d, anonymous %d, want both public", public.Code, anonAvatar.Code)
	}
}

func newServeHandlers(t *testing.T) (*media.Handlers, *activity.Handlers, *db.Queries) {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatal(err)
	}
	svc, err := media.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mediaHandlers := media.NewHandlers(svc, pool, zap.NewNop())
	activities := activity.NewHandlers(pool, activity.NewStore(pool, zap.NewNop()), mediaHandlers, zap.NewNop())
	mediaHandlers.ForViewer = func(ctx context.Context, activityID, viewerID int64) error {
		_, err := activities.ForViewer(ctx, activityID, viewerID)
		return err
	}
	return mediaHandlers, activities, db.New(pool)
}

func createServeUser(t *testing.T, ctx context.Context, queries *db.Queries, username, activitiesVisibility string) db.User {
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
		ProfileVisibility: "everyone", ActivitiesVisibility: activitiesVisibility,
		FollowPolicy: "everyone", MentionPolicy: "followers",
		TrimScope: "all", TrimRadiusM: 200, UpdatedAt: 1, ID: user.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func createServeActivity(t *testing.T, ctx context.Context, queries *db.Queries, ownerID int64, visibility, dedupe string) db.Activity {
	t.Helper()
	row, err := queries.CreateActivity(ctx, db.CreateActivityParams{
		UserID: ownerID, Title: dedupe, Sport: "ride", StartedAt: 1000,
		ElapsedTimeS: 600, MovingTimeS: 600, DistanceM: 5000, HasGps: true,
		Visibility: visibility, Source: "manual", DedupeHash: dedupe, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func createServeFile(t *testing.T, handlers *media.Handlers, userID int64, activityID *int64, kind string) int64 {
	t.Helper()
	row, err := handlers.Q.CreateMedia(context.Background(), db.CreateMediaParams{
		UserID: userID, ActivityID: activityID, Kind: kind, Position: 0, CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := handlers.Service.Path(kind, row.ID, media.VariantFull)
	if err := os.WriteFile(path, []byte("RIFF....WEBP"), 0o644); err != nil {
		t.Fatal(err)
	}
	return row.ID
}

func callServe(t *testing.T, handlers *media.Handlers, user *db.User, mediaID int64) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("id")
	c.SetParamValues(strconv.FormatInt(mediaID, 10))
	if user != nil {
		reqctx.SetUser(c, user)
	}
	if err := handlers.Serve(c); err != nil {
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, map[string]string{"message": appErr.Message})
		} else {
			t.Fatal(err)
		}
	}
	return rec
}
