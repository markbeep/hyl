package social_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
	"github.com/markbeep/hyl/internal/social"
)

// TestLikesAndCommentsFollowTheViewerRead is the social allow matrix: like,
// comment, and listing comments are allowed exactly when ForViewer would open
// the activity. A deny writes nothing. Repeated likes stay idempotent, and
// only the author can delete a comment.
func TestLikesAndCommentsFollowTheViewerRead(t *testing.T) {
	handlers, activities, queries := newSocialHandlers(t)
	ctx := context.Background()

	owner := createSocialUser(t, ctx, queries, "owner", "followers")
	follower := createSocialUser(t, ctx, queries, "follower", "everyone")
	pending := createSocialUser(t, ctx, queries, "pending", "everyone")
	stranger := createSocialUser(t, ctx, queries, "stranger", "everyone")
	if err := queries.UpsertFollow(ctx, follower.ID, owner.ID, "accepted", 1); err != nil {
		t.Fatal(err)
	}
	if err := queries.UpsertFollow(ctx, pending.ID, owner.ID, "pending", 1); err != nil {
		t.Fatal(err)
	}

	everyone := createSocialActivity(t, ctx, queries, owner.ID, "everyone", "everyone")
	followers := createSocialActivity(t, ctx, queries, owner.ID, "followers", "followers")
	onlyMe := createSocialActivity(t, ctx, queries, owner.ID, "only_me", "only-me")

	viewers := []struct {
		name string
		user db.User
	}{
		{"follower", follower},
		{"pending", pending},
		{"stranger", stranger},
	}
	rows := []struct {
		name     string
		activity db.Activity
	}{
		{"everyone", everyone},
		{"followers", followers},
		{"only_me", onlyMe},
	}

	for _, row := range rows {
		for _, viewer := range viewers {
			_, readErr := activities.ForViewer(ctx, row.activity.ID, viewer.user.ID)
			before := socialCounts(t, queries, row.activity.ID, owner.ID)
			likeRec := callSocial(t, handlers.Like, &viewer.user, map[string]string{"id": itoa(row.activity.ID)}, "")
			commentRec := callSocial(t, handlers.PostComment, &viewer.user, map[string]string{"id": itoa(row.activity.ID)}, `{"body":"nice"}`)
			listRec := callSocial(t, handlers.ListComments, &viewer.user, map[string]string{"id": itoa(row.activity.ID)}, "")
			if readErr == nil {
				if likeRec.Code != http.StatusOK {
					t.Errorf("%s like viewer %s: %d %s, want allowed", row.name, viewer.name, likeRec.Code, likeRec.Body.String())
				}
				if commentRec.Code != http.StatusCreated {
					t.Errorf("%s comment viewer %s: %d %s, want allowed", row.name, viewer.name, commentRec.Code, commentRec.Body.String())
				}
				if listRec.Code != http.StatusOK {
					t.Errorf("%s list viewer %s: %d %s, want allowed", row.name, viewer.name, listRec.Code, listRec.Body.String())
				}
				continue
			}
			assertDenied(t, row.name+" like "+viewer.name, likeRec, before, socialCounts(t, queries, row.activity.ID, owner.ID))
			assertDenied(t, row.name+" comment "+viewer.name, commentRec, before, socialCounts(t, queries, row.activity.ID, owner.ID))
			if listRec.Code != http.StatusNotFound {
				t.Errorf("%s list viewer %s: %d %s, want not-found", row.name, viewer.name, listRec.Code, listRec.Body.String())
			}
		}
	}

	hiddenList := callSocial(t, handlers.ListComments, &stranger, map[string]string{"id": itoa(onlyMe.ID)}, "")
	missingList := callSocial(t, handlers.ListComments, &stranger, map[string]string{"id": itoa(onlyMe.ID + 99)}, "")
	if hiddenList.Code != http.StatusNotFound || missingList.Code != http.StatusNotFound || hiddenList.Body.String() != missingList.Body.String() {
		t.Fatalf("hidden list %d %s, missing list %d %s", hiddenList.Code, hiddenList.Body.String(), missingList.Code, missingList.Body.String())
	}

	params := map[string]string{"id": itoa(everyone.ID)}
	first := callSocial(t, handlers.Like, &follower, params, "")
	if first.Code != http.StatusOK {
		t.Fatalf("repeated like first %d %s", first.Code, first.Body.String())
	}
	afterFirst := socialCounts(t, queries, everyone.ID, owner.ID)
	second := callSocial(t, handlers.Like, &follower, params, "")
	if second.Code != http.StatusOK {
		t.Fatalf("repeated like second %d %s", second.Code, second.Body.String())
	}
	afterSecond := socialCounts(t, queries, everyone.ID, owner.ID)
	if afterSecond != afterFirst {
		t.Fatalf("repeat like wrote records: after first %+v after second %+v", afterFirst, afterSecond)
	}

	created := callSocial(t, handlers.PostComment, &follower, params, `{"body":"keep"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("setup comment: %d %s", created.Code, created.Body.String())
	}
	var payload api.CommentCreated
	if err := json.Unmarshal(created.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	denied := callSocial(t, handlers.DeleteComment, &stranger, map[string]string{"id": itoa(payload.Comment.ID)}, "")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("stranger delete: %d %s, want 403", denied.Code, denied.Body.String())
	}
	allowed := callSocial(t, handlers.DeleteComment, &follower, map[string]string{"id": itoa(payload.Comment.ID)}, "")
	if allowed.Code != http.StatusNoContent {
		t.Fatalf("author delete: %d %s, want 204", allowed.Code, allowed.Body.String())
	}
}

// TestDeniedViewerReadWritesNothing proves the injected read is the gate: even
// a public activity is missing when ForViewer reports it missing, and nothing
// is written.
func TestDeniedViewerReadWritesNothing(t *testing.T) {
	handlers, _, queries := newSocialHandlers(t)
	ctx := context.Background()
	owner := createSocialUser(t, ctx, queries, "owner", "everyone")
	stranger := createSocialUser(t, ctx, queries, "stranger", "everyone")
	activity := createSocialActivity(t, ctx, queries, owner.ID, "everyone", "public")
	handlers.ForViewer = func(context.Context, int64, int64) (db.Activity, error) {
		return db.Activity{}, apperr.NotFound("no such activity")
	}

	before := socialCounts(t, queries, activity.ID, owner.ID)
	like := callSocial(t, handlers.Like, &stranger, map[string]string{"id": itoa(activity.ID)}, "")
	comment := callSocial(t, handlers.PostComment, &stranger, map[string]string{"id": itoa(activity.ID)}, `{"body":"hello"}`)
	list := callSocial(t, handlers.ListComments, &stranger, map[string]string{"id": itoa(activity.ID)}, "")
	assertDenied(t, "like", like, before, socialCounts(t, queries, activity.ID, owner.ID))
	assertDenied(t, "comment", comment, before, socialCounts(t, queries, activity.ID, owner.ID))
	if list.Code != http.StatusNotFound {
		t.Fatalf("list status %d %s, want not-found", list.Code, list.Body.String())
	}
}

func newSocialHandlers(t *testing.T) (*social.Handlers, *activity.Handlers, *db.Queries) {
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
	handlers := social.New(pool, zap.NewNop())
	handlers.ForViewer = func(ctx context.Context, activityID, viewerID int64) (db.Activity, error) {
		opened, err := activities.ForViewer(ctx, activityID, viewerID)
		if err != nil {
			return db.Activity{}, err
		}
		return opened.Activity, nil
	}
	return handlers, activities, db.New(pool)
}

func createSocialUser(t *testing.T, ctx context.Context, queries *db.Queries, username, activitiesVisibility string) db.User {
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

func createSocialActivity(t *testing.T, ctx context.Context, queries *db.Queries, ownerID int64, visibility, dedupe string) db.Activity {
	t.Helper()
	row, err := queries.CreateActivity(ctx, db.CreateActivityParams{
		UserID: ownerID, Title: dedupe, Sport: "ride", StartedAt: 1000,
		ElapsedTimeS: 60, MovingTimeS: 60, DistanceM: 1000, HasGps: true,
		Visibility: visibility, Source: "manual", DedupeHash: dedupe, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func callSocial(t *testing.T, handler echo.HandlerFunc, user *db.User, params map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	names := make([]string, 0, len(params))
	values := make([]string, 0, len(params))
	for name, value := range params {
		names = append(names, name)
		values = append(values, value)
	}
	c.SetParamNames(names...)
	c.SetParamValues(values...)
	if user != nil {
		reqctx.SetUser(c, user)
	}
	if err := handler(c); err != nil {
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, api.ErrorResponse{Error: api.ErrorBody{Code: appErr.Code, Message: appErr.Message}})
		} else {
			t.Fatal(err)
		}
	}
	return rec
}

type counts struct{ likes, comments, unread int64 }

func socialCounts(t *testing.T, queries *db.Queries, activityID, ownerID int64) counts {
	t.Helper()
	ctx := context.Background()
	likes, err := queries.CountLikes(ctx, activityID)
	if err != nil {
		t.Fatal(err)
	}
	comments, err := queries.CountComments(ctx, activityID)
	if err != nil {
		t.Fatal(err)
	}
	unread, err := queries.CountUnreadNotifications(ctx, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	return counts{likes, comments, unread}
}

func assertDenied(t *testing.T, label string, rec *httptest.ResponseRecorder, before, after counts) {
	t.Helper()
	if rec.Code != http.StatusNotFound {
		t.Fatalf("%s status %d %s, want not-found", label, rec.Code, rec.Body.String())
	}
	if after != before {
		t.Fatalf("%s wrote records: before %+v after %+v", label, before, after)
	}
}

func itoa(value int64) string { return strconv.FormatInt(value, 10) }
