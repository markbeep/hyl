package social

import (
	"context"
	"database/sql"
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

	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
)

// newTestHandlers builds a handlers set over a migrated temporary database,
// which is what makes the notification side effects testable end to end.
func newTestHandlers(t *testing.T) (*Handlers, *db.Queries) {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	handlers := New(pool, zap.NewNop())
	queries := db.New(pool)
	handlers.ForViewer = func(ctx context.Context, activityID, viewerID int64) (db.Activity, error) {
		activity, err := queries.GetActivity(ctx, activityID)
		if errors.Is(err, sql.ErrNoRows) {
			return activity, apperr.NotFound("no such activity")
		}
		if err != nil {
			return activity, err
		}
		owner, err := queries.GetUserByID(ctx, activity.UserID)
		if err != nil {
			return activity, err
		}
		follower := false
		if viewerID != 0 && viewerID != owner.ID {
			follow, err := queries.GetFollow(ctx, viewerID, owner.ID)
			switch {
			case err == nil:
				follower = follow.Status == "accepted"
			case errors.Is(err, sql.ErrNoRows):
			default:
				return activity, err
			}
		}
		if !VisibilityAllows(viewerID, owner.ID, follower, activity.Visibility, owner.ActivitiesVisibility) {
			return activity, apperr.NotFound("no such activity")
		}
		return activity, nil
	}
	return handlers, queries
}

// post runs one authenticated handler call against a request path.
func post(t *testing.T, h *Handlers, handler echo.HandlerFunc, user db.User, path string, params map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(http.MethodPost, path, reader)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
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
	reqctx.SetUser(c, &user)

	if err := handler(c); err != nil {
		// The real envelope is installed by internal/server; here it is enough
		// to turn one into the status under test.
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, api.ErrorResponse{Error: api.ErrorBody{Code: appErr.Code, Message: appErr.Message}})
		} else {
			e.HTTPErrorHandler(err, c)
		}
	}
	return rec
}

func TestLikeIsIdempotentIncludingNotifications(t *testing.T) {
	handlers, queries := newTestHandlers(t)
	ctx := context.Background()

	owner := createUser(t, ctx, queries, "owner", "everyone")
	liker := createUser(t, ctx, queries, "liker", "everyone")
	activity := createActivity(t, ctx, queries, owner.ID, "everyone", "like-test")

	params := map[string]string{"id": itoa(activity.ID)}
	first := post(t, handlers, handlers.Like, liker, "/api/activities/1/likes", params, "")
	if first.Code != http.StatusOK {
		t.Fatalf("first like: %d %s", first.Code, first.Body.String())
	}
	var result api.LikeResult
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.LikeCount != 1 || !result.LikedByMe {
		t.Fatalf("first like returned %+v", result)
	}

	second := post(t, handlers, handlers.Like, liker, "/api/activities/1/likes", params, "")
	if second.Code != http.StatusOK {
		t.Fatalf("second like: %d", second.Code)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.LikeCount != 1 {
		t.Fatalf("second like counted twice: %+v", result)
	}

	notifications, err := queries.CountUnreadNotifications(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatalf("owner has %d notifications for one like, want 1", notifications)
	}
}

func TestLikeOwnActivityIsForbidden(t *testing.T) {
	handlers, queries := newTestHandlers(t)
	ctx := context.Background()

	owner := createUser(t, ctx, queries, "owner", "everyone")
	activity := createActivity(t, ctx, queries, owner.ID, "everyone", "self-peak-test")
	params := map[string]string{"id": itoa(activity.ID)}

	// A repeat call must be refused too, not silently accepted by the
	// idempotency path.
	for attempt := 1; attempt <= 2; attempt++ {
		rec := post(t, handlers, handlers.Like, owner, "/api/activities/1/likes", params, "")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("attempt %d: status %d %s, want 403", attempt, rec.Code, rec.Body.String())
		}
		var envelope api.ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("attempt %d: decode: %v", attempt, err)
		}
		if envelope.Error.Code != apperr.CodeForbidden {
			t.Fatalf("attempt %d: code %q, want %q", attempt, envelope.Error.Code, apperr.CodeForbidden)
		}
	}

	count, err := queries.CountLikes(ctx, activity.ID)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("like count = %d, want 0 after self-peak refusal", count)
	}
	if _, err := queries.GetLike(ctx, activity.ID, owner.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetLike = %v, want sql.ErrNoRows", err)
	}
	notifications, err := queries.ListNotifications(ctx, owner.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(notifications) != 0 {
		t.Fatalf("self-peak created %d notifications, want 0", len(notifications))
	}
}

func TestCommentMentionPolicyAndNotifications(t *testing.T) {
	handlers, queries := newTestHandlers(t)
	ctx := context.Background()

	owner := createUser(t, ctx, queries, "owner", "everyone")
	author := createUser(t, ctx, queries, "author", "everyone")
	mentioned := createUser(t, ctx, queries, "mentioned", "everyone")
	createUser(t, ctx, queries, "blocked", "nobody")
	activity := createActivity(t, ctx, queries, owner.ID, "everyone", "comment-test")

	// mentioned follows the author, so the default `followers` policy allows it.
	if _, err := queries.UpdateUserPrivacy(ctx, db.UpdateUserPrivacyParams{
		ProfileVisibility: "everyone", ActivitiesVisibility: "everyone",
		FollowPolicy: "everyone", MentionPolicy: "followers",
		TrimScope: "all", TrimRadiusM: 200, UpdatedAt: 1, ID: mentioned.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := queries.UpsertFollow(ctx, mentioned.ID, author.ID, "accepted", 1); err != nil {
		t.Fatal(err)
	}

	body := `{"body":"nice one @mentioned and @blocked and @ghost"}`
	rec := post(t, handlers, handlers.PostComment, author, "/api/activities/1/comments",
		map[string]string{"id": itoa(activity.ID)}, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("comment: %d %s", rec.Code, rec.Body.String())
	}

	var created api.CommentCreated
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(created.Comment.Mentions) != 1 || created.Comment.Mentions[0] != "mentioned" {
		t.Fatalf("mentions = %v, want [mentioned]", created.Comment.Mentions)
	}
	if len(created.DroppedMentions) != 2 {
		t.Fatalf("dropped = %v, want the blocked and unknown users", created.DroppedMentions)
	}

	// The mentioned user gets exactly one mention notification, the blocked and
	// unknown users get none.
	for name, want := range map[string]int64{"mentioned": 1, "blocked": 0} {
		user, err := queries.GetUserByUsername(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := queries.CountUnreadNotifications(ctx, user.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s has %d notifications, want %d", name, got, want)
		}
	}

	// The activity owner gets a comment notification; the author gets nothing.
	ownerUnread, err := queries.CountUnreadNotifications(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ownerUnread != 1 {
		t.Errorf("owner has %d notifications, want 1 comment", ownerUnread)
	}
	authorUnread, err := queries.CountUnreadNotifications(ctx, author.ID)
	if err != nil {
		t.Fatal(err)
	}
	if authorUnread != 0 {
		t.Errorf("author notified about their own comment: %d", authorUnread)
	}
}

func TestCommentOnInvisibleActivityIsNotFound(t *testing.T) {
	handlers, queries := newTestHandlers(t)
	ctx := context.Background()

	owner := createUser(t, ctx, queries, "owner", "only_me")
	stranger := createUser(t, ctx, queries, "stranger", "everyone")
	activity := createActivity(t, ctx, queries, owner.ID, "default", "private-test")

	rec := post(t, handlers, handlers.PostComment, stranger, "/api/activities/1/comments",
		map[string]string{"id": itoa(activity.ID)}, `{"body":"hello"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("comment on a private activity returned %d, want 404", rec.Code)
	}
}

func itoa(value int64) string { return strconv.FormatInt(value, 10) }
