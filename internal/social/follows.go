package social

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/dto"
	"github.com/markbeep/hyl/internal/reqctx"
)

// Notification kinds stored in notifications.kind.
const (
	KindFollow         = "follow"
	KindFollowRequest  = "follow_request"
	KindFollowAccepted = "follow_accepted"
	KindLike           = "like"
	KindComment        = "comment"
	KindMention        = "mention"
)

// Handlers implements the social HTTP surface: follows, likes, comments,
// mentions and notifications.
type Handlers struct {
	Pool *sql.DB
	Q    *db.Queries
	Log  *zap.Logger
	// ForViewer is the activity-page read. Likes and comments use it so a
	// hidden activity is the same not-found as a missing id. Production
	// always sets it; a missing read fails closed.
	ForViewer func(ctx context.Context, activityID, viewerID int64) (db.Activity, error)
}

// New builds the social handlers.
func New(pool *sql.DB, log *zap.Logger) *Handlers {
	return &Handlers{Pool: pool, Q: db.New(pool), Log: log}
}

// Follow follows another user, or files a follow request when the target's
// policy is on_request. Repeating a follow is idempotent and never downgrades
// an accepted follow back to pending.
func (h *Handlers) Follow(c echo.Context) error {
	viewer, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	target, err := h.userByUsername(ctx, c.Param("username"))
	if err != nil {
		return err
	}
	if target.ID == viewer.ID {
		return apperr.BadRequest("you cannot follow yourself")
	}

	now := time.Now().Unix()
	status := "accepted"
	if target.FollowPolicy == "on_request" {
		status = "pending"
	}
	if err := h.Q.UpsertFollow(ctx, viewer.ID, target.ID, status, now); err != nil {
		return err
	}

	// Re-read the row: an already-accepted follow stays accepted.
	follow, err := h.Q.GetFollow(ctx, viewer.ID, target.ID)
	if err != nil {
		return err
	}
	if follow.Status == "pending" {
		if err := h.notify(ctx, target.ID, viewer.ID, KindFollowRequest, nil, nil); err != nil {
			return err
		}
	} else {
		if err := h.notify(ctx, target.ID, viewer.ID, KindFollow, nil, nil); err != nil {
			return err
		}
	}

	count, err := h.Q.CountFollowers(ctx, target.ID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, api.FollowResult{
		FollowState:   dto.FollowState(viewer.ID, target.ID, follow.Status == "accepted", false, follow.Status == "pending"),
		FollowerCount: count,
	})
}

// Unfollow removes the viewer's follow or outstanding request.
func (h *Handlers) Unfollow(c echo.Context) error {
	viewer, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	target, err := h.userByUsername(ctx, c.Param("username"))
	if err != nil {
		return err
	}
	if _, err := h.Q.DeleteFollow(ctx, viewer.ID, target.ID); err != nil {
		return err
	}
	count, err := h.Q.CountFollowers(ctx, target.ID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, api.FollowResult{FollowState: "none", FollowerCount: count})
}

// AcceptFollow accepts a pending request from :username.
func (h *Handlers) AcceptFollow(c echo.Context) error {
	viewer, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	requester, err := h.userByUsername(ctx, c.Param("username"))
	if err != nil {
		return err
	}

	respondedAt := time.Now().Unix()
	affected, err := h.Q.AcceptFollow(ctx, &respondedAt, requester.ID, viewer.ID)
	if err != nil {
		return err
	}
	if affected == 0 {
		return apperr.NotFound("no follow request from %s", requester.Username)
	}
	if err := h.notify(ctx, requester.ID, viewer.ID, KindFollowAccepted, nil, nil); err != nil {
		return err
	}
	count, err := h.Q.CountFollowers(ctx, viewer.ID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, api.FollowResult{FollowState: "following", FollowerCount: count})
}

// RejectFollow rejects an incoming request or withdraws the viewer's own.
func (h *Handlers) RejectFollow(c echo.Context) error {
	viewer, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	other, err := h.userByUsername(ctx, c.Param("username"))
	if err != nil {
		return err
	}

	// The followee rejects an incoming request; the follower withdraws theirs.
	if _, err := h.Q.DeleteFollow(ctx, other.ID, viewer.ID); err != nil {
		return err
	}
	if _, err := h.Q.DeleteFollow(ctx, viewer.ID, other.ID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// notify inserts a notification, silently skipping self-notification. It is
// what keeps "never notify the actor about their own action" in one place.
func (h *Handlers) notify(ctx context.Context, recipient, actor int64, kind string, activityID, commentID *int64) error {
	if recipient == actor {
		return nil
	}
	_, err := h.Q.CreateNotification(ctx, recipient, actor, kind, activityID, commentID, time.Now().Unix())
	return err
}

// userByUsername resolves the :username path parameter.
func (h *Handlers) userByUsername(ctx context.Context, username string) (db.User, error) {
	var user db.User
	if username == "" {
		return user, apperr.NotFound("unknown user")
	}
	user, err := h.Q.GetUserByUsername(ctx, username)
	if errors.Is(err, sql.ErrNoRows) {
		return user, apperr.NotFound("unknown user")
	}
	if err != nil {
		return user, err
	}
	return user, nil
}
