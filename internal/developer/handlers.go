// Package developer implements the documented public API under /api/v1. It is
// deliberately narrow: a developer key only ever reaches the key owner's own
// activities.
package developer

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/dto"
	"github.com/markbeep/hyl/internal/reqctx"
)

// Handlers serves /api/v1.
type Handlers struct {
	Q        *db.Queries
	Log      *zap.Logger
	Activity *activity.Handlers
}

// New builds the developer handlers.
func New(pool *sql.DB, log *zap.Logger, activityHandlers *activity.Handlers) *Handlers {
	return &Handlers{Q: db.New(pool), Log: log, Activity: activityHandlers}
}

// Me identifies the key owner, which is what a client uses to discover the
// account it is talking to.
func (h *Handlers) Me(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, api.DeveloperMe{
		ID:          user.ID,
		Username:    user.Username,
		Email:       user.Email,
		DisplayName: user.DisplayName,
		CreatedAt:   api.Timestamp(user.CreatedAt),
	})
}

// Upload accepts an activity file through the API. It reuses the manual upload
// path so metrics, deduplication and tombstones behave identically, and only
// the recorded source differs.
func (h *Handlers) Upload(c echo.Context) error {
	c.Set(activity.SourceContextKey, activity.SourceAPI)
	return h.Activity.Upload(c)
}

// List returns the key owner's activities.
func (h *Handlers) List(c echo.Context) error {
	return h.Activity.ListOwn(c)
}

// Get returns one activity with its full stored point stream, which is what a
// developer integrating hyl actually needs.
func (h *Handlers) Get(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	activityID, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	row, err := h.Q.GetActivity(ctx, activityID)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such activity")
	}
	if err != nil {
		return err
	}
	// A key never reaches another account's data, whatever the visibility says.
	if row.UserID != user.ID {
		return apperr.NotFound("no such activity")
	}

	points, err := h.Q.ListActivityPoints(ctx, activityID)
	if err != nil {
		return err
	}
	avatarID, err := h.avatarID(ctx, user.ID)
	if err != nil {
		return err
	}
	counts := dto.ActivityCounts{}
	if counts.LikeCount, err = h.Q.CountLikes(ctx, activityID); err != nil {
		return err
	}
	if counts.CommentCount, err = h.Q.CountComments(ctx, activityID); err != nil {
		return err
	}
	if counts.PhotoCount, err = h.Q.CountActivityMedia(ctx, activityID); err != nil {
		return err
	}

	// The key stays owner-only above. The summary map is the activity-page route,
	// so privacy zones, start and end trimming, and a hidden route match that page.
	opened, err := h.Activity.ForViewer(ctx, activityID, user.ID)
	if err != nil {
		return err
	}
	track, mapAvailable := opened.Route, opened.MapAvailable
	summary := dto.ActivitySummaryFromActivity(row, *user, avatarID, counts, track, mapAvailable, nil)

	return c.JSON(http.StatusOK, api.DeveloperActivity{
		ActivitySummary: summary,
		Source:          row.Source,
		SourceRef:       row.SourceRef,
		Points:          toDeveloperPoints(points),
	})
}

func (h *Handlers) avatarID(ctx context.Context, userID int64) (int64, error) {
	avatar, err := h.Q.GetActiveAvatar(ctx, userID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return avatar.ID, nil
}

func toDeveloperPoints(rows []db.ActivityPoint) []api.DeveloperPoint {
	points := make([]api.DeveloperPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, api.DeveloperPoint{
			Seq:        row.Seq,
			T:          api.Timestamp(row.T),
			ElapsedS:   row.ElapsedS,
			Lat:        row.Lat,
			Lon:        row.Lon,
			ElevationM: row.Ele,
			HeartRate:  row.Hr,
			Cadence:    row.Cad,
			PowerW:     row.Pwr,
			SpeedMps:   row.Spd,
			DistanceM:  row.DistM,
		})
	}
	return points
}
