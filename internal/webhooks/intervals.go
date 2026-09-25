// Package webhooks receives the provider callbacks hyl cannot poll for.
package webhooks

import (
	"crypto/subtle"
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/sync"
)

// Intervals handles intervals.icu's activity webhook.
type Intervals struct {
	Q      *db.Queries
	Cfg    config.Config
	Log    *zap.Logger
	Worker *sync.Worker
	// Activities applies provider-side deletions through the activity store;
	// optional for webhook handlers that only trigger imports.
	Activities *activity.Store
}

// NewIntervals builds the intervals webhook handler.
func NewIntervals(pool *sql.DB, cfg config.Config, log *zap.Logger, worker *sync.Worker, activities *activity.Store) *Intervals {
	return &Intervals{Q: db.New(pool), Cfg: cfg, Log: log, Worker: worker, Activities: activities}
}

// Handle accepts an event batch. The shared secret is verified in constant time
// before anything else happens, and the endpoint always answers 200 for an
// authenticated payload so intervals does not retry with backoff.
func (h *Intervals) Handle(c echo.Context) error {
	if h.Cfg.IntervalsWebhookSecret == "" {
		return echo.NotFoundHandler(c)
	}
	var payload struct {
		Secret string `json:"secret"`
		Events []struct {
			AthleteID string `json:"athlete_id"`
			Type      string `json:"type"`
			Timestamp string `json:"timestamp"`
			Activity  struct {
				ID string `json:"id"`
			} `json:"activity"`
		} `json:"events"`
	}
	if err := c.Bind(&payload); err != nil {
		return c.NoContent(http.StatusOK)
	}
	if subtle.ConstantTimeCompare([]byte(payload.Secret), []byte(h.Cfg.IntervalsWebhookSecret)) != 1 {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid webhook secret")
	}

	ctx := c.Request().Context()
	for _, event := range payload.Events {
		if event.AthleteID == "" {
			continue
		}
		switch event.Type {
		case "ACTIVITY_UPLOADED", "ACTIVITY_ANALYZED", "ACTIVITY_DELETED":
		default:
			continue
		}
		// Resolve the athlete to the account that owns the connection, then act
		// on the event. The import guards make a redundant pass harmless, which
		// is why no per-activity import work is queued here.
		athleteID := event.AthleteID
		connection, err := h.Q.GetConnectionByExternalID(ctx, sync.KindIntervalsOAuth, &athleteID)
		if err != nil {
			connection, err = h.Q.GetConnectionByExternalID(ctx, sync.KindIntervalsAPIKey, &athleteID)
		}
		if err != nil {
			h.Log.Debug("webhook for an unknown athlete", zap.String("athlete_id", event.AthleteID))
			continue
		}
		if event.Type == "ACTIVITY_DELETED" {
			// A deletion is the one event a re-import pass can never converge
			// on, so it is applied directly rather than turned into a trigger.
			if h.Activities != nil {
				if err := h.Activities.DeleteProviderActivity(ctx, connection.UserID, connection.Kind, event.Activity.ID); err != nil {
					h.Log.Warn("propagating a provider deletion failed",
						zap.String("activity_id", event.Activity.ID), zap.Error(err))
				}
			}
			continue
		}
		h.Worker.Trigger(connection.UserID)
	}
	return c.NoContent(http.StatusOK)
}
