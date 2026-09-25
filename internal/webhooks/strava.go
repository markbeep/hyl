package webhooks

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
	syncpkg "github.com/markbeep/hyl/internal/sync"
)

// stravaVerifyCooldown bounds how often one athlete's deauthorization claim may
// cost a token round trip. Strava signs neither the challenge GET nor the event
// POST, so an unauthenticated caller can replay events, and every check rotates
// the stored refresh token.
const stravaVerifyCooldown = 5 * time.Minute

// Strava handles Strava's subscription verification and deauthorization
// webhook.
//
// Strava signs neither the verification GET nor the event POST, and athlete ids
// are public, so the event body is not evidence that anything happened: an
// attacker who knows an athlete id could otherwise delete that athlete's
// connection, its queued exports and its import rules with one request. A claim
// is therefore confirmed with Strava, whose token endpoint is the only
// authority on whether access still exists.
type Strava struct {
	Q           *db.Queries
	Cfg         config.Config
	Log         *zap.Logger
	connections *syncpkg.Connections

	mu       sync.Mutex
	verified map[int64]time.Time
}

// NewStrava builds the Strava webhook handler.
func NewStrava(pool *sql.DB, cfg config.Config, log *zap.Logger, cipher *secrets.Cipher) *Strava {
	connections := syncpkg.NewConnections(pool, cfg, log, cipher)
	return &Strava{
		Q: connections.Q, Cfg: cfg, Log: log,
		connections: connections,
	}
}

// Verify answers Strava's subscription handshake.
func (h *Strava) Verify(c echo.Context) error {
	if h.Cfg.StravaWebhookVerifyToken == "" {
		return echo.NotFoundHandler(c)
	}
	if c.QueryParam("hub.mode") != "subscribe" ||
		c.QueryParam("hub.verify_token") != h.Cfg.StravaWebhookVerifyToken {
		return echo.NewHTTPError(http.StatusForbidden, "invalid webhook verification")
	}
	return c.JSON(http.StatusOK, map[string]string{"hub.challenge": c.QueryParam("hub.challenge")})
}

// Handle processes a deauthorization event.
func (h *Strava) Handle(c echo.Context) error {
	if h.Cfg.StravaWebhookVerifyToken == "" {
		return echo.NotFoundHandler(c)
	}
	var payload struct {
		ObjectType string `json:"object_type"`
		AspectType string `json:"aspect_type"`
		ObjectID   int64  `json:"object_id"`
		OwnerID    int64  `json:"owner_id"`
		Updates    struct {
			Authorized string `json:"authorized"`
		} `json:"updates"`
	}
	if err := c.Bind(&payload); err != nil {
		return c.NoContent(http.StatusOK)
	}
	// Everything hyl does not act on is acknowledged so Strava stops retrying.
	if payload.ObjectType != "athlete" || payload.OwnerID == 0 || payload.Updates.Authorized != "false" {
		return c.NoContent(http.StatusOK)
	}

	ctx := c.Request().Context()
	athleteID := strconv.FormatInt(payload.OwnerID, 10)
	conn, err := h.Q.GetConnectionByExternalID(ctx, syncpkg.KindStravaOAuth, &athleteID)
	if errors.Is(err, sql.ErrNoRows) {
		h.Log.Debug("deauthorization for an unknown athlete", zap.Int64("owner_id", payload.OwnerID))
		return c.NoContent(http.StatusOK)
	}
	if err != nil {
		return c.NoContent(http.StatusOK)
	}
	if !h.claimDue(payload.OwnerID) {
		h.Log.Debug("ignoring a repeated deauthorization claim", zap.Int64("owner_id", payload.OwnerID))
		return c.NoContent(http.StatusOK)
	}
	if err := h.connections.Deauthorize(ctx, conn); err != nil {
		// Acknowledge after logging: a 500 cannot usefully retry because
		// claimDue already started the cooldown, and a retry after a
		// rotated-but-unstored pair would look like a genuine revocation.
		h.Log.Error("confirming the strava deauthorization failed", zap.Error(err), zap.Int64("user_id", conn.UserID))
		return c.NoContent(http.StatusOK)
	}
	return c.NoContent(http.StatusOK)
}

// claimDue reports whether this athlete's claim should be checked now, and
// records the attempt so a replayed event cannot make hyl hammer Strava.
func (h *Strava) claimDue(ownerID int64) bool {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.verified == nil {
		h.verified = make(map[int64]time.Time)
	}
	if last, ok := h.verified[ownerID]; ok && now.Sub(last) < stravaVerifyCooldown {
		return false
	}
	if len(h.verified) > 512 {
		for id, at := range h.verified {
			if now.Sub(at) >= stravaVerifyCooldown {
				delete(h.verified, id)
			}
		}
	}
	h.verified[ownerID] = now
	return true
}
