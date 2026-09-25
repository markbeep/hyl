package sync

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
	"github.com/markbeep/hyl/internal/secrets"
)

// oauthStateCookie carries the OAuth state between start and callback.
const oauthStateCookie = "hyl_oauth_state"

// Handlers serves the connections, import rules and sync endpoints.
type Handlers struct {
	Pool   *sql.DB
	Q      *db.Queries
	Cfg    config.Config
	Log    *zap.Logger
	Cipher *secrets.Cipher
	Worker *Worker
}

// NewHandlers builds the sync HTTP surface.
func NewHandlers(pool *sql.DB, cfg config.Config, log *zap.Logger, cipher *secrets.Cipher, worker *Worker) *Handlers {
	return &Handlers{Pool: pool, Q: db.New(pool), Cfg: cfg, Log: log, Cipher: cipher, Worker: worker}
}

// List returns the connections, the full import-rule matrix and the last runs.
func (h *Handlers) List(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	connections, err := h.Q.ListConnectionsForUser(ctx, user.ID)
	if err != nil {
		return err
	}
	rules, err := h.importRuleMatrix(ctx, user.ID, connections)
	if err != nil {
		return err
	}
	runs, err := h.Q.ListRecentSyncRuns(ctx, user.ID, 10)
	if err != nil {
		return err
	}

	response := api.ConnectionsResponse{
		Connections: make([]api.Connection, 0, len(connections)),
		ImportRules: rules,
		LastRuns:    make([]api.SyncRun, 0, len(runs)),
	}
	for _, conn := range connections {
		response.Connections = append(response.Connections, connectionDTO(conn))
	}
	for _, run := range runs {
		dtoRun := api.SyncRun{
			ConnectionKind: run.ConnectionKind,
			StartedAt:      api.Timestamp(run.StartedAt),
			FinishedAt:     api.TimestampPtr(run.FinishedAt),
			Imported:       run.Imported,
			Skipped:        run.Skipped,
			Error:          run.Error,
		}
		response.LastRuns = append(response.LastRuns, dtoRun)
	}
	return c.JSON(http.StatusOK, response)
}

// ConnectIntervalsAPIKey stores a personal intervals.icu API key after
// validating it against the API once.
func (h *Handlers) ConnectIntervalsAPIKey(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	var req struct {
		APIKey    string `json:"apiKey"`
		AthleteID string `json:"athleteId"`
	}
	if err := c.Bind(&req); err != nil {
		return apperr.ErrInvalidRequest
	}
	apiKey := strings.TrimSpace(req.APIKey)
	if apiKey == "" {
		return apperr.BadRequest("an intervals.icu API key is required")
	}
	athleteID := strings.TrimSpace(req.AthleteID)
	defaulted := athleteID == ""
	if defaulted {
		// "0" is the key owner, which is what a self-hoster wants.
		athleteID = "0"
	}

	ctx := c.Request().Context()
	probe := &IntervalsClient{
		BaseURL:   intervalsBaseURL,
		APIKey:    apiKey,
		UserAgent: h.userAgent(),
		HTTP:      &http.Client{Timeout: 20 * time.Second},
	}
	newest := time.Now()
	oldest := newest.AddDate(0, 0, -7)
	if _, err := probe.ListActivities(ctx, athleteID, oldest.Format("2006-01-02"), newest.Format("2006-01-02"), 1); err != nil {
		h.Log.Warn("validating an intervals API key failed", zap.Error(err))
		return apperr.BadRequest("intervals.icu rejected that API key or athlete id")
	}
	if defaulted {
		// Webhook events always name the real athlete, so the placeholder has
		// to be traded for the id the events will carry.
		resolved, err := probe.AthleteID(ctx)
		if err != nil {
			h.Log.Warn("resolving the intervals athlete id failed", zap.Error(err))
		} else {
			athleteID = resolved
		}
	}

	sealed, err := h.Cipher.EncryptString(apiKey)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	conn, err := h.Q.UpsertConnection(ctx, db.UpsertConnectionParams{
		UserID: user.ID, Kind: KindIntervalsAPIKey, ExternalAthleteID: &athleteID,
		AccessTokenCipher: sealed, AutoExport: false, ExportMessage: "Imported from hyl",
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	h.Worker.Trigger(user.ID)
	return c.JSON(http.StatusOK, connectionDTO(conn))
}

// StartIntervalsOAuth redirects to intervals.icu's consent screen.
func (h *Handlers) StartIntervalsOAuth(c echo.Context) error {
	if _, err := reqctx.RequireUser(c); err != nil {
		return err
	}
	if !h.Cfg.IntervalsOAuthEnabled() {
		return apperr.NotFound("intervals.icu OAuth is not configured on this instance")
	}
	state, err := randomState()
	if err != nil {
		return err
	}
	c.SetCookie(&http.Cookie{
		Name: oauthStateCookie, Value: state, Path: "/", MaxAge: 900,
		HttpOnly: true, Secure: h.Cfg.IsHTTPS(), SameSite: http.SameSiteLaxMode,
	})
	return c.Redirect(http.StatusTemporaryRedirect, IntervalsAuthorizeURL(h.Cfg, state))
}

// IntervalsCallback exchanges the authorization code and stores the token.
func (h *Handlers) IntervalsCallback(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	if !h.Cfg.IntervalsOAuthEnabled() {
		return apperr.NotFound("intervals.icu OAuth is not configured on this instance")
	}
	state, err := c.Cookie(oauthStateCookie)
	if err != nil || state.Value == "" || state.Value != c.QueryParam("state") {
		return apperr.BadRequest("the intervals.icu authorization did not match this session")
	}
	c.SetCookie(&http.Cookie{
		Name: oauthStateCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: h.Cfg.IsHTTPS(), SameSite: http.SameSiteLaxMode,
	})
	if errParam := c.QueryParam("error"); errParam != "" {
		return apperr.BadRequest("intervals.icu refused the authorization: %s", errParam)
	}

	ctx := c.Request().Context()
	token, athleteID, err := ExchangeIntervalsCode(ctx, h.Cfg, c.QueryParam("code"))
	if err != nil {
		return err
	}
	sealed, err := h.Cipher.EncryptString(token)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	if _, err := h.Q.UpsertConnection(ctx, db.UpsertConnectionParams{
		UserID: user.ID, Kind: KindIntervalsOAuth, ExternalAthleteID: &athleteID,
		AccessTokenCipher: sealed, AutoExport: false, ExportMessage: "Imported from hyl",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	h.Worker.Trigger(user.ID)
	return c.Redirect(http.StatusSeeOther, "/settings")
}

// UpdateConnection changes the auto-export switch and message.
func (h *Handlers) UpdateConnection(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	var req struct {
		AutoExport    *bool   `json:"autoExport"`
		ExportMessage *string `json:"exportMessage"`
	}
	if err := c.Bind(&req); err != nil {
		return apperr.ErrInvalidRequest
	}
	ctx := c.Request().Context()
	kind := c.Param("kind")

	conn, err := h.Q.GetConnection(ctx, user.ID, kind)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such connection")
	}
	if err != nil {
		return err
	}

	autoExport := conn.AutoExport
	if req.AutoExport != nil {
		autoExport = *req.AutoExport
	}
	message := conn.ExportMessage
	if req.ExportMessage != nil {
		message = strings.TrimSpace(*req.ExportMessage)
		if len(message) > 200 {
			return apperr.BadRequest("the export message must be at most 200 characters")
		}
	}
	if _, err := h.Q.UpdateConnectionSettings(ctx, autoExport, message, time.Now().Unix(), user.ID, kind); err != nil {
		return err
	}
	updated, err := h.Q.GetConnection(ctx, user.ID, kind)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, connectionDTO(updated))
}

// DeleteConnection disconnects a provider. The cleanup lives on Connections.
func (h *Handlers) DeleteConnection(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	connections := NewConnections(h.Pool, h.Cfg, h.Log, h.Cipher)
	if err := connections.Disconnect(c.Request().Context(), user.ID, c.Param("kind")); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// ListImportRules returns the matrix on its own.
func (h *Handlers) ListImportRules(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	connections, err := h.Q.ListConnectionsForUser(ctx, user.ID)
	if err != nil {
		return err
	}
	rules, err := h.importRuleMatrix(ctx, user.ID, connections)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, rules)
}

// PutImportRules upserts whatever the settings page sends.
func (h *Handlers) PutImportRules(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	var req struct {
		Rules []api.ImportRule `json:"rules"`
	}
	if err := c.Bind(&req); err != nil {
		return apperr.ErrInvalidRequest
	}
	ctx := c.Request().Context()
	known := make(map[string]bool, len(activity.SportKeys())+1)
	known["all"] = true
	for _, key := range activity.SportKeys() {
		known[key] = true
	}

	for _, rule := range req.Rules {
		if !known[rule.Sport] {
			return apperr.BadRequest("unknown sport %q", rule.Sport)
		}
		if !h.validConnectionKind(rule.ConnectionKind) {
			return apperr.BadRequest("unknown connection %q", rule.ConnectionKind)
		}
		if err := h.Q.UpsertImportRule(ctx, user.ID, rule.ConnectionKind, rule.Sport, rule.Enabled, time.Now().Unix()); err != nil {
			return err
		}
	}
	connections, err := h.Q.ListConnectionsForUser(ctx, user.ID)
	if err != nil {
		return err
	}
	rules, err := h.importRuleMatrix(ctx, user.ID, connections)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, rules)
}

// RunSync triggers an immediate pass for the caller.
func (h *Handlers) RunSync(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	h.Worker.Trigger(user.ID)
	return c.JSON(http.StatusAccepted, map[string]string{"status": "queued"})
}

// importRuleMatrix materialises one row per connection kind and sport, so the
// settings page never has to guess a default.
func (h *Handlers) importRuleMatrix(ctx context.Context, userID int64, connections []db.Connection) ([]api.ImportRule, error) {
	stored, err := h.Q.ListImportRules(ctx, userID)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]bool, len(stored))
	for _, row := range stored {
		byKey[row.ConnectionKind+"|"+row.Sport] = row.Enabled
	}

	sports := append([]string{"all"}, activity.SportKeys()...)
	rules := make([]api.ImportRule, 0, len(connections)*len(sports))
	for _, conn := range connections {
		for _, sport := range sports {
			enabled, ok := byKey[conn.Kind+"|"+sport]
			if !ok {
				enabled = true
			}
			rules = append(rules, api.ImportRule{ConnectionKind: conn.Kind, Sport: sport, Enabled: enabled})
		}
	}
	return rules, nil
}

func (h *Handlers) validConnectionKind(kind string) bool {
	switch kind {
	case KindIntervalsAPIKey, KindIntervalsOAuth, KindStravaOAuth:
		return true
	default:
		return false
	}
}

func (h *Handlers) userAgent() string {
	return "hyl/" + h.Cfg.Version + " (+" + strings.TrimSuffix(h.Cfg.BaseURL, "/") + ")"
}

func connectionDTO(conn db.Connection) api.Connection {
	reauthorize := conn.LastError != nil && *conn.LastError == "reauthorize"
	return api.Connection{
		Kind:          conn.Kind,
		AthleteID:     conn.ExternalAthleteID,
		AutoExport:    conn.AutoExport,
		ExportMessage: conn.ExportMessage,
		LastError:     conn.LastError,
		LastSuccessAt: api.TimestampPtr(conn.LastSuccessAt),
		NextAttemptAt: api.TimestampPtr(conn.NextAttemptAt),
		Reauthorize:   reauthorize,
	}
}

func randomState() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
