package sync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
)

// Export pacing and retry limits.
const (
	exportBatchSize   = 50
	exportMinInterval = time.Second
	// Polling backs off geometrically from exportPollInitial to exportPollMax.
	// Strava's mean processing time is under two seconds, so the ordinary case
	// costs a single poll instead of the thirty an even 2s cadence would spend.
	exportPollInitial = time.Second
	exportPollMax     = 15 * time.Second
	exportPollTimeout = 5 * time.Minute
	// exportRetryFloor paces the drain after a rate limit that carried no
	// usable retry hint.
	exportRetryFloor    = 15 * time.Minute
	exportMaxAttempts   = 5
	exportTargetStrava  = "strava"
	exportStatusPending = "pending"
	exportStatusSent    = "sent"
	exportStatusError   = "error"
)

// Exporter drains the activity_exports queue. It runs on the same goroutine as
// the import worker, so Strava never sees more than one request at a time.
type Exporter struct {
	Q           *db.Queries
	Cfg         config.Config
	Log         *zap.Logger
	Cipher      *secrets.Cipher
	connections *Connections

	// newClient is a seam for tests.
	newClient func(accessToken string) *StravaClient
	// sleep is overridable so tests do not wait for the pacing delays.
	sleep func(context.Context, time.Duration)

	// throttledUntil is when Strava's request budget is expected to be free
	// again after a rate-limit response. Strava's limits are per application,
	// so one user tripping them pauses every user's exports. It is only ever
	// touched from the single worker goroutine, so it needs no lock.
	throttledUntil time.Time
}

// NewExporter builds the export drainer.
func NewExporter(pool *sql.DB, cfg config.Config, log *zap.Logger, cipher *secrets.Cipher) *Exporter {
	connections := NewConnections(pool, cfg, log, cipher)
	return &Exporter{
		Q: connections.Q, Cfg: cfg, Log: log, Cipher: cipher,
		connections: connections,
		newClient:   func(token string) *StravaClient { return NewStravaClient(cfg, token) },
		sleep:       sleepContext,
	}
}

// Drain pushes pending exports, at most one batch per call.
func (e *Exporter) Drain(ctx context.Context) {
	if !e.Cfg.StravaEnabled() {
		return
	}
	if time.Now().Before(e.throttledUntil) {
		return
	}
	pending, err := e.Q.ListPendingExports(ctx, exportBatchSize)
	if err != nil {
		e.Log.Error("listing pending exports failed", zap.Error(err))
		return
	}
	for _, row := range pending {
		if ctx.Err() != nil {
			return
		}
		stop, err := e.exportOne(ctx, row)
		if err != nil {
			e.Log.Warn("export failed", zap.Int64("export_id", row.ID), zap.Error(err))
		}
		if stop {
			return
		}
		e.sleep(ctx, exportMinInterval)
	}
}

// exportOne pushes one activity. It reports whether the whole drain should stop
// (a rate limit or a missing credential would make the next attempt fail too).
func (e *Exporter) exportOne(ctx context.Context, row db.ActivityExport) (stop bool, err error) {
	activityRow, err := e.Q.GetActivity(ctx, row.ActivityID)
	if errors.Is(err, sql.ErrNoRows) {
		// The activity is gone; the export row went with it via the cascade, so
		// this can only happen on a race.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	conn, err := e.Q.GetConnection(ctx, row.UserID, KindStravaOAuth)
	if errors.Is(err, sql.ErrNoRows) {
		// Strava was disconnected after the export was queued.
		return false, e.markError(ctx, row, "the Strava connection was removed")
	}
	if err != nil {
		return false, err
	}

	now := time.Now()
	conn, err = e.connections.Refresh(ctx, conn, now)
	if err != nil {
		if stravaRejectedCredential(err) {
			e.markConnectionReauthorize(ctx, conn)
			return true, e.markError(ctx, row, "reauthorize")
		}
		// A refresh that fails for any other reason is retried, and eventually
		// gives up, through the same attempt accounting as an upload failure.
		return false, e.reschedule(ctx, row, err.Error())
	}
	accessToken, err := e.Cipher.DecryptString(conn.AccessTokenCipher)
	if err != nil {
		return true, e.markError(ctx, row, "reauthorize")
	}

	points, err := e.Q.ListActivityPoints(ctx, row.ActivityID)
	if err != nil {
		return false, err
	}
	var buffer bytes.Buffer
	if err := activity.WriteFIT(&buffer, activityRow, toActivityPoints(points)); err != nil {
		return false, e.markError(ctx, row, "hyl could not build a FIT file for this activity")
	}

	client := e.newClient(accessToken)
	upload, err := client.UploadFit(ctx, fmt.Sprintf("hyl-%d.fit", activityRow.ID),
		titleOrFallback(activityRow), conn.ExportMessage,
		StravaSportType(externalSport(activityRow), activityRow.Sport),
		row.ExternalID, buffer.Bytes())
	if err != nil {
		return e.handleProviderError(ctx, row, err)
	}

	deadline := time.Now().Add(exportPollTimeout)
	wait := exportPollInitial
	for {
		// Strava reports both a failed upload and an upload whose activity was
		// deleted afterwards through these two status strings; neither is worth
		// retrying, and neither is "still processing".
		switch status := strings.ToLower(upload.Status); {
		case strings.Contains(status, "ready"):
			return false, e.markSent(ctx, row, upload.ActivityID)
		case strings.Contains(status, "error"), strings.Contains(status, "deleted"):
			message := strings.TrimSpace(upload.Error)
			if message == "" {
				message = upload.Status
			}
			return false, e.markError(ctx, row, message)
		}
		if time.Now().After(deadline) {
			// Still processing: keep it pending and try again next pass.
			return false, e.reschedule(ctx, row, "strava is still processing this upload")
		}
		e.sleep(ctx, wait)
		if wait *= 2; wait > exportPollMax {
			wait = exportPollMax
		}
		upload, err = client.GetUpload(ctx, upload.ID)
		if err != nil {
			return e.handleProviderError(ctx, row, err)
		}
	}
}

// stravaRejectedCredential reports a token response that means the stored
// refresh token is no longer accepted. Strava answers 400 with a RefreshToken
// error for an invalid refresh token; a 400 for a bad client id or secret is a
// configuration failure and must not park or delete a healthy connection.
// 401 and 403 mean the grant is gone.
func stravaRejectedCredential(err error) bool {
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		return false
	}
	switch providerErr.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return true
	case http.StatusBadRequest:
		return stravaInvalidRefreshToken(providerErr.Message)
	default:
		return false
	}
}

func stravaInvalidRefreshToken(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "refreshtoken") || strings.Contains(lower, "refresh_token")
}

// markConnectionReauthorize parks the Strava connection until the athlete
// reconnects. A failed token write must not call this.
func (e *Exporter) markConnectionReauthorize(ctx context.Context, conn db.Connection) {
	message := "reauthorize"
	nextAt := time.Now().Add(backoffMax).Unix()
	if _, err := e.Q.UpdateConnectionError(ctx, &message, &nextAt, time.Now().Unix(), conn.ID); err != nil {
		e.Log.Error("recording a strava reauthorization failed", zap.Error(err))
	}
}

// handleProviderError classifies a provider failure.
func (e *Exporter) handleProviderError(ctx context.Context, row db.ActivityExport, err error) (bool, error) {
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) {
		return false, err
	}
	if providerErr.StatusCode == http.StatusTooManyRequests {
		// A rate limit says nothing about the activity, so it must not spend an
		// attempt: five throttled ticks used to fail the export permanently.
		// Strava's budget is per application, so the whole drain pauses too.
		e.throttle(providerErr.RetryAfter)
		return true, e.deferRow(ctx, row, providerErr.Message)
	}
	if providerErr.NeedsReauthorization() {
		return true, e.markError(ctx, row, "reauthorize")
	}
	return false, e.reschedule(ctx, row, providerErr.Message)
}

// throttle pauses the drain until the provider's window is expected to be free.
func (e *Exporter) throttle(after time.Duration) {
	if after <= 0 {
		after = exportRetryFloor
	}
	if until := time.Now().Add(after); until.After(e.throttledUntil) {
		e.throttledUntil = until
	}
}

// deferRow keeps a row pending and records the reason without counting an
// attempt, so a transient provider-side delay cannot exhaust the budget.
func (e *Exporter) deferRow(ctx context.Context, row db.ActivityExport, message string) error {
	_, err := e.Q.UpdateExportStatus(ctx, exportStatusPending, nil, row.Attempts,
		&message, time.Now().Unix(), row.ID)
	return err
}

func (e *Exporter) markSent(ctx context.Context, row db.ActivityExport, remoteID int64) error {
	remote := ""
	if remoteID != 0 {
		remote = fmt.Sprintf("%d", remoteID)
	}
	_, err := e.Q.UpdateExportStatus(ctx, exportStatusSent, nilIfEmpty(remote), row.Attempts,
		nil, time.Now().Unix(), row.ID)
	return err
}

func (e *Exporter) markError(ctx context.Context, row db.ActivityExport, message string) error {
	_, err := e.Q.UpdateExportStatus(ctx, exportStatusError, nil, row.Attempts+1,
		&message, time.Now().Unix(), row.ID)
	return err
}

// reschedule keeps the row pending and counts the attempt, so a permanently
// broken upload stops after exportMaxAttempts.
func (e *Exporter) reschedule(ctx context.Context, row db.ActivityExport, message string) error {
	attempts := row.Attempts + 1
	status := exportStatusPending
	if attempts >= exportMaxAttempts {
		status = exportStatusError
	}
	_, err := e.Q.UpdateExportStatus(ctx, status, nil, attempts, &message, time.Now().Unix(), row.ID)
	return err
}

// ExportQueuer queues an activity for upload to Strava when the uploading user
// has a Strava connection that asked for automatic exports. It is the single
// automatic-export gate: the intervals connection's own flag is inert, because
// the target of the upload is what decides.
type ExportQueuer struct {
	queries *db.Queries
}

// NewExportQueuer builds the automatic-export gate.
func NewExportQueuer(q *db.Queries) *ExportQueuer {
	return &ExportQueuer{queries: q}
}

// Queue loads the user's strava_oauth connection and, when automatic export is
// enabled, inserts a pending export row. Nil is returned when no connection
// exists, when the flag is off, or when the export is already queued — a caller
// only cares about real failures.
func (q *ExportQueuer) Queue(ctx context.Context, a db.Activity) error {
	conn, err := q.queries.GetConnection(ctx, a.UserID, KindStravaOAuth)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !conn.AutoExport {
		return nil
	}
	_, err = QueueExport(ctx, q.queries, a, exportTargetStrava)
	return err
}

// QueueExport inserts a pending export row. It reports false when one is
// already queued or sent, which the handler turns into a 409.
func QueueExport(ctx context.Context, q *db.Queries, activity db.Activity, target string) (bool, error) {
	now := time.Now().Unix()
	affected, err := q.CreateExport(ctx, activity.ID, activity.UserID, target, activity.DedupeHash, now, now)
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func titleOrFallback(activityRow db.Activity) string {
	if strings.TrimSpace(activityRow.Title) != "" {
		return activityRow.Title
	}
	return activity.DefaultTitle(activityRow.Sport, time.Unix(activityRow.StartedAt, 0).UTC())
}

// externalSport dereferences the provider's activity type, when the activity has
// one.
func externalSport(a db.Activity) string {
	if a.ExternalSport == nil {
		return ""
	}
	return *a.ExternalSport
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// toActivityPoints converts stored rows into the export writer's input.
func toActivityPoints(rows []db.ActivityPoint) []activity.Point {
	points := make([]activity.Point, 0, len(rows))
	for _, row := range rows {
		point := activity.Point{
			Seq: row.Seq, T: row.T, ElapsedS: row.ElapsedS,
			Lat: row.Lat, Lon: row.Lon, Ele: row.Ele, Spd: row.Spd, DistM: row.DistM,
		}
		if row.Hr != nil {
			hr := int(*row.Hr)
			point.HR = &hr
		}
		if row.Cad != nil {
			cadence := int(*row.Cad)
			point.Cad = &cadence
		}
		if row.Pwr != nil {
			power := int(*row.Pwr)
			point.Pwr = &power
		}
		points = append(points, point)
	}
	return points
}
