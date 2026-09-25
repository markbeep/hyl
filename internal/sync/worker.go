package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/auth"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
)

// Sync window parameters.
const (
	// syncLookbackDays bounds the first import so a new connection does not
	// walk a decade of history.
	syncLookbackDays = 90
	// syncChunkDays is the width of one intervals.icu query: the API has no
	// pagination cursor, so the window is narrowed instead.
	syncChunkDays = 14
	// syncPageLimit caps one chunk's response.
	syncPageLimit = 500
	// backoffBase and backoffMax bound the retry delay after a failure.
	backoffBase = 5 * time.Minute
	backoffMax  = 6 * time.Hour
)

// Ingester is the activity store the worker writes through.
type Ingester interface {
	Ingest(ctx context.Context, ownerID int64, r io.Reader, opts activity.IngestOptions) (db.Activity, error)
}

// Provider is the slice of the intervals.icu API the worker needs.
type Provider interface {
	ListActivities(ctx context.Context, athleteID, oldest, newest string, limit int) ([]IntervalsActivity, error)
	DownloadFit(ctx context.Context, activityID string) (io.ReadCloser, error)
	AthleteID(ctx context.Context) (string, error)
}

// Worker runs the background import. One goroutine owns every provider call:
// each pass is fully sequential.
type Worker struct {
	pool   *sql.DB
	q      *db.Queries
	cfg    config.Config
	log    *zap.Logger
	cipher *secrets.Cipher
	ingest Ingester

	trigger chan int64
	running bool

	// exporter drains the outbound queue after every import pass; nil disables
	// exporting (tests that only exercise importing).
	exporter *Exporter
	// sessions is the auth module. The worker tick calls it; the delete lives there.
	sessions *auth.Service

	// newProvider is the seam tests use to replace the HTTP client.
	newProvider func(conn db.Connection, secret string) Provider
}

// NewWorker builds the sync worker.
func NewWorker(pool *sql.DB, cfg config.Config, log *zap.Logger, cipher *secrets.Cipher, ingest Ingester) *Worker {
	worker := &Worker{
		pool:    pool,
		q:       db.New(pool),
		cfg:     cfg,
		log:     log,
		cipher:  cipher,
		ingest:  ingest,
		trigger: make(chan int64, 1),
	}
	worker.newProvider = func(conn db.Connection, secret string) Provider {
		return NewIntervalsClient(cfg, conn, secret)
	}
	return worker
}

// SetExporter attaches the outbound drainer.
func (w *Worker) SetExporter(exporter *Exporter) { w.exporter = exporter }

// SetSessions attaches the auth module so the existing tick can expire sessions.
func (w *Worker) SetSessions(sessions *auth.Service) { w.sessions = sessions }

// Trigger asks for a pass for one user. The buffered channel coalesces bursts,
// so hammering the endpoint cannot queue up work.
func (w *Worker) Trigger(userID int64) {
	select {
	case w.trigger <- userID:
	default:
	}
}

// Run drives the worker until the context is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.SyncInterval)
	defer ticker.Stop()

	w.log.Info("sync worker started", zap.Duration("interval", w.cfg.SyncInterval))
	for {
		select {
		case <-ctx.Done():
			w.log.Info("sync worker stopped")
			return
		case <-ticker.C:
			w.expireSessions(ctx)
			w.syncPass(ctx, 0)
		case userID := <-w.trigger:
			w.syncPass(ctx, userID)
		}
	}
}

// syncPass processes every due connection, or only one user's when userID is
// non-zero.
func (w *Worker) syncPass(ctx context.Context, userID int64) {
	now := time.Now().Unix()
	connections, err := w.q.ListSyncableConnections(ctx, &now)
	if err != nil {
		w.log.Error("listing syncable connections failed", zap.Error(err))
		return
	}
	for _, conn := range connections {
		if ctx.Err() != nil {
			return
		}
		if userID != 0 && conn.UserID != userID {
			continue
		}
		if !importableKind(conn.Kind) {
			continue
		}
		w.syncConnection(ctx, conn)
	}
	if w.exporter != nil {
		w.exporter.Drain(ctx)
	}
}

// importableKind reports whether the import worker can read from this provider.
// The worker only speaks the intervals.icu API, and a strava_oauth row carries a
// Strava token as its credential, so handing that row to it would send the
// user's Strava token to intervals.icu. Strava connections are export-only.
func importableKind(kind string) bool {
	return kind == KindIntervalsOAuth || kind == KindIntervalsAPIKey
}

// expireSessions asks the auth module to drop sessions that have outlived their
// lifetime. The delete lives there; this tick only calls it.
func (w *Worker) expireSessions(ctx context.Context) {
	if w.sessions == nil {
		return
	}
	removed, err := w.sessions.ExpireSessions(ctx)
	if err != nil {
		w.log.Warn("expiring sessions failed", zap.Error(err))
		return
	}
	if removed > 0 {
		w.log.Info("expired sessions removed", zap.Int64("count", removed))
	}
}

// syncConnection imports everything new for one connection and advances its
// cursor. Every guard that prevents a loop lives here or in the schema.
func (w *Worker) syncConnection(ctx context.Context, conn db.Connection) {
	now := time.Now()
	run, err := w.q.CreateSyncRun(ctx, conn.UserID, conn.Kind, now.Unix())
	if err != nil {
		w.log.Error("creating a sync run failed", zap.Error(err))
		return
	}

	imported, skipped, runErr := w.importWindow(ctx, conn, now)
	finished := time.Now().Unix()
	errorText := ""
	if runErr != nil {
		errorText = runErr.Error()
	}
	if _, err := w.q.FinishSyncRun(ctx, &finished, imported, skipped, nilIfEmpty(errorText), run.ID); err != nil {
		w.log.Error("finishing a sync run failed", zap.Error(err))
	}

	if runErr != nil {
		w.log.Warn("sync failed",
			zap.Int64("user_id", conn.UserID), zap.String("kind", conn.Kind), zap.Error(runErr))
		w.recordFailure(ctx, conn, runErr, finished)
		return
	}
	if _, err := w.q.UpdateConnectionSyncState(ctx, &finished, &finished, finished, conn.ID); err != nil {
		w.log.Error("updating the connection cursor failed", zap.Error(err))
	}
	w.log.Info("sync finished",
		zap.Int64("user_id", conn.UserID), zap.String("kind", conn.Kind),
		zap.Int64("imported", imported), zap.Int64("skipped", skipped))
}

// importWindow walks the unsynced window in chunks and returns the counters.
func (w *Worker) importWindow(ctx context.Context, conn db.Connection, now time.Time) (imported, skipped int64, err error) {
	// Last line of defence for the credential-disclosure bug this package had:
	// newProvider below turns whatever secret it is handed into either a bearer
	// token or an intervals API key, so a row that does not belong to
	// intervals.icu must never reach it.
	if !importableKind(conn.Kind) {
		return 0, 0, fmt.Errorf("connection kind %q cannot be imported", conn.Kind)
	}
	secret, err := w.cipher.DecryptString(conn.AccessTokenCipher)
	if err != nil {
		return 0, 0, fmt.Errorf("decrypting the stored token failed; reconnect this provider")
	}
	if secret == "" {
		return 0, 0, errors.New("this connection carries no credential")
	}
	provider := w.newProvider(conn, secret)
	athleteID := "0"
	if conn.ExternalAthleteID != nil && *conn.ExternalAthleteID != "" {
		athleteID = *conn.ExternalAthleteID
	}
	// "0" means "whoever owns this credential": fine for reading, useless for
	// matching a webhook, which always names the real athlete. Resolve it once
	// and keep it, so an API-key connection that was stored as "0" heals.
	if athleteID == "0" {
		resolved, err := provider.AthleteID(ctx)
		switch {
		case err != nil:
			w.log.Warn("resolving the intervals athlete id failed", zap.Error(err))
		case resolved == "":
			// Nothing to learn; the credential keeps working with "0".
		default:
			if _, err := w.q.UpdateConnectionAthleteID(ctx, &resolved, now.Unix(), conn.ID); err != nil {
				w.log.Warn("storing the resolved athlete id failed", zap.Error(err))
			}
			athleteID = resolved
		}
	}

	oldest := now.AddDate(0, 0, -syncLookbackDays)
	if conn.SyncedFrom != nil && *conn.SyncedFrom > 0 {
		if from := time.Unix(*conn.SyncedFrom, 0); from.After(oldest) {
			oldest = from
		}
	}

	rules, err := w.loadRules(ctx, conn)
	if err != nil {
		return 0, 0, err
	}

	for windowStart := oldest; windowStart.Before(now); windowStart = windowStart.AddDate(0, 0, syncChunkDays) {
		windowEnd := windowStart.AddDate(0, 0, syncChunkDays)
		if windowEnd.After(now) {
			windowEnd = now
		}
		activities, err := provider.ListActivities(ctx, athleteID,
			windowStart.Format("2006-01-02"), windowEnd.Format("2006-01-02"), syncPageLimit)
		if err != nil {
			return imported, skipped, err
		}
		for _, candidate := range activities {
			if ctx.Err() != nil {
				return imported, skipped, ctx.Err()
			}
			outcome, err := w.importCandidate(ctx, conn, provider, rules, candidate)
			if err != nil {
				return imported, skipped, err
			}
			switch outcome {
			case outcomeImported:
				imported++
			case outcomeSkipped:
				skipped++
			}
		}
	}
	return imported, skipped, nil
}

type importOutcome int

const (
	outcomeUnchanged importOutcome = iota
	outcomeImported
	outcomeSkipped
)

// importCandidate applies every guard that keeps hyl from importing the same
// activity twice, and stop the loop that an hyl → Strava → intervals →
// intervals round trip would otherwise create.
func (w *Worker) importCandidate(ctx context.Context, conn db.Connection, provider Provider, rules map[string]bool, candidate IntervalsActivity) (importOutcome, error) {
	if candidate.ID == "" {
		return outcomeSkipped, nil
	}
	sport := activity.NormalizeSport(firstNonEmpty(candidate.Type, candidate.Sport))
	if !ruleEnabled(rules, sport) {
		return outcomeSkipped, nil
	}
	// intervals refuses to serve files for Strava-sourced activities, so
	// skipping them is both a courtesy and the guard against re-importing what
	// we exported.
	if strings.EqualFold(candidate.Source, "STRAVA") {
		w.log.Debug("skipping a strava-sourced activity", zap.String("activity_id", candidate.ID))
		return outcomeSkipped, nil
	}
	if _, err := w.q.GetActivityBySourceRef(ctx, conn.UserID, conn.Kind, &candidate.ID); err == nil {
		return outcomeSkipped, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return outcomeUnchanged, err
	}

	file, err := provider.DownloadFit(ctx, candidate.ID)
	if err != nil {
		if isMissingFile(err) {
			// No file to import (a manual entry, or a provider we cannot read
			// from): count it as skipped rather than failing the whole pass.
			w.log.Debug("activity has no readable file", zap.String("activity_id", candidate.ID))
			return outcomeSkipped, nil
		}
		return outcomeUnchanged, err
	}
	defer func() { _ = file.Close() }()

	title := firstNonEmpty(candidate.Name, "")
	_, err = w.ingest.Ingest(ctx, conn.UserID, file, activity.IngestOptions{
		Title:     title,
		Source:    conn.Kind,
		SourceRef: candidate.ID,
		// Kept so the export can give Strava back the type it reported rather
		// than hyl's coarse sport key.
		SourceSport: firstNonEmpty(candidate.Type, candidate.Sport),
	})
	if err != nil {
		var duplicate *activity.DuplicateError
		if errors.As(err, &duplicate) {
			// The same physical activity already exists under another source,
			// or was deleted on purpose.
			return outcomeSkipped, nil
		}
		return outcomeUnchanged, err
	}
	// Automatic export is queued by the activity store's hook, so it fires for
	// every ingestion path instead of only this one.
	return outcomeImported, nil
}

// recordFailure stores the error and schedules the next attempt with
// exponential backoff.
func (w *Worker) recordFailure(ctx context.Context, conn db.Connection, runErr error, now int64) {
	message := runErr.Error()
	next := time.Now()
	// A provider that told us when to come back is obeyed exactly; everything
	// else falls through to the streak-based backoff below.
	delayKnown := false

	var providerErr *ProviderError
	if errors.As(runErr, &providerErr) {
		if providerErr.RetryAfter > 0 {
			next = next.Add(providerErr.RetryAfter)
			delayKnown = true
		} else if providerErr.NeedsReauthorization() && conn.Kind == KindIntervalsOAuth {
			// intervals issues no refresh token, so a stale token can only be
			// fixed by the user reconnecting.
			message = "reauthorize"
		}
	}
	if !delayKnown {
		streak, err := w.q.CountRecentFailedRuns(ctx, conn.UserID, conn.Kind)
		if err != nil {
			w.log.Warn("counting failed runs failed", zap.Error(err))
		}
		delay := backoffBase
		for i := int64(0); i < streak && i < 7; i++ {
			delay *= 2
		}
		if delay > backoffMax {
			delay = backoffMax
		}
		next = next.Add(delay)
	}

	nextAt := next.Unix()
	// A reauthorization problem keeps the connection parked until the user acts.
	if message == "reauthorize" {
		nextAt = time.Now().Add(backoffMax).Unix()
	}
	if _, err := w.q.UpdateConnectionError(ctx, &message, &nextAt, now, conn.ID); err != nil {
		w.log.Error("recording a sync failure failed", zap.Error(err))
	}
}

// loadRules resolves the per-sport import switches: an exact row wins, then the
// "all" row, then enabled.
func (w *Worker) loadRules(ctx context.Context, conn db.Connection) (map[string]bool, error) {
	rows, err := w.q.ListImportRules(ctx, conn.UserID)
	if err != nil {
		return nil, err
	}
	rules := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.ConnectionKind != conn.Kind {
			continue
		}
		rules[row.Sport] = row.Enabled
	}
	return rules, nil
}

func ruleEnabled(rules map[string]bool, sport string) bool {
	if enabled, ok := rules[sport]; ok {
		return enabled
	}
	if enabled, ok := rules["all"]; ok {
		return enabled
	}
	return true
}

func isMissingFile(err error) bool {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.StatusCode == 404 || providerErr.StatusCode == 422
	}
	return false
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
