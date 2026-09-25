package activity

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
)

// maxIngestBytes bounds one uploaded file.
const maxIngestBytes = 50 << 20

// pointsPerStatement is the multi-row insert width; 1000 rows of 12 columns is
// 12000 bound parameters, comfortably inside SQLite's limit.
const pointsPerStatement = 1000

// ErrUnsupportedFormat is returned when a payload is neither FIT nor GPX.
var ErrUnsupportedFormat = errors.New("unsupported file format: expected a FIT or GPX activity")

// DuplicateError reports that an activity was already imported. ActivityID is
// zero when the previous copy was deleted (a tombstone), because in that case
// there is nothing left to link to.
type DuplicateError struct {
	ActivityID int64
}

func (e *DuplicateError) Error() string {
	if e.ActivityID == 0 {
		return "this activity was already imported and deleted"
	}
	return fmt.Sprintf("this activity is already imported as activity %d", e.ActivityID)
}

// IngestOptions carries the upload's metadata.
type IngestOptions struct {
	Title         string
	Description   string
	SportOverride string
	Source        string
	SourceRef     string
	// SourceSport is the provider's own activity type. hyl keeps only eight
	// coarse sport keys, so this is what lets an export hand Strava back the
	// type Strava itself reported.
	SourceSport string
}

// Store parses uploads and writes activities.
type Store struct {
	Pool *sql.DB
	Q    *db.Queries
	Log  *zap.Logger

	// ExportQueue, when set, is called after an ingest commits so a configured
	// upload target can queue the activity. It is a field rather than an
	// interface because internal/sync imports this package, which forbids the
	// reverse dependency.
	ExportQueue func(ctx context.Context, a db.Activity) error
	// RemovePhotos removes captured photo files after an activity deletion
	// commits. The media rows have cascaded by then, so their identities are
	// collected inside the transaction.
	RemovePhotos func(ctx context.Context, photos []db.Medium) error
}

// NewStore builds the activity store.
func NewStore(pool *sql.DB, log *zap.Logger) *Store {
	return &Store{Pool: pool, Q: db.New(pool), Log: log}
}

// Delete removes an activity owned by ownerID and records its dedupe hash so
// ingest cannot recreate it. Photo files are removed only after commit.
func (s *Store) Delete(ctx context.Context, ownerID, activityID int64) error {
	tx, err := s.Pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	queries := s.Q.WithTx(tx)
	activity, err := queries.GetActivity(ctx, activityID)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such activity")
	}
	if err != nil {
		return err
	}
	if activity.UserID != ownerID {
		return apperr.Forbidden("only the owner can delete this activity")
	}
	return s.deleteActivity(ctx, tx, activity)
}

// DeleteProviderActivity removes the owner's activity identified by its
// provider kind and provider activity ID. Unrecognized identities are a no-op.
func (s *Store) DeleteProviderActivity(ctx context.Context, ownerID int64, source, sourceRef string) error {
	if source == "" || sourceRef == "" {
		return nil
	}
	tx, err := s.Pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	activity, err := s.Q.WithTx(tx).GetActivityBySourceRef(ctx, ownerID, source, &sourceRef)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.deleteActivity(ctx, tx, activity); err != nil {
		return err
	}
	s.Log.Info("removed an activity deleted at its source",
		zap.Int64("user_id", ownerID), zap.String("source", source), zap.String("source_ref", sourceRef))
	return nil
}

// deleteActivity writes the tombstone and removes the activity in the caller's
// transaction, then cleans up captured photo files after commit.
func (s *Store) deleteActivity(ctx context.Context, tx *sql.Tx, activity db.Activity) error {
	queries := s.Q.WithTx(tx)
	activityID := activity.ID
	var err error
	var photos []db.Medium
	if s.RemovePhotos != nil {
		photos, err = queries.ListActivityMedia(ctx, activityID)
		if err != nil {
			return err
		}
	}
	if err := queries.CreateTombstone(ctx, activity.UserID, activity.DedupeHash, time.Now().Unix()); err != nil {
		return err
	}
	if _, err := queries.DeleteActivity(ctx, activityID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if s.RemovePhotos != nil && len(photos) > 0 {
		if err := s.RemovePhotos(ctx, photos); err != nil {
			s.Log.Warn("removing activity photo files failed",
				zap.Int64("activity_id", activityID), zap.Error(err))
		}
	}
	return nil
}

// Ingest parses, decimates, simplifies and stores one activity. It is
// idempotent by construction: the dedupe hash, the tombstone table and the
// (user, source, source_ref) index all refuse a second copy.
func (s *Store) Ingest(ctx context.Context, ownerID int64, r io.Reader, opts IngestOptions) (db.Activity, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxIngestBytes+1))
	if err != nil {
		return db.Activity{}, err
	}
	if len(data) > maxIngestBytes {
		return db.Activity{}, apperr.BadRequest("files must be at most %d MiB", maxIngestBytes>>20)
	}

	parsed, err := ParsePayload(data)
	if err != nil {
		return db.Activity{}, err
	}
	if opts.SportOverride != "" {
		if !ValidSport(opts.SportOverride) {
			return db.Activity{}, apperr.BadRequest("unknown sport %q", opts.SportOverride)
		}
		parsed.Sport = opts.SportOverride
	}
	sport := NormalizeSport(parsed.Sport)
	if parsed.StartedAt.IsZero() {
		parsed.StartedAt = time.Unix(0, 0).UTC()
	}

	metrics := ComputeMetrics(parsed)
	points := SamplePoints(parsed.StartedAt, Simplify(Decimate(parsed.Samples)))
	hash := DedupeHash(parsed.StartedAt, sport, metrics.DistanceM)

	if err := s.checkExisting(ctx, ownerID, opts.Source, opts.SourceRef, hash); err != nil {
		return db.Activity{}, err
	}

	title := strings.TrimSpace(opts.Title)
	if title == "" {
		title = DefaultTitle(sport, parsed.StartedAt)
	}
	description := strings.TrimSpace(opts.Description)
	source := opts.Source
	if source == "" {
		source = "manual"
	}
	sourceRef := anyOrNil(opts.SourceRef)

	now := time.Now().Unix()
	activity, err := s.insertActivity(ctx, points, db.CreateActivityParams{
		UserID:         ownerID,
		Title:          truncateString(title, 120),
		Description:    truncateString(description, 2000),
		Sport:          sport,
		ExternalSport:  anyOrNil(strings.TrimSpace(opts.SourceSport)),
		StartedAt:      parsed.StartedAt.Unix(),
		ElapsedTimeS:   metrics.ElapsedTimeS,
		MovingTimeS:    metrics.MovingTimeS,
		DistanceM:      metrics.DistanceM,
		ElevationGainM: metrics.ElevationGainM,
		ElevationLossM: metrics.ElevationLossM,
		AvgSpeedMps:    metrics.AvgSpeedMps,
		MaxSpeedMps:    metrics.MaxSpeedMps,
		AvgHeartRate:   int64OrNil(metrics.AvgHeartRate),
		MaxHeartRate:   int64OrNil(metrics.MaxHeartRate),
		AvgCadence:     metrics.AvgCadence,
		MaxCadence:     int64OrNil(metrics.MaxCadence),
		AvgPowerW:      metrics.AvgPowerW,
		MaxPowerW:      int64OrNil(metrics.MaxPowerW),
		HasGps:         metrics.HasGPS,
		RouteHidden:    false,
		Visibility:     "default",
		Source:         source,
		SourceRef:      sourceRef,
		DedupeHash:     hash,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if err != nil {
		return db.Activity{}, err
	}
	// The transaction has committed, so every ingestion path (manual upload,
	// developer API and provider sync) queues its automatic export here. A
	// failure must not fail the ingest: the activity is already stored and the
	// user can queue the export by hand.
	if s.ExportQueue != nil {
		if err := s.ExportQueue(ctx, activity); err != nil {
			s.Log.Warn("queueing an automatic export failed",
				zap.Int64("activity_id", activity.ID), zap.Error(err))
		}
	}
	return activity, nil
}

// checkExisting applies the three pre-insert guards.
func (s *Store) checkExisting(ctx context.Context, ownerID int64, source, sourceRef, hash string) error {
	if sourceRef != "" {
		existing, err := s.Q.GetActivityBySourceRef(ctx, ownerID, sourceOrDefault(source), &sourceRef)
		switch {
		case err == nil:
			return &DuplicateError{ActivityID: existing.ID}
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
	}
	if _, err := s.Q.GetTombstone(ctx, ownerID, hash); err == nil {
		return &DuplicateError{}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	existing, err := s.Q.GetActivityByDedupe(ctx, ownerID, hash)
	switch {
	case err == nil:
		return &DuplicateError{ActivityID: existing.ID}
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	return nil
}

// insertActivity writes the row and its points in one transaction, translating
// a UNIQUE violation into a DuplicateError.
func (s *Store) insertActivity(ctx context.Context, points []Point, params db.CreateActivityParams) (db.Activity, error) {
	tx, err := s.Pool.BeginTx(ctx, nil)
	if err != nil {
		return db.Activity{}, err
	}
	defer func() { _ = tx.Rollback() }()

	activity, err := s.Q.WithTx(tx).CreateActivity(ctx, params)
	if err != nil {
		if isUniqueViolation(err) {
			existing, lookupErr := s.Q.GetActivityByDedupe(ctx, params.UserID, params.DedupeHash)
			if lookupErr == nil {
				return db.Activity{}, &DuplicateError{ActivityID: existing.ID}
			}
			return db.Activity{}, &DuplicateError{}
		}
		return db.Activity{}, err
	}
	if err := insertPointsTx(ctx, tx, activity.ID, points); err != nil {
		return db.Activity{}, err
	}
	if err := tx.Commit(); err != nil {
		return db.Activity{}, err
	}
	return activity, nil
}

// insertPointsTx is the one hand-written statement in the package: sqlc cannot
// express SQLite batch inserts.
func insertPointsTx(ctx context.Context, tx *sql.Tx, activityID int64, points []Point) error {
	const columns = 12
	for start := 0; start < len(points); start += pointsPerStatement {
		end := min(start+pointsPerStatement, len(points))
		chunk := points[start:end]

		var query strings.Builder
		query.WriteString("INSERT INTO activity_points " +
			"(activity_id, seq, t, elapsed_s, lat, lon, ele, hr, cad, pwr, spd, dist_m) VALUES ")
		args := make([]any, 0, len(chunk)*columns)
		for i, point := range chunk {
			if i > 0 {
				query.WriteString(",")
			}
			query.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?)")
			args = append(args, activityID, point.Seq, point.T, point.ElapsedS,
				point.Lat, point.Lon, point.Ele, point.HR, point.Cad, point.Pwr, point.Spd, point.DistM)
		}
		if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

// ParsePayload sniffs gzip, FIT and GPX payloads and parses the activity.
func ParsePayload(data []byte) (ParsedActivity, error) {
	payload, err := maybeGunzip(data)
	if err != nil {
		return ParsedActivity{}, err
	}

	var parsed ParsedActivity
	switch {
	case looksLikeFIT(payload):
		parsed, err = ParseFIT(bytes.NewReader(payload))
	case bytes.Contains(payload, []byte("<gpx")):
		parsed, err = ParseGPX(bytes.NewReader(payload))
	default:
		return ParsedActivity{}, ErrUnsupportedFormat
	}
	if err != nil {
		return ParsedActivity{}, err
	}
	return orderChronologically(parsed), nil
}

// orderChronologically sorts a parsed activity's samples by timestamp. Neither
// format guarantees document order: a GPX can hold several <trkseg> blocks and a
// FIT recorder can flush a buffered record after newer ones. Everything
// downstream assumes ascending time - elapsed seconds are derived from the first
// sample, and the 1 Hz decimation keeps whichever sample it meets first - so an
// out-of-order file would otherwise store negative elapsed times and silently
// drop samples.
func orderChronologically(parsed ParsedActivity) ParsedActivity {
	sort.SliceStable(parsed.Samples, func(i, j int) bool {
		return parsed.Samples[i].T.Before(parsed.Samples[j].T)
	})
	if len(parsed.Samples) > 0 && parsed.Samples[0].T.Before(parsed.StartedAt) {
		// A sample that predates the recorded start means the start is unusable
		// as the elapsed-time origin; the earliest sample is not.
		parsed.StartedAt = parsed.Samples[0].T
	}
	return parsed
}

// looksLikeFIT checks the FIT header: a 12 or 14 byte header with the ".FIT"
// signature at offset 8.
func looksLikeFIT(data []byte) bool {
	if len(data) < 14 {
		return false
	}
	if data[0] != 12 && data[0] != 14 {
		return false
	}
	return string(data[8:12]) == ".FIT"
}

func maybeGunzip(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, apperr.BadRequest("that gzip file could not be read")
	}
	defer func() { _ = reader.Close() }()
	payload, err := io.ReadAll(io.LimitReader(reader, maxIngestBytes+1))
	if err != nil {
		return nil, apperr.BadRequest("that gzip file could not be read")
	}
	if len(payload) > maxIngestBytes {
		return nil, apperr.BadRequest("files must be at most %d MiB", maxIngestBytes>>20)
	}
	return payload, nil
}

// DedupeHash identifies a physical activity by its start, sport and distance.
func DedupeHash(startedAt time.Time, sport string, distanceM float64) string {
	key := fmt.Sprintf("%s|%s|%.0f", startedAt.UTC().Format("2006-01-02T15:04:05Z"), sport, distanceM)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func sourceOrDefault(source string) string {
	if source == "" {
		return "manual"
	}
	return source
}

func anyOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func int64OrNil(value *int) *int64 {
	if value == nil {
		return nil
	}
	converted := int64(*value)
	return &converted
}

// truncateString cuts a string to at most max bytes. It never splits a rune, so
// the value that lands in SQLite is always valid UTF-8 rather than a byte slice
// whose tail byte JSON has to replace with U+FFFD.
func truncateString(value string, max int) string {
	if len(value) <= max {
		return value
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

// isUniqueViolation reports whether an error is a UNIQUE constraint failure.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
