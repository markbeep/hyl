-- name: ListConnectionsForUser :many
SELECT * FROM connections WHERE user_id = ? ORDER BY kind;

-- name: GetConnection :one
SELECT * FROM connections WHERE user_id = ? AND kind = ?;

-- name: GetConnectionByExternalID :one
SELECT * FROM connections WHERE kind = ? AND external_athlete_id = ?;

-- Only the intervals.icu kinds are importable: the sync worker speaks the
-- intervals API, so a Strava row (whose stored credential is a Strava token)
-- must never be handed to it.
-- name: ListSyncableConnections :many
SELECT * FROM connections
WHERE kind IN ('intervals_oauth', 'intervals_apikey')
  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
ORDER BY id;

-- name: UpsertConnection :one
INSERT INTO connections (
    user_id, kind, external_athlete_id, access_token_cipher, refresh_token_cipher,
    token_expires_at, auto_export, export_message, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (user_id, kind) DO UPDATE SET
    external_athlete_id = excluded.external_athlete_id,
    access_token_cipher = excluded.access_token_cipher,
    refresh_token_cipher = excluded.refresh_token_cipher,
    token_expires_at = excluded.token_expires_at,
    last_error = NULL,
    next_attempt_at = NULL,
    updated_at = excluded.updated_at
RETURNING *;

-- name: UpdateConnectionTokens :execrows
UPDATE connections
SET access_token_cipher = ?, refresh_token_cipher = ?, token_expires_at = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateConnectionAthleteID :execrows
UPDATE connections
SET external_athlete_id = ?, updated_at = ?
WHERE id = ?;

-- name: UpdateConnectionSettings :execrows
UPDATE connections
SET auto_export = ?, export_message = ?, updated_at = ?
WHERE user_id = ? AND kind = ?;

-- name: UpdateConnectionSyncState :execrows
UPDATE connections
SET synced_from = ?, last_success_at = ?, last_error = NULL, next_attempt_at = NULL, updated_at = ?
WHERE id = ?;

-- name: UpdateConnectionError :execrows
UPDATE connections
SET last_error = ?, next_attempt_at = ?, updated_at = ?
WHERE id = ?;

-- name: DeleteConnection :execrows
DELETE FROM connections WHERE user_id = ? AND kind = ?;

-- name: ListImportRules :many
SELECT * FROM import_rules WHERE user_id = ? ORDER BY connection_kind, sport;

-- name: UpsertImportRule :exec
INSERT INTO import_rules (user_id, connection_kind, sport, enabled, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (user_id, connection_kind, sport) DO UPDATE
    SET enabled = excluded.enabled, updated_at = excluded.updated_at;

-- name: DeleteImportRulesForConnection :execrows
DELETE FROM import_rules WHERE user_id = ? AND connection_kind = ?;

-- A row that has already been sent, or is still pending, keeps its place, so
-- the handler can report a conflict. A row that ended in error is reset, which
-- is the only way a failed export can ever be retried.
-- name: CreateExport :execrows
INSERT INTO activity_exports (activity_id, user_id, target, status, external_id, created_at, updated_at)
VALUES (?, ?, ?, 'pending', ?, ?, ?)
ON CONFLICT (activity_id, target) DO UPDATE SET
    status = 'pending',
    attempts = 0,
    last_error = NULL,
    updated_at = excluded.updated_at
WHERE activity_exports.status = 'error';

-- name: GetExport :one
SELECT * FROM activity_exports WHERE activity_id = ? AND target = ?;

-- name: ListPendingExports :many
SELECT * FROM activity_exports
WHERE status = 'pending' AND attempts < 5
ORDER BY id
LIMIT ?;

-- name: UpdateExportStatus :execrows
UPDATE activity_exports
SET status = ?, remote_id = ?, attempts = ?, last_error = ?, updated_at = ?
WHERE id = ?;

-- Pending rows are removed only for the target the disconnected provider owns.
-- Sent and errored rows stay as history. intervals.icu owns no export target.
-- name: DeletePendingExportsForTarget :execrows
DELETE FROM activity_exports
WHERE user_id = ? AND target = ? AND status = 'pending';

-- name: CreateSyncRun :one
INSERT INTO sync_runs (user_id, connection_kind, started_at)
VALUES (?, ?, ?)
RETURNING *;

-- name: FinishSyncRun :execrows
UPDATE sync_runs
SET finished_at = ?, imported = ?, skipped = ?, error = ?
WHERE id = ?;

-- name: ListRecentSyncRuns :many
SELECT * FROM sync_runs WHERE user_id = ? ORDER BY id DESC LIMIT ?;

-- How many of the five most recent runs for a connection failed; the worker
-- uses it as the exponent of its backoff.
-- name: CountRecentFailedRuns :one
SELECT CAST(COUNT(*) AS INTEGER) FROM (
    SELECT error FROM sync_runs
    WHERE user_id = ? AND connection_kind = ?
    ORDER BY id DESC LIMIT 5
) WHERE error IS NOT NULL;
