package sync

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/secrets"
)

// Connections disconnects one provider and refreshes that provider's tokens.
// The settings route, the export drain, and the Strava webhook call it. They
// do not implement the cleanup or the token write themselves.
type Connections struct {
	Pool   *sql.DB
	Q      *db.Queries
	Cfg    config.Config
	Log    *zap.Logger
	Cipher *secrets.Cipher

	// intervalsBase and httpClient point the best-effort revoke at a fake.
	// Empty and nil use the production host and client.
	intervalsBase string
	httpClient    *http.Client
	// tokenBase points a refresh at a fake. Empty uses the configured OAuth host.
	tokenBase string
}

// NewConnections builds the shared provider connection operations.
func NewConnections(pool *sql.DB, cfg config.Config, log *zap.Logger, cipher *secrets.Cipher) *Connections {
	return &Connections{Pool: pool, Q: db.New(pool), Cfg: cfg, Log: log, Cipher: cipher}
}

// Disconnect removes one provider kind: its connection, its import rules, and,
// only when the kind owns an export target, the pending exports for that
// target. The three deletes commit together. A user-initiated OAuth disconnect
// asks the provider to revoke access first; that call is best-effort.
func (c *Connections) Disconnect(ctx context.Context, userID int64, kind string) error {
	conn, err := c.Q.GetConnection(ctx, userID, kind)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such connection")
	}
	if err != nil {
		return err
	}
	c.revoke(ctx, conn)
	return c.remove(ctx, userID, kind)
}

// Deauthorize applies a Strava deauthorization claim. Strava's token endpoint
// is the authority: a rejected refresh token runs the same local removal as
// Disconnect and does not ask Strava to revoke again. A refresh Strava still
// accepts is stored through the refresh operation and the connection stays. A
// transient failure changes nothing.
func (c *Connections) Deauthorize(ctx context.Context, conn db.Connection) error {
	refreshToken, err := c.Cipher.DecryptString(conn.RefreshTokenCipher)
	if err != nil || refreshToken == "" {
		c.Log.Warn("cannot confirm a deauthorization without a stored refresh token",
			zap.Int64("user_id", conn.UserID), zap.Error(err))
		return nil
	}
	base := c.tokenBase
	if base == "" {
		base = stravaOAuthBase(c.Cfg)
	}
	tokens, err := refreshStravaToken(ctx, base, c.Cfg, refreshToken)
	if err != nil {
		if stravaRejectedCredential(err) {
			if err := c.remove(ctx, conn.UserID, conn.Kind); err != nil {
				return err
			}
			c.Log.Info("strava connection removed after deauthorization", zap.Int64("user_id", conn.UserID))
			return nil
		}
		c.Log.Warn("could not confirm a strava deauthorization", zap.Error(err))
		return nil
	}
	if _, err = c.storeRotated(ctx, conn, tokens, time.Now()); err != nil {
		return err
	}
	c.Log.Info("ignored an unconfirmed strava deauthorization", zap.Int64("user_id", conn.UserID))
	return nil
}

// remove deletes one provider kind, its import rules, and, only when the kind
// owns an export target, the pending exports for that target. The three
// deletes commit together.
func (c *Connections) remove(ctx context.Context, userID int64, kind string) error {
	tx, err := c.Pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	q := c.Q.WithTx(tx)
	if target, ok := exportTargetFor(kind); ok {
		if _, err := q.DeletePendingExportsForTarget(ctx, userID, target); err != nil {
			return err
		}
	}
	if _, err := q.DeleteImportRulesForConnection(ctx, userID, kind); err != nil {
		return err
	}
	affected, err := q.DeleteConnection(ctx, userID, kind)
	if err != nil {
		return err
	}
	if affected == 0 {
		return apperr.NotFound("no such connection")
	}
	return tx.Commit()
}

// tokenStoreAttempts is the write of a pair Strava has already accepted, plus
// one retry. A single database blip must not drop the only refresh token Strava
// will still accept.
const tokenStoreAttempts = 2

// Refresh rotates a Strava token that is inside the refresh window. The pair
// Strava has already accepted is stored before the connection is treated as
// updated. A failed store is retried, then returned as an error.
func (c *Connections) Refresh(ctx context.Context, conn db.Connection, now time.Time) (db.Connection, error) {
	if conn.TokenExpiresAt != nil && *conn.TokenExpiresAt-now.Unix() > stravaRefreshWindow {
		return conn, nil
	}
	refreshToken, err := c.Cipher.DecryptString(conn.RefreshTokenCipher)
	if err != nil || refreshToken == "" {
		return conn, apperr.BadRequest("this Strava connection has no refresh token; reconnect it")
	}
	base := c.tokenBase
	if base == "" {
		base = stravaOAuthBase(c.Cfg)
	}
	tokens, err := refreshStravaToken(ctx, base, c.Cfg, refreshToken)
	if err != nil {
		return conn, err
	}
	return c.storeRotated(ctx, conn, tokens, now)
}

// storeRotated persists a pair Strava has already accepted. The write is
// retried once; a failure is logged and returned so the caller does not treat
// the invalidated refresh token as current.
func (c *Connections) storeRotated(ctx context.Context, conn db.Connection, tokens StravaTokens, now time.Time) (db.Connection, error) {
	access, err := c.Cipher.EncryptString(tokens.AccessToken)
	if err != nil {
		return conn, err
	}
	rotated, err := c.Cipher.EncryptString(tokens.RefreshToken)
	if err != nil {
		return conn, err
	}
	return c.storeEncrypted(ctx, conn, access, rotated, tokens.ExpiresAt, now.Unix())
}

func (c *Connections) storeEncrypted(ctx context.Context, conn db.Connection, access, rotated []byte, expiresAt, now int64) (db.Connection, error) {
	var last error
	for range tokenStoreAttempts {
		affected, err := c.Q.UpdateConnectionTokens(ctx, access, rotated, &expiresAt, now, conn.ID)
		if err != nil || affected == 0 {
			if err == nil {
				err = errors.New("rotated strava tokens were not stored")
			}
			last = err
			continue
		}
		conn.AccessTokenCipher = access
		conn.RefreshTokenCipher = rotated
		conn.TokenExpiresAt = &expiresAt
		return conn, nil
	}
	c.Log.Error("storing the rotated strava tokens failed", zap.Error(last), zap.Int64("connection_id", conn.ID))
	return conn, last
}

// exportTargetFor reports the export target a provider kind owns. intervals.icu
// kinds own none: disconnecting them must not touch a Strava queue.
func exportTargetFor(kind string) (string, bool) {
	if kind == KindStravaOAuth {
		return exportTargetStrava, true
	}
	return "", false
}

// revoke tells the provider to drop hyl's access. A failure is logged and
// ignored: the athlete asked to disconnect, so a provider outage must not keep
// the local connection.
func (c *Connections) revoke(ctx context.Context, conn db.Connection) {
	switch conn.Kind {
	case KindIntervalsOAuth:
		secret, _ := c.Cipher.DecryptString(conn.AccessTokenCipher)
		client := NewIntervalsClient(c.Cfg, conn, secret)
		if c.intervalsBase != "" {
			client.BaseURL = c.intervalsBase
		}
		if c.httpClient != nil {
			client.HTTP = c.httpClient
		}
		if err := client.DisconnectApp(ctx); err != nil {
			c.Log.Warn("telling intervals.icu about the disconnect failed", zap.Error(err))
		}
	case KindStravaOAuth:
		token, err := c.Cipher.DecryptString(conn.RefreshTokenCipher)
		if err != nil || token == "" {
			token, _ = c.Cipher.DecryptString(conn.AccessTokenCipher)
		}
		if token == "" {
			return
		}
		client := NewStravaClient(c.Cfg, "")
		if c.httpClient != nil {
			client.HTTP = c.httpClient
		}
		if err := client.RevokeAccess(ctx, token); err != nil {
			c.Log.Warn("telling Strava about the disconnect failed", zap.Error(err))
		}
	}
}
