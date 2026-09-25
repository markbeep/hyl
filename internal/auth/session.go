package auth

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/markbeep/hyl/internal/db"
)

// SessionCookieName is the application session cookie.
const SessionCookieName = "hyl_session"

// sessionRenewAfter is how old a session must be before it is extended.
const sessionRenewAfter = 168 * time.Hour

// CreateSession issues a new opaque session token and stores its hash.
func (s *Service) CreateSession(ctx context.Context, userID int64, userAgent, ip string) (string, error) {
	token, sum, err := randomToken()
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	if _, err := s.Q.CreateSession(ctx, userID, sum[:], now, now+int64(s.Cfg.SessionTTL.Seconds()), truncate(userAgent, 255), truncate(ip, 64)); err != nil {
		return "", err
	}
	return token, nil
}

// ResolveSession returns the user behind a session token, or nil when the token
// is unknown or expired.
func (s *Service) ResolveSession(ctx context.Context, token string) (*db.User, error) {
	user, _, err := s.resolve(ctx, token)
	return user, err
}

// ResolveRequest resolves the session cookie of a request, refreshing the
// cookie when the session was renewed.
func (s *Service) ResolveRequest(c echo.Context) (*db.User, error) {
	cookie, err := c.Cookie(SessionCookieName)
	if err != nil || cookie.Value == "" {
		return nil, nil
	}
	user, renewed, err := s.resolve(c.Request().Context(), cookie.Value)
	if err != nil {
		return nil, err
	}
	if user == nil {
		s.ClearSessionCookie(c)
		return nil, nil
	}
	if renewed {
		s.SetSessionCookie(c, cookie.Value)
	}
	return user, nil
}

func (s *Service) resolve(ctx context.Context, token string) (*db.User, bool, error) {
	if token == "" {
		return nil, false, nil
	}
	sum := sha256Sum(token)
	sess, err := s.Q.GetSessionByTokenHash(ctx, sum[:])
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	now := time.Now().Unix()
	if sess.ExpiresAt <= now {
		if _, err := s.Q.DeleteSession(ctx, sum[:]); err != nil {
			s.Log.Warn("deleting an expired session failed")
		}
		return nil, false, nil
	}

	renewed := false
	if sess.CreatedAt <= now-int64(sessionRenewAfter.Seconds()) {
		expires := now + int64(s.Cfg.SessionTTL.Seconds())
		if _, err := s.Q.UpdateSessionExpiry(ctx, now, expires, sess.ID); err != nil {
			s.Log.Warn("renewing a session failed")
		} else {
			renewed = true
		}
	}

	user, err := s.Q.GetUserByID(ctx, sess.UserID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &user, renewed, nil
}

// ExpireSessions deletes session rows whose lifetime has elapsed. A session
// that is still inside its lifetime remains.
func (s *Service) ExpireSessions(ctx context.Context) (int64, error) {
	return s.Q.DeleteExpiredSessions(ctx, time.Now().Unix())
}

// DeleteSession removes one session by token.
func (s *Service) DeleteSession(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	sum := sha256Sum(token)
	_, err := s.Q.DeleteSession(ctx, sum[:])
	return err
}

// DeleteUserSessions removes every session of a user (password reset).
func (s *Service) DeleteUserSessions(ctx context.Context, userID int64) error {
	_, err := s.Q.DeleteSessionsForUser(ctx, userID)
	return err
}

// DeleteOtherUserSessions keeps the caller's session and drops the rest
// (password change).
func (s *Service) DeleteOtherUserSessions(ctx context.Context, userID int64, keepToken string) error {
	if keepToken == "" {
		return s.DeleteUserSessions(ctx, userID)
	}
	sum := sha256Sum(keepToken)
	_, err := s.Q.DeleteOtherSessionsForUser(ctx, userID, sum[:])
	return err
}

// SetSessionCookie writes the session cookie.
func (s *Service) SetSessionCookie(c echo.Context, token string) {
	c.SetCookie(&http.Cookie{
		Name:     SessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(s.Cfg.SessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   s.Cfg.IsHTTPS(),
		SameSite: http.SameSiteLaxMode,
	})
}

// ClearSessionCookie expires the session cookie.
func (s *Service) ClearSessionCookie(c echo.Context) {
	c.SetCookie(&http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.Cfg.IsHTTPS(),
		SameSite: http.SameSiteLaxMode,
	})
}

// SessionToken returns the raw session cookie value of a request.
func SessionToken(c echo.Context) string {
	cookie, err := c.Cookie(SessionCookieName)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
