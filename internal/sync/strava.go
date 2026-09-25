package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
)

// Strava serves its REST API and its OAuth endpoints from separate bases as of
// the announced 2027-01-04 migration (api-v3.strava.com for the API, the
// existing host for OAuth). Both are settings rather than constants so an
// operator can follow that move without recompiling.
const (
	defaultStravaAPIBase   = "https://www.strava.com/api/v3"
	defaultStravaOAuthBase = "https://www.strava.com"
)

// stravaAPIBase resolves the configured API base, which includes the /api/v3
// path prefix that the current host carries and the future host drops.
func stravaAPIBase(cfg config.Config) string {
	if cfg.StravaAPIBase != "" {
		return strings.TrimSuffix(cfg.StravaAPIBase, "/")
	}
	return defaultStravaAPIBase
}

// stravaOAuthBase resolves the configured OAuth base: authorize, token and
// revoke live here.
func stravaOAuthBase(cfg config.Config) string {
	if cfg.StravaOAuthBase != "" {
		return strings.TrimSuffix(cfg.StravaOAuthBase, "/")
	}
	return defaultStravaOAuthBase
}

// tokenHTTPClient bounds every OAuth token call. The sync worker owns one
// goroutine, so a provider that accepts a connection and then trickles bytes
// must not be able to park imports and exports for every user.
var tokenHTTPClient = &http.Client{Timeout: 30 * time.Second}

// stravaRefreshWindow is how long before expiry a token is refreshed.
const stravaRefreshWindow = 3600

// stravaScope is the only scope hyl needs: writing activities back.
const stravaScope = "activity:write"

// StravaClient talks to the Strava v3 API with a stored OAuth token.
type StravaClient struct {
	APIBase      string
	OAuthBase    string
	ClientID     string
	ClientSecret string
	AccessToken  string
	HTTP         *http.Client
}

// NewStravaClient builds a client for one stored connection.
func NewStravaClient(cfg config.Config, accessToken string) *StravaClient {
	return &StravaClient{
		APIBase:      stravaAPIBase(cfg),
		OAuthBase:    stravaOAuthBase(cfg),
		ClientID:     cfg.StravaClientID,
		ClientSecret: cfg.StravaClientSecret,
		AccessToken:  accessToken,
		HTTP:         &http.Client{Timeout: 120 * time.Second},
	}
}

// StravaTokens is the token payload both the code exchange and the refresh
// return.
type StravaTokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	// Scope is what the athlete actually granted, which may be less than what
	// was requested: the consent screen lets them untick a scope.
	Scope   string `json:"scope"`
	Athlete struct {
		ID int64 `json:"id"`
	} `json:"athlete"`
}

// GrantsActivityWrite reports whether Strava says the upload scope was granted.
// The field has been part of the token response since 2026-04-23; an empty
// value means the response predates it, which grants benefit of the doubt.
func (t StravaTokens) GrantsActivityWrite() bool {
	if strings.TrimSpace(t.Scope) == "" {
		return true
	}
	for _, scope := range strings.FieldsFunc(t.Scope, func(r rune) bool { return r == ',' || r == ' ' }) {
		if scope == stravaScope {
			return true
		}
	}
	return false
}

// ExchangeStravaCode trades an authorization code for tokens.
func ExchangeStravaCode(ctx context.Context, cfg config.Config, code string) (StravaTokens, error) {
	return exchangeStravaCode(ctx, stravaOAuthBase(cfg), cfg, code)
}

// exchangeStravaCode and refreshStravaToken take the base URL explicitly so the
// export path can be driven against a test server.
func exchangeStravaCode(ctx context.Context, baseURL string, cfg config.Config, code string) (StravaTokens, error) {
	form := url.Values{}
	form.Set("client_id", cfg.StravaClientID)
	form.Set("client_secret", cfg.StravaClientSecret)
	form.Set("code", code)
	form.Set("grant_type", "authorization_code")
	return stravaTokenRequest(ctx, baseURL, form)
}

func refreshStravaToken(ctx context.Context, baseURL string, cfg config.Config, refreshToken string) (StravaTokens, error) {
	form := url.Values{}
	form.Set("client_id", cfg.StravaClientID)
	form.Set("client_secret", cfg.StravaClientSecret)
	form.Set("refresh_token", refreshToken)
	form.Set("grant_type", "refresh_token")
	return stravaTokenRequest(ctx, baseURL, form)
}

// StravaAuthorizeURL builds the consent URL.
func StravaAuthorizeURL(cfg config.Config, state string) string {
	query := url.Values{}
	query.Set("client_id", cfg.StravaClientID)
	query.Set("redirect_uri", cfg.BaseURL+"/api/connections/strava/callback")
	query.Set("response_type", "code")
	query.Set("approval_prompt", "auto")
	query.Set("scope", stravaScope)
	query.Set("state", state)
	return stravaOAuthBase(cfg) + "/oauth/authorize?" + query.Encode()
}

func stravaTokenRequest(ctx context.Context, baseURL string, form url.Values) (StravaTokens, error) {
	var tokens StravaTokens
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return tokens, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	response, err := tokenHTTPClient.Do(req)
	if err != nil {
		return tokens, err
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode >= 400 {
		return tokens, &ProviderError{
			StatusCode: response.StatusCode,
			Message:    fmt.Sprintf("strava rejected the token request (%d): %s", response.StatusCode, summarize(body)),
			RetryAfter: retryAfter(response),
		}
	}
	if err := json.Unmarshal(body, &tokens); err != nil {
		return tokens, apperr.BadRequest("strava returned an unexpected token response")
	}
	if tokens.AccessToken == "" {
		return tokens, apperr.BadRequest("strava returned no access token")
	}
	return tokens, nil
}

// StravaUpload is one item of the upload queue.
type StravaUpload struct {
	ID         int64  `json:"id"`
	Status     string `json:"status"`
	ActivityID int64  `json:"activity_id"`
	Error      string `json:"error"`
}

// UploadFit posts a synthesized FIT file. Strava answers with an upload id that
// has to be polled until the file is processed. sportType overrides the type
// Strava would detect from the file and is omitted when empty, because the
// parameter is validated against a fixed enumeration.
func (c *StravaClient) UploadFit(ctx context.Context, filename, name, description, sportType, externalID string, payload []byte) (StravaUpload, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return StravaUpload{}, err
	}
	if _, err := part.Write(payload); err != nil {
		return StravaUpload{}, err
	}
	fields := map[string]string{
		"data_type":   "fit",
		"name":        name,
		"description": description,
		"external_id": externalID,
		"trainer":     "0",
	}
	if sportType != "" {
		fields["sport_type"] = sportType
	}
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			return StravaUpload{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return StravaUpload{}, err
	}

	req, err := c.newRequest(ctx, http.MethodPost, "/uploads", &body)
	if err != nil {
		return StravaUpload{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	var upload StravaUpload
	if err := c.do(req, &upload); err != nil {
		return StravaUpload{}, err
	}
	return upload, nil
}

// GetUpload polls the processing state of an upload.
func (c *StravaClient) GetUpload(ctx context.Context, uploadID int64) (StravaUpload, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/uploads/"+strconv.FormatInt(uploadID, 10), nil)
	if err != nil {
		return StravaUpload{}, err
	}
	var upload StravaUpload
	if err := c.do(req, &upload); err != nil {
		return StravaUpload{}, err
	}
	return upload, nil
}

// RevokeAccess revokes the application's tokens for one athlete. Strava's
// documented method is POST /oauth/revoke with HTTP Basic client credentials;
// it is the recommended deauthorization call now and the only supported one
// from 2027-06-01, replacing POST /oauth/deauthorize. Revoking either the
// access or the refresh token invalidates the pair, and Strava answers 200
// whether or not the token was still known.
func (c *StravaClient) RevokeAccess(ctx context.Context, token string) error {
	form := url.Values{}
	form.Set("token", token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.oauthBase()+"/oauth/revoke", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.ClientID, c.ClientSecret)

	response, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode >= 400 {
		return &ProviderError{
			StatusCode: response.StatusCode,
			Message:    fmt.Sprintf("strava returned %d revoking access: %s", response.StatusCode, summarize(payload)),
			RetryAfter: retryAfter(response),
		}
	}
	return nil
}

func (c *StravaClient) oauthBase() string {
	if c.OAuthBase == "" {
		return defaultStravaOAuthBase
	}
	return c.OAuthBase
}

func (c *StravaClient) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	base := c.APIBase
	if base == "" {
		base = defaultStravaAPIBase
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	return req, nil
}

// httpClient returns the client every provider call goes through.
func (c *StravaClient) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 120 * time.Second}
}

func (c *StravaClient) do(req *http.Request, out any) error {
	response, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if response.StatusCode >= 400 {
		return &ProviderError{
			StatusCode: response.StatusCode,
			Message:    fmt.Sprintf("strava returned %d: %s", response.StatusCode, summarize(payload)),
			RetryAfter: retryAfter(response),
		}
	}
	if out == nil || len(payload) == 0 {
		return nil
	}
	return json.Unmarshal(payload, out)
}
