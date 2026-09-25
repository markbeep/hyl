package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/reqctx"
)

// newListHandlers builds the activity handlers over a migrated temporary
// database, so the list tests drive real HTTP handlers and generated queries.
func newListHandlers(t *testing.T) (*Handlers, *db.Queries) {
	t.Helper()
	pool, err := db.Open(config.Config{DBPath: filepath.Join(t.TempDir(), "hyl.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.Migrate(pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewHandlers(pool, NewStore(pool, zap.NewNop())), db.New(pool)
}

func createListUser(t *testing.T, ctx context.Context, queries *db.Queries, username string) db.User {
	t.Helper()
	hash := "x"
	user, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username: username, Email: username + "@example.com", DisplayName: username,
		PasswordHash: &hash, EmailVerified: true, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create user %s: %v", username, err)
	}
	return user
}

func createListActivity(t *testing.T, ctx context.Context, queries *db.Queries, ownerID int64, title, description string, startedAt int64, visibility, dedupe string) db.Activity {
	t.Helper()
	activity, err := queries.CreateActivity(ctx, db.CreateActivityParams{
		UserID: ownerID, Title: title, Description: description, Sport: "ride",
		StartedAt: startedAt, ElapsedTimeS: 600, MovingTimeS: 600, DistanceM: 5000,
		HasGps: true, Visibility: visibility, Source: "manual",
		DedupeHash: dedupe, CreatedAt: 1, UpdatedAt: 1,
	})
	if err != nil {
		t.Fatalf("create activity %q: %v", title, err)
	}
	return activity
}

// call runs one GET against a handler with the given user and query values.
func call(t *testing.T, handler echo.HandlerFunc, user db.User, path string, query url.Values) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.URL.RawQuery = query.Encode()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	reqctx.SetUser(c, &user)
	if err := handler(c); err != nil {
		// The real envelope is installed by internal/server; here it is enough
		// to turn one into the status under test.
		var appErr *apperr.Error
		if errors.As(err, &appErr) {
			_ = c.JSON(appErr.Status, api.ErrorResponse{Error: api.ErrorBody{Code: appErr.Code, Message: appErr.Message}})
		} else {
			e.HTTPErrorHandler(err, c)
		}
	}
	return rec
}

func decodePage(t *testing.T, rec *httptest.ResponseRecorder) api.ActivityPage {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	var page api.ActivityPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return page
}

func pageIDs(items []api.ActivitySummary) []int64 {
	out := make([]int64, 0, len(items))
	for _, item := range items {
		out = append(out, item.ID)
	}
	return out
}

// TestListOrdersByStartNotInsertion is the regression test for feed ordering: an
// activity imported late (an intervals sync, for example) has a high id but an
// old started_at, and must land at the bottom of the feed rather than the top.
func TestListOrdersByStartNotInsertion(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	user := createListUser(t, ctx, queries, "rider")

	newest := createListActivity(t, ctx, queries, user.ID, "Newest", "", 3000, "default", "d1")
	middle := createListActivity(t, ctx, queries, user.ID, "Middle", "", 2000, "default", "d2")
	oldest := createListActivity(t, ctx, queries, user.ID, "Oldest", "", 1000, "default", "d3")

	page := decodePage(t, call(t, handlers.List, user, "/api/activities", url.Values{"feed": {"me"}}))
	got := pageIDs(page.Items)
	want := []int64{newest.ID, middle.ID, oldest.ID}
	if !slices.Equal(got, want) {
		t.Fatalf("feed order = %v, want %v (insertion order was %v)", got, want, []int64{oldest.ID, middle.ID, newest.ID})
	}
}

// TestListWalksPagesInStartOrder walks the paged list with limit=2 and asserts
// the page numbering a UI needs ("page N of M") plus the row placement: every
// activity appears exactly once, in start order. Two activities share a
// started_at, and the page boundary falls between the second activity and that
// tie, so the id tiebreak and its ordering across pages are both exercised.
func TestListWalksPagesInStartOrder(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	user := createListUser(t, ctx, queries, "rider")

	first := createListActivity(t, ctx, queries, user.ID, "first", "", 500, "default", "d1")
	second := createListActivity(t, ctx, queries, user.ID, "second", "", 400, "default", "d2")
	tieA := createListActivity(t, ctx, queries, user.ID, "tie-a", "", 300, "default", "d3")
	tieB := createListActivity(t, ctx, queries, user.ID, "tie-b", "", 300, "default", "d4")
	last := createListActivity(t, ctx, queries, user.ID, "last", "", 100, "default", "d5")

	// started_at DESC, id DESC: the 300 s tie comes back b then a, together.
	wantPages := [][]int64{
		{first.ID, second.ID},
		{tieB.ID, tieA.ID},
		{last.ID},
	}
	want := []int64{first.ID, second.ID, tieB.ID, tieA.ID, last.ID}

	var walked []int64
	for i, wantItems := range wantPages {
		pageNumber := int64(i + 1)
		page := decodePage(t, call(t, handlers.List, user, "/api/activities", url.Values{
			"feed": {"me"}, "limit": {"2"}, "page": {strconv.FormatInt(pageNumber, 10)},
		}))
		got := pageIDs(page.Items)
		if !slices.Equal(got, wantItems) {
			t.Fatalf("page %d = %v, want %v", pageNumber, got, wantItems)
		}
		if page.Page != pageNumber || page.PageSize != 2 || page.Total != int64(len(want)) || page.TotalPages != 3 {
			t.Fatalf("page %d numbering = %+v, want page=%d size=2 total=%d totalPages=3",
				pageNumber, page, pageNumber, len(want))
		}
		walked = append(walked, got...)
	}
	if !slices.Equal(walked, want) {
		t.Fatalf("walked ids = %v, want %v", walked, want)
	}
	seen := map[int64]bool{}
	for _, id := range walked {
		if seen[id] {
			t.Fatalf("id %d returned twice", id)
		}
		seen[id] = true
	}

	// A page past the end clamps to the last one rather than returning nothing.
	clamped := decodePage(t, call(t, handlers.List, user, "/api/activities",
		url.Values{"feed": {"me"}, "limit": {"2"}, "page": {"4"}}))
	if clamped.Page != 3 || !slices.Equal(pageIDs(clamped.Items), []int64{last.ID}) {
		t.Fatalf("page=4 = page %d %v, want page 3 [%d]", clamped.Page, pageIDs(clamped.Items), last.ID)
	}

	// page=0 is page 1.
	zero := decodePage(t, call(t, handlers.List, user, "/api/activities",
		url.Values{"feed": {"me"}, "limit": {"2"}, "page": {"0"}}))
	if zero.Page != 1 || !slices.Equal(pageIDs(zero.Items), []int64{first.ID, second.ID}) {
		t.Fatalf("page=0 = page %d %v, want page 1 [%d %d]", zero.Page, pageIDs(zero.Items), first.ID, second.ID)
	}
}

// TestListTotalsRespectSearchAndVisibility asserts the count that drives the
// page numbering runs under the same visibility predicate as the list: a viewer
// never sees a total that includes rows they cannot open, whether the filter is
// on or off.
func TestListTotalsRespectSearchAndVisibility(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	viewer := createListUser(t, ctx, queries, "viewer")
	other := createListUser(t, ctx, queries, "other")

	if err := queries.UpsertFollow(ctx, viewer.ID, other.ID, "accepted", 1); err != nil {
		t.Fatalf("follow: %v", err)
	}

	createListActivity(t, ctx, queries, viewer.ID, "Morning Ride", "easy spin", 3000, "default", "d1")
	createListActivity(t, ctx, queries, viewer.ID, "Evening run", "hills and rain", 2000, "default", "d2")
	createListActivity(t, ctx, queries, other.ID, "Secret Ride", "private hills notes", 2500, "only_me", "d3")
	createListActivity(t, ctx, queries, other.ID, "Sunset loop", "no hills here", 1500, "default", "d4")

	cases := []struct {
		q         string
		wantTotal int64
		wantPages int64
	}{
		// Both of the viewer's own rows plus the followee's visible one.
		{"", 3, 1},
		// "hills" matches all three text-wise, but the only_me row is not counted.
		{"hills", 2, 1},
		// Case-insensitive; "RIDE" matches the viewer's own row and the hidden one.
		{"RIDE", 1, 1},
		// No rows: total 0 and totalPages 0, never a phantom page.
		{"nothing matches this", 0, 0},
	}
	for _, tc := range cases {
		query := url.Values{"limit": {"20"}}
		if tc.q != "" {
			query.Set("q", tc.q)
		}
		page := decodePage(t, call(t, handlers.List, viewer, "/api/activities", query))
		if page.Total != tc.wantTotal || page.TotalPages != tc.wantPages {
			t.Errorf("q=%q total=%d totalPages=%d, want %d/%d",
				tc.q, page.Total, page.TotalPages, tc.wantTotal, tc.wantPages)
		}
		if int64(len(page.Items)) > page.Total {
			t.Errorf("q=%q returned %d rows but counted only %d", tc.q, len(page.Items), page.Total)
		}
	}

	// An out-of-range page clamps against the visible total, not the unfiltered one.
	clamped := decodePage(t, call(t, handlers.List, viewer, "/api/activities",
		url.Values{"limit": {"20"}, "page": {"9"}}))
	if clamped.Page != 1 || len(clamped.Items) != 3 {
		t.Fatalf("page=9 with 3 visible rows = page %d with %d rows", clamped.Page, len(clamped.Items))
	}
}

// TestListClampsLimit pins the ?limit= contract: absent or 0 uses the default
// 20 and anything above 100 is capped, and the page size the response reports is
// the one the query actually used.
func TestListClampsLimit(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	user := createListUser(t, ctx, queries, "rider")
	for i := range 25 {
		createListActivity(t, ctx, queries, user.ID, "ride", "", int64(1000-i), "default", fmt.Sprintf("d%d", i))
	}

	// The default 20 splits 25 rows across two pages.
	for _, query := range []url.Values{{}, {"limit": {"0"}}} {
		page := decodePage(t, call(t, handlers.List, user, "/api/activities", query))
		if page.PageSize != 20 || page.Total != 25 || page.TotalPages != 2 || len(page.Items) != 20 {
			t.Fatalf("limit %v: pageSize=%d total=%d totalPages=%d rows=%d, want 20/25/2/20",
				query, page.PageSize, page.Total, page.TotalPages, len(page.Items))
		}
	}

	// A limit above the cap is clamped to 100, which fits all 25 rows on one page.
	page := decodePage(t, call(t, handlers.List, user, "/api/activities", url.Values{"limit": {"1000"}}))
	if page.PageSize != 100 || page.TotalPages != 1 || len(page.Items) != 25 {
		t.Fatalf("limit=1000: pageSize=%d totalPages=%d rows=%d, want 100/1/25",
			page.PageSize, page.TotalPages, len(page.Items))
	}
}

// TestListSearchMatchesTitleAndDescription also pins the rule that search runs
// inside the visibility predicate: a followed user's matching activity may
// appear, but their only_me activity never does, however well it matches.
func TestListSearchMatchesTitleAndDescription(t *testing.T) {
	handlers, queries := newListHandlers(t)
	ctx := context.Background()
	viewer := createListUser(t, ctx, queries, "viewer")
	other := createListUser(t, ctx, queries, "other")

	if err := queries.UpsertFollow(ctx, viewer.ID, other.ID, "accepted", 1); err != nil {
		t.Fatalf("follow: %v", err)
	}

	ride := createListActivity(t, ctx, queries, viewer.ID, "Morning Ride", "easy spin", 3000, "default", "d1")
	run := createListActivity(t, ctx, queries, viewer.ID, "Evening run", "hills and rain", 2000, "default", "d2")
	secret := createListActivity(t, ctx, queries, other.ID, "Secret Ride", "private notes", 2500, "only_me", "d3")
	sunset := createListActivity(t, ctx, queries, other.ID, "Sunset loop", "no hills here", 1500, "default", "d4")

	search := func(handler echo.HandlerFunc, path, q string) []int64 {
		t.Helper()
		page := decodePage(t, call(t, handler, viewer, path, url.Values{"q": {q}}))
		return pageIDs(page.Items)
	}

	// Case-insensitive, matching against the title, and scoped to what the
	// viewer may see: the followee's visible row is absent only because its text
	// does not match, and their only_me row is absent despite matching.
	if got := search(handlers.List, "/api/activities", "RIDE"); !slices.Equal(got, []int64{ride.ID}) {
		t.Fatalf("q=RIDE = %v, want [%d] (secret %d must not leak)", got, ride.ID, secret.ID)
	}
	// Description matches count too, and both users' visible rows come back in
	// start order.
	if got := search(handlers.List, "/api/activities", "hills"); !slices.Equal(got, []int64{run.ID, sunset.ID}) {
		t.Fatalf("q=hills = %v, want [%d %d]", got, run.ID, sunset.ID)
	}
	// Surrounding whitespace is trimmed before the filter is applied.
	if got := search(handlers.List, "/api/activities", "  ride  "); !slices.Equal(got, []int64{ride.ID}) {
		t.Fatalf("q with whitespace = %v, want [%d]", got, ride.ID)
	}
	if got := search(handlers.List, "/api/activities", "nothing matches this"); len(got) != 0 {
		t.Fatalf("miss returned %v", got)
	}
	// The developer API shares the same list handler.
	if got := search(handlers.ListOwn, "/api/v1/activities", "hills"); !slices.Equal(got, []int64{run.ID}) {
		t.Fatalf("developer list q=hills = %v, want [%d]", got, run.ID)
	}
}
