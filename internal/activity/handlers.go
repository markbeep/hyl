package activity

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/markbeep/hyl/internal/api"
	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/dto"
	"github.com/markbeep/hyl/internal/reqctx"
	"github.com/markbeep/hyl/internal/social"
)

// SourceContextKey lets a caller (the developer API) declare that an upload
// arrived through the API rather than the web UI.
const SourceContextKey = "hyl.source"

// Sources recorded in activities.source.
const (
	SourceManual = "manual"
	SourceAPI    = "api"
)

// feedPhotoCount is how many photos a list row carries, so feed cards can show
// a thumbnail strip without a second request.
const feedPhotoCount = 4

// Handlers serves the activity HTTP surface.
type Handlers struct {
	Q     *db.Queries
	Store *Store
}

// NewHandlers builds the activity handlers.
func NewHandlers(pool *sql.DB, store *Store) *Handlers {
	return &Handlers{Q: db.New(pool), Store: store}
}

// listParams reads the list query parameters shared by the feed, the profile
// and the developer API (?q=, ?page=, ?limit=). The page number is resolved
// against the matching row count in listPage, once that count is known.
func (h *Handlers) listParams(c echo.Context) db.ListActivitiesParams {
	return db.ListActivitiesParams{
		ViewerID:   reqctx.UserID(c),
		Search:     dto.Search(c),
		LimitCount: dto.Limit(c),
	}
}

// listPage counts the rows the viewer may see under the same visibility
// predicate as the list, clamps ?page= to the pages that exist, then loads that
// page. Counting first is what lets a page past the end resolve to the last
// page instead of returning an empty list, and what gives the UI "page N of M".
func (h *Handlers) listPage(ctx context.Context, c echo.Context, params db.ListActivitiesParams) (api.ActivityPage, error) {
	total, err := h.Q.CountActivities(ctx,
		params.OwnerID, params.FeedMe, params.ViewerID, params.FollowingOnly, params.SportFilter, params.Search)
	if err != nil {
		return api.ActivityPage{}, err
	}
	page, totalPages := dto.Page(c, total, params.LimitCount)
	params.OffsetCount = (page - 1) * params.LimitCount
	items, err := h.page(ctx, params)
	if err != nil {
		return api.ActivityPage{}, err
	}
	return dto.ActivityPage(items, page, params.LimitCount, total, totalPages), nil
}

// List serves the home feed. ?feed=following is the default; ?feed=me returns
// only the viewer's own activities.
func (h *Handlers) List(c echo.Context) error {
	params := h.listParams(c)
	switch c.QueryParam("feed") {
	case "me":
		params.FeedMe = 1
	case "following", "":
		params.FollowingOnly = 1
	default:
		return apperr.BadRequest("feed must be following or me")
	}
	page, err := h.listPage(c.Request().Context(), c, params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, page)
}

// ListOwn serves the caller's own activities, newest first.
func (h *Handlers) ListOwn(c echo.Context) error {
	params := h.listParams(c)
	params.FeedMe = 1
	page, err := h.listPage(c.Request().Context(), c, params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, page)
}

// UserActivities serves one profile's activity list.
func (h *Handlers) UserActivities(c echo.Context) error {
	ctx := c.Request().Context()
	owner, err := h.Q.GetUserByUsername(ctx, c.Param("username"))
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("unknown user")
	}
	if err != nil {
		return err
	}
	sport := c.QueryParam("sport")
	if sport != "" && !ValidSport(sport) {
		return apperr.BadRequest("unknown sport %q", sport)
	}

	params := h.listParams(c)
	params.OwnerID = owner.ID
	params.SportFilter = sport
	page, err := h.listPage(ctx, c, params)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, page)
}

// Upload accepts a FIT or GPX file (optionally gzipped).
func (h *Handlers) Upload(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	file, err := c.FormFile("file")
	if err != nil {
		return apperr.BadRequest("expected a file part named \"file\"")
	}
	stream, err := file.Open()
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	activity, err := h.Store.Ingest(c.Request().Context(), user.ID, stream, IngestOptions{
		Title:         c.FormValue("title"),
		Description:   c.FormValue("description"),
		SportOverride: c.FormValue("sport"),
		Source:        sourceFromRequest(c),
	})
	if err != nil {
		return h.ingestError(err)
	}
	detail, err := h.detail(c, activity.ID, user.ID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, detail)
}

// Get serves one activity.
func (h *Handlers) Get(c echo.Context) error {
	id, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	detail, err := h.detail(c, id, reqctx.UserID(c))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, detail)
}

// Update patches the owner's activity metadata.
func (h *Handlers) Update(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	id, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	current, err := h.Q.GetActivity(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such activity")
	}
	if err != nil {
		return err
	}
	if current.UserID != user.ID {
		return apperr.Forbidden("only the owner can edit this activity")
	}

	var req struct {
		Title       *string `json:"title"`
		Description *string `json:"description"`
		Sport       *string `json:"sport"`
		Visibility  *string `json:"visibility"`
		RouteHidden *bool   `json:"routeHidden"`
	}
	if err := c.Bind(&req); err != nil {
		return apperr.ErrInvalidRequest
	}

	params := db.UpdateActivityParams{
		ID:          current.ID,
		Title:       current.Title,
		Description: current.Description,
		Sport:       current.Sport,
		Visibility:  current.Visibility,
		RouteHidden: current.RouteHidden,
		UpdatedAt:   time.Now().Unix(),
	}
	if req.Title != nil {
		params.Title = truncateString(strings.TrimSpace(*req.Title), 120)
	}
	if req.Description != nil {
		params.Description = truncateString(strings.TrimSpace(*req.Description), 2000)
	}
	if req.Sport != nil {
		if !ValidSport(*req.Sport) {
			return apperr.BadRequest("unknown sport %q", *req.Sport)
		}
		params.Sport = *req.Sport
	}
	if req.Visibility != nil {
		if !validVisibility(*req.Visibility) {
			return apperr.BadRequest("visibility must be default, everyone, followers or only_me")
		}
		params.Visibility = *req.Visibility
	}
	if req.RouteHidden != nil {
		params.RouteHidden = *req.RouteHidden
	}

	if _, err := h.Q.UpdateActivity(ctx, params); err != nil {
		return err
	}
	detail, err := h.detail(c, id, user.ID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, detail)
}

// Delete delegates the owner's activity deletion to the store, which records
// its tombstone and cleans up photo files after committing.
func (h *Handlers) Delete(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	id, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	if err := h.Store.Delete(c.Request().Context(), user.ID, id); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// page runs one list query and renders every row, sharing the point, media and
// privacy work across the page.
func (h *Handlers) page(ctx context.Context, params db.ListActivitiesParams) ([]api.ActivitySummary, error) {
	rows, err := h.Q.ListActivities(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return []api.ActivitySummary{}, nil
	}

	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	points, err := h.pointsByActivity(ctx, ids)
	if err != nil {
		return nil, err
	}
	zones := newZoneCache(ctx, h.Q)

	items := make([]api.ActivitySummary, 0, len(rows))
	for _, row := range rows {
		zonesForOwner, err := zones.forOwner(row.UserID, row.TrimScope)
		if err != nil {
			return nil, err
		}
		track, mapAvailable := TrackCoordinates(
			points[row.ID], row.RouteHidden, zonesForOwner, row.TrimScope,
			float64(row.TrimRadiusM), row.DistanceM, listTrackPoints,
		)
		// Photos are loaded per row: an activity holds at most ten of them, so
		// the extra local query is cheaper than a batched IN list.
		photos, err := h.Q.ListActivityMedia(ctx, row.ID)
		if err != nil {
			return nil, err
		}
		items = append(items, dto.ActivitySummary(row, track, mapAvailable, dto.PhotosFromMedia(photos, feedPhotoCount)))
	}
	return items, nil
}

// detail loads everything the single-activity payload needs.
func (h *Handlers) detail(c echo.Context, id, viewerID int64) (api.ActivityDetail, error) {
	ctx := c.Request().Context()

	activity, err := h.Q.GetActivity(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return api.ActivityDetail{}, apperr.NotFound("no such activity")
	}
	if err != nil {
		return api.ActivityDetail{}, err
	}
	owner, err := h.Q.GetUserByID(ctx, activity.UserID)
	if err != nil {
		return api.ActivityDetail{}, err
	}

	follower := false
	if viewerID != 0 && viewerID != owner.ID {
		follow, err := h.Q.GetFollow(ctx, viewerID, owner.ID)
		switch {
		case err == nil:
			follower = follow.Status == "accepted"
		case errors.Is(err, sql.ErrNoRows):
		default:
			return api.ActivityDetail{}, err
		}
	}
	if !social.VisibilityAllows(viewerID, owner.ID, follower, activity.Visibility, owner.ActivitiesVisibility) {
		return api.ActivityDetail{}, apperr.NotFound("no such activity")
	}

	points, err := h.points(ctx, id)
	if err != nil {
		return api.ActivityDetail{}, err
	}
	zones, err := newZoneCache(ctx, h.Q).forOwner(owner.ID, owner.TrimScope)
	if err != nil {
		return api.ActivityDetail{}, err
	}

	track, mapAvailable := TrackCoordinates(points, activity.RouteHidden, zones, owner.TrimScope,
		float64(owner.TrimRadiusM), activity.DistanceM, listTrackPoints)
	route, _ := TrackCoordinates(points, activity.RouteHidden, zones, owner.TrimScope,
		float64(owner.TrimRadiusM), activity.DistanceM, detailRoutePoints)

	counts, err := h.counts(ctx, id, viewerID)
	if err != nil {
		return api.ActivityDetail{}, err
	}
	photos, err := h.Q.ListActivityMedia(ctx, id)
	if err != nil {
		return api.ActivityDetail{}, err
	}
	avatarID, err := h.avatarID(ctx, owner.ID)
	if err != nil {
		return api.ActivityDetail{}, err
	}

	summary := dto.ActivitySummaryFromActivity(activity, owner, avatarID, counts, track, mapAvailable, dto.PhotosFromMedia(photos, 0))
	elapsed, hr, cadence, power, speed, elevation := Streams(points)
	streams := api.ActivityStreams{
		ElapsedS: elapsed, HR: hr, Cadence: cadence, Power: power, SpeedMps: speed, ElevationM: elevation,
	}
	return dto.ActivityDetail(summary, route, streams, viewerID == owner.ID), nil
}

func (h *Handlers) counts(ctx context.Context, activityID, viewerID int64) (dto.ActivityCounts, error) {
	var counts dto.ActivityCounts
	var err error
	if counts.LikeCount, err = h.Q.CountLikes(ctx, activityID); err != nil {
		return counts, err
	}
	if counts.CommentCount, err = h.Q.CountComments(ctx, activityID); err != nil {
		return counts, err
	}
	if counts.PhotoCount, err = h.Q.CountActivityMedia(ctx, activityID); err != nil {
		return counts, err
	}
	if viewerID != 0 {
		if _, err := h.Q.GetLike(ctx, activityID, viewerID); err == nil {
			counts.LikedByMe = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return counts, err
		}
	}
	return counts, nil
}

func (h *Handlers) points(ctx context.Context, activityID int64) ([]Point, error) {
	rows, err := h.Q.ListActivityPoints(ctx, activityID)
	if err != nil {
		return nil, err
	}
	return toPoints(rows), nil
}

func (h *Handlers) pointsByActivity(ctx context.Context, ids []int64) (map[int64][]Point, error) {
	rows, err := h.Q.ListPointsForActivities(ctx, ids)
	if err != nil {
		return nil, err
	}
	grouped := make(map[int64][]Point, len(ids))
	for _, row := range rows {
		grouped[row.ActivityID] = append(grouped[row.ActivityID], toPoint(row))
	}
	return grouped, nil
}

func (h *Handlers) avatarID(ctx context.Context, userID int64) (int64, error) {
	avatar, err := h.Q.GetActiveAvatar(ctx, userID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return avatar.ID, nil
}

// ingestError maps a parsing or duplicate failure onto the error envelope.
func (h *Handlers) ingestError(err error) error {
	var duplicate *DuplicateError
	switch {
	case errors.As(err, &duplicate):
		mapped := apperr.New(apperr.CodeDuplicateActivity, http.StatusConflict, duplicate.Error())
		if duplicate.ActivityID != 0 {
			mapped.ActivityID = duplicate.ActivityID
		}
		return mapped
	case errors.Is(err, ErrUnsupportedFormat), errors.Is(err, ErrNotAnActivity), errors.Is(err, ErrNotAGPX):
		return apperr.BadRequest("%s", err.Error())
	default:
		return err
	}
}

func toPoints(rows []db.ActivityPoint) []Point {
	out := make([]Point, 0, len(rows))
	for _, row := range rows {
		out = append(out, toPoint(row))
	}
	return out
}

func toPoint(row db.ActivityPoint) Point {
	return Point{
		Seq:      row.Seq,
		T:        row.T,
		ElapsedS: row.ElapsedS,
		Lat:      row.Lat,
		Lon:      row.Lon,
		Ele:      row.Ele,
		HR:       intOf(row.Hr),
		Cad:      intOf(row.Cad),
		Pwr:      intOf(row.Pwr),
		Spd:      row.Spd,
		DistM:    row.DistM,
	}
}

func intOf(value *int64) *int {
	if value == nil {
		return nil
	}
	converted := int(*value)
	return &converted
}

func validVisibility(value string) bool {
	switch value {
	case "default", "everyone", "followers", "only_me":
		return true
	default:
		return false
	}
}

// sourceFromRequest keeps the developer API distinguishable from manual
// uploads while sharing one Ingest path.
func sourceFromRequest(c echo.Context) string {
	if source, ok := c.Get("hyl.source").(string); ok && source != "" {
		return source
	}
	return SourceManual
}

// zoneCache loads an owner's privacy zones at most once per page.
type zoneCache struct {
	ctx    context.Context
	q      *db.Queries
	byUser map[int64][]Zone
}

func newZoneCache(ctx context.Context, q *db.Queries) *zoneCache {
	return &zoneCache{ctx: ctx, q: q, byUser: make(map[int64][]Zone)}
}

func (z *zoneCache) forOwner(ownerID int64, trimScope string) ([]Zone, error) {
	if trimScope != "zones" {
		return nil, nil
	}
	if cached, ok := z.byUser[ownerID]; ok {
		return cached, nil
	}
	rows, err := z.q.ListPrivacyZones(z.ctx, ownerID)
	if err != nil {
		return nil, err
	}
	zones := make([]Zone, 0, len(rows))
	for _, row := range rows {
		zones = append(zones, Zone{Lat: row.Lat, Lon: row.Lon, RadiusM: float64(row.RadiusM)})
	}
	z.byUser[ownerID] = zones
	return zones, nil
}
