package activity

import (
	"context"
	"database/sql"
	"errors"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/social"
)

// ViewerActivity is an activity the viewer may open. The map coordinates are
// already trimmed the way the activity page trims them. A missing activity and
// an activity this viewer cannot see are both reported as not found.
type ViewerActivity struct {
	Activity     db.Activity
	Owner        db.User
	Points       []Point
	Track        []float64
	Route        []float64
	MapAvailable bool
}

// ForViewer loads one activity for a viewer. Viewer id zero is anonymous. The
// owner is always allowed. Otherwise the activity visibility, or the owner's
// account default when the activity says default, plus an accepted follow,
// decides. The route trim happens here, including a route the owner hid.
func (h *Handlers) ForViewer(ctx context.Context, activityID, viewerID int64) (ViewerActivity, error) {
	row, err := h.Q.GetActivity(ctx, activityID)
	if errors.Is(err, sql.ErrNoRows) {
		return ViewerActivity{}, apperr.NotFound("no such activity")
	}
	if err != nil {
		return ViewerActivity{}, err
	}
	owner, err := h.Q.GetUserByID(ctx, row.UserID)
	if err != nil {
		return ViewerActivity{}, err
	}

	follower := false
	if viewerID != 0 && viewerID != owner.ID {
		follow, err := h.Q.GetFollow(ctx, viewerID, owner.ID)
		switch {
		case err == nil:
			follower = follow.Status == "accepted"
		case errors.Is(err, sql.ErrNoRows):
		default:
			return ViewerActivity{}, err
		}
	}
	if !social.VisibilityAllows(viewerID, owner.ID, follower, row.Visibility, owner.ActivitiesVisibility) {
		return ViewerActivity{}, apperr.NotFound("no such activity")
	}

	points, err := h.points(ctx, activityID)
	if err != nil {
		return ViewerActivity{}, err
	}
	zones, err := newZoneCache(ctx, h.Q).forOwner(owner.ID, owner.TrimScope)
	if err != nil {
		return ViewerActivity{}, err
	}
	track, mapAvailable := TrackCoordinates(points, row.RouteHidden, zones, owner.TrimScope,
		float64(owner.TrimRadiusM), row.DistanceM, listTrackPoints)
	route, _ := TrackCoordinates(points, row.RouteHidden, zones, owner.TrimScope,
		float64(owner.TrimRadiusM), row.DistanceM, detailRoutePoints)
	if track == nil {
		track = []float64{}
	}
	if route == nil {
		route = []float64{}
	}
	return ViewerActivity{
		Activity: row, Owner: owner, Points: points,
		Track: track, Route: route, MapAvailable: mapAvailable,
	}, nil
}
