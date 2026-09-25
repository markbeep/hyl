package media

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/apperr"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/dto"
	"github.com/markbeep/hyl/internal/reqctx"
)

// Limits for uploads and per-activity photo counts.
const (
	maxUploadBytes    = 20 << 20
	maxActivityPhotos = 10
	feedPhotoCount    = 4
)

// Handler kinds stored in media.kind.
const (
	KindAvatar   = "avatar"
	KindActivity = "activity"
)

// Handlers serves stored images and accepts uploads.
type Handlers struct {
	Service *Service
	Q       *db.Queries
	Log     *zap.Logger
	// ForViewer is the activity-page read. Activity photos use it only for
	// allow or deny; avatars skip it. Production always sets it; a missing
	// read fails closed.
	ForViewer func(ctx context.Context, activityID, viewerID int64) error
}

// NewHandlers builds the media HTTP surface.
func NewHandlers(service *Service, pool *sql.DB, log *zap.Logger) *Handlers {
	return &Handlers{Service: service, Q: db.New(pool), Log: log}
}

// Serve streams one image variant, applying the activity visibility rules for
// activity photos. Avatars are public.
func (h *Handlers) Serve(c echo.Context) error {
	ctx := c.Request().Context()
	id, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	row, err := h.Q.GetMedia(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such file")
	}
	if err != nil {
		return err
	}
	if row.DeletedAt != nil {
		return apperr.NotFound("no such file")
	}

	if row.Kind == KindActivity {
		if row.ActivityID == nil {
			return apperr.NotFound("no such file")
		}
		if err := h.visibleActivity(ctx, *row.ActivityID, reqctx.UserID(c)); err != nil {
			return err
		}
	}

	variant := c.QueryParam("variant")
	if variant != "thumb" {
		variant = "full"
	}
	return serveFile(c, h.Service.Path(row.Kind, row.ID, variant))
}

// UploadAvatar replaces the caller's avatar.
func (h *Handlers) UploadAvatar(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	data, err := readUpload(c, "file")
	if err != nil {
		return err
	}

	previous, err := h.Q.GetActiveAvatar(ctx, user.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	row, err := h.Q.CreateMedia(ctx, db.CreateMediaParams{
		UserID: user.ID, Kind: KindAvatar, Position: 0, CreatedAt: time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	processed, err := h.Service.ProcessImage(bytes.NewReader(data), KindAvatar, row.ID)
	if err != nil {
		_, _ = h.Q.SoftDeleteMedia(ctx, ptr(time.Now().Unix()), row.ID)
		h.Log.Warn("avatar processing failed", zap.Int64("user_id", user.ID), zap.Error(err))
		return apperr.BadRequest("that file could not be read as an image")
	}
	if _, err := h.Q.UpdateMediaSize(ctx, int64(processed.Width), int64(processed.Height), processed.Bytes, row.ID); err != nil {
		return err
	}

	if err == nil && previous.ID != 0 {
		if _, err := h.Q.SoftDeleteMedia(ctx, ptr(time.Now().Unix()), previous.ID); err != nil {
			return err
		}
		if err := h.Service.Remove(KindAvatar, previous.ID); err != nil {
			h.Log.Warn("removing the previous avatar failed", zap.Int64("media_id", previous.ID), zap.Error(err))
		}
	}

	return c.JSON(http.StatusOK, dto.Me(*user, row.ID))
}

// DeleteAvatar removes the caller's avatar.
func (h *Handlers) DeleteAvatar(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	avatar, err := h.Q.GetActiveAvatar(ctx, user.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return c.NoContent(http.StatusNoContent)
	}
	if err != nil {
		return err
	}
	if _, err := h.Q.SoftDeleteMedia(ctx, ptr(time.Now().Unix()), avatar.ID); err != nil {
		return err
	}
	if err := h.Service.Remove(KindAvatar, avatar.ID); err != nil {
		h.Log.Warn("removing the avatar file failed", zap.Int64("media_id", avatar.ID), zap.Error(err))
	}
	return c.NoContent(http.StatusNoContent)
}

// UploadPhotos attaches up to ten photos in total to an activity.
func (h *Handlers) UploadPhotos(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	activityID, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	activity, err := h.Q.GetActivity(ctx, activityID)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such activity")
	}
	if err != nil {
		return err
	}
	if activity.UserID != user.ID {
		return apperr.Forbidden("only the owner can add photos")
	}

	form, err := c.MultipartForm()
	if err != nil {
		return apperr.BadRequest("expected a multipart upload with one or more files fields")
	}
	uploads := form.File["files"]
	if len(uploads) == 0 {
		uploads = form.File["file"]
	}
	if len(uploads) == 0 {
		return apperr.BadRequest("no files were uploaded")
	}

	existing, err := h.Q.CountActivityMedia(ctx, activityID)
	if err != nil {
		return err
	}
	if existing+int64(len(uploads)) > maxActivityPhotos {
		return apperr.BadRequest("an activity can hold at most %d photos", maxActivityPhotos)
	}

	for _, header := range uploads {
		file, err := header.Open()
		if err != nil {
			return err
		}
		data, err := readLimited(file)
		_ = file.Close()
		if err != nil {
			return err
		}
		if _, err := sniffImage(data); err != nil {
			return err
		}

		position, err := h.Q.NextMediaPosition(ctx, activityID)
		if err != nil {
			return err
		}
		row, err := h.Q.CreateMedia(ctx, db.CreateMediaParams{
			UserID: user.ID, ActivityID: &activityID, Kind: KindActivity,
			Position: position, CreatedAt: time.Now().Unix(),
		})
		if err != nil {
			return err
		}
		processed, err := h.Service.ProcessImage(bytes.NewReader(data), KindActivity, row.ID)
		if err != nil {
			_, _ = h.Q.SoftDeleteMedia(ctx, ptr(time.Now().Unix()), row.ID)
			h.Log.Warn("photo processing failed", zap.Int64("media_id", row.ID), zap.Error(err))
			return apperr.BadRequest("one of the files could not be read as an image")
		}
		if _, err := h.Q.UpdateMediaSize(ctx, int64(processed.Width), int64(processed.Height), processed.Bytes, row.ID); err != nil {
			return err
		}
	}

	rows, err := h.Q.ListActivityMedia(ctx, activityID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, dto.PhotosFromMedia(rows, 0))
}

// DeletePhoto soft-deletes one photo and removes its files.
func (h *Handlers) DeletePhoto(c echo.Context) error {
	user, err := reqctx.RequireUser(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	photoID, err := dto.IDParam(c, "id")
	if err != nil {
		return err
	}
	row, err := h.Q.GetMedia(ctx, photoID)
	if errors.Is(err, sql.ErrNoRows) {
		return apperr.NotFound("no such photo")
	}
	if err != nil {
		return err
	}
	if row.DeletedAt != nil {
		return apperr.NotFound("no such photo")
	}
	if row.UserID != user.ID {
		return apperr.Forbidden("only the owner can delete this photo")
	}
	if _, err := h.Q.SoftDeleteMedia(ctx, ptr(time.Now().Unix()), row.ID); err != nil {
		return err
	}
	if err := h.Service.Remove(row.Kind, row.ID); err != nil {
		h.Log.Warn("removing photo files failed", zap.Int64("media_id", row.ID), zap.Error(err))
	}
	return c.NoContent(http.StatusNoContent)
}

// RemoveActivityMedia deletes the files of an activity's photos. The rows go
// with the activity cascade, so this only touches the disk.
func (h *Handlers) RemoveActivityMedia(ctx context.Context, activityID int64) {
	rows, err := h.Q.ListActivityMedia(ctx, activityID)
	if err != nil {
		h.Log.Warn("listing activity media failed", zap.Int64("activity_id", activityID), zap.Error(err))
		return
	}
	for _, row := range rows {
		if err := h.Service.Remove(row.Kind, row.ID); err != nil {
			h.Log.Warn("removing photo files failed", zap.Int64("media_id", row.ID), zap.Error(err))
		}
	}
}

// visibleActivity reports whether the viewer may open the activity a photo
// belongs to. A missing read, a missing activity, and a hidden activity are
// all the same not-found as a missing file.
func (h *Handlers) visibleActivity(ctx context.Context, activityID, viewerID int64) error {
	if h.ForViewer == nil {
		return apperr.NotFound("no such file")
	}
	if err := h.ForViewer(ctx, activityID, viewerID); err != nil {
		var appErr *apperr.Error
		if errors.As(err, &appErr) && appErr.Status == http.StatusNotFound {
			return apperr.NotFound("no such file")
		}
		return err
	}
	return nil
}

func serveFile(c echo.Context, path string) error {
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return apperr.NotFound("no such file")
	}
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	header := c.Response().Header()
	header.Set("Content-Type", "image/webp")
	header.Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%x-%x", info.ModTime().UnixNano(), info.Size())))
	header.Set("Cache-Control", "private, max-age=3600")
	http.ServeContent(c.Response(), c.Request(), path, info.ModTime(), file)
	return nil
}

// readUpload buffers one multipart part, enforcing the size limit.
func readUpload(c echo.Context, field string) ([]byte, error) {
	header, err := c.FormFile(field)
	if err != nil {
		return nil, apperr.BadRequest("expected a %q file part", field)
	}
	file, err := header.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return readLimited(file)
}

func readLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxUploadBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxUploadBytes {
		return nil, apperr.BadRequest("images must be at most %d MiB", maxUploadBytes>>20)
	}
	if _, err := sniffImage(data); err != nil {
		return nil, err
	}
	return data, nil
}

// sniffImage accepts the raster formats libvips can decode. HEIC has no
// net/http sniffing entry, so its ISO-BMFF brand is checked by hand.
func sniffImage(data []byte) (string, error) {
	detected := http.DetectContentType(data)
	switch detected {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return detected, nil
	}
	if len(data) >= 12 && string(data[4:8]) == "ftyp" {
		switch string(data[8:12]) {
		case "heic", "heix", "hevc", "hevx", "mif1", "msf1":
			return "image/heic", nil
		}
	}
	return "", apperr.BadRequest("images must be JPEG, PNG, WebP, HEIC or GIF")
}

func ptr[T any](value T) *T { return &value }
