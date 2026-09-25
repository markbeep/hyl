// Command hyl is an open-source, self-hosted Strava alternative: one binary
// serving both the REST API and the embedded SolidJS frontend.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/markbeep/hyl/internal/activity"
	"github.com/markbeep/hyl/internal/auth"
	"github.com/markbeep/hyl/internal/config"
	"github.com/markbeep/hyl/internal/db"
	"github.com/markbeep/hyl/internal/developer"
	"github.com/markbeep/hyl/internal/logging"
	"github.com/markbeep/hyl/internal/media"
	"github.com/markbeep/hyl/internal/secrets"
	"github.com/markbeep/hyl/internal/server"
	"github.com/markbeep/hyl/internal/social"
	syncpkg "github.com/markbeep/hyl/internal/sync"
	"github.com/markbeep/hyl/internal/users"
	"github.com/markbeep/hyl/internal/webhooks"
)

// version is overridden at build time with -ldflags "-X main.version=…".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

// run dispatches the only two things this binary does: serve, or migrate and
// exit. An unrecognised argument is an error rather than a server launch, so a
// typo in an init container cannot silently start a second server.
func run(args []string) error {
	if len(args) == 0 {
		return serve()
	}
	switch args[0] {
	case "migrate":
		return migrate()
	default:
		return fmt.Errorf("unknown command %q (this binary supports: migrate)", args[0])
	}
}

// migrate applies every pending migration and exits. It exists for init
// containers: the migration files are embedded in this binary, so the schema
// they apply can never drift from the one this server expects — which is
// exactly the risk of running a separately versioned migration tool against the
// same database.
//
// The server also migrates on boot, so this is a pre-flight that moves a
// failure out of the serving container, not a replacement for it.
func migrate() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, err := logging.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	if err := cfg.EnsureDirs(); err != nil {
		return err
	}
	pool, err := db.Open(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = pool.Close() }()
	if err := db.Migrate(pool); err != nil {
		return err
	}
	log.Info("migrations applied", zap.String("db", cfg.DBPath))
	return nil
}

func serve() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, err := logging.New(cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	cfg.Version = version
	if err := cfg.EnsureDirs(); err != nil {
		return err
	}

	static, err := staticHandler()
	if err != nil {
		return err
	}

	pool, err := db.Open(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = pool.Close() }()
	if err := db.Migrate(pool); err != nil {
		return err
	}

	authSvc, err := auth.New(pool, cfg, log)
	if err != nil {
		return err
	}

	// govips cannot be restarted, so the image service is started once here and
	// shut down after the HTTP server has drained.
	mediaService, err := media.New(cfg.MediaDir)
	if err != nil {
		return err
	}
	defer media.Shutdown()
	mediaHandlers := media.NewHandlers(mediaService, pool, log)
	activityStore := activity.NewStore(pool, log)
	// Every ingest queues its automatic Strava export through this hook, so
	// manual uploads, the developer API and provider sync all behave alike.
	activityStore.ExportQueue = syncpkg.NewExportQueuer(db.New(pool)).Queue
	// Media rows cascade with the activity, so the store captures their IDs
	// before commit and hands them to this post-commit cleanup hook.
	activityStore.RemovePhotos = mediaHandlers.RemovePhotos

	cipher, err := secrets.New(cfg.SecretKey)
	if err != nil {
		return err
	}
	worker := syncpkg.NewWorker(pool, cfg, log, cipher, activityStore)
	worker.SetExporter(syncpkg.NewExporter(pool, cfg, log, cipher))
	activityHandlers := activity.NewHandlers(pool, activityStore)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	go worker.Run(workerCtx)

	e, err := server.New(server.Deps{
		Cfg:          cfg,
		Log:          log,
		Version:      version,
		DB:           pool,
		Auth:         authSvc,
		Users:        users.New(pool, log),
		Activity:     activityHandlers,
		Developer:    developer.New(pool, log, activityHandlers),
		Social:       social.New(pool, log),
		Media:        mediaHandlers,
		Sync:         syncpkg.NewHandlers(pool, cfg, log, cipher, worker),
		Webhooks:     webhooks.NewIntervals(pool, cfg, log, worker, activityStore),
		StravaEvents: webhooks.NewStrava(pool, cfg, log, cipher),
		Static:       static,
	})
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		if err := e.Start(cfg.ListenAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	log.Info("hyl started", zap.String("listen", cfg.ListenAddr), zap.String("env", cfg.Env), zap.String("version", version))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-quit:
		log.Info("shutting down", zap.String("signal", sig.String()))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		return err
	}
	log.Info("stopped")
	return nil
}
