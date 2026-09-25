# hyl

hyl is an open-source, self-hosted Strava alternative: one Go binary serving
both the REST API and the embedded SolidJS frontend.

## What it does

- **Activities.** Upload FIT or GPX files (optionally gzipped) from the web UI
  or the developer API. Metrics are computed server-side, sports are normalised
  to `run`, `ride`, `swim`, `hike`, `walk`, `ski`, `row` and `other`, and
  per-user privacy zones hide chosen areas from other viewers. Deleting an
  activity removes its photo files and prevents that activity from being
  re-imported or uploaded again by the same athlete.
- **Social feed.** Follows (with approval for private accounts), likes,
  comments, @mentions and notifications, with per-activity visibility.
- **Import from intervals.icu.** Connect with a personal API key or through
  intervals.icu's OAuth app. A single background worker syncs on
  `HYL_SYNC_INTERVAL` and on intervals' activity webhook, driven by a per-sport
  import-rule matrix.
- **Export to Strava.** Every ingested activity can be queued for Strava's
  upload API, paced by Strava's rate limits, with a per-connection toggle and a
  deauthorization webhook that cleans up the connection.
- **API keys.** A developer API under `/api/v1` authenticated with bearer keys.
- **Tiles.** Serve an operator-supplied `.pmtiles` basemap for the map view.
- **Accounts.** Email/password login plus optional Google and GitHub OAuth,
  email verification and password reset over SMTP, and HMAC-hashed API keys and
  AES-256-GCM-encrypted provider tokens derived from `HYL_SECRET_KEY`.

## Requirements

- Go 1.27.1 and Node 22 (the frontend declares `engines.node >= 22.12`).
- libvips with its development headers and `pkg-config`: the image pipeline uses
  `govips` through CGO, so the binary cannot be built without it. Install
  `libvips-dev` (Debian/Ubuntu), `vips-devel` (Fedora) or `vips` (Homebrew).
  Install `vips-heif`/`libvips-heif` at runtime to accept HEIC photos.
- SQLite is provided by the pure-Go `modernc.org/sqlite` driver, so no separate
  database server is needed.

## Quick start from source

```sh
git clone https://github.com/markbeep/hyl
cd hyl
mise install          # Go, Node, just, sqlc and goose as pinned in mise.toml
just setup            # go mod download + npm --prefix frontend ci
export HYL_SECRET_KEY="$(openssl rand -base64 48)"
just build            # builds the frontend, then bin/hyl
just run              # or ./bin/hyl
```

Open `http://localhost:8080`.

The frontend must be built before the Go binary: `assets.go` embeds
`frontend/dist` with `//go:embed all:frontend/dist` and the server serves
`index.html` out of it, so `just build` depends on `build-web`. The same applies
to `just test` and `just vet`, which also depend on `build-web`; run
`just build-web` once before `just dev` or `just run` for the same reason.

## Docker

```sh
docker build -t hyl .
docker run -d --name hyl -p 8080:8080 -v hyl-data:/data \
  -e HYL_SECRET_KEY="$(openssl rand -base64 48)" \
  -e HYL_BASE_URL=http://localhost:8080 \
  hyl
```

The image builds the frontend in a Node stage, compiles the binary in a Go stage
and runs it on the same Alpine series (3.24) as the build, so the CGO-linked
libvips matches the one loaded at runtime. It runs as the unprivileged `hyl`
user (uid 10001), keeps its state in the `/data` volume and already sets
`HYL_DATA_DIR=/data` and `HYL_LISTEN=:8080`. Pass `--build-arg VERSION=<sha>` to
stamp a version into the binary.

The binary takes one optional argument: `hyl migrate` applies pending migrations
and exits. The server applies them on boot anyway, so this exists for container
orchestrators that want the database prepared before the server container starts.

## Kubernetes

`deploy/kubernetes/hyl.jsonnet` renders the namespace, a `ReadWriteOnce` claim, a
single-replica Deployment and an ingress:

```sh
kubectl -n hyl create secret generic hyl-secrets \
  --from-literal=HYL_SECRET_KEY="$(openssl rand -hex 32)"
jsonnet -S deploy/kubernetes/hyl.jsonnet > hyl.yaml
kubectl apply -f hyl.yaml
```

The Deployment runs one replica with the `Recreate` strategy, because hyl keeps
its state in a single SQLite file on a `ReadWriteOnce` volume: a rolling update
would run two pods against one database.

Two init containers run before the server container, and an unrecognised
argument is an error rather than a second server:

- **`migrate`** runs `hyl migrate`, applying the migrations embedded in the same
  binary, so the schema cannot drift from the server that expects it. It costs
  nothing once the database is current.
- **`basemap`** extracts the configured region (Switzerland by default) from a
  Protomaps build into `HYL_TILES_DIR`. It records what it produced in a marker
  file and re-extracts only when the source, bounding box or max zoom change, so
  pod restarts and new nodes reuse the previous run's archive. Both the binary it
  downloads and the image it runs in are pinned by checksum and digest.

Set `HYL_TRUSTED_PROXIES` to your pod CIDR when running behind the ingress, or
the rate limiters and the recorded session address will see the ingress
controller rather than the client.

## Configuration

hyl is configured entirely through the environment; there is no config file and
no dotenv loader. Every variable is read once at boot, and `HYL_SECRET_KEY` and
`HYL_BASE_URL` are validated before the server starts. Empty provider
credentials simply disable that integration.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HYL_BASE_URL` | `http://localhost:8080` | Absolute public origin of the instance. Must be an absolute URL. Also decides the `Secure` flag on cookies (`https://` enables it) and the OAuth redirect URIs. |
| `HYL_LISTEN` | `:8080` | Address the HTTP server listens on. |
| `HYL_DATA_DIR` | `./data` | Root directory for runtime state; created at boot. |
| `HYL_DB_PATH` | `<HYL_DATA_DIR>/hyl.db` | SQLite database file. |
| `HYL_MEDIA_DIR` | `<HYL_DATA_DIR>/media` | Where avatars and activity photos are stored as WebP variants; created at boot. |
| `HYL_TILES_DIR` | `<HYL_DATA_DIR>/tiles` | Directory served at `/tiles/:name`; created at boot. |
| `HYL_PMTILES_FILE` | unset | Bare filename of the basemap inside `HYL_TILES_DIR`. When set, the frontend is told to load `/tiles/<name>`; must be a bare `*.pmtiles` filename, never a path. |
| `HYL_SECRET_KEY` | unset (**required**) | At least 32 bytes. Signs sessions and derives the AES-256-GCM key that encrypts stored provider tokens and hashes API keys. Changing it invalidates stored tokens. |
| `HYL_ENV` | `development` | `production` selects JSON logging and production mode; anything else uses the human-readable development logger. |
| `HYL_LOG_LEVEL` | `info` | zap log level (`debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`). |
| `HYL_REGISTRATION_OPEN` | `true` | Whether self-registration is allowed. Exposed to the frontend through `/api/config`. |
| `HYL_SESSION_TTL` | `720h` | Session lifetime, as a Go duration (`24h`, `30m`). |
| `HYL_SYNC_INTERVAL` | `15m` | How often the background worker runs a sync/export pass over every connected user. |
| `HYL_TRUSTED_PROXIES` | unset | Comma-separated IPs or CIDR blocks whose `X-Forwarded-For` hyl may believe, for example `10.0.0.0/8,192.168.0.1`. Unset means the header is ignored and the connecting peer's address is used, which is what a directly exposed listener needs: rate limits and the recorded session addresses are keyed on it, and a client that can set the header could otherwise pick its own rate-limit bucket. |
| `HYL_GOOGLE_KEY` | unset | Google OAuth client id. |
| `HYL_GOOGLE_SECRET` | unset | Google OAuth client secret. Both are required to register the provider. |
| `HYL_GITHUB_KEY` | unset | GitHub OAuth client id. |
| `HYL_GITHUB_SECRET` | unset | GitHub OAuth client secret. Both are required to register the provider. |
| `HYL_INTERVALS_CLIENT_ID` | unset | intervals.icu OAuth app client id. |
| `HYL_INTERVALS_CLIENT_SECRET` | unset | intervals.icu OAuth app client secret. Both are required to register the OAuth connect route; API-key connections work without them. |
| `HYL_INTERVALS_WEBHOOK_SECRET` | unset | Shared secret expected on `POST /webhooks/intervals`, compared in constant time. Unset makes the route answer 404 and disables webhook-driven sync. |
| `HYL_STRAVA_CLIENT_ID` | unset | Strava API application client id. |
| `HYL_STRAVA_CLIENT_SECRET` | unset | Strava API application client secret. Both are required for the connect route and for export. |
| `HYL_STRAVA_WEBHOOK_VERIFY_TOKEN` | unset | `hub.verify_token` answered on `GET /webhooks/strava`. Unset makes both webhook routes answer 404. |
| `HYL_STRAVA_API_BASE` | `https://www.strava.com/api/v3` | Base URL of the Strava v3 API used for activity uploads and upload polling. Strava is moving this endpoint to `https://api-v3.strava.com` on 2027-01-04; set this to the new host to follow that migration without a rebuild. Must be an absolute URL and must include the path prefix. |
| `HYL_STRAVA_OAUTH_BASE` | `https://www.strava.com` | Base URL of Strava's OAuth endpoints (`/oauth/authorize`, `/oauth/token`, `/oauth/revoke`). These stay on the original host through the API migration, which is why it is a separate setting. Must be an absolute URL. |
| `HYL_SMTP_HOST` | unset | SMTP host for verification and password-reset mail. Setting it requires `HYL_SMTP_FROM`. |
| `HYL_SMTP_PORT` | `587` | SMTP port. |
| `HYL_SMTP_USER` | unset | SMTP username. |
| `HYL_SMTP_PASS` | unset | SMTP password. |
| `HYL_SMTP_FROM` | unset | Envelope sender of outgoing mail. |
| `HYL_SMTP_TLS` | `starttls` | One of `starttls`, `implicit` or `none`. |

## Basemap tiles

The map view loads a Protomaps `.pmtiles` file served from `HYL_TILES_DIR`.
Fetch one for a region with the `tiles` recipe and point `HYL_PMTILES_FILE` at
it:

```sh
just tiles '-122.6,37.2,-121.8,37.9' 12        # -> data/tiles/region.pmtiles
export HYL_PMTILES_FILE=region.pmtiles
```

The recipe defaults to a dated protomaps build. Protomaps prunes its old dated
builds, so the default URL eventually 404s; pass a newer one as the optional
third argument (`just tiles <bbox> <maxzoom> https://build.protomaps.com/<date>.pmtiles`).
There is no `latest` alias, so check https://maps.protomaps.com/builds/ for a
current date. On Kubernetes the `basemap` init container performs the same
extract for you and re-runs it whenever the configured source, bounding box or
max zoom change.

## Provider setup

### Google and GitHub

Create OAuth credentials with the redirect URI
`{HYL_BASE_URL}/auth/google/callback` or `{HYL_BASE_URL}/auth/github/callback`.
hyl asks GitHub for the `user:email` scope. Set `HYL_GOOGLE_KEY` /
`HYL_GOOGLE_SECRET` or `HYL_GITHUB_KEY` / `HYL_GITHUB_SECRET`; a provider with
either half missing is not registered and `/auth/<provider>` returns 404.

### Strava (export)

1. Create an application at <https://www.strava.com/settings/api>. hyl requests
   only the `activity:write` scope and uses the redirect URI
   `{HYL_BASE_URL}/api/connections/strava/callback`, which must match the
   application's authorization callback domain.
2. Set `HYL_STRAVA_CLIENT_ID` and `HYL_STRAVA_CLIENT_SECRET`. Both are required
   before users can connect or export.
3. Optional webhook: pick a random string, set it as
   `HYL_STRAVA_WEBHOOK_VERIFY_TOKEN`, and create a push subscription with
   `callback_url = {HYL_BASE_URL}/webhooks/strava` and that string as
   `verify_token`. hyl answers the `hub.challenge` handshake and, when Strava
   reports that an athlete deauthorized the app, deletes that connection and its
   pending exports. Without the variable both routes answer 404.

### intervals.icu (import)

- **API key.** The user pastes their personal API key on the settings page. hyl
  validates it once against the intervals.icu API and then authenticates with
  HTTP Basic (`API_KEY:<key>`). This is the primary self-hosting path and needs
  no OAuth app.
- **OAuth app.** Create one in the intervals.icu developer settings, set
  `HYL_INTERVALS_CLIENT_ID` and `HYL_INTERVALS_CLIENT_SECRET`, and use the
  redirect URI `{HYL_BASE_URL}/api/connections/intervals/callback`. hyl asks for
  the `ACTIVITY:READ` scope only: it reads activities to import them and never
  writes to intervals.icu.
- **Webhook.** Set `HYL_INTERVALS_WEBHOOK_SECRET` to the same shared secret you
  configure on intervals' side. hyl verifies it in constant time on
  `POST /webhooks/intervals`; uploads and analyses trigger a re-sync, while
  deletions remove the matching imported activity, its photo files, and prevent
  a later re-import. With the variable unset the route answers 404.

## Developer API

Create keys under *Settings → API keys* (`POST /api/me/api-keys`). The plaintext
key is returned exactly once; only a SHA-256 hash is stored. Keys look like
`hyl_<base64url>` and are sent as a bearer token:

```sh
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/api/v1/activities
```

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/me` | Identify the key owner. |
| `GET` | `/api/v1/activities` | List the owner's activities. |
| `POST` | `/api/v1/activities` | Upload a FIT or GPX activity. |
| `GET` | `/api/v1/activities/:id` | One activity with its full point stream. |

Each key gets its own budget of 60 requests per minute with a full burst, and a
key can only ever reach its owner's own activities. `/api/health` and
`/api/config` are public and unauthenticated.

## just recipes

| Recipe | What it runs |
| --- | --- |
| `just setup` | `go mod download` and `npm --prefix frontend ci`. |
| `just build` | `build-web`, then `go build -o bin/hyl .`. |
| `just build-web` | `npm --prefix frontend run build`. |
| `just run` | `go run .` (needs `frontend/dist` to exist). |
| `just dev` | `go run .` and the Vite dev server together, torn down on exit. |
| `just dev-api` | `go run .` only. |
| `just dev-web` | `npm --prefix frontend run dev` (port 5173, proxying the backend prefixes to 127.0.0.1:8080). |
| `just test` | `build-web`, then `go test ./...`. |
| `just vet` | `build-web`, then `go vet ./...`. |
| `just fmt` | `gofmt -w .`. |
| `just generate` | `sqlc`, then `tygo`. |
| `just sqlc` | `sqlc generate`. |
| `just tygo` | `go tool tygo generate`. |
| `just migrate-up` / `migrate-down` / `migrate-status` | goose against `$HYL_DB_PATH` (default `data/hyl.db`). |
| `just migrate-create <name>` | New sequential goose migration in `internal/db/migrations`. |
| `just tiles <bbox> <maxzoom> [src]` | Extract a `.pmtiles` region into `data/tiles/region.pmtiles`. |
| `just mailpit` | Mailpit on ports 1025 (SMTP) and 8025 (UI) for local mail testing. |

## Development

`just dev` runs the Go server and Vite together; Vite serves the frontend on
port 5173 and proxies `/api`, `/auth`, `/media`, `/tiles` and `/webhooks` to the
backend on 127.0.0.1:8080. Run `just build-web` once first so the embedded
`frontend/dist` exists.

**SQL.** Queries live in `internal/queries`, the schema in
`internal/db/migrations`, and `sqlc.yaml` generates `internal/db` from both.
Run `just sqlc` after changing either. `sqlc generate` is deterministic and CI
fails if the committed output drifts.

**TypeScript types.** `tygo.yaml` generates `frontend/src/api.gen.ts` from
`internal/api`, so the frontend's types are derived from the Go structs. Run
`just tygo` after touching `internal/api`. The file is committed and CI fails if
`go tool tygo generate` produces a diff.

**Migrations.** `internal/db/migrate.go` embeds `internal/db/migrations` and
applies every pending goose migration at boot. For manual control use the goose
CLI through `just migrate-up`, `just migrate-down` (rolls back to version 0) and
`just migrate-status`, all of which target `$HYL_DB_PATH`.

**Mail.** `just mailpit` starts a local SMTP catcher; point
`HYL_SMTP_HOST=localhost`, `HYL_SMTP_PORT=1025`, `HYL_SMTP_TLS=none` and
`HYL_SMTP_FROM=hyl@localhost` at it.

**CI.** `.github/workflows/build.yml` runs the frontend typecheck and build,
`go vet ./...` and `go test ./...`, then checks that the sqlc and tygo output is
up to date, and finally builds and pushes the Docker image.

## License

MIT — see [LICENSE](LICENSE).
