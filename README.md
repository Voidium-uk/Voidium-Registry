# Package Cache Proxy

A small Node.js cache/proxy for reducing repeated npm traffic.

## What it does

- Tracks request frequency per package
- Caches npm metadata and tarballs
- Keeps the most recent `10` versions for hot packages on disk
- Exposes basic stats at `/admin/stats`

## Requirements

- Node.js 18 or newer

## Run

```bash
npm start
```

By default it listens on `0.0.0.0:4873`.

## Test

```bash
npm test
```

That runs the syntax checks first and then starts a local registry smoke test that exercises the root page, admin stats, and a real `npm` client flow against the local proxy.

## Configure clients

### npm

Point npm at the proxy registry:

```bash
npm config set registry http://localhost:4873/npm/
```

## Environment variables

- `PORT` default `4873`
- `HOST` default `0.0.0.0`
- `BASE_URL` public URL the server should hand back in rewritten links
- `KEEP_VERSIONS` default `10`
- `CACHE_LIMIT_GB` default `90`
- `METADATA_TTL_MS` default `21600000` (6 hours)
- `UPSTREAM_CONCURRENCY` default `4`
- `UPSTREAM_TIMEOUT_MS` default `90000`
- `UPSTREAM_RETRIES` default `3`
- `LOG_LEVEL` default `info` (`debug`, `info`, `warn`, `error`)
- `PREWARM` default enabled, set to `0` to skip startup prewarming
- `REGISTRY_ROOT` override the on-disk state/cache root

## Notes

- Logs are colorized in a terminal and fall back to plain text when redirected.
- Cached npm package files are stored compressed on disk and served directly from cache when possible.
- The cache keeps the most-used package data and evicts least-used packages when it grows past the size limit.
- This is a practical cache/proxy, not a full private registry implementation.
- If you want, I can add authentication, HTTPS, a Dockerfile, or SQLite persistence next.
