// index.js
import http from "node:http";
import path from "node:path";
import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { fileURLToPath } from "node:url";
import { Readable, PassThrough } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Store } from "./store.js";
import { handleNpmMetadata, handleNpmTarball, ensureNpmMetadata } from "./npm.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT ?? 4873);
const HOST = process.env.HOST ?? "0.0.0.0";
const BASE_URL = process.env.BASE_URL ?? `http://localhost:${PORT}`;
const METADATA_TTL_MS = Number(process.env.METADATA_TTL_MS ?? 7 * 24 * 60 * 60 * 1000); // 7 days default
const KEEP_VERSIONS = Number(process.env.KEEP_VERSIONS ?? 10000); // effectively keep all versions
const CACHE_LIMIT_GB = Number(process.env.CACHE_LIMIT_GB ?? 90);
const CACHE_LIMIT_BYTES = CACHE_LIMIT_GB * 1024 * 1024 * 1024;
const UPSTREAM_CONCURRENCY = Number(process.env.UPSTREAM_CONCURRENCY ?? 4);
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS ?? 90000);
const UPSTREAM_RETRIES = Number(process.env.UPSTREAM_RETRIES ?? 3);
const LOG_LEVEL = process.env.LOG_LEVEL ?? "info";

const LEVELS = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const ANSI = {
  reset: "\u001b[0m",
  dim: "\u001b[2m",
  bold: "\u001b[1m",
  cyan: "\u001b[36m",
  green: "\u001b[32m",
  yellow: "\u001b[33m",
  red: "\u001b[31m",
  magenta: "\u001b[35m",
};

function isFresh(filePath, maxAgeMs) {
  try {
    const stat = fs.statSync(filePath);
    return Date.now() - stat.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

function normalizePackageName(raw) {
  return decodeURIComponent(raw);
}

function parseNpmPath(pathname) {
  const rest = pathname.slice("/npm/".length);
  if (!rest) return null;
  const tarballMatch = rest.match(/^(.*)\/-\/([^/]+\.tgz)$/);
  if (tarballMatch) {
    return {
      kind: "tarball",
      packageName: normalizePackageName(tarballMatch[1]),
      tarballFileName: decodeURIComponent(tarballMatch[2]),
    };
  }
  return {
    kind: "metadata",
    packageName: normalizePackageName(rest),
  };
}

function createLogger(level = "info") {
  const threshold = LEVELS[level] ?? LEVELS.info;
  const useColor = Boolean(process.stdout.isTTY) && process.env.NO_COLOR !== "1";

  function colorize(text, color) {
    if (!useColor) return text;
    return `${color}${text}${ANSI.reset}`;
  }

  function colorForLevel(currentLevel) {
    switch (currentLevel) {
      case "debug":
        return ANSI.dim;
      case "info":
        return ANSI.cyan;
      case "warn":
        return ANSI.yellow;
      case "error":
        return ANSI.red;
      default:
        return ANSI.green;
    }
  }

  function formatDetails(details = {}) {
    const parts = [];
    for (const [key, value] of Object.entries(details)) {
      if (value === undefined) continue;
      const rendered = typeof value === "string" ? value : JSON.stringify(value);
      parts.push(`${key}=${rendered}`);
    }
    return parts.length > 0 ? ` ${parts.join(" ")}` : "";
  }
  function write(currentLevel, event, details = {}) {
    if ((LEVELS[currentLevel] ?? LEVELS.info) < threshold) return;
    const timestamp = colorize(new Date().toISOString(), ANSI.dim);
    const levelText = colorize(currentLevel.toUpperCase(), colorForLevel(currentLevel));
    const eventText = colorize(event, ANSI.bold + ANSI.magenta);
    const line = `${timestamp} ${levelText} ${eventText}${formatDetails(details)}`;
    process.stdout.write(`${line}\n`);
  }
  return {
    debug: (event, details) => write("debug", event, details),
    info: (event, details) => write("info", event, details),
    warn: (event, details) => write("warn", event, details),
    error: (event, details) => write("error", event, details),
  };
}

const logger = createLogger(LOG_LEVEL);

function createLimiter(limit) {
  let active = 0;
  const queue = [];

  const pump = () => {
    if (active >= limit) return;
    const next = queue.shift();
    if (!next) return;
    active += 1;
    next();
  };

  return function run(fn) {
    return new Promise((resolve, reject) => {
      const start = () => {
        Promise.resolve()
          .then(fn)
          .then(resolve, reject)
          .finally(() => {
            active -= 1;
            pump();
          });
      };

      if (active < limit) {
        active += 1;
        start();
      } else {
        queue.push(start);
      }
    });
  };
}

function createSingleFlight() {
  const flights = new Map();
  return async function singleFlight(key, fn) {
    if (flights.has(key)) return flights.get(key);
    const promise = Promise.resolve()
      .then(fn)
      .finally(() => flights.delete(key));
    flights.set(key, promise);
    return promise;
  };
}

const runUpstream = createLimiter(UPSTREAM_CONCURRENCY);
const singleFlight = createSingleFlight();

// Prewarm cache with project dependencies on startup
async function prewarmCache() {
  try {
    const pkgPath = path.join(__dirname, "..", "package.json");
    const pkg = JSON.parse(await fs.readFile(pkgPath, "utf8"));
    const dependencies = pkg.dependencies || {};
    const devDependencies = pkg.devDependencies || {};
    
    logger.info("prewarm_start", { count: Object.keys(dependencies).length + Object.keys(devDependencies).length });
    logger.debug("prewarm_dependencies", { dependencies: { ...dependencies, ...devDependencies } });
    
    // Fetch metadata for all dependencies (including dev) in parallel
    await Promise.all(
      [...Object.keys(dependencies), ...Object.keys(devDependencies)].map((pkgName) =>
        ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, pkgName).catch((error) => {
          logger.warn("prewarm_failed", { packageName: pkgName, error: error.message });
        })
      )
    );
    
    logger.info("prewarm_complete");
  } catch (error) {
    logger.warn("prewarm_skipped", { error: error.message });
  }
}

const store = new Store(path.join(__dirname, ".."), {
  keepVersions: KEEP_VERSIONS,
  cacheLimitBytes: CACHE_LIMIT_BYTES,
  logger,
});

const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url, BASE_URL);
  const pathname = urlObj.pathname;
  const startedAt = Date.now();

  logger.debug("request_received", { method: req.method, path: pathname });

  res.on("finish", () => {
    logger.info("http_request", {
      method: req.method,
      path: pathname,
      statusCode: res.statusCode,
      durationMs: Date.now() - startedAt,
    });
  });

  try {
    if (pathname === "/" || pathname === "/admin/stats") {
      if (pathname === "/admin/stats") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({
          npm: store.topPackages("npm", 50),
          keepVersions: KEEP_VERSIONS,
          cacheLimitGb: CACHE_LIMIT_GB,
          cacheBytes: store.totalCacheBytes,
          metadataTtlMs: METADATA_TTL_MS,
          upstreamConcurrency: UPSTREAM_CONCURRENCY,
          upstreamTimeoutMs: UPSTREAM_TIMEOUT_MS,
          upstreamRetries: UPSTREAM_RETRIES,
        }, null, 2));
        return;
      }
      res.writeHead(200, { "content-type": "text/plain" });
      res.end([
        "package cache proxy",
        "",
        `npm registry: ${BASE_URL}/npm/`,
        `stats: ${BASE_URL}/admin/stats`,
        "",
      ].join("\n"));
      return;
    }

    if (
      pathname === "/npm/-/npm/v1/security/advisories/bulk" ||
      pathname === "/npm/-/npm/v1/security/audits/quick"
    ) {
      // Security endpoint bypass - return empty audit
      res.writeHead(200, {
        "content-type": "application/json",
        "content-length": 2,
      });
      res.end("{}\n");
      return;
    }

    if (pathname.startsWith("/npm/")) {
      const npmPath = parseNpmPath(pathname);
      if (!npmPath) return notFound(res);
      if (npmPath.kind === "tarball") {
        await handleNpmTarball(req, res, npmPath.packageName, npmPath.tarballFileName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS);
        return;
      }
      await handleNpmMetadata(req, res, npmPath.packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS);
      return;
    }

    notFound(res);
  } catch (error) {
    logger.error("request_failed", { error: error.message });
    res.writeHead(500);
    res.end(`internal error: ${error.message}\n`);
  }
});

await store.init();

await prewarmCache();

logger.info("startup", {
  host: HOST,
  port: PORT,
  baseUrl: BASE_URL,
  keepVersions: KEEP_VERSIONS,
  cacheLimitGb: CACHE_LIMIT_GB,
  metadataTtlMs: METADATA_TTL_MS,
  upstreamConcurrency: UPSTREAM_CONCURRENCY,
  upstreamTimeoutMs: UPSTREAM_TIMEOUT_MS,
  upstreamRetries: UPSTREAM_RETRIES,
});

server.listen(PORT, HOST, () => {
  logger.info("listening", { host: HOST, port: PORT });
});

process.on("SIGINT", async () => {
  logger.warn("shutdown", { signal: "SIGINT" });
  await store.flush();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  logger.warn("shutdown", { signal: "SIGTERM" });
  await store.flush();
  process.exit(0);
});

function notFound(res) {
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("not found\n");
}
