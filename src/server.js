import http from "node:http";
import path from "node:path";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Store } from "./store.js";
import { createLimiter } from "./singleflight.js";
import { handleNpmMetadata, handleNpmTarball, ensureNpmMetadata } from "./npm.js";
import { handlePypiSimple, handlePypiDownload } from "./pip.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function readNumberEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const PORT = readNumberEnv("PORT", 1234);
const HOST = process.env.HOST ?? "0.0.0.0";
const BASE_URL = process.env.BASE_URL ?? "http://213.31.30.140:1234";
const METADATA_TTL_MS = readNumberEnv("METADATA_TTL_MS", 7 * 24 * 60 * 60 * 1000);
const KEEP_VERSIONS = readNumberEnv("KEEP_VERSIONS", 10000);
const CACHE_LIMIT_GB = readNumberEnv("CACHE_LIMIT_GB", 90);
const CACHE_LIMIT_BYTES = CACHE_LIMIT_GB * 1024 * 1024 * 1024;
const UPSTREAM_CONCURRENCY = readNumberEnv("UPSTREAM_CONCURRENCY", 4);
const UPSTREAM_TIMEOUT_MS = readNumberEnv("UPSTREAM_TIMEOUT_MS", 90000);
const UPSTREAM_RETRIES = readNumberEnv("UPSTREAM_RETRIES", 3);
const LOG_LEVEL = process.env.LOG_LEVEL ?? "info";
const PREWARM = process.env.PREWARM !== "0";
const ROOT_DIR = process.env.REGISTRY_ROOT ?? path.join(__dirname, "..");

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

async function isFresh(filePath, maxAgeMs) {
  try {
    const stat = await fs.stat(filePath);
    return Date.now() - stat.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

function normalizePackageName(raw) {
  return safeDecodeURIComponent(raw);
}

function parsePypiPath(pathname) {
  const simplePrefixes = ["/pypi/simple/", "/pip/simple/"];
  for (const prefix of simplePrefixes) {
    if (!pathname.startsWith(prefix)) continue;
    const rest = pathname.slice(prefix.length);
    if (!rest) return null;
    return {
      kind: "simple",
      packageName: normalizePackageName(rest.replace(/\/$/, "")),
    };
  }

  if (!pathname.startsWith("/packages/pypi/")) return null;
  const rest = pathname.slice("/packages/pypi/".length);
  const parts = rest.split("/");
  if (parts.length < 2) return null;
  const packageName = normalizePackageName(parts.shift());
  const filename = safeDecodeURIComponent(parts.join("/"));
  if (!packageName || !filename) return null;
  return { kind: "download", packageName, filename };
}

function parseNpmPath(pathname) {
  const rest = pathname.slice("/npm/".length);
  if (!rest) return null;

  const tarballMatch = rest.match(/^(.*)\/-\/([^/]+\.tgz)$/);
  if (tarballMatch) {
    const packageName = normalizePackageName(tarballMatch[1]);
    const tarballFileName = safeDecodeURIComponent(tarballMatch[2]);
    if (!packageName || !tarballFileName) return null;
    return {
      kind: "tarball",
      packageName,
      tarballFileName,
    };
  }

  const packageName = normalizePackageName(rest);
  if (!packageName) return null;
  return {
    kind: "metadata",
    packageName,
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let amount = value / 1024;
  let unitIndex = 0;
  while (amount >= 1024 && unitIndex < units.length - 1) {
    amount /= 1024;
    unitIndex += 1;
  }
  return `${amount.toFixed(amount >= 10 ? 1 : 2)} ${units[unitIndex]}`;
}

function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  const seconds = Math.floor(value / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatNumber(value) {
  return Number(value ?? 0).toLocaleString("en-GB");
}

function createMetricCard({ id, label, value, detail }) {
  return `
    <article class="card">
      <span class="label">${escapeHtml(label)}</span>
      <strong class="value" id="${escapeHtml(id)}-value">${escapeHtml(value)}</strong>
      <span class="detail" id="${escapeHtml(id)}-detail">${escapeHtml(detail)}</span>
    </article>
  `;
}

function createLoadSnapshot(store, runtimeState, runUpstream) {
  const upstream = typeof runUpstream.stats === "function"
    ? runUpstream.stats()
    : { limit: UPSTREAM_CONCURRENCY, active: 0, queued: 0, available: UPSTREAM_CONCURRENCY };

  return {
    baseUrl: BASE_URL,
    uptimeMs: Date.now() - runtimeState.startedAt,
    http: {
      active: runtimeState.activeRequests,
      total: runtimeState.totalRequests,
    },
    upstream,
    cache: {
      bytes: store.totalCacheBytes,
      limitBytes: CACHE_LIMIT_BYTES,
      limitGb: CACHE_LIMIT_GB,
    },
    npm: {
      packageCount: Object.keys(store.state.npm ?? {}).length,
    },
    pypi: {
      packageCount: Object.keys(store.state.pypi ?? {}).length,
    },
  };
}

function createAdminStats(store, runtimeState, runUpstream) {
  return {
    ...createLoadSnapshot(store, runtimeState, runUpstream),
    config: {
      keepVersions: KEEP_VERSIONS,
      cacheLimitGb: CACHE_LIMIT_GB,
      metadataTtlMs: METADATA_TTL_MS,
      upstreamConcurrency: UPSTREAM_CONCURRENCY,
      upstreamTimeoutMs: UPSTREAM_TIMEOUT_MS,
      upstreamRetries: UPSTREAM_RETRIES,
      prewarm: PREWARM,
    },
    npm: {
      packageCount: Object.keys(store.state.npm ?? {}).length,
      topPackages: store.topPackages("npm", 25),
    },
    pypi: {
      packageCount: Object.keys(store.state.pypi ?? {}).length,
      topPackages: store.topPackages("pypi", 25),
    },
  };
}

function renderRootPage(stats) {
  const upstreamLoadPercent = Math.min(100, Math.round((stats.upstream.active / Math.max(1, stats.upstream.limit)) * 100));
  const cacheUsagePercent = Math.min(100, Math.round((stats.cache.bytes / Math.max(1, stats.cache.limitBytes)) * 100));
  const loadScore = stats.http.active + stats.upstream.active + stats.upstream.queued;

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="cache-control" content="no-store" />
  <title>Voidium Registry</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #08111f;
      --bg-soft: rgba(10, 18, 32, 0.74);
      --panel: rgba(15, 23, 42, 0.84);
      --border: rgba(148, 163, 184, 0.18);
      --text: #e5eefc;
      --muted: #97a6bb;
      --accent: #38bdf8;
      --accent-2: #22c55e;
      --warning: #f59e0b;
      --shadow: 0 30px 90px rgba(0, 0, 0, 0.35);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 34%),
        radial-gradient(circle at 85% 15%, rgba(34, 197, 94, 0.16), transparent 24%),
        linear-gradient(180deg, #050b14, var(--bg));
    }

    a {
      color: #93c5fd;
      text-decoration: none;
    }

    a:hover {
      text-decoration: underline;
    }

    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.96em;
      padding: 0.18rem 0.4rem;
      border-radius: 0.5rem;
      background: rgba(15, 23, 42, 0.65);
      border: 1px solid rgba(148, 163, 184, 0.18);
    }

    .shell {
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
      padding: 48px 0 56px;
    }

    .hero {
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: flex-end;
      margin-bottom: 28px;
    }

    .eyebrow {
      margin: 0 0 12px;
      text-transform: uppercase;
      letter-spacing: 0.22em;
      font-size: 0.76rem;
      color: var(--muted);
    }

    h1 {
      margin: 0;
      font-size: clamp(2.8rem, 7vw, 5.2rem);
      line-height: 0.96;
      letter-spacing: -0.06em;
    }

    .lede {
      max-width: 64ch;
      margin: 16px 0 0;
      font-size: 1.03rem;
      line-height: 1.65;
      color: var(--muted);
    }

    .hero-meta {
      padding: 16px 18px;
      border-radius: 20px;
      background: var(--bg-soft);
      border: 1px solid var(--border);
      box-shadow: var(--shadow);
      min-width: min(100%, 340px);
    }

    .hero-meta .label {
      margin-bottom: 8px;
    }

    .hero-meta .value {
      font-size: 1.25rem;
      display: block;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 16px;
    }

    .card,
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 22px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(20px);
    }

    .card {
      padding: 18px;
      min-height: 130px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .label {
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: var(--muted);
    }

    .value {
      font-size: clamp(1.5rem, 3vw, 2.2rem);
      font-weight: 700;
      line-height: 1.05;
      margin: 12px 0 8px;
    }

    .detail {
      color: var(--muted);
      line-height: 1.4;
    }

    .panel {
      padding: 20px;
      margin-top: 16px;
    }

    .panel-header {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 14px;
    }

    .panel-header h2 {
      margin: 0;
      font-size: 1.08rem;
    }

    .status-row {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      margin-top: 16px;
    }

    .status {
      padding: 14px;
      border-radius: 18px;
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid rgba(148, 163, 184, 0.14);
    }

    .status .value {
      font-size: 1.1rem;
      margin: 10px 0 4px;
    }

    .bar {
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: rgba(148, 163, 184, 0.18);
      overflow: hidden;
    }

    .bar > span {
      display: block;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      transition: width 240ms ease;
      width: 0%;
    }

    .muted {
      color: var(--muted);
    }

    .packages {
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 10px;
    }

    .packages li {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      padding: 12px 0;
      border-top: 1px solid rgba(148, 163, 184, 0.14);
    }

    .packages li:first-child {
      border-top: 0;
      padding-top: 0;
    }

    .packages strong {
      display: block;
      font-size: 1rem;
      margin-bottom: 4px;
    }

    .packages span {
      color: var(--muted);
    }

    .pill {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.12);
      color: #cffafe;
      border: 1px solid rgba(56, 189, 248, 0.18);
      white-space: nowrap;
      font-size: 0.9rem;
    }

    .footer {
      margin-top: 18px;
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.6;
    }

    @media (max-width: 960px) {
      .grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .hero {
        flex-direction: column;
        align-items: flex-start;
      }

      .status-row {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 640px) {
      .shell {
        width: min(100% - 20px, 1120px);
        padding-top: 28px;
      }

      .grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div>
        <p class="eyebrow">Voidium Registry</p>
        <h1>Stable npm caching with live load</h1>
        <p class="lede">
          Serving <code>${escapeHtml(stats.baseUrl)}</code> with safer file handling, lighter stats, and a root page that shows what the proxy is doing right now.
        </p>
      </div>
      <div class="hero-meta">
        <span class="label">Current load</span>
        <strong class="value" id="load-score-value">${formatNumber(stats.http.active + stats.upstream.active + stats.upstream.queued)}</strong>
        <span class="detail" id="load-score-detail">${formatNumber(stats.http.active)} HTTP active, ${formatNumber(stats.upstream.active)} upstream active, ${formatNumber(stats.upstream.queued)} queued</span>
      </div>
    </section>

    <section class="grid">
      ${createMetricCard({
        id: "http-active",
        label: "HTTP requests in flight",
        value: formatNumber(stats.http.active),
        detail: `${formatNumber(stats.http.total)} handled since startup`,
      })}
      ${createMetricCard({
        id: "upstream-active",
        label: "Upstream workers",
        value: `${formatNumber(stats.upstream.active)} / ${formatNumber(stats.upstream.limit)}`,
        detail: `${formatNumber(stats.upstream.queued)} queued, ${formatNumber(stats.upstream.available)} free`,
      })}
      ${createMetricCard({
        id: "cache-used",
        label: "Cache usage",
        value: formatBytes(stats.cache.bytes),
        detail: `${cacheUsagePercent}% of ${formatBytes(stats.cache.limitBytes)}`,
      })}
      ${createMetricCard({
        id: "package-count",
        label: "Cached packages",
        value: formatNumber(stats.npm.packageCount),
        detail: `${formatDuration(stats.uptimeMs)} uptime`,
      })}
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2>Load meter</h2>
          <p class="muted">The page refreshes itself every five seconds.</p>
        </div>
        <div class="pill" id="load-pill">Upstream ${upstreamLoadPercent}% busy</div>
      </div>
      <div class="bar" aria-hidden="true">
        <span id="upstream-bar" style="width: ${upstreamLoadPercent}%"></span>
      </div>
      <div class="status-row">
        <div class="status">
          <span class="label">Combined load</span>
          <strong class="value" id="combined-load-value">${formatNumber(loadScore)}</strong>
          <span class="detail" id="combined-load-detail">Requests and upstream queue together</span>
        </div>
        <div class="status">
          <span class="label">Uptime</span>
          <strong class="value" id="uptime-value">${formatDuration(stats.uptimeMs)}</strong>
          <span class="detail" id="uptime-detail">${escapeHtml(stats.baseUrl)}</span>
        </div>
        <div class="status">
          <span class="label">Cache fill</span>
          <strong class="value" id="cache-fill-value">${cacheUsagePercent}%</strong>
          <span class="detail" id="cache-fill-detail">${formatBytes(stats.cache.bytes)} used of ${formatBytes(stats.cache.limitBytes)}</span>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2>Quick links</h2>
          <p class="muted">Need raw numbers? Use the JSON stats endpoint.</p>
        </div>
        <a href="/admin/stats">/admin/stats</a>
      </div>
      <div class="footer">
        Cached metadata and tarballs are served from <code>/npm/</code>. Broken or malformed package paths now fail closed with a 404 instead of taking the process down.
      </div>
    </section>
  </main>

  <script>
    const formatBytes = (bytes) => {
      const value = Number(bytes) || 0;
      if (value < 1024) return value + " B";
      const units = ["KB", "MB", "GB", "TB"];
      let amount = value / 1024;
      let unitIndex = 0;
      while (amount >= 1024 && unitIndex < units.length - 1) {
        amount /= 1024;
        unitIndex += 1;
      }
      return amount.toFixed(amount >= 10 ? 1 : 2) + " " + units[unitIndex];
    };

    const formatDuration = (ms) => {
      const value = Math.max(0, Number(ms) || 0);
      const seconds = Math.floor(value / 1000);
      if (seconds < 60) return seconds + "s";
      const minutes = Math.floor(seconds / 60);
      if (minutes < 60) return minutes + "m " + (seconds % 60) + "s";
      const hours = Math.floor(minutes / 60);
      return hours + "h " + (minutes % 60) + "m";
    };

    const formatNumber = (value) => Number(value ?? 0).toLocaleString("en-GB");

    const update = (stats) => {
      const upstreamLoadPercent = Math.min(100, Math.round((stats.upstream.active / Math.max(1, stats.upstream.limit)) * 100));
      const cacheUsagePercent = Math.min(100, Math.round((stats.cache.bytes / Math.max(1, stats.cache.limitBytes)) * 100));
      const loadScore = stats.http.active + stats.upstream.active + stats.upstream.queued;

      document.getElementById("http-active-value").textContent = formatNumber(stats.http.active);
      document.getElementById("http-active-detail").textContent = formatNumber(stats.http.total) + " handled since startup";
      document.getElementById("upstream-active-value").textContent = formatNumber(stats.upstream.active) + " / " + formatNumber(stats.upstream.limit);
      document.getElementById("upstream-active-detail").textContent = formatNumber(stats.upstream.queued) + " queued, " + formatNumber(stats.upstream.available) + " free";
      document.getElementById("cache-used-value").textContent = formatBytes(stats.cache.bytes);
      document.getElementById("cache-used-detail").textContent = cacheUsagePercent + "% of " + formatBytes(stats.cache.limitBytes);
      document.getElementById("package-count-value").textContent = formatNumber(stats.npm.packageCount);
      document.getElementById("package-count-detail").textContent = formatDuration(stats.uptimeMs) + " uptime";
      document.getElementById("load-score-value").textContent = formatNumber(loadScore);
      document.getElementById("load-score-detail").textContent = formatNumber(stats.http.active) + " HTTP active, " + formatNumber(stats.upstream.active) + " upstream active, " + formatNumber(stats.upstream.queued) + " queued";
      document.getElementById("combined-load-value").textContent = formatNumber(loadScore);
      document.getElementById("uptime-value").textContent = formatDuration(stats.uptimeMs);
      document.getElementById("cache-fill-value").textContent = cacheUsagePercent + "%";
      document.getElementById("cache-fill-detail").textContent = formatBytes(stats.cache.bytes) + " used of " + formatBytes(stats.cache.limitBytes);
      document.getElementById("load-pill").textContent = "Upstream " + upstreamLoadPercent + "% busy";
      document.getElementById("upstream-bar").style.width = upstreamLoadPercent + "%";
    };

    const refresh = async () => {
      try {
        const response = await fetch("/admin/stats", { cache: "no-store" });
        if (!response.ok) return;
        update(await response.json());
      } catch {
        // Ignore transient fetch failures; the page still shows the last known state.
      }
    };

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>`;
}

const logger = createLogger(LOG_LEVEL);
const runUpstream = createLimiter(UPSTREAM_CONCURRENCY);
const requestState = {
  activeRequests: 0,
  totalRequests: 0,
  startedAt: Date.now(),
};

async function prewarmCache() {
  try {
    const pkgPath = path.join(ROOT_DIR, "package.json");
    const pkg = JSON.parse(await fs.readFile(pkgPath, "utf8"));
    const dependencies = pkg.dependencies || {};
    const devDependencies = pkg.devDependencies || {};
    const packageNames = [...Object.keys(dependencies), ...Object.keys(devDependencies)];

    logger.info("prewarm_start", { count: packageNames.length });
    logger.debug("prewarm_dependencies", { dependencies: { ...dependencies, ...devDependencies } });

    await Promise.all(
      packageNames.map((pkgName) =>
        ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, pkgName, runUpstream).catch((error) => {
          logger.warn("prewarm_failed", { packageName: pkgName, error: error.message });
        })
      )
    );

    logger.info("prewarm_complete");
  } catch (error) {
    logger.warn("prewarm_skipped", { error: error.message });
  }
}

const store = new Store(ROOT_DIR, {
  keepVersions: KEEP_VERSIONS,
  cacheLimitBytes: CACHE_LIMIT_BYTES,
  logger,
});

const server = http.createServer(async (req, res) => {
  requestState.activeRequests += 1;
  requestState.totalRequests += 1;
  const startedAt = Date.now();

  try {
    const urlObj = new URL(req.url ?? "/", BASE_URL);
    const pathname = urlObj.pathname;
    const upstreamStats = typeof runUpstream.stats === "function"
      ? runUpstream.stats()
      : { limit: UPSTREAM_CONCURRENCY, active: 0, queued: 0, available: UPSTREAM_CONCURRENCY };

    logger.debug("request_received", { method: req.method, path: pathname });

    res.on("finish", () => {
      logger.info("http_request", {
        method: req.method,
        path: pathname,
        statusCode: res.statusCode,
        durationMs: Date.now() - startedAt,
        activeRequests: requestState.activeRequests,
        upstreamActive: upstreamStats.active,
        upstreamQueued: upstreamStats.queued,
      });
    });

    if (pathname === "/" || pathname === "/admin/stats") {
      if (pathname === "/admin/stats") {
        res.writeHead(200, {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
        });
        res.end(JSON.stringify(createAdminStats(store, requestState, runUpstream), null, 2));
        return;
      }

      const stats = createLoadSnapshot(store, requestState, runUpstream);
      res.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      res.end(renderRootPage(stats));
      return;
    }

    if (
      pathname === "/npm/-/npm/v1/security/advisories/bulk" ||
      pathname === "/npm/-/npm/v1/security/audits/quick"
    ) {
      res.writeHead(200, {
        "content-type": "application/json",
        "content-length": 3,
        "cache-control": "no-store",
      });
      res.end("{}\n");
      return;
    }

    const pypiPath = parsePypiPath(pathname);
    if (pypiPath) {
      if (pypiPath.kind === "simple") {
        await handlePypiSimple(req, res, pypiPath.packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream);
        return;
      }
      await handlePypiDownload(req, res, pypiPath.packageName, pypiPath.filename, urlObj.searchParams, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream);
      return;
    }

    if (pathname.startsWith("/npm/")) {
      const npmPath = parseNpmPath(pathname);
      if (!npmPath) return notFound(res);

      if (npmPath.kind === "tarball") {
        await handleNpmTarball(
          req,
          res,
          npmPath.packageName,
          npmPath.tarballFileName,
          store,
          logger,
          BASE_URL,
          KEEP_VERSIONS,
          METADATA_TTL_MS,
          runUpstream
        );
        return;
      }

      await handleNpmMetadata(
        req,
        res,
        npmPath.packageName,
        store,
        logger,
        BASE_URL,
        KEEP_VERSIONS,
        METADATA_TTL_MS,
        runUpstream
      );
      return;
    }

    notFound(res);
  } catch (error) {
    logger.error("request_failed", { error: error.message });
    if (!res.headersSent && !res.writableEnded) {
      res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      res.end(`internal error: ${error.message}\n`);
    }
  } finally {
    requestState.activeRequests = Math.max(0, requestState.activeRequests - 1);
  }
});

await store.init();

if (PREWARM) {
  await prewarmCache();
}

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
  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end("not found\n");
}
