import fs from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { Store } from "./store.js";
import { fetchJson, fetchText, fetchBytes, fetchResponse, fetchWithRetry } from "./fetch.js";
import path from "node:path";

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

const singleFlight = createSingleFlight();

async function isFresh(filePath, maxAgeMs) {
  try {
    const stat = await fs.stat(filePath);
    return Date.now() - stat.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

export async function handlePypiSimple(req, res, packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS) {
  try {
    store.recordRequest("pypi", packageName);
    const htmlFile = store.htmlPath("pypi", packageName);
    if (await isFresh(htmlFile, METADATA_TTL_MS)) {
      const cachedStream = await store.openHtmlStream("pypi", packageName);
      if (cachedStream) {
        logger.info("cache_hit", { ecosystem: "pypi", packageName, kind: "simple_index", source: "cache" });
        logger.debug("serve_simple_index", { packageName, source: "cache" });
        res.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
        });
        await pipeline(cachedStream, res);
        return;
      }
    }

    logger.info("cache_miss", { ecosystem: "pypi", packageName, kind: "simple_index" });
    logger.debug("fetch_simple_index_upstream", { packageName });
    const html = await ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName);
    logger.info("serve", { ecosystem: "pypi", packageName, kind: "simple_index", source: "upstream" });
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
    });
    res.end(html);
  } catch (error) {
    logger.error("request_failed", { ecosystem: "pypi", packageName, kind: "simple_index", error: error.message });
    res.writeHead(502);
    res.end(`pypi proxy error: ${error.message}\n`);
  }
}

export async function handlePypiDownload(req, res, packageName, filename, query, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS) {
  const upstreamUrl = query.get("u");
  const queryVersion = query.get("v");
  if (!upstreamUrl) {
    res.writeHead(400);
    res.end("missing upstream url\n");
    return;
  }

  let resolvedVersion = queryVersion || store.getVersionEntryByUpstreamUrl("pypi", packageName, upstreamUrl)?.versionEntry?.version;
  if (!resolvedVersion) {
    await ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName);
    resolvedVersion = queryVersion || store.getVersionEntryByUpstreamUrl("pypi", packageName, upstreamUrl)?.versionEntry?.version;
  }
  if (!resolvedVersion) {
    res.writeHead(404);
    res.end("version mapping not found\n");
    return;
  }

const cachedStream = await store.openSavedFileStream("pypi", packageName, resolvedVersion, filename);
if (cachedStream) {
  store.markFileAccess("pypi", packageName, resolvedVersion, filename);
  const fileRecord = store.getFileRecord("pypi", packageName, resolvedVersion, filename);
  const headers = { "content-type": "application/octet-stream" };
  if (fileRecord?.contentEncoding) {
    headers["content-encoding"] = fileRecord.contentEncoding;
  }
  if (fileRecord?.contentLength) {
    headers["content-length"] = fileRecord.contentLength;
  }
  logger.info("cache_hit", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, kind: "file", source: "cache" });
  logger.debug("serve_file", { packageName, version: resolvedVersion, filename, source: "cache" });
  res.writeHead(200, headers);
  await pipeline(cachedStream, res);
  return;
}

  try {
    await singleFlight(`pypi:${packageName}:file:${upstreamUrl}`, async () => {
      const response = await fetchResponse(upstreamUrl);
      const tempDir = store.versionDir("pypi", packageName, resolvedVersion);
      await fs.mkdir(tempDir, { recursive: true });
      const filePath = store.cachedFilePath("pypi", packageName, resolvedVersion, filename);
      const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
      try {
        await pipeline(Readable.fromWeb(response.body), createWriteStream(tempPath));
        await fs.rename(tempPath, filePath);
      } catch (error) {
        await fs.rm(tempPath, { force: true }).catch(() => {});
        throw error;
      }
const saved = await fs.stat(filePath);
      store.recordVersion("pypi", packageName, resolvedVersion, {
        seenAt: new Date().toISOString(),
      });
      store.recordFile("pypi", packageName, resolvedVersion, {
        filename,
        upstreamUrl,
        cached: true,
        sizeBytes: saved.size,
        lastAccessedAt: new Date().toISOString(),
        contentEncoding: contentEncoding, // Store content encoding
      });
      store.recordFile("pypi", packageName, resolvedVersion, {
        filename,
        upstreamUrl,
        cached: true,
        sizeBytes: saved.size,
        lastAccessedAt: new Date().toISOString(),
        contentEncoding: contentEncoding, // Store content encoding
      });
      logger.info("cached", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, size: saved.size });
      logger.info("serve", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, kind: "file", source: "cache", cached: true });
      const cachedStream = await store.openSavedFileStream("pypi", packageName, resolvedVersion, filename);
      if (!cachedStream) {
        res.writeHead(500);
        res.end("cached file missing after save\n");
        return;
      }
      res.writeHead(200, {
        "content-type": "application/octet-stream",
      });
      await pipeline(cachedStream, res);
    });
  } catch (error) {
    logger.error("request_failed", { ecosystem: "pypi", packageName, version: resolvedVersion, kind: "file", error: error.message });
    res.writeHead(502);
    res.end(`pypi download error: ${error.message}\n`);
  }
}

export async function ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName) {
  return singleFlight(`pypi:${packageName}:simple`, async () => {
    const htmlFile = store.htmlPath("pypi", packageName);
    if (await isFresh(htmlFile, METADATA_TTL_MS)) {
      logger.debug("cache_hit", { ecosystem: "pypi", packageName, kind: "simple_index", source: "cache" });
      return store.readHtml("pypi", packageName);
    }
    logger.info("cache_miss", { ecosystem: "pypi", packageName, kind: "simple_index" });
    logger.debug("fetch_simple_index_upstream", { packageName });

    const [html, json] = await Promise.all([
      fetchText(pypiSimpleUpstream(packageName)),
      fetchJson(pypiJsonUpstream(packageName)).catch(() => null),
    ]);

    const fileProxyMap = new Map();
    if (json?.releases) {
      const versions = Object.entries(json.releases)
        .map(([version, files]) => {
          const latest = (files ?? [])
            .map((file) => file.upload_time_iso_8601 ?? file.upload_time ?? null)
            .filter(Boolean)
            .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0];
          return { version, latest };
        })
        .filter((entry) => entry.latest)
        .sort((a, b) => new Date(b.latest).getTime() - new Date(a).getTime())
        .slice(0, KEEP_VERSIONS);

      for (const entry of versions) {
        store.recordVersion("pypi", packageName, entry.version, { seenAt: entry.latest });
      }

      for (const [version, files] of Object.entries(json.releases)) {
        for (const file of files ?? {}) {
          if (file.url) {
            const fileName = file.filename ?? safeBaseName(file.url);
            fileProxyMap.set(
              file.url,
              `${BASE_URL}/packages/pypi/${encodeURIComponent(packageName)}/${encodeURIComponent(fileName)}?u=${encodeURIComponent(file.url)}&v=${encodeURIComponent(version)}`
            );
          }
        }
      }
    }

    const rewritten = rewritePypiSimpleHtml(html, fileProxyMap);
    await store.saveHtml("pypi", packageName, rewritten);
    return rewritten;
  });
}

function pypiJsonUpstream(packageName) {
  return `https://pypi.org/pypi/${encodeURIComponent(packageName)}/json`;
}

function pypiSimpleUpstream(packageName) {
  return `https://pypi.org/simple/${encodeURIComponent(packageName)}/`;
}

function rewritePypiSimpleHtml(html, fileProxyMap) {
  const linkRegex = /<a\s+([^>]*href=")([^"]+)(")([^>]*)>(.*?)<\/a>/gis;
  return html.replace(linkRegex, (match, prefix, href, suffix, rest, label) => {
    const proxyUrl = fileProxyMap.get(href);
    if (!proxyUrl) return match;
    return `<a ${prefix}${escapeHtml(proxyUrl)}${suffix}${rest}>${label}</a`;
  });
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&")
    .replaceAll("<", "<")
    .replaceAll(">", ">")
    .replaceAll('"', "\"")
}

function safeBaseName(urlString) {
  const pathname = new URL(urlString).pathname;
  const base = path.posix.basename(pathname);
  return base || "download.bin";
}