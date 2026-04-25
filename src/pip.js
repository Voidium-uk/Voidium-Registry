import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { createSingleFlight } from "./singleflight.js";
import { fetchJson, fetchText, fetchResponse } from "./fetch.js";

const singleFlight = createSingleFlight();

async function isFresh(filePath, maxAgeMs) {
  try {
    const stat = await fs.stat(filePath);
    return Date.now() - stat.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

export async function handlePypiSimple(req, res, packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream) {
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
    const html = await ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream);
    logger.info("serve", { ecosystem: "pypi", packageName, kind: "simple_index", source: "upstream" });
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
    });
    res.end(html);
  } catch (error) {
    logger.error("request_failed", { ecosystem: "pypi", packageName, kind: "simple_index", error: error.message });
    if (!res.headersSent && !res.writableEnded) {
      res.writeHead(502);
      res.end(`pypi proxy error: ${error.message}\n`);
    }
  }
}

export async function handlePypiDownload(req, res, packageName, filename, query, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream) {
  const run = typeof runUpstream === "function" ? runUpstream : (fn) => fn();
  const upstreamUrl = query.get("u");
  const queryVersion = query.get("v");

  if (!upstreamUrl) {
    res.writeHead(400);
    res.end("missing upstream url\n");
    return;
  }

  let resolvedVersion = queryVersion || store.getVersionEntryByUpstreamUrl("pypi", packageName, upstreamUrl)?.versionEntry?.version;
  if (!resolvedVersion) {
    await ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, run);
    resolvedVersion = queryVersion || store.getVersionEntryByUpstreamUrl("pypi", packageName, upstreamUrl)?.versionEntry?.version;
  }

  if (!resolvedVersion) {
    res.writeHead(404);
    res.end("version mapping not found\n");
    return;
  }

  try {
    const cachedStream = await store.openSavedFileStream("pypi", packageName, resolvedVersion, filename);
    if (cachedStream) {
      store.markFileAccess("pypi", packageName, resolvedVersion, filename);
      const fileRecord = store.getFileRecord("pypi", packageName, resolvedVersion, filename);
      const headers = { "content-type": fileRecord?.contentType ?? "application/octet-stream" };
      if (fileRecord?.sizeBytes) headers["content-length"] = String(fileRecord.sizeBytes);
      logger.info("cache_hit", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, kind: "file", source: "cache" });
      logger.debug("serve_file", { packageName, version: resolvedVersion, filename, source: "cache" });
      res.writeHead(200, headers);
      await pipeline(cachedStream, res);
      return;
    }

    const response = await run(() => fetchResponse(upstreamUrl));
    if (!response.body) {
      throw new Error("upstream file stream missing");
    }

    const tempDir = store.versionDir("pypi", packageName, resolvedVersion);
    const filePath = store.cachedFilePath("pypi", packageName, resolvedVersion, filename);
    const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    await fs.mkdir(tempDir, { recursive: true });

    const contentType = response.headers.get("content-type") ?? "application/octet-stream";
    const headers = { "content-type": contentType };
    res.writeHead(200, headers);

    const [clientBody, diskBody] = response.body.tee();
    const clientStream = pipeline(Readable.fromWeb(clientBody), res);
    const diskStream = pipeline(Readable.fromWeb(diskBody), createWriteStream(tempPath));
    const [clientResult, diskResult] = await Promise.allSettled([clientStream, diskStream]);

    if (diskResult.status === "fulfilled") {
      await fs.rename(tempPath, filePath);
      const saved = await fs.stat(filePath).catch(() => null);
      if (!saved) {
        throw new Error("cached file missing after save");
      }
      store.recordVersion("pypi", packageName, resolvedVersion, {
        seenAt: new Date().toISOString(),
      });
      store.recordFile("pypi", packageName, resolvedVersion, {
        filename,
        upstreamUrl,
        cached: true,
        sizeBytes: saved.size,
        lastAccessedAt: new Date().toISOString(),
        contentType,
      });
      logger.info("cached", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, size: saved.size });
      logger.info("serve", { ecosystem: "pypi", packageName, version: resolvedVersion, file: filename, kind: "file", source: "cache", cached: true });
    } else {
      await fs.rm(tempPath, { force: true }).catch(() => {});
      logger.warn("cache_stream_failed", {
        ecosystem: "pypi",
        packageName,
        version: resolvedVersion,
        error: diskResult.reason?.message ?? "disk stream failed",
      });
    }

    if (clientResult.status === "rejected" && !res.writableEnded) {
      logger.debug("client_stream_closed", {
        ecosystem: "pypi",
        packageName,
        version: resolvedVersion,
        error: clientResult.reason?.message ?? "client stream failed",
      });
    }
  } catch (error) {
    logger.error("request_failed", { ecosystem: "pypi", packageName, version: resolvedVersion, kind: "file", error: error.message });
    if (!res.headersSent && !res.writableEnded) {
      res.writeHead(502);
      res.end(`pypi download error: ${error.message}\n`);
    }
  }
}

export async function ensurePypiHtml(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream) {
  const run = typeof runUpstream === "function" ? runUpstream : (fn) => fn();
  return singleFlight(`pypi:${packageName}:simple`, async () => {
    const htmlFile = store.htmlPath("pypi", packageName);
    if (await isFresh(htmlFile, METADATA_TTL_MS)) {
      logger.debug("cache_hit", { ecosystem: "pypi", packageName, kind: "simple_index", source: "cache" });
      return store.readHtml("pypi", packageName);
    }

    logger.info("cache_miss", { ecosystem: "pypi", packageName, kind: "simple_index" });
    logger.debug("fetch_simple_index_upstream", { packageName });

    const [html, json] = await Promise.all([
      run(() => fetchText(pypiSimpleUpstream(packageName))),
      run(() => fetchJson(pypiJsonUpstream(packageName)).catch(() => null)),
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
        .sort((a, b) => new Date(b.latest).getTime() - new Date(a.latest).getTime())
        .slice(0, KEEP_VERSIONS);

      for (const entry of versions) {
        store.recordVersion("pypi", packageName, entry.version, { seenAt: entry.latest });
      }

      for (const [version, files] of Object.entries(json.releases)) {
        for (const file of files ?? []) {
          if (!file.url) continue;
          const fileName = file.filename ?? safeBaseName(file.url);
          fileProxyMap.set(
            file.url,
            `${BASE_URL}/packages/pypi/${encodeURIComponent(packageName)}/${encodeURIComponent(fileName)}?u=${encodeURIComponent(file.url)}&v=${encodeURIComponent(version)}`
          );
        }
      }
    }

    const rewritten = rewritePypiSimpleHtml(html, fileProxyMap);
    await store.saveHtml("pypi", packageName, rewritten);
    return rewritten;
  });
}

export async function ensurePypiRegistry(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream) {
  const run = (fn) => (typeof runUpstream === "function" ? runUpstream(fn) : fn());
  return singleFlight(`pypi:registry:${packageName}`, async () => {
    logger.info("cache_miss", { ecosystem: "pypi-registry", packageName, kind: "registry" });
    logger.debug("fetch_registry_upstream", { packageName });

    const tempDir = await fs.mkdtemp(path.join(store.cacheDir, "tmp-"));
    const tempFile = path.join(tempDir, "registry.json");

    try {
      await run(() =>
        new Promise(async (resolve, reject) => {
          const url = `https://pypi.org/pypi/${encodeURIComponent(packageName)}/json`;
          const response = await fetchResponse(url);
          if (!response.body) {
            reject(new Error("response body missing"));
            return;
          }
          const fileStream = createWriteStream(tempFile);
          await pipeline(Readable.fromWeb(response.body), fileStream);
          resolve();
        })
      );

      const rawData = await fs.readFile(tempFile, "utf8");
      const jsonData = JSON.parse(rawData);
      const info = jsonData.info || {};
      const releases = jsonData.releases || {};

      const result = {
        info: {
          name: info.name || packageName,
          version: info.version || "",
          summary: info.summary || "",
          description: info.description || "",
          author: info.author || "",
          author_email: info.author_email || "",
          maintainer: info.maintainer || "",
          maintainer_email: info.maintainer_email || "",
          license: info.license || "",
          home_page: info.home_page || "",
          project_urls: info.project_urls || {},
          requires_python: info.requires_python || "",
          requires_dist: info.requires_dist || [],
          classifiers: info.classifiers || [],
          keywords: info.keywords || "",
          platform: info.platform || "",
          download_url: info.download_url || "",
          metadata_version: info.metadata_version || "",
        },
        releases: {},
        urls: [],
      };

      const seenFiles = new Set();
      const versionEntries = [];

      for (const [version, files] of Object.entries(releases)) {
        const versionFiles = [];
        const versionHashes = {};

        for (const file of files ?? []) {
          if (!file.url) continue;
          if (seenFiles.has(file.url)) continue;
          seenFiles.add(file.url);

          const fileName = file.filename || safeBaseName(file.url);
          const proxyUrl = `${BASE_URL}/packages/pypi/${encodeURIComponent(packageName)}/${encodeURIComponent(fileName)}?u=${encodeURIComponent(file.url)}&v=${encodeURIComponent(version)}`;

          const entry = {
            filename: fileName,
            url: proxyUrl,
            size: file.size || 0,
            upload_time: file.upload_time_iso_8601 || file.upload_time || null,
            upload_time_iso_8601: file.upload_time_iso_8601 || file.upload_time || null,
            comment_text: file.comment_text || "",
            has_sig: file.has_sig || false,
            md5_digest: file.md5_digest || "",
            digests: file.digests || {
              md5: file.md5_digest || "",
              sha256: file.sha256_digest || "",
            },
            python_version: file.python_version || "source",
            requires_python: file.requires_python || null,
            packagetype: file.packagetype || "sdist",
            distribution: {
              name: fileName,
              version: version,
              size: file.size || 0,
              upload_time: file.upload_time_iso_8601 || file.upload_time || null,
            },
          };

          result.urls.push(entry);
          result.versions[version] = result.versions[version] || { urls: [], version };
          result.versions[version].urls.push(entry);

          const hashInfo = {};
          if (file.md5_digest) hashInfo.md5 = file.md5_digest;
          if (file.sha256_digest) hashInfo.sha256 = file.sha256_digest;
          if (Object.keys(hashInfo).length > 0) {
            versionHashes[fileName] = hashInfo;
          }

          versionFiles.push({
            filename: fileName,
            url: proxyUrl,
            size: file.size || 0,
            python_version: file.python_version || "source",
            packagetype: file.packagetype || "sdist",
            hashes: hashInfo,
          });
        }

        if (versionFiles.length > 0) {
          versionEntries.push({
            version,
            files: versionFiles,
          });

          store.recordVersion("pypi", packageName, version, {
            seenAt: new Date().toISOString(),
          });
        }
      }

      result.releases = Object.fromEntries(
        versionEntries.map((v) => [v.version, v.files])
      );

      return result;
    } finally {
      await fs.rm(tempDir, { recursive: true, force: true });
    }
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
    const [hrefBase, fragment = ""] = href.split("#", 2);
    const proxyBase = fileProxyMap.get(href) ?? fileProxyMap.get(hrefBase);
    if (!proxyBase) return match;
    const proxyUrl = fragment ? `${proxyBase}#${fragment}` : proxyBase;
    return `<a ${prefix}${escapeHtml(proxyUrl)}${suffix}${rest}>${label}</a>`;
  });
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function safeBaseName(urlString) {
  const pathname = new URL(urlString).pathname;
  const base = path.posix.basename(pathname);
  return base || "download.bin";
}
