import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable, PassThrough } from "node:stream";
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

export async function ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName) {
  return singleFlight(`npm:${packageName}:metadata`, async () => {
    const metadataFile = store.metadataPath("npm", packageName);
    if (await isFresh(metadataFile, METADATA_TTL_MS)) {
      logger.debug("cache_hit", { ecosystem: "npm", packageName, kind: "metadata", source: "cache" });
      const snapshot = buildNpmMetadataSnapshot(store, packageName);
      if (snapshot) return snapshot;
    }
    logger.info("cache_miss", { ecosystem: "npm", packageName, kind: "metadata" });
    logger.debug("fetch_metadata_upstream", { packageName });

    const upstream = await fetchJson(npmUpstream(packageName));
    const versions = upstream.versions ?? {};
    const time = upstream.time ?? {};

    // Record all versions (except time metadata) in the store
    for (const [version, info] of Object.entries(versions)) {
      if (info?.dist?.tarball) {
        const tarballFileName = tarballFileNameFromUrl(info.dist.tarball);
        store.recordVersion("npm", packageName, version, {
          upstreamTarballUrl: info.dist.tarball,
          tarballFileName,
          seenAt: time[version] ?? new Date().toISOString(),
        });
        logger.debug("recorded_version", { packageName, version, tarballFileName });
      }
    }

    // Save the full metadata (including all versions)
    await store.saveMetadata("npm", packageName, JSON.stringify(upstream, null, 2));
    logger.debug("saved_metadata", { packageName });
    return rewriteNpmMetadata(packageName, upstream, Object.keys(versions), BASE_URL);
  });
}

export async function handleNpmMetadata(req, res, packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS) {
  try {
    store.recordRequest("npm", packageName);
    const metadataFile = store.metadataPath("npm", packageName);
    if (await isFresh(metadataFile, METADATA_TTL_MS)) {
      const cachedStream = await store.openMetadataStream("npm", packageName);
      if (cachedStream) {
        logger.info("cache_hit", { ecosystem: "npm", packageName, kind: "metadata", source: "cache" });
        logger.debug("serve_metadata", { packageName, source: "cache" });
        res.writeHead(200, {
          "content-type": "application/json; charset=utf-8",
        });
        await pipeline(cachedStream, res);
        return;
      }
    }

    logger.info("cache_miss", { ecosystem: "npm", packageName, kind: "metadata" });
    logger.debug("fetch_metadata_upstream", { packageName });
    const metadata = await ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName);
    logger.info("serve", { ecosystem: "npm", packageName, kind: "metadata", source: "upstream" });
    res.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
    });
    res.end(JSON.stringify(metadata, null, 2));
  } catch (error) {
    logger.error("request_failed", { ecosystem: "npm", packageName, kind: "metadata", error: error.message });
    res.writeHead(502);
    res.end(`npm proxy error: ${error.message}\n`);
  }
}

export async function handleNpmTarball(req, res, packageName, tarballFileName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS) {
  store.recordRequest("npm", packageName);
  
  try {
    let versionLookup = store.getVersionEntryByTarballFileName("npm", packageName, tarballFileName);
    if (!versionLookup) {
      logger.debug("cache_miss_version", { packageName, tarballFileName });
      const metadata = await ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName);
      const fallback = findTarballInNpmMetadata(metadata, tarballFileName);
      if (fallback?.version) {
        versionLookup = {
          versionEntry: {
            version: fallback.version,
            upstreamTarballUrl: fallback.upstreamTarballUrl,
            tarballFileName: fallback.tarballFileName,
          },
          file: null,
        };
        logger.debug("found_tarball_in_metadata", { packageName, version: fallback.version, tarballFileName });
      }
    }

    const version = versionLookup?.versionEntry?.version;
    if (version) {
      const cachedStream = await store.openSavedFileStream("npm", packageName, version, tarballFileName);
      if (cachedStream) {
        store.markFileAccess("npm", packageName, version, tarballFileName);
        const fileRecord = store.getFileRecord("npm", packageName, version, tarballFileName);
        const headers = { "content-type": "application/octet-stream" };
        if (fileRecord?.contentEncoding) {
          headers["content-encoding"] = fileRecord.contentEncoding;
        }
        if (fileRecord?.contentLength) {
          headers["content-length"] = fileRecord.contentLength;
        }
        logger.info("cache_hit", { ecosystem: "npm", packageName, version, file: tarballFileName, kind: "tarball", source: "cache" });
        logger.debug("serve_tarball", { packageName, version, source: "cache" });
        res.writeHead(200, headers);
        await pipeline(cachedStream, res);
        return;
      }
    }

    // Tarball not in cache, fetch from upstream
    const versionEntry = versionLookup?.versionEntry;
    let url = versionEntry?.upstreamTarballUrl;
    
    if (!url) {
      logger.debug("cache_miss_tarball", { packageName, tarballFileName });
      await ensureNpmMetadata(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName);
      const refreshedLookup = store.getVersionEntryByTarballFileName("npm", packageName, tarballFileName);
      url = refreshedLookup?.versionEntry?.upstreamTarballUrl;
    }
    
    if (!url) {
      logger.warn("tarball_url_not_found", { packageName, tarballFileName });
      res.writeHead(404);
      res.end("tarball url not found\n");
      return;
    }

    logger.info("fetch_upstream", { ecosystem: "npm", packageName, version: versionEntry?.version, url, kind: "tarball" });
    const response = await fetchResponse(url);
    const webStream = Readable.fromWeb(response.body);
    
    // Preserve headers from upstream response
    const headers = {};
    const contentType = response.headers.get("content-type");
    if (contentType) headers["content-type"] = contentType;
    const contentEncoding = response.headers.get("content-encoding");
    if (contentEncoding) headers["content-encoding"] = contentEncoding;
    const contentLength = response.headers.get("content-length");
    if (contentLength) headers["content-length"] = contentLength;
    
    res.writeHead(200, headers);
    
    const tempDir = store.versionDir("npm", packageName, version);
    const filePath = store.cachedFilePath("npm", packageName, version, tarballFileName);
    const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    
    try {
      await fs.mkdir(tempDir, { recursive: true });
      
      const teeClient = new PassThrough();
      const teeDisk = new PassThrough();
      
      webStream.on('data', (chunk) => {
        teeClient.write(chunk);
        teeDisk.write(chunk);
      });
      
      webStream.on('end', () => {
        teeClient.end();
        teeDisk.end();
      });
      
      webStream.on('error', (err) => {
        teeClient.destroy(err);
        teeDisk.destroy(err);
      });
      
      const fileStream = createWriteStream(tempPath);
      
      const [clientResult] = await Promise.allSettled([
        pipeline(teeClient, res),
        pipeline(teeDisk, fileStream)
      ]);
      
      if (clientResult.status === 'fulfilled') {
        try {
          await fs.rename(tempPath, filePath);
          const saved = await fs.stat(filePath);
          store.recordVersion("npm", packageName, version, {
            upstreamTarballUrl: url,
            tarballFileName,
            seenAt: new Date().toISOString(),
          });
          store.recordFile("npm", packageName, version, {
            filename: tarballFileName,
            upstreamUrl: url,
            cached: true,
            sizeBytes: saved.size,
            lastAccessedAt: new Date().toISOString(),
            contentEncoding: contentEncoding, // Store content encoding
          });
          logger.info("cached", { ecosystem: "npm", packageName, version, file: tarballFileName, size: saved.size });
          logger.info("serve", { ecosystem: "npm", packageName, version, file: tarballFileName, kind: "tarball", source: "cache", cached: true });
        } catch (err) {
          logger.warn("cache_finalize_failed", { ecosystem: "npm", packageName, error: error.message });
          await fs.rm(tempPath, { force: true }).catch(() => {});
        }
      } else {
        await fs.rm(tempPath, { force: true }).catch(() => {});
      }
      
    } catch (error) {
      logger.error("request_failed", { ecosystem: "npm", packageName, kind: "tarball", error: error.message });
      if (!res.headersSent) {
        res.writeHead(502);
        res.end(`npm tarball error: ${error.message}\n`);
      }
    }
    
  } catch (error) {
    logger.error("request_failed", { ecosystem: "npm", packageName, kind: "tarball", error: error.message });
    if (!res.headersSent) {
      res.writeHead(502);
      res.end(`npm tarball error: ${error.message}\n`);
    }
  }
}

function tarballFileNameFromUrl(tarballUrl) {
  return path.posix.basename(new URL(tarballUrl).pathname);
}

function findTarballInNpmMetadata(metadata, tarballFileName) {
  for (const [version, info] of Object.entries(metadata?.versions ?? {})) {
    const tarballUrl = info?.dist?.tarball;
    if (!tarballUrl) continue;
    if (tarballFileNameFromUrl(tarballUrl) === tarballFileName) {
      return { version, upstreamTarballUrl: tarballUrl, tarballFileName };
    }
  }
  return null;
}

function buildNpmMetadataSnapshot(store, packageName) {
  const pkg = store.state.npm[packageName];
  if (!pkg) return null;
  const versions = {};
  let latestVersion = null;
  for (const entry of pkg.versions ?? []) {
    if (!latestVersion) latestVersion = entry.version ?? null;
    if (!entry.upstreamTarballUrl || !entry.version) continue;
    versions[entry.version] = {
      dist: {
        tarball: entry.upstreamTarballUrl,
      },
    };
  }
  if (Object.keys(versions).length === 0) return null;
  return {
    "dist-tags": latestVersion ? { latest: latestVersion } : {},
    versions,
    readme: "",
  };
}

function rewriteNpmMetadata(pkgName, upstream, versions, baseUrl) {
  const rewritten = structuredClone(upstream);
  rewritten.distTags = rewritten["dist-tags"] ?? rewritten.distTags ?? {};
  rewritten.versions = rewritten.versions ?? {};
  for (const [version, info] of Object.entries(rewritten.versions)) {
    if (info?.dist?.tarball) {
      const tarballFileName = tarballFileNameFromUrl(info.dist.tarball);
      info.dist.tarball = `${baseUrl}/npm/${encodeURIComponent(pkgName)}/-/${encodeURIComponent(tarballFileName)}`;
    }
  }
  rewritten.readme = rewritten.readme ?? "";
  rewritten._cachedVersions = versions;
  return rewritten;
}

function npmUpstream(packageName) {
  return `https://registry.npmjs.org/${encodeURIComponent(packageName)}`;
}
