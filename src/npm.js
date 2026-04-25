import fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { createSingleFlight } from "./singleflight.js";
import { fetchJson, fetchResponse } from "./fetch.js";

const singleFlight = createSingleFlight();

async function isFresh(filePath, maxAgeMs) {
  try {
    const stat = await fs.stat(filePath);
    return Date.now() - stat.mtimeMs < maxAgeMs;
  } catch {
    return false;
  }
}

function tarballFileNameFromUrl(tarballUrl) {
  return path.posix.basename(new URL(tarballUrl).pathname);
}

function npmUpstream(packageName) {
  return `https://registry.npmjs.org/${encodeURIComponent(packageName)}`;
}

function rewriteNpmMetadata(pkgName, upstream, baseUrl) {
  const rewritten = structuredClone(upstream);
  rewritten["dist-tags"] = rewritten["dist-tags"] ?? rewritten.distTags ?? {};
  delete rewritten.distTags;
  rewritten.versions = rewritten.versions ?? {};

  for (const [version, info] of Object.entries(rewritten.versions)) {
    if (!info?.dist?.tarball) continue;
    const tarballFileName = tarballFileNameFromUrl(info.dist.tarball);
    info.dist.tarball = `${baseUrl}/npm/${encodeURIComponent(pkgName)}/-/${encodeURIComponent(tarballFileName)}`;
  }

  rewritten.readme = rewritten.readme ?? "";
  return rewritten;
}

function buildNpmMetadataSnapshot(store, packageName) {
  const pkg = store.state.npm?.[packageName];
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

export async function ensureNpmRegistry(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream) {
  const run = typeof runUpstream === "function" ? runUpstream : (fn) => fn();

  return singleFlight(`npm:metadata:${packageName}`, async () => {
    const metadataPath = store.metadataPath("npm", packageName);
    if (await isFresh(metadataPath, METADATA_TTL_MS)) {
      const cached = await store.readMetadata("npm", packageName);
      if (cached) {
        try {
          const parsed = JSON.parse(cached);
          logger.debug("cache_hit", { ecosystem: "npm", packageName, kind: "metadata", source: "cache" });
          return rewriteNpmMetadata(packageName, parsed, BASE_URL);
        } catch {
          logger.warn("metadata_parse_failed", { ecosystem: "npm", packageName });
        }
      }
    }

    logger.info("cache_miss", { ecosystem: "npm", packageName, kind: "metadata" });
    logger.debug("fetch_metadata_upstream", { packageName });

    const upstream = await run(() => fetchJson(npmUpstream(packageName)));
    const versions = upstream.versions ?? {};
    const time = upstream.time ?? {};

    for (const [version, info] of Object.entries(versions)) {
      if (!info?.dist?.tarball) continue;
      const tarballFileName = tarballFileNameFromUrl(info.dist.tarball);
      store.recordVersion("npm", packageName, version, {
        upstreamTarballUrl: info.dist.tarball,
        tarballFileName,
        seenAt: time[version] ?? new Date().toISOString(),
      });
      logger.debug("recorded_version", { packageName, version, tarballFileName });
    }

    await store.saveMetadata("npm", packageName, JSON.stringify(upstream, null, 2));
    logger.debug("saved_metadata", { packageName });

    return rewriteNpmMetadata(packageName, upstream, BASE_URL);
  });
}

export const ensureNpmMetadata = ensureNpmRegistry;

export async function handleNpmMetadata(req, res, packageName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream) {
  try {
    store.recordRequest("npm", packageName);

    const metadataPath = store.metadataPath("npm", packageName);
    if (await isFresh(metadataPath, METADATA_TTL_MS)) {
      const cached = await store.readMetadata("npm", packageName);
      if (cached) {
        try {
          const parsed = JSON.parse(cached);
          const metadata = rewriteNpmMetadata(packageName, parsed, BASE_URL);
          logger.info("cache_hit", { ecosystem: "npm", packageName, kind: "metadata", source: "cache" });
          logger.debug("serve_metadata", { packageName, source: "cache" });
          res.writeHead(200, {
            "content-type": "application/json; charset=utf-8",
          });
          res.end(JSON.stringify(metadata, null, 2));
          return;
        } catch {
          logger.warn("metadata_parse_failed", { ecosystem: "npm", packageName });
        }
      }
    }

    const metadata = await ensureNpmRegistry(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream);
    logger.info("serve", { ecosystem: "npm", packageName, kind: "metadata", source: "upstream" });
    res.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
    });
    res.end(JSON.stringify(metadata, null, 2));
  } catch (error) {
    logger.error("request_failed", { ecosystem: "npm", packageName, kind: "metadata", error: error.message });
    if (!res.headersSent && !res.writableEnded) {
      res.writeHead(502);
      res.end(`npm proxy error: ${error.message}\n`);
    }
  }
}

export async function handleNpmTarball(req, res, packageName, tarballFileName, store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, runUpstream) {
  store.recordRequest("npm", packageName);
  const run = typeof runUpstream === "function" ? runUpstream : (fn) => fn();

  try {
    let versionLookup = store.getVersionEntryByTarballFileName("npm", packageName, tarballFileName);
    if (!versionLookup) {
      logger.debug("cache_miss_version", { packageName, tarballFileName });
      const metadata = await ensureNpmRegistry(store, logger, BASE_URL, KEEP_VERSIONS, METADATA_TTL_MS, packageName, runUpstream);
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
    if (!version) {
      res.writeHead(404);
      res.end("tarball url not found\n");
      return;
    }

    const cachedStream = await store.openSavedFileStream("npm", packageName, version, tarballFileName);
    if (cachedStream) {
      store.markFileAccess("npm", packageName, version, tarballFileName);
      const fileRecord = store.getFileRecord("npm", packageName, version, tarballFileName);
      const headers = { "content-type": fileRecord?.contentType ?? "application/octet-stream" };
      if (fileRecord?.contentEncoding) headers["content-encoding"] = fileRecord.contentEncoding;
      if (fileRecord?.contentLength) headers["content-length"] = fileRecord.contentLength;
      logger.info("cache_hit", { ecosystem: "npm", packageName, version, file: tarballFileName, kind: "tarball", source: "cache" });
      logger.debug("serve_tarball", { packageName, version, source: "cache" });
      res.writeHead(200, headers);
      await pipeline(cachedStream, res);
      return;
    }

    const upstreamUrl = versionLookup?.versionEntry?.upstreamTarballUrl;
    if (!upstreamUrl) {
      logger.warn("tarball_url_not_found", { packageName, tarballFileName });
      res.writeHead(404);
      res.end("tarball url not found\n");
      return;
    }

    logger.info("fetch_upstream", { ecosystem: "npm", packageName, version, url: upstreamUrl, kind: "tarball" });
    const response = await run(() => fetchResponse(upstreamUrl));
    if (!response.body) {
      throw new Error("upstream tarball stream missing");
    }

    const tempDir = store.versionDir("npm", packageName, version);
    const filePath = store.cachedFilePath("npm", packageName, version, tarballFileName);
    const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    await fs.mkdir(tempDir, { recursive: true });

    const contentType = response.headers.get("content-type") ?? "application/octet-stream";
    const contentEncoding = response.headers.get("content-encoding");
    const contentLength = response.headers.get("content-length");
    const headers = { "content-type": contentType };
    if (contentEncoding) headers["content-encoding"] = contentEncoding;
    if (contentLength) headers["content-length"] = contentLength;
    res.writeHead(200, headers);

    const [clientBody, diskBody] = response.body.tee();
    const clientStream = pipeline(Readable.fromWeb(clientBody), res);
    const diskStream = pipeline(Readable.fromWeb(diskBody), createWriteStream(tempPath));
    const [clientResult, diskResult] = await Promise.allSettled([clientStream, diskStream]);

    if (diskResult.status === "fulfilled") {
      await fs.rename(tempPath, filePath);
      const saved = await fs.stat(filePath);
      store.recordVersion("npm", packageName, version, {
        upstreamTarballUrl: upstreamUrl,
        tarballFileName,
        seenAt: new Date().toISOString(),
      });
      store.recordFile("npm", packageName, version, {
        filename: tarballFileName,
        upstreamUrl: upstreamUrl,
        cached: true,
        sizeBytes: saved.size,
        lastAccessedAt: new Date().toISOString(),
        contentType,
        contentEncoding,
        contentLength,
      });
      logger.info("cached", { ecosystem: "npm", packageName, version, file: tarballFileName, size: saved.size });
    } else {
      await fs.rm(tempPath, { force: true }).catch(() => {});
      logger.warn("cache_stream_failed", {
        ecosystem: "npm",
        packageName,
        version,
        error: diskResult.reason?.message ?? "disk stream failed",
      });
    }

    if (clientResult.status === "rejected" && !res.writableEnded) {
      logger.debug("client_stream_closed", {
        ecosystem: "npm",
        packageName,
        version,
        error: clientResult.reason?.message ?? "client stream failed",
      });
    }
  } catch (error) {
    logger.error("request_failed", { ecosystem: "npm", packageName, kind: "tarball", error: error.message });
    if (!res.headersSent && !res.writableEnded) {
      res.writeHead(502);
      res.end(`npm tarball error: ${error.message}\n`);
    }
  }
}
