import fs from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";

const DEFAULT_STATE = {
  npm: {},
};

const DEFAULT_CACHE_LIMIT_BYTES = 90 * 1024 * 1024 * 1024;

function makeDirSafe(value) {
  return value
    .replaceAll("/", "__")
    .replaceAll("@", "_at_")
    .replaceAll("%", "_pct_")
    .replace(/[^a-zA-Z0-9._-]/g, "_");
}

async function exists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

export class Store {
  constructor(rootDir, options = {}) {
    this.rootDir = rootDir;
    this.dataDir = path.join(rootDir, "data");
    this.cacheDir = path.join(this.dataDir, "cache");
    this.statePath = path.join(this.dataDir, "state.json");
    this.keepVersions = options.keepVersions ?? 10;
    this.cacheLimitBytes = options.cacheLimitBytes ?? DEFAULT_CACHE_LIMIT_BYTES;
    this.logger = options.logger ?? {
      debug() {},
      info() {},
      warn() {},
      error() {},
    };
    this.state = structuredClone(DEFAULT_STATE);
    this.totalCacheBytes = 0;
    this._saveTimer = null;
    this._savePromise = Promise.resolve();
  }

  async init() {
    await fs.mkdir(this.cacheDir, { recursive: true });
    if (await exists(this.statePath)) {
      const text = await fs.readFile(this.statePath, "utf8");
      this.state = this._mergeState(JSON.parse(text));
    } else {
      await this.persistState();
    }
    this._compactState();
    await this.rebuildCacheAccounting();
    await this.enforceCacheLimit();
  }

  _mergeState(parsed) {
    const state = structuredClone(DEFAULT_STATE);
    if (parsed && typeof parsed === "object") {
      for (const ecosystem of Object.keys(DEFAULT_STATE)) {
        const bucket = parsed[ecosystem];
        if (bucket && typeof bucket === "object") {
          state[ecosystem] = bucket;
        }
      }
    }
    return state;
  }

  _compactState() {
    for (const ecosystem of Object.keys(this.state)) {
      if (!this.state[ecosystem] || typeof this.state[ecosystem] !== "object") {
        this.state[ecosystem] = {};
      }
    }
  }

  _ensureEcosystem(ecosystem) {
    if (!this.state[ecosystem] || typeof this.state[ecosystem] !== "object") {
      this.state[ecosystem] = {};
    }
    return this.state[ecosystem];
  }

  packageDir(ecosystem, packageName) {
    return path.join(this.cacheDir, ecosystem, makeDirSafe(packageName));
  }

  versionDir(ecosystem, packageName, version) {
    return path.join(this.packageDir(ecosystem, packageName), makeDirSafe(version));
  }

  cachedFilePath(ecosystem, packageName, version, filename) {
    return path.join(this.versionDir(ecosystem, packageName, version), filename);
  }

  metadataPath(ecosystem, packageName) {
    return path.join(this.packageDir(ecosystem, packageName), "metadata.json");
  }

  getPackage(ecosystem, packageName) {
    const bucket = this._ensureEcosystem(ecosystem);
    if (!bucket[packageName]) {
      bucket[packageName] = {
        requests: 0,
        lastRequestedAt: null,
        versions: [],
        hot: false,
        cacheBytes: 0,
      };
    }
    const pkg = bucket[packageName];
    pkg.requests ??= 0;
    pkg.lastRequestedAt ??= null;
    pkg.versions ??= [];
    pkg.hot ??= false;
    pkg.cacheBytes ??= 0;
    return pkg;
  }

  _findFileEntry(versionEntry, fileRecord) {
    return (versionEntry.files ?? []).find((file) => {
      if (fileRecord.upstreamUrl && file.upstreamUrl === fileRecord.upstreamUrl) return true;
      if (fileRecord.filename && file.filename === fileRecord.filename) return true;
      return false;
    });
  }

  _normalizeFileRecord(fileRecord = {}) {
    return {
      filename: fileRecord.filename ?? null,
      upstreamUrl: fileRecord.upstreamUrl ?? null,
      cached: Boolean(fileRecord.cached),
      accesses: fileRecord.accesses ?? 0,
      sizeBytes: fileRecord.sizeBytes ?? 0,
      lastAccessedAt: fileRecord.lastAccessedAt ?? null,
      contentType: fileRecord.contentType ?? null,
      contentEncoding: fileRecord.contentEncoding ?? null,
      contentLength: fileRecord.contentLength ?? null,
    };
  }

  async rebuildCacheAccounting() {
    this.totalCacheBytes = 0;
    for (const ecosystem of Object.keys(this.state)) {
      const bucket = this._ensureEcosystem(ecosystem);
      for (const [packageName, pkg] of Object.entries(bucket)) {
        pkg.cacheBytes = 0;
        for (const versionEntry of pkg.versions ?? []) {
          versionEntry.files = versionEntry.files ?? [];
          if (versionEntry.upstreamTarballUrl && versionEntry.files.length === 0) {
            versionEntry.files.push({
              filename: `${versionEntry.version}.tgz`,
              upstreamUrl: versionEntry.upstreamTarballUrl,
              cached: false,
              accesses: 0,
              sizeBytes: 0,
              lastAccessedAt: null,
              contentType: null,
              contentEncoding: null,
              contentLength: null,
            });
          }

          for (const file of versionEntry.files) {
            file.accesses ??= 0;
            file.cached ??= false;
            file.sizeBytes ??= 0;
            file.lastAccessedAt ??= null;
            file.contentType ??= null;
            file.contentEncoding ??= null;
            file.contentLength ??= null;

            const cachedPath = file.filename
              ? this.cachedFilePath(ecosystem, packageName, versionEntry.version, file.filename)
              : null;
            let stat = null;
            if (cachedPath) {
              stat = await fs.stat(cachedPath).catch(() => null);
            }

            if (stat) {
              file.cached = true;
              file.sizeBytes = stat.size;
              pkg.cacheBytes += stat.size;
              this.totalCacheBytes += stat.size;
            } else {
              file.cached = false;
              file.sizeBytes = 0;
            }
          }
        }
      }
    }
  }

  recordRequest(ecosystem, packageName) {
    const pkg = this.getPackage(ecosystem, packageName);
    pkg.requests += 1;
    pkg.lastRequestedAt = new Date().toISOString();
    pkg.hot = pkg.requests >= 5;
    this.logger.debug?.("request", {
      ecosystem,
      packageName,
      requests: pkg.requests,
      hot: pkg.hot,
    });
    this.scheduleSave();
    return pkg;
  }

  recordVersion(ecosystem, packageName, version, extra = {}) {
    const pkg = this.getPackage(ecosystem, packageName);
    const idx = pkg.versions.findIndex((entry) => entry.version === version);
    const entry = {
      version,
      seenAt: new Date().toISOString(),
      ...extra,
    };
    if (idx >= 0) {
      pkg.versions[idx] = { ...pkg.versions[idx], ...entry };
      const [moved] = pkg.versions.splice(idx, 1);
      pkg.versions.unshift(moved);
    } else {
      pkg.versions.unshift(entry);
    }
    this.pruneVersions(ecosystem, packageName).catch(() => {});
    this.scheduleSave();
    return pkg;
  }

  recordFile(ecosystem, packageName, version, fileRecord) {
    const pkg = this.getPackage(ecosystem, packageName);
    let versionEntry = pkg.versions.find((entry) => entry.version === version);
    const normalized = this._normalizeFileRecord(fileRecord);

    if (!versionEntry) {
      versionEntry = {
        version,
        seenAt: new Date().toISOString(),
        files: [normalized],
      };
      pkg.versions.unshift(versionEntry);
      if (normalized.cached && normalized.sizeBytes) {
        pkg.cacheBytes += normalized.sizeBytes;
        this.totalCacheBytes += normalized.sizeBytes;
      }
    } else {
      versionEntry.files = versionEntry.files ?? [];
      const existing = this._findFileEntry(versionEntry, normalized);
      if (existing) {
        const oldSize = existing.cached ? existing.sizeBytes ?? 0 : 0;
        Object.assign(existing, {
          filename: normalized.filename ?? existing.filename ?? null,
          upstreamUrl: normalized.upstreamUrl ?? existing.upstreamUrl ?? null,
          cached: existing.cached || normalized.cached,
          accesses: existing.accesses ?? 0,
          sizeBytes: normalized.sizeBytes ?? existing.sizeBytes ?? 0,
          lastAccessedAt: normalized.lastAccessedAt ?? existing.lastAccessedAt ?? null,
          contentType: normalized.contentType ?? existing.contentType ?? null,
          contentEncoding: normalized.contentEncoding ?? existing.contentEncoding ?? null,
          contentLength: normalized.contentLength ?? existing.contentLength ?? null,
        });
        const newSize = existing.cached ? existing.sizeBytes ?? 0 : 0;
        const delta = newSize - oldSize;
        if (delta !== 0) {
          pkg.cacheBytes = (pkg.cacheBytes ?? 0) + delta;
          this.totalCacheBytes += delta;
        }
      } else {
        versionEntry.files.unshift(normalized);
        if (normalized.cached && normalized.sizeBytes) {
          pkg.cacheBytes += normalized.sizeBytes;
          this.totalCacheBytes += normalized.sizeBytes;
        }
      }
      versionEntry.seenAt = new Date().toISOString();
    }

    this.logger.debug?.("file_recorded", {
      ecosystem,
      packageName,
      version,
      cached: fileRecord.cached ?? false,
      sizeBytes: fileRecord.sizeBytes ?? 0,
    });
    if (fileRecord.cached && (fileRecord.sizeBytes ?? 0) > 0) {
      this.enforceCacheLimit().catch(() => {});
    }
    this.scheduleSave();
    return pkg;
  }

  markFileAccess(ecosystem, packageName, version, identifier) {
    const pkg = this.state[ecosystem]?.[packageName];
    if (!pkg) return;
    const versionEntry = pkg.versions.find((entry) => entry.version === version);
    if (!versionEntry) return;
    const file = (versionEntry.files ?? []).find(
      (entry) => entry.filename === identifier || entry.upstreamUrl === identifier
    );
    if (!file) return;
    file.accesses = (file.accesses ?? 0) + 1;
    file.lastAccessedAt = new Date().toISOString();
    this.scheduleSave();
  }

  async openMetadataStream(ecosystem, packageName) {
    const filePath = this.metadataPath(ecosystem, packageName);
    if (!(await exists(filePath))) return null;
    return createReadStream(filePath, { encoding: "utf8" });
  }

  async readMetadata(ecosystem, packageName) {
    if (!(await exists(this.metadataPath(ecosystem, packageName)))) return null;
    return fs.readFile(this.metadataPath(ecosystem, packageName), "utf8");
  }

  async saveMetadata(ecosystem, packageName, text) {
    await fs.mkdir(this.packageDir(ecosystem, packageName), { recursive: true });
    await fs.writeFile(this.metadataPath(ecosystem, packageName), text, "utf8");
  }

  async saveFile(ecosystem, packageName, version, filename, bytes) {
    const dir = this.versionDir(ecosystem, packageName, version);
    await fs.mkdir(dir, { recursive: true });
    const filePath = this.cachedFilePath(ecosystem, packageName, version, filename);
    await fs.writeFile(filePath, bytes);
    const stat = await fs.stat(filePath);
    return { filePath, sizeBytes: stat.size };
  }

  async readSavedFile(ecosystem, packageName, version, filename) {
    const filePath = this.cachedFilePath(ecosystem, packageName, version, filename);
    if (!(await exists(filePath))) return null;
    return fs.readFile(filePath);
  }

  async openSavedFileStream(ecosystem, packageName, version, filename) {
    const filePath = this.cachedFilePath(ecosystem, packageName, version, filename);
    if (!(await exists(filePath))) return null;
    return createReadStream(filePath);
  }

  getFileRecord(ecosystem, packageName, version, identifier) {
    const versionEntry = this.state[ecosystem]?.[packageName]?.versions?.find((entry) => entry.version === version);
    if (!versionEntry) return null;
    return (
      versionEntry.files?.find((file) => file.filename === identifier || file.upstreamUrl === identifier) ?? null
    );
  }

  getVersionEntryByTarballFileName(ecosystem, packageName, filename) {
    const pkg = this.state[ecosystem]?.[packageName];
    if (!pkg) return null;
    for (const versionEntry of pkg.versions ?? []) {
      if (versionEntry.tarballFileName === filename) {
        return { versionEntry, file: null };
      }
      const file = (versionEntry.files ?? []).find((entry) => entry.filename === filename);
      if (file) return { versionEntry, file };
    }
    return null;
  }

  async pruneVersions(ecosystem, packageName) {
    const pkg = this.state[ecosystem]?.[packageName];
    if (!pkg || !pkg.hot || (pkg.versions?.length ?? 0) <= this.keepVersions) return;

    const removed = pkg.versions.slice(this.keepVersions);
    let removedBytes = 0;
    this.logger.info?.("prune", {
      ecosystem,
      packageName,
      kept: this.keepVersions,
      removed: removed.map((entry) => entry.version),
    });

    for (const entry of removed) {
      for (const file of entry.files ?? []) {
        if (file.cached && file.sizeBytes) {
          removedBytes += file.sizeBytes;
          file.cached = false;
          file.sizeBytes = 0;
        }
        file.accesses ??= 0;
        file.lastAccessedAt ??= null;
      }
      const dir = this.versionDir(ecosystem, packageName, entry.version);
      await fs.rm(dir, { recursive: true, force: true });
    }

    pkg.cacheBytes = Math.max(0, (pkg.cacheBytes ?? 0) - removedBytes);
    this.totalCacheBytes = Math.max(0, this.totalCacheBytes - removedBytes);
  }

  async persistState() {
    await fs.mkdir(this.dataDir, { recursive: true });
    const tempPath = `${this.statePath}.tmp`;
    await fs.writeFile(tempPath, JSON.stringify(this.state, null, 2), "utf8");
    await fs.rename(tempPath, this.statePath);
    this.logger.debug?.("state_saved", { path: this.statePath });
  }

  async removePackage(ecosystem, packageName, reason = "cache_limit") {
    const pkg = this.state[ecosystem]?.[packageName];
    if (!pkg) return false;
    const packageBytes = pkg.cacheBytes ?? 0;
    await fs.rm(this.packageDir(ecosystem, packageName), { recursive: true, force: true });
    delete this.state[ecosystem][packageName];
    this.totalCacheBytes = Math.max(0, this.totalCacheBytes - packageBytes);
    this.logger.info?.("evict", {
      ecosystem,
      packageName,
      requests: pkg.requests ?? 0,
      cacheBytes: packageBytes,
      reason,
    });
    return true;
  }

  async enforceCacheLimit() {
    if (this.totalCacheBytes <= this.cacheLimitBytes) return;
    const candidates = Object.entries(this.state)
      .flatMap(([ecosystem, bucket]) =>
        Object.entries(bucket).map(([packageName, pkg]) => ({
          ecosystem,
          packageName,
          pkg,
        }))
      )
      .filter(({ pkg }) => (pkg.cacheBytes ?? 0) > 0)
      .sort((a, b) => {
        const requestDiff = (a.pkg.requests ?? 0) - (b.pkg.requests ?? 0);
        if (requestDiff !== 0) return requestDiff;
        const usageA = a.pkg.lastRequestedAt ? Date.parse(a.pkg.lastRequestedAt) : 0;
        const usageB = b.pkg.lastRequestedAt ? Date.parse(b.pkg.lastRequestedAt) : 0;
        if (usageA !== usageB) return usageA - usageB;
        return (b.pkg.cacheBytes ?? 0) - (a.pkg.cacheBytes ?? 0);
      });

    for (const candidate of candidates) {
      if (this.totalCacheBytes <= this.cacheLimitBytes) break;
      await this.removePackage(candidate.ecosystem, candidate.packageName, "cache_limit");
    }
  }

  scheduleSave() {
    if (this._saveTimer) return;
    this._saveTimer = setTimeout(() => {
      this._saveTimer = null;
      this._savePromise = this._savePromise.then(() => this.persistState()).catch(() => {});
    }, 1000);
    this._saveTimer.unref?.();
  }

  async flush() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
      this._savePromise = this._savePromise.then(() => this.persistState()).catch(() => {});
    }
    await this._savePromise;
  }

  topPackages(ecosystem, limit = 20) {
    const bucket = this.state[ecosystem] ?? {};
    return Object.entries(bucket)
      .map(([name, info]) => ({
        name,
        requests: info.requests ?? 0,
        lastRequestedAt: info.lastRequestedAt ?? null,
        versionCount: info.versions?.length ?? 0,
        cacheBytes: info.cacheBytes ?? 0,
        hot: Boolean(info.hot),
        seenAt: info.versions?.[0]?.seenAt ?? null,
      }))
      .sort((a, b) => (b.requests ?? 0) - (a.requests ?? 0))
      .slice(0, limit);
  }
}
