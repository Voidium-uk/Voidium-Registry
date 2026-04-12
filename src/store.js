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
      if (parsed.npm && typeof parsed.npm === "object") state.npm = parsed.npm;
    }
    return state;
  }

  _compactState() {
    // PyPI support has been removed, so there is no secondary ecosystem state to compact.
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

  htmlPath(ecosystem, packageName) {
    return path.join(this.packageDir(ecosystem, packageName), "simple.html");
  }

  getPackage(ecosystem, packageName) {
    const bucket = this.state[ecosystem];
    if (!bucket[packageName]) {
      bucket[packageName] = {
        requests: 0,
        lastRequestedAt: null,
        versions: [],
        hot: false,
        cacheBytes: 0,
      };
    }
    bucket[packageName].cacheBytes ??= 0;
    return bucket[packageName];
  }

  async rebuildCacheAccounting() {
    this.totalCacheBytes = 0;
    for (const ecosystem of Object.keys(this.state)) {
      for (const [packageName, pkg] of Object.entries(this.state[ecosystem])) {
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
            });
          }
          for (const file of versionEntry.files) {
            file.accesses ??= 0;
            file.cached ??= false;
            file.sizeBytes ??= 0;
            file.lastAccessedAt ??= null;
            const cachedPath = file.filename
              ? this.cachedFilePath(ecosystem, packageName, versionEntry.version, file.filename)
              : null;
            if (cachedPath && (await exists(cachedPath))) {
              const stat = await fs.stat(cachedPath);
              file.cached = true;
              file.sizeBytes = stat.size;
              pkg.cacheBytes += stat.size;
              this.totalCacheBytes += stat.size;
            } else if (!file.cached) {
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

  _findFileEntry(versionEntry, fileRecord) {
    return (versionEntry.files ?? []).find((file) => {
      if (fileRecord.upstreamUrl && file.upstreamUrl === fileRecord.upstreamUrl) return true;
      if (fileRecord.filename && file.filename === fileRecord.filename) return true;
      return false;
    });
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
    const versionEntry = pkg.versions.find((entry) => entry.version === version);
    const normalized = this._normalizeFileRecord(fileRecord);
    if (!versionEntry) {
      pkg.versions.unshift({
        version,
        seenAt: new Date().toISOString(),
        files: [normalized],
      });
      if (normalized.cached && normalized.sizeBytes) {
        pkg.cacheBytes = (pkg.cacheBytes ?? 0) + normalized.sizeBytes;
        this.totalCacheBytes += normalized.sizeBytes;
      }
    } else {
      versionEntry.files = versionEntry.files ?? [];
      const existing = this._findFileEntry(versionEntry, normalized);
      if (existing) {
        const oldSize = existing.cached ? existing.sizeBytes ?? 0 : 0;
        Object.assign(existing, {
          ...existing,
          ...normalized,
          cached: existing.cached || normalized.cached,
          accesses: existing.accesses ?? 0,
          sizeBytes: normalized.sizeBytes ?? existing.sizeBytes ?? 0,
          lastAccessedAt: normalized.lastAccessedAt ?? existing.lastAccessedAt ?? null,
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

  _normalizeFileRecord(fileRecord = {}) {
    return {
      filename: fileRecord.filename ?? null,
      upstreamUrl: fileRecord.upstreamUrl ?? null,
      cached: Boolean(fileRecord.cached),
      accesses: fileRecord.accesses ?? 0,
      sizeBytes: fileRecord.sizeBytes ?? 0,
      lastAccessedAt: fileRecord.lastAccessedAt ?? null,
      contentEncoding: fileRecord.contentEncoding ?? null,
    };
  }

  markFileAccess(ecosystem, packageName, version, identifier) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg) return;
    const versionEntry = pkg.versions.find((entry) => entry.version === version);
    if (!versionEntry) return;
    const file = (versionEntry.files ?? []).find((entry) => entry.filename === identifier || entry.upstreamUrl === identifier);
    if (!file) return;
    file.accesses = (file.accesses ?? 0) + 1;
    file.lastAccessedAt = new Date().toISOString();
    this.scheduleSave();
  }

async isFresh(filePath, maxAgeMs) {
    try {
      const stat = await fs.stat(filePath);
      return Date.now() - stat.mtimeMs < maxAgeMs;
    } catch {
      return false;
    }
  }

  async openMetadataStream(ecosystem, packageName) {
    const filePath = this.metadataPath(ecosystem, packageName);
    if (!(await exists(filePath))) return null;
    return fs.createReadStream(filePath, { encoding: "utf8" });
  }

  async openHtmlStream(ecosystem, packageName) {
    const filePath = this.htmlPath(ecosystem, packageName);
    if (!(await exists(filePath))) return null;
    return fs.createReadStream(filePath, { encoding: "utf8" });
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
    this.scheduleSave();
    return pkg;
  }

  recordFile(ecosystem, packageName, version, fileRecord) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg) return null;
    const versionEntry = pkg.versions.find((entry) => entry.version === version);
    if (!versionEntry) return null;
    const existing = this._findFileEntry(versionEntry, fileRecord);
    if (!existing) {
      versionEntry.files = versionEntry.files ?? [];
      const normalized = this._normalizeFileRecord(fileRecord);
      versionEntry.files.unshift(normalized);
      if (normalized.cached && normalized.sizeBytes) {
        pkg.cacheBytes = (pkg.cacheBytes ?? 0) + normalized.sizeBytes;
        this.totalCacheBytes += normalized.sizeBytes;
      }
      this.scheduleSave();
      return normalized;
    }
    const oldSize = existing.cached ? existing.sizeBytes ?? 0 : 0;
    Object.assign(existing, {
      ...existing,
      ...fileRecord,
      cached: existing.cached || fileRecord.cached,
      accesses: existing.accesses ?? 0,
      sizeBytes: fileRecord.sizeBytes ?? existing.sizeBytes ?? 0,
      lastAccessedAt: fileRecord.lastAccessedAt ?? existing.lastAccessedAt ?? null,
    });
    const newSize = existing.cached ? existing.sizeBytes ?? 0 : 0;
    const delta = newSize - oldSize;
    if (delta !== 0) {
      pkg.cacheBytes = (pkg.cacheBytes ?? 0) + delta;
      this.totalCacheBytes += delta;
    }
    this.scheduleSave();
    return existing;
  }

  getFileRecord(ecosystem, packageName, version, filename) {
    const versionEntry = this.state[ecosystem][packageName]?.versions?.find(entry => entry.version === version);
    if (!versionEntry) return null;
    return versionEntry.files?.find(file => file.filename === filename) ?? null;
  }

  getFileRecord(ecosystem, packageName, version, filename) {
    const versionEntry = this.state[ecosystem][packageName]?.versions?.find(entry => entry.version === version);
    if (!versionEntry) return null;
    return versionEntry.files?.find(file => file.filename === filename) ?? null;
  }

  markFileAccess(ecosystem, packageName, version, filename) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg) return;
    const versionEntry = pkg.versions.find((entry) => entry.version === version);
    if (!versionEntry) return;
    const file = versionEntry.files.find((entry) => entry.filename === filename);
    if (!file) return;
    file.accesses = (file.accesses ?? 0) + 1;
    file.lastAccessedAt = new Date().toISOString();
    this.scheduleSave();
  }

  getVersionEntryByUpstreamUrl(ecosystem, packageName, upstreamUrl) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg) return null;
    for (const versionEntry of pkg.versions) {
      const files = versionEntry.files ?? [];
      const file = files.find((entry) => entry.upstreamUrl === upstreamUrl);
      if (file) return { versionEntry, file };
    }
    return null;
  }

  getVersionEntryByTarballFileName(ecosystem, packageName, filename) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg) return null;
    for (const versionEntry of pkg.versions) {
      if (versionEntry.tarballFileName === filename) {
        return { versionEntry, file: null };
      }
      const files = versionEntry.files ?? [];
      const file = files.find((entry) => entry.filename === filename);
      if (file) return { versionEntry, file };
    }
    return null;
  }

  getFileRecord(ecosystem, packageName, version, filename) {
    const versionEntry = this.state[ecosystem][packageName]?.versions?.find(entry => entry.version === version);
    if (!versionEntry) return null;
    return versionEntry.files?.find(file => file.filename === filename) ?? null;
  }

  async pruneVersions(ecosystem, packageName) {
    const pkg = this.state[ecosystem][packageName];
    if (!pkg || !pkg.hot || pkg.versions.length <= this.keepVersions) return;

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

  async saveMetadata(ecosystem, packageName, text) {
    await fs.mkdir(this.packageDir(ecosystem, packageName), { recursive: true });
    await fs.writeFile(this.metadataPath(ecosystem, packageName), text, "utf8");
  }

  async saveHtml(ecosystem, packageName, text) {
    await fs.mkdir(this.packageDir(ecosystem, packageName), { recursive: true });
    await fs.writeFile(this.htmlPath(ecosystem, packageName), text, "utf8");
  }

  async readMetadata(ecosystem, packageName) {
    if (!(await exists(this.metadataPath(ecosystem, packageName)))) return null;
    return fs.readFile(this.metadataPath(ecosystem, packageName), "utf8");
  }

  async openMetadataStream(ecosystem, packageName) {
    const filePath = this.metadataPath(ecosystem, packageName);
    if (!(await exists(filePath))) return null;
    return createReadStream(filePath, { encoding: "utf8" });
  }

  async readHtml(ecosystem, packageName) {
    if (!(await exists(this.htmlPath(ecosystem, packageName)))) return null;
    return fs.readFile(this.htmlPath(ecosystem, packageName), "utf8");
  }

  async openHtmlStream(ecosystem, packageName) {
    const filePath = this.htmlPath(ecosystem, packageName);
    if (!(await exists(filePath))) return null;
    return createReadStream(filePath, { encoding: "utf8" });
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

  async persistState() {
    await fs.mkdir(this.dataDir, { recursive: true });
    const tempPath = `${this.statePath}.tmp`;
    await fs.writeFile(tempPath, JSON.stringify(this.state, null, 2), "utf8");
    await fs.rename(tempPath, this.statePath);
    this.logger.debug?.("state_saved", { path: this.statePath });
  }

  async removePackage(ecosystem, packageName, reason = "cache_limit") {
    const pkg = this.state[ecosystem][packageName];
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
    return Object.entries(this.state[ecosystem])
      .map(([name, info]) => ({ name, ...info }))
      .sort((a, b) => (b.requests ?? 0) - (a.requests ?? 0))
      .slice(0, limit);
  }
}
