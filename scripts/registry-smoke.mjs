#!/usr/bin/env node

import { once } from "node:events";
import { spawn, spawnSync } from "node:child_process";
import { access, mkdtemp, readdir, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const syntaxFiles = [
  "index.js",
  "src/fetch.js",
  "src/npm.js",
  "src/pip.js",
  "src/server.js",
  "src/singleflight.js",
  "src/store.js",
  "scripts/registry-smoke.mjs",
];

function checkSyntax() {
  for (const relativePath of syntaxFiles) {
    const result = spawnSync(process.execPath, ["--check", relativePath], {
      cwd: repoRoot,
      stdio: "inherit",
    });

    if (result.error) {
      throw result.error;
    }

    if (result.status !== 0) {
      throw new Error(`syntax check failed for ${relativePath}`);
    }
  }
}

function spawnChecked(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      stdio: "inherit",
      ...options,
    });

    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`${command} ${args.join(" ")} exited ${signal ? `with signal ${signal}` : `with code ${code}`}`));
    });
  });
}

async function getFreePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("failed to allocate a port")));
        return;
      }

      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(address.port);
      });
    });
  });
}

async function waitForUrl(url, label) {
  const deadline = Date.now() + 30_000;
  let lastError = null;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(2_000),
      });
      if (response.ok) {
        return;
      }
      lastError = new Error(`${label} returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }

    await delay(250);
  }

  throw new Error(`timed out waiting for ${label} at ${url}${lastError ? `: ${lastError.message}` : ""}`);
}

function pythonCommand() {
  return process.env.PYTHON ?? (process.platform === "win32" ? "python" : "python3");
}

function npmInvocation() {
  if (process.env.npm_execpath) {
    return {
      command: process.execPath,
      args: [process.env.npm_execpath],
    };
  }

  if (process.platform === "win32") {
    const npmCli = path.join(path.dirname(process.execPath), "node_modules", "npm", "bin", "npm-cli.js");
    return { command: process.execPath, args: [npmCli] };
  }

  return {
    command: process.env.NPM ?? "npm",
    args: [],
  };
}

async function stopProcess(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }

  child.kill("SIGTERM");

  const outcome = await Promise.race([
    once(child, "exit").then(() => "exit"),
    delay(5_000).then(() => "timeout"),
  ]);

  if (outcome === "timeout" && child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await once(child, "exit");
  }
}

async function main() {
  checkSyntax();

  const registryRoot = await mkdtemp(path.join(os.tmpdir(), "voidium-registry-smoke-"));
  const port = await getFreePort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const serverEnv = {
    ...process.env,
    BASE_URL: baseUrl,
    HOST: "127.0.0.1",
    LOG_LEVEL: process.env.LOG_LEVEL ?? "warn",
    PORT: String(port),
    PREWARM: "0",
    REGISTRY_ROOT: registryRoot,
  };

  const server = spawn(process.execPath, ["src/server.js"], {
    cwd: repoRoot,
    env: serverEnv,
    stdio: ["ignore", "inherit", "inherit"],
  });

  try {
    await waitForUrl(`${baseUrl}/`, "registry root");
    await waitForUrl(`${baseUrl}/admin/stats`, "registry stats");

    const rootResponse = await fetch(`${baseUrl}/`, {
      signal: AbortSignal.timeout(2_000),
    });
    const rootText = await rootResponse.text();
    if (!rootText.includes("HTTP requests in flight") || !rootText.includes("Stable npm caching with live load")) {
      throw new Error("root page did not expose the live load dashboard");
    }

    const statsResponse = await fetch(`${baseUrl}/admin/stats`, {
      signal: AbortSignal.timeout(2_000),
    });
    const stats = await statsResponse.json();
    if (stats.baseUrl !== baseUrl) {
      throw new Error(`expected baseUrl ${baseUrl}, got ${stats.baseUrl}`);
    }
    if (!stats.http || !stats.upstream || !stats.cache) {
      throw new Error("admin stats missing load fields");
    }

    const npmDir = await mkdtemp(path.join(registryRoot, "npm-"));
    const npmConfig = {
      ...serverEnv,
      npm_config_audit: "false",
      npm_config_cache: path.join(registryRoot, "npm-cache"),
      npm_config_fund: "false",
      npm_config_loglevel: "error",
      npm_config_registry: `${baseUrl}/npm/`,
      npm_config_ignore_scripts: "true",
      npm_config_update_notifier: "false",
    };
    const npm = npmInvocation();
    await spawnChecked(
      npm.command,
      [...npm.args, "install", "is-number@7.0.0", "--prefix", npmDir, "--registry", `${baseUrl}/npm/`],
      {
        cwd: repoRoot,
        env: npmConfig,
      }
    );

    await access(path.join(npmDir, "node_modules", "is-number", "package.json"));

    const pipDir = await mkdtemp(path.join(registryRoot, "pip-"));
    await spawnChecked(
      pythonCommand(),
      [
        "-m", "pip", "download",
        "--dest", pipDir,
        "--index-url", `${baseUrl}/pypi/simple/`,
        "--no-deps",
        "six",
      ],
      {
        cwd: pipDir,
        env: {
          ...serverEnv,
          PIP_DISABLE_PIP_VERSION_CHECK: "1",
          PIP_NO_CACHE_DIR: "1",
          PIP_NO_INPUT: "1",
        },
      }
    );

    const pipArtifacts = await readdir(pipDir);
    if (pipArtifacts.length === 0) {
      throw new Error("pip smoke did not download any files");
    }

    console.log(`smoke test passed against ${baseUrl}`);
  } finally {
    await stopProcess(server);
    await rm(registryRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error?.stack ?? error?.message ?? String(error));
  process.exitCode = 1;
});
