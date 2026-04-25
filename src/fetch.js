const fetch = global.fetch;

const USER_AGENT = "package-cache-proxy/1.0";
const DEFAULT_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS ?? 90000);
const DEFAULT_RETRIES = Number(process.env.UPSTREAM_RETRIES ?? 3);

function buildHeaders(headers = {}) {
  return { "user-agent": USER_AGENT, "accept-encoding": "identity", ...headers };
}

function timeoutFor(value) {
  const timeoutMs = Number(value ?? DEFAULT_TIMEOUT_MS);
  return Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
}

export async function fetchJson(url, options = {}) {
  const response = await fetchWithRetry(
    url,
    options.headers,
    options.retries ?? DEFAULT_RETRIES,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    options.fetchOptions
  );
  return response.json();
}

export async function fetchText(url, options = {}) {
  const response = await fetchWithRetry(
    url,
    options.headers,
    options.retries ?? DEFAULT_RETRIES,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    options.fetchOptions
  );
  return response.text();
}

export async function fetchBytes(url, options = {}) {
  const response = await fetchWithRetry(
    url,
    options.headers,
    options.retries ?? DEFAULT_RETRIES,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    options.fetchOptions
  );
  return Buffer.from(await response.arrayBuffer());
}

export async function fetchResponse(url, headers = {}, options = {}) {
  return fetchWithRetry(
    url,
    headers,
    options.retries ?? DEFAULT_RETRIES,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    options.fetchOptions
  );
}

export async function fetchWithRetry(url, headers = {}, retries = DEFAULT_RETRIES, timeoutMs = DEFAULT_TIMEOUT_MS, fetchOptions = {}) {
  const attempts = Math.max(1, Number(retries) || 1);
  let lastError = null;

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        ...fetchOptions,
        headers: buildHeaders(headers),
        signal: AbortSignal.timeout(timeoutFor(timeoutMs)),
      });
      if (!response.ok) {
        throw new Error(`upstream ${response.status} for ${url}`);
      }
      return response;
    } catch (error) {
      lastError = error;
      if (attempt === attempts - 1) throw error;
      const backoff = Math.min(1000 * 2 ** attempt, 5000);
      await new Promise((resolve) => setTimeout(resolve, backoff));
    }
  }

  throw lastError ?? new Error(`upstream request failed for ${url}`);
}
