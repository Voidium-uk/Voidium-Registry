const fetch = global.fetch;

export async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { "user-agent": "package-cache-proxy/1.0", ...options.headers },
  });
  if (!response.ok) {
    throw new Error(`upstream ${response.status} for ${url}`);
  }
  return response.json();
}

export async function fetchText(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { "user-agent": "package-cache-proxy/1.0", ...options.headers },
  });
  if (!response.ok) {
    throw new Error(`upstream ${response.status} for ${url}`);
  }
  return response.text();
}

export async function fetchBytes(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { "user-agent": "package-cache-proxy/1.0", ...options.headers },
  });
  if (!response.ok) {
    throw new Error(`upstream ${response.status} for ${url}`);
  }
  return Buffer.from(await response.arrayBuffer());
}

export async function fetchResponse(url, headers = {}) {
  return fetch(url, {
    headers: { "user-agent": "package-cache-proxy/1.0", ...headers },
    signal: AbortSignal.timeout(90000), // 90s timeout
  });
}

export async function fetchWithRetry(url, headers = {}, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetchResponse(url, headers);
      if (!response.ok) {
        throw new Error(`upstream ${response.status} for ${url}`);
      }
      return response;
    } catch (error) {
      if (attempt === retries - 1) throw error;
      const backoff = Math.min(1000 * Math.pow(2, attempt), 5000);
      await new Promise((res) => setTimeout(res, backoff));
    }
  }
}