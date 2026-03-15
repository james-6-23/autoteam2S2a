type CacheEntry<T> = {
  data: T;
  timestamp: number;
  staleAt: number;
};

const cache = new Map<string, CacheEntry<unknown>>();
const inflight = new Map<string, Promise<unknown>>();

const DEFAULT_TTL = 15_000;     // 15s fresh
const DEFAULT_STALE = 60_000;   // 60s stale-while-revalidate

export function cachedGet<T>(
  fetcher: () => Promise<T>,
  key: string,
  opts?: { ttl?: number; stale?: number },
): Promise<T> {
  const ttl = opts?.ttl ?? DEFAULT_TTL;
  const stale = opts?.stale ?? DEFAULT_STALE;
  const now = Date.now();
  const entry = cache.get(key) as CacheEntry<T> | undefined;

  // Fresh hit
  if (entry && now < entry.timestamp + ttl) {
    return Promise.resolve(entry.data);
  }

  // Stale hit — return cached data but revalidate in background
  if (entry && now < entry.staleAt) {
    revalidate(fetcher, key, ttl, stale);
    return Promise.resolve(entry.data);
  }

  // Miss or expired — fetch and deduplicate
  return dedup(fetcher, key, ttl, stale);
}

function dedup<T>(
  fetcher: () => Promise<T>,
  key: string,
  ttl: number,
  stale: number,
): Promise<T> {
  const existing = inflight.get(key);
  if (existing) return existing as Promise<T>;

  const promise = fetcher()
    .then((data) => {
      cache.set(key, { data, timestamp: Date.now(), staleAt: Date.now() + stale });
      return data;
    })
    .finally(() => inflight.delete(key));

  inflight.set(key, promise);
  return promise;
}

function revalidate<T>(
  fetcher: () => Promise<T>,
  key: string,
  ttl: number,
  stale: number,
): void {
  if (inflight.has(key)) return;
  const promise = fetcher()
    .then((data) => {
      cache.set(key, { data, timestamp: Date.now(), staleAt: Date.now() + stale });
    })
    .catch(() => {})
    .finally(() => inflight.delete(key));
  inflight.set(key, promise);
}

/** Invalidate a specific cache key or all keys matching a prefix */
export function invalidateCache(keyOrPrefix?: string): void {
  if (!keyOrPrefix) {
    cache.clear();
    return;
  }
  for (const k of cache.keys()) {
    if (k === keyOrPrefix || k.startsWith(keyOrPrefix)) {
      cache.delete(k);
    }
  }
}
