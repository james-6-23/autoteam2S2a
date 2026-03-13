import { useState, useCallback } from 'react';

interface UseApiOptions {
  onError?: (error: Error) => void;
}

export function useApi<T>(fetcher: () => Promise<T>, options?: UseApiOptions) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const execute = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetcher();
      setData(result);
      return result;
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      setError(err);
      options?.onError?.(err);
      return null;
    } finally {
      setLoading(false);
    }
  }, [fetcher, options]);

  return { data, loading, error, execute, setData };
}
