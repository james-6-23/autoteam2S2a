import { useState, useEffect, useRef, useCallback } from 'react';

export function useWebSocket(path: string) {
  const [logs, setLogs] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const autoScrollRef = useRef(true);

  const connect = useCallback(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}${path}`);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      // Reconnect after 3 seconds
      setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (event) => {
      setLogs(prev => {
        const next = [...prev, event.data];
        // Keep last 2000 lines
        return next.length > 2000 ? next.slice(-2000) : next;
      });
    };
  }, [path]);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
    };
  }, [connect]);

  const clearLogs = useCallback(() => setLogs([]), []);

  const toggleAutoScroll = useCallback(() => {
    autoScrollRef.current = !autoScrollRef.current;
    return autoScrollRef.current;
  }, []);

  return { logs, connected, clearLogs, autoScroll: autoScrollRef, toggleAutoScroll };
}
