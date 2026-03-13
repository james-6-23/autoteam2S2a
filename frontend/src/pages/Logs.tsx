import { useEffect, useRef, useState, useCallback } from 'react';

export default function Logs() {
  const [lines, setLines] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [count, setCount] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);
  const pendingRef = useRef<string[]>([]);
  const rafRef = useRef<number | null>(null);

  const MAX_LINES = 800;

  const detectLevel = (line: string) => {
    if (!line) return '';
    if (line.includes('[ERR]') || line.includes('失败')) return 'log-line-error';
    if (line.includes('[OK]') || line.includes('[SIG-END][OK]')) return 'log-line-success';
    return '';
  };

  const flush = useCallback(() => {
    rafRef.current = null;
    if (!pendingRef.current.length) return;
    const batch = pendingRef.current.splice(0);
    setLines(prev => {
      const next = [...prev, ...batch];
      return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
    });
    setCount(prev => prev + batch.length);
  }, []);

  useEffect(() => {
    const connect = () => {
      const es = new EventSource('/api/logs/stream');
      esRef.current = es;
      es.onopen = () => setConnected(true);
      es.onmessage = (e) => {
        pendingRef.current.push(e.data);
        if (!rafRef.current) rafRef.current = requestAnimationFrame(flush);
      };
      es.onerror = () => {
        setConnected(false);
        es.close(); esRef.current = null;
        setTimeout(connect, 3000);
      };
    };
    connect();
    return () => { esRef.current?.close(); if (rafRef.current) cancelAnimationFrame(rafRef.current); };
  }, [flush]);

  // Auto scroll
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const clearLogs = () => { setLines([]); setCount(0); pendingRef.current = []; };

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <div className="section-title mb-0">实时日志</div>
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? 'bg-teal-400 animate-pulse' : 'bg-red-400'}`} />
          <span className="text-xs c-dim">{connected ? '已连接' : '已断开'}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono c-dim">{count} 条</span>
          <button onClick={() => setAutoScroll(!autoScroll)} className="btn btn-ghost text-xs py-1">自动滚动: {autoScroll ? '开' : '关'}</button>
          <button onClick={clearLogs} className="btn btn-ghost text-xs py-1">清空</button>
        </div>
      </div>
      <div
        ref={containerRef}
        className="font-mono text-xs leading-relaxed p-4 rounded-lg overflow-y-auto"
        style={{ background: 'var(--bg-inner)', border: '1px solid var(--border)', height: '60vh', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}
      >
        {lines.map((line, i) => (
          <span key={i} className={`log-line ${detectLevel(line)}`}>{line}{'\n'}</span>
        ))}
      </div>
    </div>
  );
}
