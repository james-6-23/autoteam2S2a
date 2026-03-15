import { useEffect, useRef, useState, useCallback } from 'react';
import { ArrowDown } from 'lucide-react';
import { Button, Card, SectionTitle } from '../components/ui';

export default function Logs() {
  const [lines, setLines] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [count, setCount] = useState(0);
  const [showBottom, setShowBottom] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);
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

  useEffect(() => { autoScrollRef.current = autoScroll; }, [autoScroll]);

  useEffect(() => {
    if (autoScrollRef.current && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines]);

  const isNearBottom = (el: HTMLElement) => el.scrollHeight - el.scrollTop - el.clientHeight < 40;

  const handleScroll = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    const nearBottom = isNearBottom(el);
    setShowBottom(!nearBottom && !autoScrollRef.current);
  }, []);

  const toggleAutoScroll = () => {
    const next = !autoScroll;
    setAutoScroll(next);
    autoScrollRef.current = next;
    if (next && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
      setShowBottom(false);
    }
  };

  const scrollToBottom = () => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
      setShowBottom(false);
    }
  };

  const clearLogs = () => { setLines([]); setCount(0); pendingRef.current = []; };

  return (
    <Card className="p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <SectionTitle className="mb-0">实时日志</SectionTitle>
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? 'bg-teal-400 animate-pulse' : 'bg-red-400'}`} />
          <span className="text-xs c-dim">{connected ? '已连接' : '已断开'}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono c-dim">{count} 条</span>
          <Button onClick={toggleAutoScroll} variant="ghost" className="text-xs py-1">自动滚动: {autoScroll ? '开' : '关'}</Button>
          <Button onClick={clearLogs} variant="ghost" className="text-xs py-1">清空</Button>
        </div>
      </div>
      <div className="relative">
        <div
          ref={containerRef}
          onScroll={handleScroll}
          className="font-mono text-xs leading-relaxed p-4 rounded-lg overflow-y-auto"
          style={{ background: 'var(--bg-inner)', border: '1px solid var(--border)', height: '60vh', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}
        >
          {lines.map((line, i) => (
            <span key={i} className={`log-line ${detectLevel(line)}`}>{line}{'\n'}</span>
          ))}
        </div>
        {showBottom && (
          <button
            onClick={scrollToBottom}
            className="absolute bottom-4 right-4 flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium shadow-lg transition-all duration-200 hover:scale-105"
            style={{
              background: 'var(--ghost-hover)',
              border: '1px solid var(--border-hover)',
              color: 'var(--text-heading)',
              backdropFilter: 'blur(8px)',
            }}
          >
            <ArrowDown size={13} />
            回到底部
          </button>
        )}
      </div>
    </Card>
  );
}
