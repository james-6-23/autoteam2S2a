import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Globe,
  Loader2,
  Plus,
  RefreshCw,
  Shield,
  ShieldCheck,
  Signal,
  Trash2,
  Wifi,
  WifiOff,
  X,
  Zap,
} from "lucide-react";

import { useToast } from "../components/Toast";
import * as api from "../lib/api";

/* ─── 类型定义 ─── */

interface ProxyEntry {
  url: string;
  /** 健康检测结果 */
  health?: { ok: boolean; reason: string };
  /** 测试连通性结果 */
  test?: {
    success: boolean;
    latency_ms?: number;
    ip_address?: string;
    city?: string;
    region?: string;
    country?: string;
    country_code?: string;
  };
  /** 质量评分 */
  quality?: {
    score: number;
    grade: string;
    summary: string;
    exit_ip?: string;
    country?: string;
    country_code?: string;
    base_latency_ms?: number;
    passed_count: number;
    warn_count: number;
    failed_count: number;
  };
  testing?: boolean;
}

/* ─── 工具函数 ─── */

/** 解析代理 URL 为结构化信息 */
function parseProxy(url: string) {
  try {
    // 兼容 scheme://user:pass@host:port 格式
    const match = url.match(/^(https?|socks[45]?):\/\/(?:([^:]+):([^@]+)@)?(.+)$/i);
    if (!match) return { protocol: "http", host: url, hasAuth: false };
    return {
      protocol: match[1].toUpperCase(),
      user: match[2],
      host: match[4],
      hasAuth: Boolean(match[2]),
    };
  } catch {
    return { protocol: "?", host: url, hasAuth: false };
  }
}

function latencyColor(ms: number | undefined): string {
  if (ms == null) return "var(--text-dim)";
  if (ms < 300) return "#2dd4bf";
  if (ms < 800) return "#fbbf24";
  return "#f87171";
}

function gradeColor(grade: string): string {
  switch (grade) {
    case "A": return "#2dd4bf";
    case "B": return "#34d399";
    case "C": return "#fbbf24";
    case "D": return "#fb923c";
    default: return "#f87171";
  }
}

/* ─── 组件 ─── */

function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center py-20">
      <div
        className="mb-5 flex h-20 w-20 items-center justify-center rounded-2xl"
        style={{ background: "var(--ghost)", border: "1px solid var(--border)" }}
      >
        <Globe size={32} style={{ color: "var(--text-dim)" }} />
      </div>
      <div className="text-base font-semibold c-heading">暂无代理</div>
      <div className="mt-1 text-sm c-dim">添加您的第一个代理以开始使用</div>
      <button
        type="button"
        onClick={onAdd}
        className="mt-5 btn flex items-center gap-2 px-5 py-2.5 text-sm"
        style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))", color: "#fff" }}
      >
        <Plus size={16} /> 添加代理
      </button>
    </div>
  );
}

function ProxyTableHeader() {
  return (
    <div className="proxy-table-grid proxy-table-header">
      <span />
      <span>地址</span>
      <span>协议</span>
      <span>认证</span>
      <span>地理位置</span>
      <span>延迟</span>
      <span>评分</span>
      <span>状态</span>
      <span>操作</span>
    </div>
  );
}

function ProxyRow({
  entry,
  selected,
  onToggleSelect,
  onTest,
  onDelete,
}: {
  entry: ProxyEntry;
  selected: boolean;
  onToggleSelect: () => void;
  onTest: () => void;
  onDelete: () => void;
}) {
  const parsed = useMemo(() => parseProxy(entry.url), [entry.url]);

  const statusOk = entry.health?.ok;
  const statusLabel = statusOk === true ? "可用" : statusOk === false ? "异常" : "未检测";
  const statusColor = statusOk === true ? "#2dd4bf" : statusOk === false ? "#f87171" : "var(--text-dim)";

  const location = entry.test?.city || entry.quality?.country || "--";
  const latency = entry.test?.latency_ms ?? entry.quality?.base_latency_ms;

  return (
    <div className={`proxy-table-grid proxy-table-row ${statusOk === false ? "proxy-table-row--error" : ""}`}>
      {/* 复选框 */}
      <div className="flex items-center justify-center">
        <button
          type="button"
          onClick={event => { event.stopPropagation(); onToggleSelect(); }}
          className={`proxy-check ${selected ? "proxy-check--selected" : ""}`}
        >
          {selected && <CheckCircle2 size={14} />}
        </button>
      </div>

      {/* 地址 */}
      <div className="min-w-0">
        <div className="truncate text-[.8rem] font-mono font-medium c-heading" title={entry.url}>
          {parsed.host}
        </div>
      </div>

      {/* 协议 */}
      <div>
        <span
          className="inline-flex items-center rounded-md px-2 py-0.5 text-[.6rem] font-bold tracking-wider"
          style={{
            background: parsed.protocol === "SOCKS5" ? "rgba(139,92,246,0.12)" : "rgba(20,184,166,0.1)",
            color: parsed.protocol === "SOCKS5" ? "#a78bfa" : "#2dd4bf",
            border: `1px solid ${parsed.protocol === "SOCKS5" ? "rgba(139,92,246,0.25)" : "rgba(20,184,166,0.2)"}`,
          }}
        >
          {parsed.protocol}
        </span>
      </div>

      {/* 认证 */}
      <div className="flex items-center">
        {parsed.hasAuth ? (
          <span className="inline-flex items-center gap-1 text-[.65rem] font-medium" style={{ color: "#fbbf24" }}>
            <Shield size={12} /> 有
          </span>
        ) : (
          <span className="text-[.65rem] c-dim">无</span>
        )}
      </div>

      {/* 地理位置 */}
      <div className="text-[.72rem] c-dim truncate" title={location}>
        {entry.test?.country_code ? (
          <span className="flex items-center gap-1">
            <span>{entry.test.country_code}</span>
            <span className="c-dim2">{entry.test.city || entry.test.region || ""}</span>
          </span>
        ) : entry.quality?.country ? (
          <span>{entry.quality.country}</span>
        ) : (
          "--"
        )}
      </div>

      {/* 延迟 */}
      <div>
        {latency != null ? (
          <span className="font-mono text-[.72rem] font-semibold tabular-nums" style={{ color: latencyColor(latency) }}>
            {latency}ms
          </span>
        ) : (
          <span className="text-[.65rem] c-dim">--</span>
        )}
      </div>

      {/* 评分 */}
      <div>
        {entry.quality ? (
          <span
            className="inline-flex items-center justify-center rounded-md px-2 py-0.5 text-[.65rem] font-bold font-mono"
            style={{
              background: `${gradeColor(entry.quality.grade)}15`,
              color: gradeColor(entry.quality.grade),
              border: `1px solid ${gradeColor(entry.quality.grade)}30`,
            }}
          >
            {entry.quality.grade}
          </span>
        ) : (
          <span className="text-[.65rem] c-dim">--</span>
        )}
      </div>

      {/* 状态 */}
      <div>
        <span
          className="inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[.6rem] font-semibold"
          style={{
            background: statusOk === true ? "rgba(45,212,191,0.1)" : statusOk === false ? "rgba(248,113,113,0.1)" : "var(--ghost)",
            color: statusColor,
            border: `1px solid ${statusOk === true ? "rgba(45,212,191,0.2)" : statusOk === false ? "rgba(248,113,113,0.2)" : "var(--border)"}`,
          }}
        >
          <span className="w-1.5 h-1.5 rounded-full" style={{ background: statusColor }} />
          {statusLabel}
        </span>
      </div>

      {/* 操作 */}
      <div className="flex items-center justify-end gap-1">
        <button
          type="button"
          onClick={event => { event.stopPropagation(); onTest(); }}
          disabled={entry.testing}
          className="proxy-action-btn"
          title="测试连通性"
        >
          {entry.testing ? <Loader2 size={13} className="animate-spin" /> : <Zap size={13} />}
        </button>
        <button
          type="button"
          onClick={event => { event.stopPropagation(); onDelete(); }}
          className="proxy-action-btn proxy-action-btn--danger"
          title="删除"
        >
          <Trash2 size={13} />
        </button>
      </div>
    </div>
  );
}

/* ─── 仪表盘卡片 ─── */

function StatCard({ icon: Icon, label, value, color }: {
  icon: React.ComponentType<{ size?: number }>;
  label: string;
  value: string | number;
  color: string;
}) {
  return (
    <div className="proxy-stat-card" style={{ borderColor: `${color}25` }}>
      <div className="proxy-stat-icon" style={{ background: `${color}12`, color }}>
        <Icon size={16} />
      </div>
      <div>
        <div className="text-[.68rem] c-dim">{label}</div>
        <div className="mt-0.5 text-lg font-bold font-mono c-heading tabular-nums">{value}</div>
      </div>
    </div>
  );
}

/* ─── 主页面 ─── */

export default function Proxy() {
  const { toast } = useToast();
  const [proxyEnabled, setProxyEnabled] = useState(true);
  const [proxies, setProxies] = useState<ProxyEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  const [showAddModal, setShowAddModal] = useState(false);
  const [addMode, setAddMode] = useState<"single" | "batch">("single");
  const [singleInput, setSingleInput] = useState("");
  const [addInput, setAddInput] = useState("");
  const [adding, setAdding] = useState(false);

  const [batchChecking, setBatchChecking] = useState(false);
  const [batchTesting, setBatchTesting] = useState(false);
  const [batchTestProgress, setBatchTestProgress] = useState({ done: 0, total: 0 });

  // localStorage 持久化：保存/恢复检测结果
  const CACHE_KEY = "proxy-test-results";

  const saveCache = useCallback((entries: ProxyEntry[]) => {
    const cache: Record<string, Pick<ProxyEntry, "health" | "test" | "quality">> = {};
    for (const e of entries) {
      if (e.health || e.test || e.quality) {
        cache[e.url] = { health: e.health, test: e.test, quality: e.quality };
      }
    }
    try { localStorage.setItem(CACHE_KEY, JSON.stringify(cache)); } catch { /* quota exceeded */ }
  }, []);

  const loadCache = useCallback((): Record<string, Pick<ProxyEntry, "health" | "test" | "quality">> => {
    try {
      const raw = localStorage.getItem(CACHE_KEY);
      return raw ? JSON.parse(raw) : {};
    } catch { return {}; }
  }, []);

  // 加载代理列表（合并 localStorage 缓存）
  const load = useCallback(async () => {
    try {
      const data = await api.fetchConfig() as { proxy_pool?: string[]; proxy_enabled?: boolean };
      setProxyEnabled(data.proxy_enabled !== false);
      const cached = loadCache();
      setProxies(prev => {
        const prevMap = new Map(prev.map(p => [p.url, p]));
        return (data.proxy_pool || []).map(url => {
          const existing = prevMap.get(url);
          if (existing) return existing;
          const c = cached[url];
          return c ? { url, ...c } : { url };
        });
      });
    } catch {
      toast("加载代理配置失败", "error");
    } finally {
      setLoading(false);
    }
  }, [toast, loadCache]);

  useEffect(() => { void load(); }, [load]);

  // 每次 proxies 变化时持久化结果
  useEffect(() => {
    if (proxies.length > 0) saveCache(proxies);
  }, [proxies, saveCache]);

  // 统计
  const stats = useMemo(() => {
    const total = proxies.length;
    const healthy = proxies.filter(p => p.health?.ok === true).length;
    const unhealthy = proxies.filter(p => p.health?.ok === false).length;
    const avgLatency = proxies.reduce((sum, p) => {
      const ms = p.test?.latency_ms ?? p.quality?.base_latency_ms;
      return ms != null ? sum + ms : sum;
    }, 0);
    const latencyCount = proxies.filter(p => (p.test?.latency_ms ?? p.quality?.base_latency_ms) != null).length;
    return {
      total,
      healthy,
      unhealthy,
      unchecked: total - healthy - unhealthy,
      avgLatency: latencyCount > 0 ? Math.round(avgLatency / latencyCount) : 0,
    };
  }, [proxies]);

  // 开关切换
  const toggleEnabled = async (enabled: boolean) => {
    setProxyEnabled(enabled);
    try {
      await api.setProxyEnabled(enabled);
      toast(enabled ? "代理池已启用" : "代理池已禁用，使用直连", "success");
    } catch (err) {
      setProxyEnabled(!enabled);
      toast(`切换失败: ${err}`, "error");
    }
  };

  // 添加单个代理
  const handleAddSingle = async () => {
    const url = singleInput.trim();
    if (!url) return;
    setAdding(true);
    try {
      await api.addProxy(url);
      setSingleInput("");
      toast("代理已添加", "success");
      void load();
    } catch (err) {
      toast(`添加失败: ${err}`, "error");
    } finally {
      setAdding(false);
    }
  };

  // 批量添加代理
  const handleAdd = async () => {
    const lines = addInput.split("\n").map(l => l.trim()).filter(Boolean);
    if (lines.length === 0) return;
    setAdding(true);
    let ok = 0;
    let fail = 0;
    for (const line of lines) {
      try {
        await api.addProxy(line);
        ok++;
      } catch {
        fail++;
      }
    }
    setAdding(false);
    setShowAddModal(false);
    setAddInput("");
    toast(fail > 0 ? `添加完成: ${ok} 成功, ${fail} 失败` : `已添加 ${ok} 个代理`, fail > 0 ? "error" : "success");
    void load();
  };

  // 删除代理
  const handleDelete = async (url: string) => {
    try {
      await api.deleteProxy(url);
      setProxies(prev => prev.filter(p => p.url !== url));
      setSelectedIds(prev => { const next = new Set(prev); next.delete(url); return next; });
      toast("代理已删除", "success");
    } catch (err) {
      toast(`删除失败: ${err}`, "error");
    }
  };

  // 批量删除
  const handleBatchDelete = async () => {
    if (selectedIds.size === 0) return;
    if (!confirm(`确定要删除选中的 ${selectedIds.size} 个代理？`)) return;
    let ok = 0;
    for (const url of selectedIds) {
      try {
        await api.deleteProxy(url);
        ok++;
      } catch { /* skip */ }
    }
    setSelectedIds(new Set());
    toast(`已删除 ${ok} 个代理`, "success");
    void load();
  };

  // 测试单个代理
  const handleTest = async (url: string) => {
    setProxies(prev => prev.map(p => p.url === url ? { ...p, testing: true } : p));
    try {
      const result = await api.testProxy(url);
      setProxies(prev => prev.map(p => p.url === url ? {
        ...p,
        testing: false,
        test: result,
        health: { ok: result.success, reason: result.message },
      } : p));
    } catch (err) {
      setProxies(prev => prev.map(p => p.url === url ? {
        ...p,
        testing: false,
        health: { ok: false, reason: String(err) },
      } : p));
    }
  };

  // 批量健康检测
  const handleBatchCheck = async () => {
    setBatchChecking(true);
    try {
      const results = await api.checkProxyHealth();
      setProxies(prev => prev.map(p => {
        const result = results.find(r => r.proxy === p.url);
        return result ? { ...p, health: { ok: result.ok, reason: result.reason } } : p;
      }));
      const okCount = results.filter(r => r.ok).length;
      toast(`检测完成: ${okCount}/${results.length} 可用`, okCount === results.length ? "success" : "error");
    } catch (err) {
      toast(`检测失败: ${err}`, "error");
    } finally {
      setBatchChecking(false);
    }
  };

  // 批量测试连通性（逐个调用 testProxy，获取延迟/地理信息）
  const handleBatchTest = async () => {
    if (proxies.length === 0) return;
    setBatchTesting(true);
    setBatchTestProgress({ done: 0, total: proxies.length });
    let okCount = 0;
    for (let i = 0; i < proxies.length; i++) {
      const url = proxies[i].url;
      setProxies(prev => prev.map(p => p.url === url ? { ...p, testing: true } : p));
      try {
        const result = await api.testProxy(url);
        if (result.success) okCount++;
        setProxies(prev => prev.map(p => p.url === url ? {
          ...p, testing: false, test: result,
          health: { ok: result.success, reason: result.message },
        } : p));
      } catch (err) {
        setProxies(prev => prev.map(p => p.url === url ? {
          ...p, testing: false,
          health: { ok: false, reason: String(err) },
        } : p));
      }
      setBatchTestProgress({ done: i + 1, total: proxies.length });
    }
    setBatchTesting(false);
    toast(`测试完成: ${okCount}/${proxies.length} 可用`, okCount === proxies.length ? "success" : "error");
  };

  // 全选切换
  const allSelected = proxies.length > 0 && proxies.every(p => selectedIds.has(p.url));
  const toggleSelectAll = () => {
    if (allSelected) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(proxies.map(p => p.url)));
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20 c-dim">
        <Loader2 size={20} className="animate-spin mr-2" /> 加载中...
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* 页头 */}
      <div className="card p-5">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <div className="section-title mb-0">代理管理</div>
            <p className="mt-1 text-xs c-dim">管理代理池、健康检测与连通性测试</p>
          </div>
          <div className="flex items-center gap-3">
            {/* 启用开关 */}
            <label className="inline-flex items-center gap-2 cursor-pointer select-none">
              <span className="text-xs font-medium" style={{ color: proxyEnabled ? "#2dd4bf" : "var(--text-dim)" }}>
                {proxyEnabled ? "代理已启用" : "直连模式"}
              </span>
              <button
                type="button"
                role="switch"
                aria-checked={proxyEnabled}
                onClick={() => void toggleEnabled(!proxyEnabled)}
                className="relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors duration-200"
                style={{ background: proxyEnabled ? "linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))" : "var(--ghost)" }}
              >
                <span
                  className="inline-block h-4.5 w-4.5 rounded-full bg-white shadow-sm transition-transform duration-200"
                  style={{ width: 18, height: 18, transform: proxyEnabled ? "translateX(23px)" : "translateX(3px)" }}
                />
              </button>
            </label>

            <span className="h-5 w-px shrink-0" style={{ background: "var(--border)" }} />

            <button
              type="button"
              onClick={handleBatchCheck}
              disabled={batchChecking || batchTesting || proxies.length === 0}
              className="btn btn-ghost flex items-center gap-1.5 py-1.5 text-xs"
            >
              {batchChecking ? <><Loader2 size={13} className="animate-spin" /> 检测中...</> : <><Activity size={13} /> 健康检测</>}
            </button>
            <button
              type="button"
              onClick={() => void handleBatchTest()}
              disabled={batchTesting || batchChecking || proxies.length === 0}
              className="btn btn-ghost flex items-center gap-1.5 py-1.5 text-xs"
            >
              {batchTesting
                ? <><Loader2 size={13} className="animate-spin" /> 测试中 {batchTestProgress.done}/{batchTestProgress.total}</>
                : <><Zap size={13} /> 测试连通性</>
              }
            </button>
            <button
              type="button"
              onClick={() => void load()}
              className="btn btn-ghost flex items-center gap-1.5 py-1.5 text-xs"
            >
              <RefreshCw size={13} /> 刷新
            </button>
            <button
              type="button"
              onClick={() => setShowAddModal(true)}
              className="btn flex items-center gap-1.5 py-1.5 text-xs"
              style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))", color: "#fff" }}
            >
              <Plus size={13} /> 添加代理
            </button>
          </div>
        </div>

        {/* 统计卡片 */}
        {proxies.length > 0 && (
          <div className="proxy-stats-grid mt-4">
            <StatCard icon={Globe} label="代理总数" value={stats.total} color="#3b82f6" />
            <StatCard icon={Wifi} label="可用" value={stats.healthy} color="#2dd4bf" />
            <StatCard icon={WifiOff} label="异常" value={stats.unhealthy} color="#f87171" />
            <StatCard icon={Signal} label="平均延迟" value={stats.avgLatency > 0 ? `${stats.avgLatency}ms` : "--"} color="#fbbf24" />
          </div>
        )}
      </div>

      {/* 批量操作栏 */}
      {selectedIds.size > 0 && (
        <div className="card px-4 py-3 flex items-center justify-between">
          <span className="text-xs c-dim">
            已选 <span className="font-mono font-bold c-heading">{selectedIds.size}</span> 个代理
          </span>
          <div className="flex items-center gap-2">
            <button type="button" onClick={handleBatchDelete} className="btn btn-danger py-1 px-3 text-xs flex items-center gap-1">
              <Trash2 size={12} /> 批量删除
            </button>
            <button type="button" onClick={() => setSelectedIds(new Set())} className="btn btn-ghost py-1 px-3 text-xs">
              清空选择
            </button>
          </div>
        </div>
      )}

      {/* 代理表格 */}
      <div className="card overflow-hidden">
        {proxies.length === 0 ? (
          <EmptyState onAdd={() => setShowAddModal(true)} />
        ) : (
          <div className="proxy-table">
            <ProxyTableHeader />
            <div className="proxy-table-body">
              {proxies.map(entry => (
                <ProxyRow
                  key={entry.url}
                  entry={entry}
                  selected={selectedIds.has(entry.url)}
                  onToggleSelect={() => {
                    setSelectedIds(prev => {
                      const next = new Set(prev);
                      if (next.has(entry.url)) next.delete(entry.url);
                      else next.add(entry.url);
                      return next;
                    });
                  }}
                  onTest={() => void handleTest(entry.url)}
                  onDelete={() => void handleDelete(entry.url)}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* 添加代理 Modal */}
      {showAddModal && (
        <div
          className="team-modal"
          onClick={event => { if (event.target === event.currentTarget && !adding) setShowAddModal(false); }}
        >
          <div
            className="team-modal-card p-6"
            style={{ maxWidth: 600, width: "96vw" }}
            onClick={event => event.stopPropagation()}
          >
            {/* 标题 + 关闭 */}
            <div className="mb-5 flex items-start justify-between">
              <div className="section-title mb-0 text-base">添加代理</div>
              <button
                type="button"
                onClick={() => setShowAddModal(false)}
                disabled={adding}
                className="flex items-center justify-center w-8 h-8 rounded-lg transition-colors hover:bg-[var(--ghost-hover)]"
                style={{ color: "var(--text-dim)" }}
              >
                <X size={16} />
              </button>
            </div>

            {/* Tab 切换 */}
            <div className="mb-5 flex items-center gap-1 border-b" style={{ borderColor: "var(--border)" }}>
              <button
                type="button"
                onClick={() => setAddMode("single")}
                className="flex items-center gap-1.5 px-4 py-2.5 text-sm font-semibold transition-colors"
                style={{
                  color: addMode === "single" ? "#14b8a6" : "var(--text-dim)",
                  borderBottom: addMode === "single" ? "2px solid #14b8a6" : "2px solid transparent",
                  marginBottom: -1,
                }}
              >
                <Plus size={14} /> 标准添加
              </button>
              <button
                type="button"
                onClick={() => setAddMode("batch")}
                className="flex items-center gap-1.5 px-4 py-2.5 text-sm font-semibold transition-colors"
                style={{
                  color: addMode === "batch" ? "#14b8a6" : "var(--text-dim)",
                  borderBottom: addMode === "batch" ? "2px solid #14b8a6" : "2px solid transparent",
                  marginBottom: -1,
                }}
              >
                <Activity size={14} /> 快捷添加
              </button>
            </div>

            {addMode === "single" ? (
              /* ── 标准添加：单行输入 ── */
              <>
                <p className="mb-3 text-sm c-dim">每行一个，支持 HTTP / SOCKS5 协议</p>
                <div
                  className="flex items-center rounded-xl px-4 py-3 transition-all"
                  style={{
                    border: "1.5px solid rgba(20,184,166,0.4)",
                    background: "var(--input-bg)",
                    boxShadow: "0 0 0 3px rgba(20,184,166,0.06)",
                  }}
                >
                  <input
                    type="text"
                    value={singleInput}
                    onChange={e => setSingleInput(e.target.value)}
                    onKeyDown={e => { if (e.key === "Enter") void handleAddSingle(); }}
                    placeholder="http://127.0.0.1:7890"
                    className="flex-1 bg-transparent text-sm font-mono outline-none c-heading"
                    style={{ border: "none" }}
                    autoFocus
                  />
                  <button
                    type="button"
                    onClick={() => void handleAddSingle()}
                    disabled={adding || !singleInput.trim()}
                    className="flex items-center justify-center w-8 h-8 rounded-lg shrink-0 ml-2 transition-all disabled:opacity-30"
                    style={{ background: "linear-gradient(135deg, #14b8a6, #3b82f6)", color: "#fff" }}
                    title="添加"
                  >
                    {adding ? <Loader2 size={16} className="animate-spin" /> : <Plus size={16} />}
                  </button>
                </div>
                <div className="mt-6 flex justify-end gap-3">
                  <button type="button" onClick={() => setShowAddModal(false)} disabled={adding} className="btn btn-ghost px-5 py-2.5">
                    取消
                  </button>
                </div>
              </>
            ) : (
              /* ── 快捷添加：多行批量 ── */
              <>
                <div className="mb-2 text-sm font-medium c-heading">代理列表</div>
                <textarea
                  value={addInput}
                  onChange={e => setAddInput(e.target.value)}
                  placeholder={"每行输入一个代理，支持以下格式：\nsocks5://user:pass@192.168.1.1:1080\nhttp://192.168.1.1:8080\nhttps://user:pass@proxy.example.com:443"}
                  rows={10}
                  className="field-input w-full resize-y font-mono text-xs"
                  style={{ lineHeight: 2, minHeight: 200 }}
                  autoFocus
                />
                <p className="mt-2.5 text-xs c-dim">
                  支持 http、https、socks5 协议，格式：<span className="font-mono c-dim2">协议://[用户名:密码@]主机:端口</span>
                </p>
                <div className="mt-5 flex items-center justify-between border-t pt-4" style={{ borderColor: "var(--border)" }}>
                  <span className="text-xs c-dim">
                    已输入 <span className="font-mono font-bold c-heading">{addInput.split("\n").filter(l => l.trim()).length}</span> 个地址
                  </span>
                  <div className="flex gap-3">
                    <button type="button" onClick={() => setShowAddModal(false)} disabled={adding} className="btn btn-ghost px-5 py-2.5">
                      取消
                    </button>
                    <button
                      type="button"
                      onClick={() => void handleAdd()}
                      disabled={adding || addInput.trim().length === 0}
                      className="btn px-5 py-2.5"
                      style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))", color: "#fff" }}
                    >
                      {adding
                        ? <span className="inline-flex items-center gap-2"><Loader2 size={14} className="animate-spin" /> 导入中</span>
                        : `导入 ${addInput.split("\n").filter(l => l.trim()).length} 个代理`
                      }
                    </button>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
