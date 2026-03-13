import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';

interface RunItem { id: number; schedule_name?: string; trigger_type: string; status: string; target_count: number; started_at: string; elapsed_secs?: number; registered_ok: number; registered_failed: number; rt_ok: number; rt_failed: number; total_s2a_ok: number; total_s2a_failed: number; error?: string }
interface RunDetail extends RunItem {}
interface Distribution { team_name: string; percent: number; assigned_count: number; s2a_ok: number; s2a_failed: number }
interface RunStats { total_runs: number; completed: number; failed: number; total_reg_ok: number; total_reg_failed: number; total_rt_ok: number; total_rt_failed: number; total_s2a_ok: number; total_s2a_failed: number; total_target: number; total_elapsed_secs: number; avg_secs_per_account: number }

function fmtBj(iso: string) {
  try { const d = new Date(iso); const utc = d.getTime() + d.getTimezoneOffset() * 60000; const bj = new Date(utc + 8 * 3600000); return `${String(bj.getMonth() + 1).padStart(2, '0')}-${String(bj.getDate()).padStart(2, '0')} ${String(bj.getHours()).padStart(2, '0')}:${String(bj.getMinutes()).padStart(2, '0')}:${String(bj.getSeconds()).padStart(2, '0')}`; } catch { return iso || '-'; }
}

export default function Runs() {
  const { toast } = useToast();
  const [runs, setRuns] = useState<RunItem[]>([]);
  const [page, setPage] = useState(1); const [total, setTotal] = useState(0); const [perPage] = useState(15);
  const [filter, setFilter] = useState(''); const [schedules, setSchedules] = useState<string[]>([]);
  const [stats, setStats] = useState<RunStats | null>(null);
  const [detail, setDetail] = useState<{ run: RunDetail; distributions?: Distribution[] } | null>(null);
  const [detailId, setDetailId] = useState<number | null>(null);

  const loadStats = useCallback(async () => {
    try { const s = await api.get<RunStats>('/api/runs/stats'); setStats(s); } catch {}
  }, []);

  const loadScheduleList = useCallback(async () => {
    try { const d = await api.fetchSchedules() as { schedules: { name: string }[] }; setSchedules((d.schedules || []).map(s => s.name)); } catch {}
  }, []);

  const loadRuns = useCallback(async (p: number) => {
    const params = new URLSearchParams({ page: String(p), per_page: String(perPage) });
    if (filter === '__manual__') params.set('trigger', 'manual');
    else if (filter.startsWith('sched:')) params.set('schedule', decodeURIComponent(filter.slice(6)));
    try {
      const d = await api.get<{ runs: RunItem[]; total: number; per_page: number }>(`/api/runs?${params}`);
      setRuns(d.runs || []); setTotal(d.total); setPage(p);
    } catch {}
  }, [filter, perPage]);

  useEffect(() => { loadStats(); loadScheduleList(); loadRuns(1); }, [loadStats, loadScheduleList, loadRuns]);

  const showDetail = async (id: number) => {
    if (detailId === id) { setDetailId(null); setDetail(null); return; }
    setDetailId(id); setDetail(null);
    try { const d = await api.get<{ run: RunDetail; distributions?: Distribution[] }>(`/api/runs/${id}`); setDetail(d); } catch { setDetailId(null); }
  };

  const totalPages = Math.ceil(total / perPage);
  const pct = (ok: number, fail: number) => { const t = ok + fail; return t > 0 ? Math.round(ok / t * 100) : 0; };

  return (
    <div className="space-y-4">
      {/* Stats */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <div className="card p-3"><div className="field-label">总运行</div><div className="stat-num text-lg c-heading">{stats.total_runs}</div></div>
          <div className="card p-3"><div className="field-label">总注册</div><div className="stat-num text-lg c-heading">{stats.total_reg_ok} <span className="text-xs c-dim">{pct(stats.total_reg_ok, stats.total_reg_failed)}%</span></div></div>
          <div className="card p-3"><div className="field-label">总 RT</div><div className="stat-num text-lg c-heading">{stats.total_rt_ok} <span className="text-xs c-dim">{pct(stats.total_rt_ok, stats.total_rt_failed)}%</span></div></div>
          <div className="card p-3"><div className="field-label">总 S2A</div><div className="stat-num text-lg text-teal-400">{stats.total_s2a_ok} <span className="text-xs c-dim">{pct(stats.total_s2a_ok, stats.total_s2a_failed)}%</span></div></div>
          <div className="card p-3"><div className="field-label">平均耗时</div><div className="stat-num text-lg text-amber-400">{stats.avg_secs_per_account > 0 ? `${stats.avg_secs_per_account.toFixed(1)}s` : '-'}</div></div>
        </div>
      )}

      {/* Filter + List */}
      <div className="card p-5">
        <div className="flex items-center justify-between mb-4">
          <div className="section-title mb-0">运行记录</div>
          <Select
            value={filter}
            onChange={v => { setFilter(v); loadRuns(1); }}
            options={[
              { label: '全部', value: '' },
              { label: '手动任务', value: '__manual__' },
              ...schedules.map(n => ({ label: n, value: `sched:${encodeURIComponent(n)}` }))
            ]}
            className="w-auto min-w-[150px]"
          />
        </div>
        <div className="space-y-2">
          {runs.length === 0 ? <p className="text-sm c-dim text-center py-8">暂无记录</p> :
            runs.map(r => {
              const bc: Record<string, string> = { running: 'badge-run', completed: 'badge-ok', failed: 'badge-err' };
              const lb: Record<string, string> = { running: '运行中', completed: '完成', failed: '失败' };
              const regT = r.registered_ok + r.registered_failed; const rtT = r.rt_ok + r.rt_failed; const s2aT = r.total_s2a_ok + r.total_s2a_failed;
              const elapsed = r.elapsed_secs ? `${r.elapsed_secs.toFixed(1)}s` : '-';
              const avgSec = (r.elapsed_secs && r.target_count > 0) ? `${(r.elapsed_secs / r.target_count).toFixed(1)}s/个` : '-';
              return (
                <div key={r.id}>
                  <div className="row-item cursor-pointer" onClick={() => showDetail(r.id)} style={{ padding: '10px 14px' }}>
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <span className={`badge ${bc[r.status] || 'badge-off'}`}>{lb[r.status] || r.status}</span>
                        <span className="text-xs font-mono c-dim">#{r.id}</span>
                        <span className="text-xs c-dim2">{r.schedule_name || r.trigger_type}</span>
                      </div>
                      <div className="flex items-center gap-2 text-[.7rem] c-dim">
                        <span>{fmtBj(r.started_at)}</span>
                        <span style={{ color: 'var(--border)' }}>|</span>
                        <span>{elapsed}</span>
                        <span style={{ color: 'var(--border)' }}>|</span>
                        <span className="text-amber-400">{avgSec}</span>
                        <span className="text-[.65rem] c-dim">[{r.target_count}]</span>
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-2">
                      {[{ l: '注册', ok: r.registered_ok, t: regT }, { l: 'RT', ok: r.rt_ok, t: rtT }, { l: 'S2A', ok: r.total_s2a_ok, t: s2aT }].map(x => (
                        <div key={x.l} className="flex items-center gap-2">
                          <span className={`text-[.7rem] w-8 ${x.l === 'S2A' ? 'text-teal-400' : 'c-dim'}`}>{x.l}</span>
                          <div className="progress-bar flex-1"><div className="progress-fill" style={{ width: `${pct(x.ok, x.t - x.ok)}%` }} /></div>
                          <span className="text-[.7rem] font-mono min-w-[56px] text-right"><span className="c-heading">{x.ok}</span><span className="c-dim">/{x.t}</span></span>
                        </div>
                      ))}
                    </div>
                    {r.error && <div className="text-[.7rem] text-red-400 mt-1.5 truncate">{r.error}</div>}
                  </div>
                  {detailId === r.id && detail && (
                    <div className="card p-5 mt-1">
                      <div className="flex items-center justify-between mb-4">
                        <div className="section-title mb-0">运行详情</div>
                        <button onClick={() => { setDetailId(null); setDetail(null); }} className="btn btn-ghost text-xs py-1">关闭</button>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                        <div className="card-inner p-3"><div className="field-label">注册成功</div><div className="stat-num text-lg c-heading">{detail.run.registered_ok}</div></div>
                        <div className="card-inner p-3"><div className="field-label">RT 成功</div><div className="stat-num text-lg c-heading">{detail.run.rt_ok}</div></div>
                        <div className="card-inner p-3"><div className="field-label">S2A 成功</div><div className="stat-num text-lg text-teal-400">{detail.run.total_s2a_ok}</div></div>
                        <div className="card-inner p-3"><div className="field-label">总耗时</div><div className="stat-num text-lg c-heading">{detail.run.elapsed_secs?.toFixed(1)}s</div></div>
                        <div className="card-inner p-3"><div className="field-label">平均耗时</div><div className="stat-num text-lg text-amber-400">{detail.run.elapsed_secs && detail.run.target_count ? (detail.run.elapsed_secs / detail.run.target_count).toFixed(1) : '-'}s/个</div></div>
                      </div>
                      {detail.distributions && detail.distributions.length > 0 && (
                        <div>
                          <div className="section-title">分发详情</div>
                          <div className="space-y-2">
                            {detail.distributions.map((ds, i) => {
                              const tot = ds.s2a_ok + ds.s2a_failed;
                              return (
                                <div key={i} className="card-inner p-3 flex items-center justify-between">
                                  <div className="flex items-center gap-3"><span className="text-sm font-medium c-heading">{ds.team_name}</span><span className="text-xs c-dim font-mono">{ds.percent}%</span><span className="text-xs c-dim">x{ds.assigned_count}</span></div>
                                  <div className="flex items-center gap-3 text-xs font-mono">
                                    <span className="text-teal-400">{ds.s2a_ok}</span><span className="text-red-400">{ds.s2a_failed}</span>
                                    <div className="progress-bar w-20"><div className="progress-fill" style={{ width: `${pct(ds.s2a_ok, ds.s2a_failed)}%` }} /></div>
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })
          }
        </div>
        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-center gap-3 mt-4 text-xs">
            <button onClick={() => loadRuns(1)} disabled={page <= 1} className="btn btn-ghost text-xs py-1">首页</button>
            <button onClick={() => loadRuns(page - 1)} disabled={page <= 1} className="btn btn-ghost text-xs py-1">上一页</button>
            <span className="c-dim">第 {page}/{totalPages} 页 (共 {total} 条)</span>
            <button onClick={() => loadRuns(page + 1)} disabled={page >= totalPages} className="btn btn-ghost text-xs py-1">下一页</button>
            <button onClick={() => loadRuns(totalPages)} disabled={page >= totalPages} className="btn btn-ghost text-xs py-1">末页</button>
          </div>
        )}
      </div>
    </div>
  );
}
