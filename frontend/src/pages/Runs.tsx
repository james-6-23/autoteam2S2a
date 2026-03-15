import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';
import { Badge, Button, Card, ProgressBar, RowItem, SectionTitle, StatCard } from '../components/ui';

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
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <StatCard label="总运行" value={stats.total_runs} className="p-3" valueClassName="text-lg c-heading" />
          <StatCard label="总注册" value={<>{stats.total_reg_ok} <span className="text-xs c-dim">{pct(stats.total_reg_ok, stats.total_reg_failed)}%</span></>} className="p-3" valueClassName="text-lg c-heading" />
          <StatCard label="总 RT" value={<>{stats.total_rt_ok} <span className="text-xs c-dim">{pct(stats.total_rt_ok, stats.total_rt_failed)}%</span></>} className="p-3" valueClassName="text-lg c-heading" />
          <StatCard label="总 S2A" value={<>{stats.total_s2a_ok} <span className="text-xs c-dim">{pct(stats.total_s2a_ok, stats.total_s2a_failed)}%</span></>} className="p-3" valueClassName="text-lg text-teal-400" />
          <StatCard label="平均耗时" value={stats.avg_secs_per_account > 0 ? `${stats.avg_secs_per_account.toFixed(1)}s` : '-'} className="p-3" valueClassName="text-lg text-amber-400" />
        </div>
      )}

      <Card className="p-5">
        <div className="flex items-center justify-between mb-4">
          <SectionTitle className="mb-0">运行记录</SectionTitle>
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
              const bc: Record<string, 'run' | 'ok' | 'err'> = { running: 'run', completed: 'ok', failed: 'err' };
              const lb: Record<string, string> = { running: '运行中', completed: '完成', failed: '失败' };
              const regT = r.registered_ok + r.registered_failed; const rtT = r.rt_ok + r.rt_failed; const s2aT = r.total_s2a_ok + r.total_s2a_failed;
              const elapsed = r.elapsed_secs ? `${r.elapsed_secs.toFixed(1)}s` : '-';
              const avgSec = (r.elapsed_secs && r.target_count > 0) ? `${(r.elapsed_secs / r.target_count).toFixed(1)}s/个` : '-';
              return (
                <div key={r.id}>
                  <RowItem className="cursor-pointer" onClick={() => showDetail(r.id)} style={{ padding: '10px 14px' }}>
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Badge variant={bc[r.status] || 'off'}>{lb[r.status] || r.status}</Badge>
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
                          <ProgressBar value={pct(x.ok, x.t - x.ok)} className="flex-1" />
                          <span className="text-[.7rem] font-mono min-w-[56px] text-right"><span className="c-heading">{x.ok}</span><span className="c-dim">/{x.t}</span></span>
                        </div>
                      ))}
                    </div>
                    {r.error && <div className="text-[.7rem] text-red-400 mt-1.5 truncate">{r.error}</div>}
                  </RowItem>
                  {detailId === r.id && detail && (
                    <Card className="p-5 mt-1">
                      <div className="flex items-center justify-between mb-4">
                        <SectionTitle className="mb-0">运行详情</SectionTitle>
                        <Button onClick={() => { setDetailId(null); setDetail(null); }} variant="ghost" className="text-xs py-1">关闭</Button>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                        <StatCard label="注册成功" value={detail.run.registered_ok} className="p-3" valueClassName="text-lg c-heading" />
                        <StatCard label="RT 成功" value={detail.run.rt_ok} className="p-3" valueClassName="text-lg c-heading" />
                        <StatCard label="S2A 成功" value={detail.run.total_s2a_ok} className="p-3" valueClassName="text-lg text-teal-400" />
                        <StatCard label="总耗时" value={`${detail.run.elapsed_secs?.toFixed(1)}s`} className="p-3" valueClassName="text-lg c-heading" />
                        <StatCard label="平均耗时" value={`${detail.run.elapsed_secs && detail.run.target_count ? (detail.run.elapsed_secs / detail.run.target_count).toFixed(1) : '-'}s/个`} className="p-3" valueClassName="text-lg text-amber-400" />
                      </div>
                      {detail.distributions && detail.distributions.length > 0 && (
                        <div>
                          <SectionTitle>分发详情</SectionTitle>
                          <div className="space-y-2">
                            {detail.distributions.map((ds, i) => {
                              const tot = ds.s2a_ok + ds.s2a_failed;
                              return (
                                <Card key={i} inner className="p-3 flex items-center justify-between">
                                  <div className="flex items-center gap-3"><span className="text-sm font-medium c-heading">{ds.team_name}</span><span className="text-xs c-dim font-mono">{ds.percent}%</span><span className="text-xs c-dim">x{ds.assigned_count}</span></div>
                                  <div className="flex items-center gap-3 text-xs font-mono">
                                    <span className="text-teal-400">{ds.s2a_ok}</span><span className="text-red-400">{ds.s2a_failed}</span>
                                    <ProgressBar value={pct(ds.s2a_ok, ds.s2a_failed)} className="w-20" />
                                  </div>
                                </Card>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </Card>
                  )}
                </div>
              );
            })
          }
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-center gap-3 mt-4 text-xs">
            <Button onClick={() => loadRuns(1)} disabled={page <= 1} variant="ghost" className="text-xs py-1">首页</Button>
            <Button onClick={() => loadRuns(page - 1)} disabled={page <= 1} variant="ghost" className="text-xs py-1">上一页</Button>
            <span className="c-dim">第 {page}/{totalPages} 页 (共 {total} 条)</span>
            <Button onClick={() => loadRuns(page + 1)} disabled={page >= totalPages} variant="ghost" className="text-xs py-1">下一页</Button>
            <Button onClick={() => loadRuns(totalPages)} disabled={page >= totalPages} variant="ghost" className="text-xs py-1">末页</Button>
          </div>
        )}
      </Card>
    </div>
  );
}
