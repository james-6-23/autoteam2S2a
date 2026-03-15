import { useEffect, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Users, Globe, Mail, Rocket, Clock, CheckCircle, XCircle, AlertTriangle, Activity, Shield, UserCheck, Zap } from 'lucide-react';
import * as api from '../lib/api';
import { StatCard, Card, SectionTitle, Badge } from '../components/ui';
import type { TeamManageDashboardSummary } from '../lib/team-manage-types';

interface ConfigData {
  teams: { name: string; api_base: string; admin_key: string; concurrency: number; priority: number }[];
  proxy_pool: string[];
  email_domains: string[];
  defaults: Record<string, unknown>;
  register: Record<string, unknown>;
}
interface TaskItem { task_id: string; team: string; target: number; status: string; created_at: string; stage?: string; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number }
interface SchedItem { name: string; enabled: boolean; running?: boolean; cooldown?: boolean; pending?: boolean; start_time: string; end_time: string; target_count: number; batch_interval_mins: number; distribution: { team: string; percent: number }[] }
interface RunItem { id: number; schedule_name?: string; trigger_type: string; status: string; target_count: number; started_at: string; elapsed_secs?: number; registered_ok: number; registered_failed: number; rt_ok: number; rt_failed: number; total_s2a_ok: number; total_s2a_failed: number }
interface RunStats { total_runs: number; completed: number; failed: number; total_reg_ok: number; total_reg_failed: number; total_rt_ok: number; total_rt_failed: number; total_s2a_ok: number; total_s2a_failed: number; total_target: number; total_elapsed_secs: number; avg_secs_per_account: number }

function fmtBj(iso: string) {
  try { const d = new Date(iso); const utc = d.getTime() + d.getTimezoneOffset() * 60000; const bj = new Date(utc + 8 * 3600000); return `${String(bj.getMonth() + 1).padStart(2, '0')}-${String(bj.getDate()).padStart(2, '0')} ${String(bj.getHours()).padStart(2, '0')}:${String(bj.getMinutes()).padStart(2, '0')}`; } catch { return iso || '-'; }
}

export default function Dashboard() {
  const navigate = useNavigate();
  const [config, setConfig] = useState<ConfigData | null>(null);
  const [tasks, setTasks] = useState<TaskItem[]>([]);
  const [schedules, setSchedules] = useState<SchedItem[]>([]);
  const [recentRuns, setRecentRuns] = useState<RunItem[]>([]);
  const [runStats, setRunStats] = useState<RunStats | null>(null);
  const [ownerSummary, setOwnerSummary] = useState<TeamManageDashboardSummary | null>(null);

  const load = useCallback(async () => {
    const results = await Promise.allSettled([
      api.fetchConfig() as Promise<ConfigData>,
      api.fetchTasks() as Promise<{ tasks: TaskItem[] }>,
      api.fetchSchedules() as Promise<{ schedules: SchedItem[] }>,
      api.get<{ runs: RunItem[] }>('/api/runs?page=1&per_page=5'),
      api.get<RunStats>('/api/runs/stats'),
      api.fetchTeamManageDashboard(),
    ]);
    if (results[0].status === 'fulfilled') setConfig(results[0].value);
    if (results[1].status === 'fulfilled') setTasks((results[1].value).tasks || []);
    if (results[2].status === 'fulfilled') setSchedules((results[2].value).schedules || []);
    if (results[3].status === 'fulfilled') setRecentRuns((results[3].value).runs || []);
    if (results[4].status === 'fulfilled') setRunStats(results[4].value);
    if (results[5].status === 'fulfilled') setOwnerSummary(results[5].value);
  }, []);

  useEffect(() => { load(); const id = setInterval(load, 10000); return () => clearInterval(id); }, [load]);

  const runningTasks = tasks.filter(t => t.status === 'running' || t.status === 'pending');
  const activeScheds = schedules.filter(s => s.running || s.cooldown || s.pending);
  const enabledScheds = schedules.filter(s => s.enabled);

  const pct = (ok: number, fail: number) => { const t = ok + fail; return t > 0 ? Math.round(ok / t * 100) : 0; };

  return (
    <div className="space-y-5">
      {/* Row 1: Core Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="S2A 号池" value={config?.teams.length ?? '--'} />
        <StatCard label="可用代理" value={config?.proxy_pool.length ?? '--'} />
        <StatCard label="邮箱域名" value={config?.email_domains.length ?? '--'} />
        <StatCard label="活跃任务" value={runningTasks.length} valueClassName="text-amber-400" />
      </div>

      {/* Row 2: Owner Summary + Run Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Owner 概况 */}
        <Card className="p-5">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle className="mb-0">Owner 概况</SectionTitle>
            <button onClick={() => navigate('/team-manage')} className="text-xs c-dim hover:text-teal-400 transition-colors">查看全部 →</button>
          </div>
          {ownerSummary ? (
            <div className="grid grid-cols-2 gap-3">
              <div className="card-inner p-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(45, 212, 191, 0.1)' }}>
                  <Users size={15} className="text-teal-400" />
                </div>
                <div>
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">总 Owner</div>
                  <div className="text-lg font-bold c-heading font-display">{ownerSummary.total_owners}</div>
                </div>
              </div>
              <div className="card-inner p-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(45, 212, 191, 0.1)' }}>
                  <UserCheck size={15} className="text-teal-400" />
                </div>
                <div>
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">活跃</div>
                  <div className="text-lg font-bold c-heading font-display">{ownerSummary.active_owners}</div>
                </div>
              </div>
              <div className="card-inner p-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(248, 113, 113, 0.1)' }}>
                  <Shield size={15} className="text-red-400" />
                </div>
                <div>
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">封禁</div>
                  <div className="text-lg font-bold text-red-400 font-display">{ownerSummary.banned_owners}</div>
                </div>
              </div>
              <div className="card-inner p-3 flex items-center gap-3">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(139, 92, 246, 0.1)' }}>
                  <Zap size={15} className="text-primary" />
                </div>
                <div>
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">有空位</div>
                  <div className="text-lg font-bold c-heading font-display">{ownerSummary.owners_with_slots}</div>
                </div>
              </div>
            </div>
          ) : (
            <div className="grid grid-cols-2 gap-3">
              {[0, 1, 2, 3].map(i => <div key={i} className="skeleton h-[68px] rounded-xl" />)}
            </div>
          )}
        </Card>

        {/* 运行统计 */}
        <Card className="p-5">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle className="mb-0">运行统计</SectionTitle>
            <button onClick={() => navigate('/runs')} className="text-xs c-dim hover:text-teal-400 transition-colors">查看全部 →</button>
          </div>
          {runStats ? (
            <div className="space-y-3">
              <div className="grid grid-cols-3 gap-3">
                <div className="card-inner p-3 text-center">
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">总运行</div>
                  <div className="text-lg font-bold c-heading font-display">{runStats.total_runs}</div>
                </div>
                <div className="card-inner p-3 text-center">
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">总 S2A</div>
                  <div className="text-lg font-bold text-teal-400 font-display">{runStats.total_s2a_ok}</div>
                </div>
                <div className="card-inner p-3 text-center">
                  <div className="text-[.68rem] c-dim uppercase tracking-wide">平均耗时</div>
                  <div className="text-lg font-bold text-amber-400 font-display">{runStats.avg_secs_per_account > 0 ? `${runStats.avg_secs_per_account.toFixed(1)}s` : '--'}</div>
                </div>
              </div>
              <div className="space-y-2">
                {[
                  { label: '注册', ok: runStats.total_reg_ok, fail: runStats.total_reg_failed, color: 'c-heading' },
                  { label: 'RT', ok: runStats.total_rt_ok, fail: runStats.total_rt_failed, color: 'c-heading' },
                  { label: 'S2A', ok: runStats.total_s2a_ok, fail: runStats.total_s2a_failed, color: 'text-teal-400' },
                ].map(r => (
                  <div key={r.label} className="flex items-center gap-3 text-xs">
                    <span className={`w-8 font-medium ${r.color}`}>{r.label}</span>
                    <div className="progress-bar flex-1"><div className="progress-fill" style={{ width: `${pct(r.ok, r.fail)}%` }} /></div>
                    <span className="font-mono min-w-[90px] text-right">
                      <span className={r.color}>{r.ok}</span>
                      <span className="c-dim"> / {r.fail} 失败</span>
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="grid grid-cols-3 gap-3">{[0, 1, 2].map(i => <div key={i} className="skeleton h-[60px] rounded-xl" />)}</div>
              <div className="space-y-2">{[0, 1, 2].map(i => <div key={i} className="skeleton h-4 rounded" />)}</div>
            </div>
          )}
        </Card>
      </div>

      {/* Row 3: Active Tasks + Schedules */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* 当前任务 */}
        <Card className="p-5">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle className="mb-0">当前任务</SectionTitle>
            <button onClick={() => navigate('/tasks')} className="text-xs c-dim hover:text-teal-400 transition-colors">查看全部 →</button>
          </div>
          <div className="space-y-2">
            {tasks.length === 0 ? (
              <div className="text-center py-8">
                <Rocket size={28} className="mx-auto c-dim mb-2 opacity-40" />
                <p className="text-sm c-dim">暂无任务</p>
              </div>
            ) : (
              tasks.slice(0, 5).map(t => {
                const badgeMap: Record<string, 'warn' | 'run' | 'ok' | 'err' | 'off'> = { pending: 'warn', running: 'run', completed: 'ok', failed: 'err', cancelled: 'off' };
                const labelMap: Record<string, string> = { pending: '等待', running: '运行', completed: '完成', failed: '失败', cancelled: '取消' };
                return (
                  <div key={t.task_id} className="row-item flex items-center justify-between cursor-pointer" onClick={() => navigate('/tasks')} style={{ padding: '10px 14px' }}>
                    <div className="flex items-center gap-2.5 min-w-0">
                      <Badge variant={badgeMap[t.status] || 'off'}>{labelMap[t.status] || t.status}</Badge>
                      <span className="text-xs font-mono c-dim truncate">{t.task_id.slice(0, 8)}</span>
                      <span className="text-xs c-dim">{t.team}</span>
                      <span className="text-xs c-dim">x{t.target}</span>
                    </div>
                    <div className="flex items-center gap-2 text-xs shrink-0">
                      {t.status === 'running' && <span className="text-teal-400 font-mono">S2A:{t.s2a_ok}</span>}
                      <span className="c-dim">{new Date(t.created_at).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}</span>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </Card>

        {/* 定时计划 */}
        <Card className="p-5">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle className="mb-0">定时计划</SectionTitle>
            <button onClick={() => navigate('/schedules')} className="text-xs c-dim hover:text-teal-400 transition-colors">查看全部 →</button>
          </div>
          <div className="space-y-2">
            {schedules.length === 0 ? (
              <div className="text-center py-8">
                <Clock size={28} className="mx-auto c-dim mb-2 opacity-40" />
                <p className="text-sm c-dim">暂无定时计划</p>
              </div>
            ) : (
              schedules.slice(0, 5).map(s => {
                const isActive = s.running || s.cooldown || s.pending;
                const variant = s.running ? 'run' as const : s.cooldown || s.pending ? 'warn' as const : s.enabled ? 'ok' as const : 'off' as const;
                const label = s.running ? '运行中' : s.cooldown ? '准备中' : s.pending ? '等待中' : s.enabled ? '已启用' : '已禁用';
                return (
                  <div key={s.name} className="row-item flex items-center justify-between cursor-pointer" onClick={() => navigate('/schedules')} style={{ padding: '10px 14px' }}>
                    <div className="flex items-center gap-2.5 min-w-0">
                      <Badge variant={variant}>{label}</Badge>
                      <span className="text-sm font-medium c-heading truncate">{s.name}</span>
                    </div>
                    <div className="flex items-center gap-2 text-xs c-dim shrink-0">
                      <code className="font-mono text-[.68rem] px-1.5 py-0.5 rounded" style={{ background: 'var(--ghost)' }}>{s.start_time}-{s.end_time}</code>
                      <span>x{s.target_count}</span>
                      <span>{s.batch_interval_mins}分</span>
                    </div>
                  </div>
                );
              })
            )}
          </div>
          {schedules.length > 0 && (
            <div className="flex items-center gap-4 mt-3 pt-3 text-xs c-dim" style={{ borderTop: '1px solid var(--border)' }}>
              <span className="flex items-center gap-1.5"><Activity size={12} className="text-teal-400" /> 已启用 {enabledScheds.length}/{schedules.length}</span>
              {activeScheds.length > 0 && <span className="flex items-center gap-1.5"><Zap size={12} className="text-amber-400" /> 运行中 {activeScheds.length}</span>}
            </div>
          )}
        </Card>
      </div>

      {/* Row 4: Recent Runs */}
      <Card className="p-5">
        <div className="flex items-center justify-between mb-4">
          <SectionTitle className="mb-0">最近运行</SectionTitle>
          <button onClick={() => navigate('/runs')} className="text-xs c-dim hover:text-teal-400 transition-colors">查看全部 →</button>
        </div>
        <div className="space-y-2">
          {recentRuns.length === 0 ? (
            <div className="text-center py-8">
              <CheckCircle size={28} className="mx-auto c-dim mb-2 opacity-40" />
              <p className="text-sm c-dim">暂无运行记录</p>
            </div>
          ) : (
            recentRuns.map(r => {
              const variant = r.status === 'completed' ? 'ok' as const : r.status === 'running' ? 'run' as const : r.status === 'failed' ? 'err' as const : 'off' as const;
              const label = r.status === 'completed' ? '完成' : r.status === 'running' ? '运行中' : r.status === 'failed' ? '失败' : r.status;
              return (
                <div key={r.id} className="row-item flex items-center justify-between" style={{ padding: '10px 14px' }}>
                  <div className="flex items-center gap-2.5 min-w-0">
                    <Badge variant={variant}>{label}</Badge>
                    <span className="text-xs font-mono c-dim">#{r.id}</span>
                    <span className="text-xs c-dim2">{r.schedule_name || r.trigger_type}</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs shrink-0">
                    <div className="flex items-center gap-1.5">
                      <span className="c-dim">注册</span>
                      <span className="font-mono c-heading">{r.registered_ok}</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="c-dim">S2A</span>
                      <span className="font-mono text-teal-400">{r.total_s2a_ok}</span>
                    </div>
                    <span className="c-dim font-mono">{r.elapsed_secs ? `${r.elapsed_secs.toFixed(0)}s` : '-'}</span>
                    <span className="c-dim">{fmtBj(r.started_at)}</span>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </Card>

      {/* Row 5: Teams overview */}
      {config && config.teams.length > 0 && (
        <Card className="p-5">
          <div className="flex items-center justify-between mb-4">
            <SectionTitle className="mb-0">号池一览</SectionTitle>
            <button onClick={() => navigate('/teams')} className="text-xs c-dim hover:text-teal-400 transition-colors">管理 →</button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {config.teams.map(t => (
              <div key={t.name} className="card-inner p-3 flex items-center gap-3 cursor-pointer hover:border-[var(--border-hover)]" onClick={() => navigate('/teams')}>
                <div className="w-9 h-9 rounded-xl flex items-center justify-center" style={{ background: 'linear-gradient(135deg, rgba(45, 212, 191, 0.12), rgba(139, 92, 246, 0.12))' }}>
                  <Globe size={16} className="text-teal-400" />
                </div>
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-semibold c-heading truncate">{t.name}</div>
                  <div className="text-[.68rem] c-dim font-mono truncate">{t.api_base}</div>
                </div>
                <div className="text-right shrink-0">
                  <div className="text-[.68rem] c-dim">并发</div>
                  <div className="text-sm font-bold c-heading">{t.concurrency}</div>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}
