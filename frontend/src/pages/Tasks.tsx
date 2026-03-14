import { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { Loader2 } from 'lucide-react';

import { useToast } from '../components/Toast';
import { HSelect } from '../components/ui/HSelect';
import * as api from '../lib/api';
import type {
  InviteTaskDetail,
  TeamManageBatchJobDetail,
  TeamManageBatchJobSummary,
  TeamManageBatchRetryMode,
} from '../lib/team-manage-types';

interface TaskItem { task_id: string; team: string; target: number; status: string; created_at: string; stage?: string; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number; error?: string; report?: Record<string, unknown> }
interface Progress { status: string; stage?: string; target: number; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number }

const INVITE_TASK_EMAIL_PREVIEW = 12;

export default function Tasks() {
  const { toast } = useToast();
  const [tasks, setTasks] = useState<TaskItem[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [report, setReport] = useState<Record<string, unknown> | null>(null);
  const [elapsed, setElapsed] = useState(0);

  const [batchJobs, setBatchJobs] = useState<TeamManageBatchJobSummary[]>([]);
  const [activeBatchJobId, setActiveBatchJobId] = useState<string | null>(null);
  const [activeBatchJob, setActiveBatchJob] = useState<TeamManageBatchJobDetail | null>(null);
  const [batchJobLoading, setBatchJobLoading] = useState(false);
  const [retryFailedLoading, setRetryFailedLoading] = useState(false);
  const [retryMode, setRetryMode] = useState<TeamManageBatchRetryMode>('all');
  const [activeInviteTaskId, setActiveInviteTaskId] = useState<string | null>(null);
  const [activeInviteTask, setActiveInviteTask] = useState<InviteTaskDetail | null>(null);
  const [inviteTaskLoading, setInviteTaskLoading] = useState(false);

  const startRef = useRef(0);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const visibleInviteEmails = useMemo(
    () => activeInviteTask?.emails.slice(0, INVITE_TASK_EMAIL_PREVIEW) ?? [],
    [activeInviteTask],
  );

  const load = useCallback(async () => {
    try { const d = await api.fetchTasks() as { tasks: TaskItem[] }; setTasks(d.tasks || []); } catch {}
  }, []);

  const loadBatchJobs = useCallback(async () => {
    try {
      const data = await api.fetchTeamManageBatchJobs();
      setBatchJobs(data.jobs || []);
    } catch {
      setBatchJobs([]);
    }
  }, []);

  const loadBatchJobDetail = useCallback(async (
    jobId: string,
    options?: { preserveInviteTask?: boolean },
  ) => {
    setBatchJobLoading(true);
    if (!options?.preserveInviteTask) {
      setActiveInviteTask(null);
      setActiveInviteTaskId(null);
    }
    try {
      const data = await api.fetchTeamManageBatchJob(jobId);
      setActiveBatchJob(data);
      setActiveBatchJobId(jobId);
    } catch (error) {
      toast(`获取批量任务详情失败: ${error}`, 'error');
    } finally {
      setBatchJobLoading(false);
    }
  }, [toast]);

  const loadInviteTaskDetail = useCallback(async (taskId: string) => {
    setInviteTaskLoading(true);
    try {
      const data = await api.fetchInviteTaskDetail(taskId);
      setActiveInviteTask(data);
      setActiveInviteTaskId(taskId);
    } catch (error) {
      toast(`获取邀请任务详情失败: ${error}`, 'error');
    } finally {
      setInviteTaskLoading(false);
    }
  }, [toast]);

  useEffect(() => { load(); const id = setInterval(load, 3000); return () => clearInterval(id); }, [load]);

  useEffect(() => {
    void loadBatchJobs();
  }, [loadBatchJobs]);

  useEffect(() => {
    if (!batchJobs.some(job => job.status === 'pending' || job.status === 'running')) {
      return;
    }
    const timer = window.setInterval(() => {
      void loadBatchJobs();
      if (activeBatchJobId) void loadBatchJobDetail(activeBatchJobId, { preserveInviteTask: true });
    }, 3000);
    return () => window.clearInterval(timer);
  }, [activeBatchJobId, batchJobs, loadBatchJobDetail, loadBatchJobs]);

  useEffect(() => {
    if (!activeInviteTaskId || !activeInviteTask) {
      return;
    }
    if (activeInviteTask.task.status !== 'pending' && activeInviteTask.task.status !== 'running') {
      return;
    }
    const timer = window.setInterval(() => {
      void loadInviteTaskDetail(activeInviteTaskId);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [activeInviteTask, activeInviteTaskId, loadInviteTaskDetail]);

  const cancel = async (id: string) => {
    if (!confirm('取消此任务?')) return;
    try { await api.post(`/api/tasks/${id}/cancel`); toast('取消请求已发送', 'success'); load(); } catch {}
  };

  const retryFailedBatchItems = async (jobId: string) => {
    setRetryFailedLoading(true);
    try {
      const result = await api.retryFailedTeamManageBatchItems(jobId, { retry_mode: retryMode });
      toast(result.message, 'success');
      void loadBatchJobs();
      void loadBatchJobDetail(result.job_id);
    } catch (error) {
      toast(`重试失败子项失败: ${error}`, 'error');
    } finally {
      setRetryFailedLoading(false);
    }
  };

  const showDetail = (taskId: string) => {
    setActiveId(taskId); setProgress(null); setReport(null);
    startRef.current = Date.now();
    // fetch initial info
    api.get<TaskItem>(`/api/tasks/${taskId}`).then(t => {
      if (t?.created_at) { const s = Date.parse(t.created_at); if (!isNaN(s)) startRef.current = s; }
      if (t?.report) setReport(t.report);
    }).catch(() => {});
    // start polling
    const poll = async () => {
      try {
        const p = await api.get<Progress>(`/api/tasks/${taskId}/progress`);
        setProgress(p);
        if (p.status === 'completed' || p.status === 'failed' || p.status === 'cancelled') {
          if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
          if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; }
          api.get<TaskItem>(`/api/tasks/${taskId}`).then(t => { if (t?.report) setReport(t.report); }).catch(() => {});
        }
      } catch {}
    };
    poll();
    pollRef.current = setInterval(poll, 1500);
    tickRef.current = setInterval(() => setElapsed((Date.now() - startRef.current) / 1000), 100);
  };

  const closeDetail = () => {
    setActiveId(null); setProgress(null); setReport(null);
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
    if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; }
  };

  useEffect(() => () => { closeDetail(); }, []);

  const badgeCls: Record<string, string> = { pending: 'badge-warn', running: 'badge-run', completed: 'badge-ok', failed: 'badge-err', cancelled: 'badge-off' };
  const badgeLbl: Record<string, string> = { pending: '等待', running: '运行', completed: '完成', failed: '失败', cancelled: '取消' };

  const pct = (ok: number, fail: number, total: number) => Math.min(100, ((ok + fail) / Math.max(1, total)) * 100).toFixed(1);

  return (
    <div className="space-y-4">
      <div className="card p-5">
        <div className="section-title">任务队列</div>
        <div className="space-y-2">
          {tasks.length === 0 ? <p className="text-sm c-dim text-center py-8">暂无任务</p> :
            tasks.map(t => (
              <div key={t.task_id} className="row-item flex items-center justify-between cursor-pointer" onClick={() => showDetail(t.task_id)}>
                <div className="flex items-center gap-3 flex-1 min-w-0">
                  <span className={`badge ${badgeCls[t.status] || 'badge-off'}`}>{badgeLbl[t.status] || t.status}</span>
                  <span className="text-xs font-mono c-dim">{t.task_id}</span>
                  <span className="text-xs c-dim">{t.team}</span>
                  <span className="text-xs c-dim">x{t.target}</span>
                  <span className="text-xs c-dim">{new Date(t.created_at).toLocaleTimeString('zh-CN')}</span>
                </div>
                {(t.status === 'pending' || t.status === 'running') && (
                  <button onClick={e => { e.stopPropagation(); cancel(t.task_id); }} className="btn btn-danger text-xs py-1 px-2">取消</button>
                )}
              </div>
            ))
          }
        </div>
      </div>

      {/* Progress Panel */}
      {activeId && (
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <span className={`badge ${badgeCls[progress?.status || 'running'] || 'badge-run'}`}>{badgeLbl[progress?.status || 'running'] || progress?.status}</span>
              <span className="text-xs font-mono c-dim">{activeId}</span>
              <span className="text-xs c-dim">{progress?.stage || ''}</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs font-mono c-dim">耗时 {(report?.elapsed_secs != null ? Number(report.elapsed_secs) : elapsed).toFixed(1)}s</span>
              <button onClick={closeDetail} className="btn btn-ghost text-xs py-1">关闭</button>
            </div>
          </div>
          {progress && (
            <>
              <div className="grid grid-cols-3 gap-3 mb-4">
                <div className="card-inner p-3 text-center"><div className="field-label">注册</div><div className="stat-num text-lg c-heading">{progress.reg_ok}</div></div>
                <div className="card-inner p-3 text-center"><div className="field-label">RT</div><div className="stat-num text-lg c-heading">{progress.rt_ok}</div></div>
                <div className="card-inner p-3 text-center"><div className="field-label">S2A</div><div className="stat-num text-lg text-teal-400">{progress.s2a_ok}</div></div>
              </div>
              <div className="space-y-3">
                {[{ label: '注册', ok: progress.reg_ok, fail: progress.reg_failed },
                  { label: 'RT', ok: progress.rt_ok, fail: progress.rt_failed },
                  { label: 'S2A', ok: progress.s2a_ok, fail: progress.s2a_failed }].map(r => (
                  <div key={r.label} className="flex items-center gap-3 text-xs">
                    <span className="w-8 c-dim">{r.label}</span>
                    <div className="progress-bar h-2 flex-1"><div className="progress-fill" style={{ width: `${pct(r.ok, r.fail, progress.target)}%` }} /></div>
                    <span className="font-mono min-w-[80px] text-right"><span className="text-teal-400">{r.ok}</span> <span className="c-dim">/ {r.fail} 失败</span></span>
                  </div>
                ))}
              </div>
            </>
          )}
          {report && (
            <div className="card-inner p-3 mt-4 text-xs c-dim space-y-1">
              <div className="font-medium c-heading mb-1">报告</div>
              <div>耗时: {(report.elapsed_secs as number)?.toFixed(1)}s</div>
              <div>注册: {String(report.registered_ok)} 成功 / {String(report.registered_failed)} 失败</div>
              <div>RT: {String(report.rt_ok)} 成功 / {String(report.rt_failed)} 失败</div>
              {report.s2a_ok !== undefined && <div>S2A: {String(report.s2a_ok)} 成功 / {String(report.s2a_failed)} 失败</div>}
            </div>
          )}
        </div>
      )}

      <div className="card p-5">
        <div className="team-manage-jobs">
          <div className="team-manage-jobs__header">
            <span className="text-sm font-medium c-heading">批量任务</span>
            <div className="flex items-center gap-2">
              <span className="text-xs c-dim">展示最近 {batchJobs.length} 条</span>
              <button type="button" onClick={() => void loadBatchJobs()} className="btn btn-ghost py-1.5 text-xs">
                刷新任务
              </button>
            </div>
          </div>
          <div className="space-y-2">
            {batchJobs.length === 0 ? (
              <p className="text-sm c-dim text-center py-6">暂无批量任务</p>
            ) : (
              batchJobs.slice(0, 6).map(job => (
                <button
                  key={job.job_id}
                  type="button"
                  className={`team-manage-jobs__item ${activeBatchJobId === job.job_id ? 'team-manage-jobs__item--active' : ''}`}
                  onClick={() => {
                    void loadBatchJobDetail(job.job_id);
                  }}
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium c-heading">{job.job_type}</span>
                      <span className="badge badge-off">{job.status}</span>
                    </div>
                    <div className="mt-1 text-[.7rem] c-dim font-mono">
                      {job.job_id} · {job.created_at}
                    </div>
                  </div>
                  <div className="text-right text-[.75rem]">
                    <div className="font-mono c-heading">
                      {job.success_count}/{job.total_count}
                    </div>
                    <div className="c-dim">
                      失败 {job.failed_count} · 跳过 {job.skipped_count}
                    </div>
                  </div>
                </button>
              ))
            )}
          </div>
        </div>

        {(batchJobLoading || activeBatchJob) && (
          <div className="team-manage-job-detail">
            <div className="team-manage-jobs__header">
              <span className="text-sm font-medium c-heading inline-flex items-center gap-2">
                {batchJobLoading ? <Loader2 size={14} className="animate-spin" /> : null}
                {batchJobLoading ? '任务详情加载中...' : `任务详情 · ${activeBatchJob?.job_id}`}
              </span>
              {activeBatchJob && activeBatchJob.failed_count > 0 && activeBatchJob.job_type === 'batch_invite' && (
                <div className="team-manage-job-detail__toolbar">
                  <HSelect
                    value={retryMode}
                    onChange={value => setRetryMode(value as TeamManageBatchRetryMode)}
                    style={{ minWidth: 160 }}
                    options={[
                      { value: 'all', label: '重试全部失败项' },
                      { value: 'network', label: '仅网络类错误' },
                      { value: 'recoverable', label: '仅可恢复错误' },
                    ]}
                  />
                  <button
                    type="button"
                    onClick={() => void retryFailedBatchItems(activeBatchJob.job_id)}
                    disabled={retryFailedLoading}
                    className="btn btn-ghost py-1.5 text-xs"
                  >
                    {retryFailedLoading ? '重试中...' : '重试失败子项'}
                  </button>
                </div>
              )}
            </div>
            {activeBatchJob && (
              <div className="space-y-2">
                {activeBatchJob.items.map(item => (
                  <div key={`${activeBatchJob.job_id}-${item.account_id}`} className="team-manage-jobs__detail-item">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-xs c-heading">{item.account_id}</span>
                        <span className="badge badge-off">{item.status}</span>
                      </div>
                      <div className="mt-1 text-[.72rem] c-dim">
                        invite_count: {item.invite_count}
                        {item.child_task_id ? (
                          <>
                            {' · '}
                            <button
                              type="button"
                              className="team-manage-link-button"
                              onClick={() => void loadInviteTaskDetail(item.child_task_id!)}
                            >
                              task: {item.child_task_id}
                            </button>
                          </>
                        ) : null}
                      </div>
                      {item.message && <div className="mt-1 text-[.72rem] c-dim">{item.message}</div>}
                      {item.error && <div className="mt-1 text-[.72rem] text-red-400">{item.error}</div>}
                    </div>
                    <div className="text-[.72rem] c-dim">{item.updated_at}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {(inviteTaskLoading || activeInviteTask) && (
          <div className="team-manage-job-detail">
            <div className="team-manage-jobs__header">
              <span className="text-sm font-medium c-heading inline-flex items-center gap-2">
                {inviteTaskLoading ? <Loader2 size={14} className="animate-spin" /> : null}
                {inviteTaskLoading ? '邀请任务详情加载中...' : `邀请任务详情 · ${activeInviteTask?.task.id}`}
              </span>
              {activeInviteTask && (
                <button
                  type="button"
                  onClick={() => {
                    setActiveInviteTaskId(null);
                    setActiveInviteTask(null);
                  }}
                  className="btn btn-ghost py-1.5 text-xs"
                >
                  关闭
                </button>
              )}
            </div>
            {activeInviteTask && (
              <div className="space-y-3">
                <div className="team-manage-invite-task__summary">
                  <div>
                    <div className="text-[.72rem] c-dim">Owner</div>
                    <div className="font-mono text-xs c-heading">{activeInviteTask.task.owner_account_id}</div>
                    <div className="text-[.72rem] c-dim">{activeInviteTask.task.owner_email}</div>
                  </div>
                  <div>
                    <div className="text-[.72rem] c-dim">状态</div>
                    <div className="flex items-center gap-2">
                      <span className="badge badge-off">{activeInviteTask.task.status}</span>
                      <span className="text-[.72rem] c-dim">{activeInviteTask.task.s2a_team || '--'}</span>
                    </div>
                  </div>
                  <div>
                    <div className="text-[.72rem] c-dim">进度</div>
                    <div className="text-[.78rem] c-heading">
                      邀请 {activeInviteTask.task.invited_ok}/{activeInviteTask.task.invite_count}
                      {' · '}
                      注册 {activeInviteTask.task.reg_ok}/{activeInviteTask.task.invite_count}
                    </div>
                    <div className="text-[.72rem] c-dim">
                      RT {activeInviteTask.task.rt_ok} · 入库 {activeInviteTask.task.s2a_ok}
                    </div>
                  </div>
                </div>
                {activeInviteTask.task.error && (
                  <div className="rounded-xl border border-red-500/20 bg-red-500/8 px-3 py-2 text-[.75rem] text-red-300">
                    {activeInviteTask.task.error}
                  </div>
                )}
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-sm font-medium c-heading">邮箱明细</span>
                    <span className="text-[.72rem] c-dim">
                      展示前 {Math.min(INVITE_TASK_EMAIL_PREVIEW, activeInviteTask.emails.length)} / {activeInviteTask.emails.length} 条
                    </span>
                  </div>
                  {visibleInviteEmails.map(email => (
                    <div key={email.id} className="team-manage-jobs__detail-item">
                      <div className="min-w-0">
                        <div className="font-mono text-xs c-heading">{email.email}</div>
                        <div className="mt-1 text-[.72rem] c-dim">
                          invite:{email.invite_status} · reg:{email.reg_status} · rt:{email.rt_status} · s2a:{email.s2a_status}
                        </div>
                        {email.error && <div className="mt-1 text-[.72rem] text-red-400">{email.error}</div>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
