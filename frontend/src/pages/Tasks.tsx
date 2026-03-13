import { useEffect, useState, useCallback, useRef } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';

interface TaskItem { task_id: string; team: string; target: number; status: string; created_at: string; stage?: string; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number; error?: string; report?: Record<string, unknown> }
interface Progress { status: string; stage?: string; target: number; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number }

export default function Tasks() {
  const { toast } = useToast();
  const [tasks, setTasks] = useState<TaskItem[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [report, setReport] = useState<Record<string, unknown> | null>(null);
  const [elapsed, setElapsed] = useState(0);
  const startRef = useRef(0);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    try { const d = await api.fetchTasks() as { tasks: TaskItem[] }; setTasks(d.tasks || []); } catch {}
  }, []);

  useEffect(() => { load(); const id = setInterval(load, 3000); return () => clearInterval(id); }, [load]);

  const cancel = async (id: string) => {
    if (!confirm('取消此任务?')) return;
    try { await api.post(`/api/tasks/${id}/cancel`); toast('取消请求已发送', 'success'); load(); } catch {}
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
    </div>
  );
}
