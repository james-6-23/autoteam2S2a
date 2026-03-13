import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';
interface ConfigData {
  teams: { name: string; api_base: string; admin_key: string; concurrency: number; priority: number; group_ids?: number[]; free_group_ids?: number[]; free_priority?: number; free_concurrency?: number; extra?: Record<string, unknown> }[];
  proxy_pool: string[];
  email_domains: string[];
  defaults: Record<string, unknown>;
  register: Record<string, unknown>;
}

export default function Dashboard() {
  const { toast } = useToast();
  const [config, setConfig] = useState<ConfigData | null>(null);
  const [runningCount, setRunningCount] = useState(0);
  const [team, setTeam] = useState('');
  const [target, setTarget] = useState(4);
  const [regWorkers, setRegWorkers] = useState(4);
  const [rtWorkers, setRtWorkers] = useState(4);
  const [mailSystem, setMailSystem] = useState('false');
  const [mode, setMode] = useState('team');
  const [creating, setCreating] = useState(false);

  const loadConfig = useCallback(async () => {
    try {
      const data = await api.fetchConfig() as ConfigData;
      setConfig(data);
      if (data.teams.length > 0 && !team) setTeam(data.teams[0].name);
      // Apply defaults
      const d = data.defaults as Record<string, unknown>;
      const modeData = (mode === 'free' ? (d?.free as Record<string, unknown>) : (d?.team as Record<string, unknown>)) || {};
      setTarget(Number(modeData.target_count ?? d?.target_count ?? 4));
      setRegWorkers(Number(modeData.register_workers ?? d?.register_workers ?? 4));
      setRtWorkers(Number(modeData.rt_workers ?? d?.rt_workers ?? 4));
    } catch {}
  }, [mode, team]);

  const loadRunning = useCallback(async () => {
    try {
      const [tasks, scheds] = await Promise.all([
        api.fetchTasks() as Promise<{ tasks: { status: string }[] }>,
        api.fetchSchedules().catch(() => ({ schedules: [] })) as Promise<{ schedules: { running?: boolean }[] }>,
      ]);
      const running = (tasks.tasks || []).filter(t => t.status === 'running' || t.status === 'pending').length;
      const runScheds = ((scheds.schedules || []) as { running?: boolean }[]).filter(s => s.running).length;
      setRunningCount(running + runScheds);
    } catch {}
  }, []);

  useEffect(() => { loadConfig(); loadRunning(); }, [loadConfig, loadRunning]);

  const handleCreate = async () => {
    if (!team) { toast('请先配置号池', 'error'); return; }
    setCreating(true);
    try {
      const d = await api.createTask({
        team, target, register_workers: regWorkers, rt_workers: rtWorkers,
        push_s2a: true, use_chatgpt_mail: mailSystem === 'true', free_mode: mode === 'free',
      }) as { task_id: string };
      toast(`任务 ${d.task_id} 已创建`, 'success');
    } catch (e) { toast(String(e), 'error'); }
    finally { setCreating(false); }
  };

  return (
    <div className="space-y-4">
      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="card p-4">
          <div className="field-label">S2A 号池</div>
          <div className="stat-num c-heading">{config?.teams.length ?? '--'}</div>
        </div>
        <div className="card p-4">
          <div className="field-label">可用代理</div>
          <div className="stat-num c-heading">{config?.proxy_pool.length ?? '--'}</div>
        </div>
        <div className="card p-4">
          <div className="field-label">邮箱域名</div>
          <div className="stat-num c-heading">{config?.email_domains.length ?? '--'}</div>
        </div>
        <div className="card p-4">
          <div className="field-label">活跃任务</div>
          <div className="stat-num text-amber-400">{runningCount}</div>
        </div>
      </div>

      {/* Quick Create */}
      <div className="card p-5">
        <div className="section-title">快速创建</div>
        <div className="grid grid-cols-2 md:grid-cols-7 gap-3">
          <div>
            <label className="field-label">号池</label>
            <Select
              value={team}
              onChange={setTeam}
              options={(config?.teams || []).map(t => ({ label: t.name, value: t.name }))}
            />
          </div>
          <div>
            <label className="field-label">目标数量</label>
            <input type="number" min={1} step={25} className="field-input" value={target} onChange={e => setTarget(Number(e.target.value))} />
          </div>
          <div>
            <label className="field-label">注册并发</label>
            <input type="number" min={1} max={512} step={25} className="field-input" value={regWorkers} onChange={e => setRegWorkers(Number(e.target.value))} />
          </div>
          <div>
            <label className="field-label">RT 并发</label>
            <input type="number" min={1} max={512} step={25} className="field-input" value={rtWorkers} onChange={e => setRtWorkers(Number(e.target.value))} />
          </div>
          <div>
            <label className="field-label">邮箱系统</label>
            <Select
              value={mailSystem}
              onChange={setMailSystem}
              options={[
                { label: 'kyx-cloud', value: 'false' },
                { label: 'chatgpt.org.uk', value: 'true' }
              ]}
            />
          </div>
          <div>
            <label className="field-label">模式</label>
            <Select
              value={mode}
              onChange={setMode}
              options={[
                { label: 'Team（支付）', value: 'team' },
                { label: 'Free（免费）', value: 'free' }
              ]}
            />
          </div>
          <div>
            <label className="field-label">&nbsp;</label>
            <button onClick={handleCreate} disabled={creating} className="btn btn-amber w-full justify-center h-[40px]">
              {creating ? '创建中...' : '执行'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
