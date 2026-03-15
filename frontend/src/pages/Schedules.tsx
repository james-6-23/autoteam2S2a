import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';
import { Badge, Button, Card, FieldLabel, Modal, RowItem, SectionTitle } from '../components/ui';
interface SchedItem { name: string; enabled: boolean; running?: boolean; cooldown?: boolean; pending?: boolean; start_time: string; end_time: string; target_count: number; batch_interval_mins: number; priority: number; distribution: { team: string; percent: number }[]; register_workers?: number; rt_workers?: number; rt_retries?: number; use_chatgpt_mail?: boolean; free_mode?: boolean; register_log_mode?: string; register_perf_mode?: string; push_s2a?: boolean; run_info?: { batch_num: number; next_batch_at?: string }; pending_info?: { blocked_by?: string; pending_since?: string } }
interface TeamItem { name: string; free_group_ids?: number[] }

export default function Schedules() {
  const { toast } = useToast();
  const [schedules, setSchedules] = useState<SchedItem[]>([]);
  const [teams, setTeams] = useState<TeamItem[]>([]);
  const [showAdd, setShowAdd] = useState(false);
  const [editName, setEditName] = useState<string | null>(null);
  const [form, setForm] = useState({ name: '', start: '08:00', end: '23:00', target: 4, interval: 30, priority: 100, regW: '', rtW: '', rtR: '', mail: 'false', mode: 'team', logMode: 'inherit', perfMode: 'inherit', enabled: true, pushS2a: true, dist: [{ team: '', percent: 100 }] as { team: string; percent: number }[] });

  const load = useCallback(async () => {
    try {
      const [sd, cd] = await Promise.all([api.fetchSchedules() as Promise<{ schedules: SchedItem[] }>, api.fetchConfig() as Promise<{ teams: TeamItem[] }>]);
      setSchedules(sd.schedules || []); setTeams(cd.teams || []);
    } catch {}
  }, []);

  useEffect(() => { load(); const id = setInterval(load, 5000); return () => clearInterval(id); }, [load]);

  const resetForm = () => setForm({ name: '', start: '08:00', end: '23:00', target: 4, interval: 30, priority: 100, regW: '', rtW: '', rtR: '', mail: 'false', mode: 'team', logMode: 'inherit', perfMode: 'inherit', enabled: true, pushS2a: true, dist: [{ team: teams[0]?.name || '', percent: 100 }] });

  const openAdd = () => { resetForm(); setShowAdd(true); setEditName(null); };
  const openEdit = (name: string) => {
    const s = schedules.find(x => x.name === name); if (!s) return;
    setForm({ name: s.name, start: s.start_time, end: s.end_time, target: s.target_count, interval: s.batch_interval_mins, priority: s.priority, regW: String(s.register_workers || ''), rtW: String(s.rt_workers || ''), rtR: String(s.rt_retries || ''), mail: s.use_chatgpt_mail ? 'true' : 'false', mode: s.free_mode ? 'free' : 'team', logMode: s.register_log_mode || 'inherit', perfMode: s.register_perf_mode || 'inherit', enabled: s.enabled, pushS2a: s.push_s2a !== false, dist: s.distribution.length ? s.distribution : [{ team: '', percent: 100 }] });
    setEditName(name); setShowAdd(true);
  };

  const submitForm = async () => {
    if (!form.name.trim()) { toast('名称不能为空', 'error'); return; }
    const tot = form.dist.reduce((s, d) => s + d.percent, 0);
    if (tot !== 100) { toast(`百分比总和必须为100，当前${tot}`, 'error'); return; }
    const body: Record<string, unknown> = { name: form.name.trim(), start_time: form.start, end_time: form.end, target_count: form.target, batch_interval_mins: form.interval, priority: form.priority, distribution: form.dist.filter(d => d.team && d.percent > 0), enabled: form.enabled, push_s2a: form.pushS2a, use_chatgpt_mail: form.mail === 'true', free_mode: form.mode === 'free', register_log_mode: form.logMode === 'inherit' ? null : form.logMode, register_perf_mode: form.perfMode === 'inherit' ? null : form.perfMode };
    if (form.regW) body.register_workers = Number(form.regW);
    if (form.rtW) body.rt_workers = Number(form.rtW);
    if (form.rtR) body.rt_retries = Number(form.rtR);
    try {
      if (editName) { await api.put(`/api/schedules/${encodeURIComponent(editName)}`, body); toast('已更新', 'success'); }
      else { await api.post('/api/schedules', body); toast(`${form.name} 已创建`, 'success'); }
      setShowAdd(false); setEditName(null); load();
    } catch (e) { toast(String(e), 'error'); }
  };

  const toggle = async (n: string) => { try { const d = await api.post<{ message: string }>(`/api/schedules/${encodeURIComponent(n)}/toggle`); toast(d.message, 'success'); load(); } catch {} };
  const trigger = async (n: string) => { if (!confirm(`手动启动 "${n}"?`)) return; try { const d = await api.post<{ message: string }>(`/api/schedules/${encodeURIComponent(n)}/trigger`); toast(d.message, 'success'); load(); } catch {} };
  const stop = async (n: string) => { if (!confirm(`停止 "${n}"?`)) return; try { const d = await api.post<{ message: string }>(`/api/schedules/${encodeURIComponent(n)}/stop`); toast(d.message, 'success'); load(); } catch {} };
  const del = async (n: string) => { if (!confirm(`删除 "${n}"?`)) return; try { await api.del(`/api/schedules/${encodeURIComponent(n)}`); toast(`${n} 已删除`, 'success'); load(); } catch {} };
  const runOnce = async (n: string) => {
    if (!confirm(`运行一次 "${n}"？`)) return;
    try { const d = await api.post<{ rt_ok: number; total_s2a_ok: number; elapsed_secs: number }>(`/api/schedules/${encodeURIComponent(n)}/run-once`); toast(`完成 | RT: ${d.rt_ok} | S2A: ${d.total_s2a_ok} | 耗时: ${d.elapsed_secs.toFixed(1)}s`, 'success'); load(); } catch {}
  };

  const setBadge = (s: SchedItem): ['run' | 'warn' | 'ok' | 'off', string] => {
    if (s.running) return ['run', '运行中'];
    if (s.cooldown) return ['warn', '准备中'];
    if (s.pending) return ['warn', '等待中'];
    return s.enabled ? ['ok', '已启用'] : ['off', '已禁用'];
  };

  return (
    <div className="space-y-4">
      <Card className="p-5">
        <div className="flex items-center justify-between mb-4">
          <SectionTitle className="mb-0">定时计划</SectionTitle>
          <Button onClick={openAdd} variant="teal" className="text-xs py-1.5">+ 新建</Button>
        </div>
        <div className="space-y-2">
          {schedules.length === 0 ? <p className="text-sm c-dim text-center py-8">暂无定时计划</p> :
            schedules.map(s => {
              const [bc, bt] = setBadge(s);
              const isActive = s.running || s.cooldown || s.pending;
              return (
                <RowItem key={s.name} style={{ padding: '10px 14px' }}>
                  <div className="flex items-center justify-between mb-1.5">
                    <div className="flex items-center gap-2.5">
                      <span className="text-sm font-medium c-heading">{s.name}</span>
                      <Badge variant={bc}>{bt}</Badge>
                      <code className="text-xs font-mono c-dim px-2 py-0.5 rounded" style={{ background: 'var(--ghost)' }}>{s.start_time} - {s.end_time}</code>
                    </div>
                    <div className="flex items-center gap-1">
                      {!isActive && <Button onClick={() => runOnce(s.name)} variant="ghost" className="text-xs py-1 px-2">运行一次</Button>}
                      {isActive ? <Button onClick={() => stop(s.name)} variant="danger" className="text-xs py-1 px-2">{s.pending ? '取消等待' : '停止'}</Button>
                        : <Button onClick={() => trigger(s.name)} variant="ghost" className="text-xs py-1 px-2">启动</Button>}
                      <Button onClick={() => openEdit(s.name)} variant="ghost" className="text-xs py-1 px-2">编辑</Button>
                      <Button onClick={() => toggle(s.name)} variant="ghost" className="text-xs py-1 px-2">{s.enabled ? '禁用' : '启用'}</Button>
                      <Button onClick={() => del(s.name)} variant="danger" className="text-xs py-1 px-2">删除</Button>
                    </div>
                  </div>
                  <div className="flex items-center gap-4 text-xs c-dim flex-wrap">
                    <span>每批RT目标 <span className="font-mono c-dim2">{s.target_count}</span></span>
                    <span>间隔 <span className="font-mono c-dim2">{s.batch_interval_mins}分</span></span>
                    <span>优先级 <span className="font-mono c-dim2">{s.priority}</span></span>
                    <span>邮箱 <span className="c-dim2">{s.use_chatgpt_mail ? 'chatgpt' : 'kyx'}</span></span>
                    <span>模式 <span className="c-dim2">{s.free_mode ? 'free' : 'team'}</span></span>
                    <span>分发 <span className="c-dim2 font-mono">{s.distribution.map(d => `${d.team}:${d.percent}%`).join(' / ') || '无'}</span></span>
                  </div>
                  {s.running && s.run_info && (
                    <div className="flex items-center gap-3 mt-1.5 text-xs">
                      <span className="text-amber-400 font-medium">{s.run_info.batch_num > 0 ? `第 ${s.run_info.batch_num} 批` : '执行中'}</span>
                      <span className="c-dim font-mono">{s.run_info.next_batch_at ? `下批 ${s.run_info.next_batch_at}` : '执行中...'}</span>
                    </div>
                  )}
                </RowItem>
              );
            })
          }
        </div>
      </Card>

      {showAdd && (
        <Modal open={showAdd} onClose={() => { setShowAdd(false); setEditName(null); }} className="p-5">
          <SectionTitle>{editName ? `编辑: ${editName}` : '新建定时计划'}</SectionTitle>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div><FieldLabel>名称 *</FieldLabel><input className="field-input" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} /></div>
            <div><FieldLabel>开始时间 *</FieldLabel><input type="time" className="field-input" value={form.start} onChange={e => setForm({ ...form, start: e.target.value })} /></div>
            <div><FieldLabel>结束时间 *</FieldLabel><input type="time" className="field-input" value={form.end} onChange={e => setForm({ ...form, end: e.target.value })} /></div>
            <div><FieldLabel>每批目标 *</FieldLabel><input type="number" className="field-input" value={form.target} onChange={e => setForm({ ...form, target: Number(e.target.value) })} /></div>
            <div><FieldLabel>间隔(分)</FieldLabel><input type="number" className="field-input" value={form.interval} onChange={e => setForm({ ...form, interval: Number(e.target.value) })} /></div>
            <div><FieldLabel>优先级</FieldLabel><input type="number" className="field-input" value={form.priority} onChange={e => setForm({ ...form, priority: Number(e.target.value) })} /></div>
            <div><FieldLabel>注册并发</FieldLabel><input type="number" className="field-input" placeholder="默认" value={form.regW} onChange={e => setForm({ ...form, regW: e.target.value })} /></div>
            <div><FieldLabel>RT 并发</FieldLabel><input type="number" className="field-input" placeholder="默认" value={form.rtW} onChange={e => setForm({ ...form, rtW: e.target.value })} /></div>
            <div><FieldLabel>邮箱</FieldLabel>
              <Select value={form.mail} onChange={v => setForm({ ...form, mail: v })} options={[{ label: 'kyx-cloud', value: 'false' }, { label: 'chatgpt', value: 'true' }]} />
            </div>
            <div><FieldLabel>模式</FieldLabel>
              <Select value={form.mode} onChange={v => setForm({ ...form, mode: v })} options={[{ label: 'Team', value: 'team' }, { label: 'Free', value: 'free' }]} />
            </div>
            <div><label className="switch mt-4"><input className="switch-input" type="checkbox" checked={form.enabled} onChange={e => setForm({ ...form, enabled: e.target.checked })} /><span className="switch-slider" /><span>启用</span></label></div>
            <div><label className="switch mt-4"><input className="switch-input" type="checkbox" checked={form.pushS2a} onChange={e => setForm({ ...form, pushS2a: e.target.checked })} /><span className="switch-slider" /><span>推送S2A</span></label></div>
          </div>
          <div className="mt-3">
            <div className="flex items-center justify-between mb-2"><FieldLabel className="mb-0">分发比例</FieldLabel><Button onClick={() => setForm({ ...form, dist: [...form.dist, { team: teams[0]?.name || '', percent: 0 }] })} variant="ghost" className="text-xs py-0.5 px-2">+ 行</Button></div>
            <div className="space-y-2">
              {form.dist.map((d, i) => (
                <div key={i} className="flex items-center gap-2">
                  <div className="flex-1">
                    <Select value={d.team} onChange={v => { const n = [...form.dist]; n[i] = { ...n[i], team: v }; setForm({ ...form, dist: n }); }} options={teams.map(t => ({ label: t.name, value: t.name }))} />
                  </div>
                  <input type="number" min={1} max={100} className="field-input w-20" placeholder="%" value={d.percent} onChange={e => { const n = [...form.dist]; n[i] = { ...n[i], percent: Number(e.target.value) }; setForm({ ...form, dist: n }); }} />
                  <Button onClick={() => setForm({ ...form, dist: form.dist.filter((_, j) => j !== i) })} variant="danger" className="text-xs py-1 px-2">&times;</Button>
                </div>
              ))}
            </div>
          </div>
          <div className="flex gap-2 mt-4">
            <Button onClick={submitForm} variant="amber">{editName ? '保存' : '创建'}</Button>
            <Button onClick={() => { setShowAdd(false); setEditName(null); }} variant="ghost">取消</Button>
          </div>
        </Modal>
      )}
    </div>
  );
}
