import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';

interface TeamData {
  name: string; api_base: string; admin_key: string; concurrency: number; priority: number;
  group_ids?: number[]; free_group_ids?: number[]; free_priority?: number; free_concurrency?: number;
  extra?: Record<string, unknown>;
}
interface GroupInfo { id: number; name: string; account_count: number; status: string; }
interface TeamStats { available: number; active: number; rate_limited: number; free_available?: number; free_active?: number; free_rate_limited?: number; }

export default function Teams() {
  const { toast } = useToast();
  const [teams, setTeams] = useState<TeamData[]>([]);
  const [stats, setStats] = useState<Record<string, TeamStats>>({});
  const [addModal, setAddModal] = useState(false);
  const [editModal, setEditModal] = useState<string | null>(null);
  const [groupsModal, setGroupsModal] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState<Record<string, boolean>>({});

  // Add form
  const [atName, setAtName] = useState(''); const [atApi, setAtApi] = useState(''); const [atKey, setAtKey] = useState('');
  const [atConc, setAtConc] = useState(50); const [atPri, setAtPri] = useState(30);
  const [atFreePri, setAtFreePri] = useState(''); const [atFreeConc, setAtFreeConc] = useState('');
  const [atGroups, setAtGroups] = useState<GroupInfo[]>([]); const [atChecked, setAtChecked] = useState<number[]>([]);
  const [atFreeChecked, setAtFreeChecked] = useState<number[]>([]);
  const [atPassthrough, setAtPassthrough] = useState(true); const [atWsV2, setAtWsV2] = useState(true);
  const [fetchingGroups, setFetchingGroups] = useState(false);

  // Edit form
  const [etApi, setEtApi] = useState(''); const [etKey, setEtKey] = useState('');
  const [etConc, setEtConc] = useState(50); const [etPri, setEtPri] = useState(30);
  const [etFreePri, setEtFreePri] = useState(''); const [etFreeConc, setEtFreeConc] = useState('');
  const [etGroups, setEtGroups] = useState<GroupInfo[]>([]); const [etChecked, setEtChecked] = useState<number[]>([]);
  const [etFreeChecked, setEtFreeChecked] = useState<number[]>([]);
  const [etPassthrough, setEtPassthrough] = useState(true); const [etWsV2, setEtWsV2] = useState(true);

  // Groups modal data
  const [modalGroups, setModalGroups] = useState<GroupInfo[]>([]);
  const [modalTeam, setModalTeam] = useState<TeamData | null>(null);

  const loadTeams = useCallback(async () => {
    try {
      const data = await api.fetchConfig() as { teams: TeamData[] };
      setTeams(data.teams || []);
      // Load stats for each team
      for (const t of (data.teams || [])) {
        loadTeamStats(t.name);
      }
    } catch {}
  }, []);

  const loadTeamStats = async (name: string) => {
    setRefreshing(prev => ({ ...prev, [name]: true }));
    try {
      const s = await api.get<TeamStats>(`/api/config/s2a/${encodeURIComponent(name)}/stats`);
      setStats(prev => ({ ...prev, [name]: s }));
    } catch {
      setStats(prev => ({ ...prev, [name]: { available: -1, active: -1, rate_limited: -1 } }));
    } finally {
      setTimeout(() => setRefreshing(prev => ({ ...prev, [name]: false })), 400);
    }
  };

  useEffect(() => { loadTeams(); }, [loadTeams]);

  // Escape handler
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { setAddModal(false); setEditModal(null); setGroupsModal(null); }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, []);

  const doFetchGroups = async (apiBase: string, adminKey: string, setter: (g: GroupInfo[]) => void) => {
    if (!apiBase || !adminKey) { toast('请先填写 API 地址和 Admin Key', 'error'); return; }
    setFetchingGroups(true);
    try {
      const res = await api.post<GroupInfo[]>('/api/s2a/fetch-groups', { api_base: apiBase, admin_key: adminKey });
      setter(Array.isArray(res) ? res : []);
      toast(`共 ${(res as GroupInfo[]).length} 个分组`, 'success');
    } catch (e) { toast(String(e), 'error'); }
    finally { setFetchingGroups(false); }
  };

  const handleAdd = async () => {
    const extra: Record<string, unknown> = {};
    if (atPassthrough) extra.openai_passthrough = true;
    if (atWsV2) { extra.openai_oauth_responses_websockets_v2_enabled = true; extra.openai_oauth_responses_websockets_v2_mode = 'passthrough'; }
    try {
      await api.post('/api/config/s2a', {
        name: atName.trim(), api_base: atApi.trim(), admin_key: atKey.trim(),
        concurrency: atConc, priority: atPri, group_ids: atChecked, free_group_ids: atFreeChecked,
        free_priority: atFreePri ? Number(atFreePri) : undefined,
        free_concurrency: atFreeConc ? Number(atFreeConc) : undefined, extra,
      });
      toast('号池已添加', 'success'); setAddModal(false);
      setAtName(''); setAtApi(''); setAtKey(''); setAtGroups([]); setAtChecked([]); setAtFreeChecked([]);
      loadTeams();
    } catch (e) { toast(String(e), 'error'); }
  };

  const handleEdit = async () => {
    if (!editModal) return;
    const extra: Record<string, unknown> = {};
    if (etPassthrough) extra.openai_passthrough = true;
    if (etWsV2) { extra.openai_oauth_responses_websockets_v2_enabled = true; extra.openai_oauth_responses_websockets_v2_mode = 'passthrough'; }
    const body: Record<string, unknown> = {
      api_base: etApi.trim() || undefined, admin_key: etKey.trim() || undefined,
      concurrency: etConc || undefined, priority: etPri || undefined,
      free_priority: etFreePri ? Number(etFreePri) : null,
      free_concurrency: etFreeConc ? Number(etFreeConc) : null, extra,
    };
    if (etGroups.length > 0) { body.group_ids = etChecked; body.free_group_ids = etFreeChecked; }
    try {
      await api.put(`/api/config/s2a/${encodeURIComponent(editModal)}`, body);
      toast(`${editModal} 已更新`, 'success'); setEditModal(null); loadTeams();
    } catch (e) { toast(String(e), 'error'); }
  };

  const handleDelete = async (name: string) => {
    if (!confirm(`删除号池 "${name}"?`)) return;
    try { await api.del(`/api/config/s2a/${encodeURIComponent(name)}`); toast(`${name} 已删除`, 'success'); loadTeams(); }
    catch (e) { toast(String(e), 'error'); }
  };

  const handleTest = async (name: string) => {
    try { const d = await api.post<{ message: string }>(`/api/config/s2a/${encodeURIComponent(name)}/test`, {}); toast(`${name}: ${d.message}`, 'success'); }
    catch { toast(`${name}: 连接失败`, 'error'); }
  };

  const openEdit = (name: string) => {
    const t = teams.find(x => x.name === name);
    if (!t) return;
    setEditModal(name); setEtApi(t.api_base || ''); setEtKey(t.admin_key || '');
    setEtConc(t.concurrency || 50); setEtPri(t.priority || 30);
    setEtFreePri(String(t.free_priority || '')); setEtFreeConc(String(t.free_concurrency || ''));
    setEtChecked(t.group_ids || []); setEtFreeChecked(t.free_group_ids || []);
    setEtPassthrough(t.extra?.openai_passthrough === true);
    setEtWsV2((t.extra as Record<string, unknown>)?.openai_oauth_responses_websockets_v2_enabled === true);
    setEtGroups([]);
    if (t.api_base && t.admin_key) doFetchGroups(t.api_base, t.admin_key, setEtGroups);
  };

  const openGroupsModal = async (name: string) => {
    const t = teams.find(x => x.name === name);
    if (!t) return;
    setGroupsModal(name); setModalTeam(t); setModalGroups([]);
    try {
      const groups = await api.post<GroupInfo[]>('/api/s2a/fetch-groups', { api_base: t.api_base, admin_key: t.admin_key });
      setModalGroups(Array.isArray(groups) ? groups : []);
    } catch {}
  };

  const toggleCheck = (list: number[], id: number, setter: (v: number[]) => void) => {
    setter(list.includes(id) ? list.filter(x => x !== id) : [...list, id]);
  };

  const renderGroupCheckboxes = (groups: GroupInfo[], checked: number[], setter: (v: number[]) => void, color: string) => (
    groups.length === 0 ? <span className="text-xs c-dim">点击获取分组</span> :
    <div className="flex flex-wrap gap-1.5">
      {groups.map(g => {
        const isChecked = checked.includes(g.id);
        return (
          <label
            key={g.id}
            className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg cursor-pointer text-xs transition-all duration-150 select-none"
            style={{
              background: isChecked ? `${color}18` : 'var(--ghost)',
              border: `1px solid ${isChecked ? `${color}50` : 'var(--border)'}`,
              color: isChecked ? color : 'var(--text-dim)',
            }}
          >
            <input
              type="checkbox" checked={isChecked}
              onChange={() => toggleCheck(checked, g.id, setter)}
              className="hidden"
            />
            <span className="font-medium" style={{ color: isChecked ? color : 'var(--text-heading)' }}>{g.name}</span>
            <span className="font-mono opacity-50">#{g.id}</span>
            <span className="opacity-60">{g.account_count}号</span>
            {g.status === 'active' && (
              <span className="text-[.55rem] px-1 py-px rounded" style={{ background: 'rgba(45,212,191,.15)', color: '#2dd4bf' }}>active</span>
            )}
          </label>
        );
      })}
    </div>
  );

  const statDisplay = (v: number | undefined) => v === undefined ? '-' : v === -1 ? '?' : String(v);

  return (
    <div className="space-y-4">
      <div className="card p-5">
        <div className="flex items-center justify-between mb-4">
          <div className="section-title mb-0">S2A 号池</div>
          <button onClick={() => setAddModal(true)} className="btn btn-teal text-xs py-1.5">+ 添加</button>
        </div>

        {teams.length === 0 ? (
          <p className="text-sm c-dim text-center py-6">暂无号池</p>
        ) : (
          <div className="team-grid">
            {teams.map(t => {
              const s = stats[t.name];
              return (
                <div key={t.name} className="row-item">
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex-1 grid grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6 gap-2 text-[.8125rem]">
                      <div><span className="field-label">名称</span><div className="font-medium c-heading">{t.name}</div></div>
                      <div><span className="field-label">API</span><div className="font-mono text-xs c-dim2 truncate max-w-[200px]">{t.api_base}</div></div>
                      <div><span className="field-label">并发</span><div className="font-mono c-heading">{t.concurrency}</div></div>
                      <div><span className="field-label">优先级</span><div className="font-mono c-heading">{t.priority}</div></div>
                      <div><span className="field-label">分组</span><div className="font-mono c-heading">{t.group_ids?.length ? `已配 ${t.group_ids.length} 组` : '未配置'}</div></div>
                      <div><span className="field-label">Free 分组</span><div className={`font-mono ${t.free_group_ids?.length ? 'text-amber-400' : 'c-dim'}`}>{t.free_group_ids?.length ? `已配 ${t.free_group_ids.length} 组` : '未配置'}</div></div>
                    </div>
                    <div className="flex items-center gap-1 ml-3 shrink-0">
                      <button onClick={() => openGroupsModal(t.name)} className="btn btn-ghost text-xs py-1 px-2">分组详情</button>
                      <button onClick={() => handleTest(t.name)} className="btn btn-ghost text-xs py-1 px-2">测试</button>
                      <button onClick={() => openEdit(t.name)} className="btn btn-ghost text-xs py-1 px-2">编辑</button>
                      <button onClick={() => handleDelete(t.name)} className="btn btn-danger text-xs py-1 px-2">删除</button>
                    </div>
                  </div>
                  {/* Stats */}
                  <div className="flex flex-col gap-1 text-xs c-dim pt-1 border-t mt-2" style={{ borderColor: 'var(--border)' }}>
                    <div className="flex items-center gap-4">
                      <span className="c-dim">Team 统计</span>
                      <span>可用 <span className="font-mono text-teal-400">{statDisplay(s?.available)}</span></span>
                      <span>活跃 <span className="font-mono c-dim2">{statDisplay(s?.active)}</span></span>
                      <span>限流 <span className="font-mono text-red-400">{statDisplay(s?.rate_limited)}</span></span>
                      <button onClick={() => loadTeamStats(t.name)} className={`refresh-btn ml-auto${refreshing[t.name] ? ' spinning' : ''}`} disabled={!!refreshing[t.name]} title="刷新">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.992 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182" />
                        </svg>
                      </button>
                    </div>
                    {(t.free_group_ids?.length ?? 0) > 0 && (
                      <div className="flex items-center gap-4">
                        <span className="text-amber-400">Free 统计</span>
                        <span>可用 <span className="font-mono text-amber-400">{statDisplay(s?.free_available)}</span></span>
                        <span>活跃 <span className="font-mono c-dim2">{statDisplay(s?.free_active)}</span></span>
                        <span>限流 <span className="font-mono text-red-400">{statDisplay(s?.free_rate_limited)}</span></span>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ─── Add Modal ─── */}
      {addModal && (
        <div className="team-modal" onClick={e => { if (e.target === e.currentTarget) setAddModal(false); }}>
          <div className="team-modal-card p-5" onClick={e => e.stopPropagation()}>
            <div className="section-title">新建号池</div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div><label className="field-label">名称 *</label><input className="field-input" placeholder="agmn" value={atName} onChange={e => setAtName(e.target.value)} /></div>
              <div><label className="field-label">API 地址 *</label><input className="field-input" placeholder="https://..." value={atApi} onChange={e => setAtApi(e.target.value)} /></div>
              <div><label className="field-label">Admin Key *</label><input className="field-input" placeholder="admin-xxx" value={atKey} onChange={e => setAtKey(e.target.value)} /></div>
              <div><label className="field-label">并发</label><input type="number" min={1} step={25} className="field-input" value={atConc} onChange={e => setAtConc(Number(e.target.value))} /></div>
              <div><label className="field-label">优先级</label><input type="number" className="field-input" value={atPri} onChange={e => setAtPri(Number(e.target.value))} /></div>
              <div className="md:col-span-3 grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="card-inner p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <label className="field-label mb-0">分组</label>
                    <button onClick={() => doFetchGroups(atApi, atKey, setAtGroups)} disabled={fetchingGroups} className="text-[.65rem] px-2 py-0.5 rounded" style={{ background: 'var(--ghost)', color: 'var(--text-dim)', cursor: 'pointer' }}>
                      {fetchingGroups ? '获取中...' : '获取分组'}
                    </button>
                    {atChecked.length > 0 && <span className="text-[.6rem] font-mono ml-auto" style={{ color: '#14b8a6' }}>已选 {atChecked.length}</span>}
                  </div>
                  <div style={{ maxHeight: 180, overflowY: 'auto' }}>
                    {renderGroupCheckboxes(atGroups, atChecked, setAtChecked, '#14b8a6')}
                  </div>
                </div>
                <div className="card-inner p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <label className="field-label mb-0">Free 分组 <span className="c-dim">(可选)</span></label>
                    {atFreeChecked.length > 0 && <span className="text-[.6rem] font-mono ml-auto" style={{ color: '#f59e0b' }}>已选 {atFreeChecked.length}</span>}
                  </div>
                  <div style={{ maxHeight: 180, overflowY: 'auto' }}>
                    {atGroups.length ? renderGroupCheckboxes(atGroups, atFreeChecked, setAtFreeChecked, '#f59e0b') : <span className="text-xs c-dim">获取分组后可选</span>}
                  </div>
                </div>
              </div>
              <div><label className="field-label">Free 优先级 <span className="c-dim">(可选)</span></label><input type="number" className="field-input" placeholder="默认同 team" value={atFreePri} onChange={e => setAtFreePri(e.target.value)} /></div>
              <div><label className="field-label">Free 并发 <span className="c-dim">(可选)</span></label><input type="number" min={1} step={25} className="field-input" placeholder="默认同 team" value={atFreeConc} onChange={e => setAtFreeConc(e.target.value)} /></div>
              <div className="card-inner p-3 space-y-3">
                <div className="field-label mb-0">OpenAI Extra</div>
                <div className="flex flex-col gap-2.5">
                  <label className="switch"><input className="switch-input" type="checkbox" checked={atPassthrough} onChange={e => setAtPassthrough(e.target.checked)} /><span className="switch-slider" /><span className="switch-text">自动透传</span></label>
                  <label className="switch"><input className="switch-input" type="checkbox" checked={atWsV2} onChange={e => setAtWsV2(e.target.checked)} /><span className="switch-slider" /><span className="switch-text">启用 WS v2</span></label>
                </div>
              </div>
            </div>
            <div className="flex gap-2 mt-4">
              <button onClick={handleAdd} className="btn btn-teal">确认</button>
              <button onClick={() => setAddModal(false)} className="btn btn-ghost">取消</button>
            </div>
          </div>
        </div>
      )}

      {/* ─── Edit Modal ─── */}
      {editModal && (
        <div className="team-modal" onClick={e => { if (e.target === e.currentTarget) setEditModal(null); }}>
          <div className="team-modal-card p-5" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-3">
              <div className="section-title mb-0">编辑号池: <span className="text-amber-400">{editModal}</span></div>
              <button onClick={() => setEditModal(null)} className="btn btn-ghost text-xs py-1">关闭</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div><label className="field-label">API 地址</label><input className="field-input" value={etApi} onChange={e => setEtApi(e.target.value)} /></div>
              <div><label className="field-label">Admin Key</label><input className="field-input" value={etKey} onChange={e => setEtKey(e.target.value)} /></div>
              <div><label className="field-label">并发</label><input type="number" min={1} step={25} className="field-input" value={etConc} onChange={e => setEtConc(Number(e.target.value))} /></div>
              <div><label className="field-label">优先级</label><input type="number" className="field-input" value={etPri} onChange={e => setEtPri(Number(e.target.value))} /></div>
              <div className="md:col-span-3 grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="card-inner p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <label className="field-label mb-0">分组</label>
                    <button onClick={() => doFetchGroups(etApi, etKey, setEtGroups)} disabled={fetchingGroups} className="text-[.65rem] px-2 py-0.5 rounded" style={{ background: 'var(--ghost)', color: 'var(--text-dim)', cursor: 'pointer' }}>
                      {fetchingGroups ? '获取中...' : '获取分组'}
                    </button>
                    {etChecked.length > 0 && <span className="text-[.6rem] font-mono ml-auto" style={{ color: '#14b8a6' }}>已选 {etChecked.length}</span>}
                  </div>
                  <div style={{ maxHeight: 180, overflowY: 'auto' }}>
                    {renderGroupCheckboxes(etGroups, etChecked, setEtChecked, '#14b8a6')}
                  </div>
                </div>
                <div className="card-inner p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <label className="field-label mb-0">Free 分组 <span className="c-dim">(可选)</span></label>
                    {etFreeChecked.length > 0 && <span className="text-[.6rem] font-mono ml-auto" style={{ color: '#f59e0b' }}>已选 {etFreeChecked.length}</span>}
                  </div>
                  <div style={{ maxHeight: 180, overflowY: 'auto' }}>
                    {etGroups.length ? renderGroupCheckboxes(etGroups, etFreeChecked, setEtFreeChecked, '#f59e0b') : <span className="text-xs c-dim">获取分组后可选</span>}
                  </div>
                </div>
              </div>
              <div><label className="field-label">Free 优先级 <span className="c-dim">(可选)</span></label><input type="number" className="field-input" placeholder="默认同 team" value={etFreePri} onChange={e => setEtFreePri(e.target.value)} /></div>
              <div><label className="field-label">Free 并发 <span className="c-dim">(可选)</span></label><input type="number" min={1} step={25} className="field-input" placeholder="默认同 team" value={etFreeConc} onChange={e => setEtFreeConc(e.target.value)} /></div>
              <div className="card-inner p-3 space-y-3">
                <div className="field-label mb-0">OpenAI Extra</div>
                <div className="flex flex-col gap-2.5">
                  <label className="switch"><input className="switch-input" type="checkbox" checked={etPassthrough} onChange={e => setEtPassthrough(e.target.checked)} /><span className="switch-slider" /><span className="switch-text">自动透传</span></label>
                  <label className="switch"><input className="switch-input" type="checkbox" checked={etWsV2} onChange={e => setEtWsV2(e.target.checked)} /><span className="switch-slider" /><span className="switch-text">启用 WS v2</span></label>
                </div>
              </div>
            </div>
            <div className="flex gap-2 mt-4">
              <button onClick={handleEdit} className="btn btn-amber">保存</button>
              <button onClick={() => setEditModal(null)} className="btn btn-ghost">取消</button>
            </div>
          </div>
        </div>
      )}

      {/* ─── Groups Detail Modal ─── */}
      {groupsModal && modalTeam && (
        <div className="team-modal" onClick={e => { if (e.target === e.currentTarget) setGroupsModal(null); }}>
          <div className="team-modal-card p-5" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-3">
              <div className="section-title mb-0">分组详情: <span className="text-amber-400">{groupsModal}</span></div>
              <button onClick={() => setGroupsModal(null)} className="btn btn-ghost text-xs py-1">关闭</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="card-inner p-3">
                <div className="field-label mb-2">Team 分组</div>
                <div className="space-y-2" style={{ maxHeight: '58vh', overflow: 'auto' }}>
                  {(modalTeam.group_ids || []).length === 0 ? <div className="text-xs c-dim">未配置 Team 分组</div> :
                    (modalTeam.group_ids || []).map(id => {
                      const g = modalGroups.find(x => x.id === id);
                      return (
                        <div key={id} className="card-inner px-3 py-2.5 flex items-center justify-between gap-2 text-xs">
                          <div className="min-w-0">
                            <div className="c-heading font-medium truncate">{g?.name || '未命名分组'}</div>
                            <div className="c-dim font-mono">#{id}</div>
                          </div>
                          <div className="text-right shrink-0">
                            <div className="c-dim">{g ? `${g.account_count} 账号` : '账号数未知'}</div>
                            <div className={g?.status === 'active' ? 'text-teal-400' : 'c-dim'}>{g?.status || 'unknown'}</div>
                          </div>
                        </div>
                      );
                    })}
                </div>
              </div>
              <div className="card-inner p-3">
                <div className="field-label mb-2">Free 分组</div>
                <div className="space-y-2" style={{ maxHeight: '58vh', overflow: 'auto' }}>
                  {(modalTeam.free_group_ids || []).length === 0 ? <div className="text-xs c-dim">未配置 Free 分组</div> :
                    (modalTeam.free_group_ids || []).map(id => {
                      const g = modalGroups.find(x => x.id === id);
                      return (
                        <div key={id} className="card-inner px-3 py-2.5 flex items-center justify-between gap-2 text-xs">
                          <div className="min-w-0">
                            <div className="c-heading font-medium truncate">{g?.name || '未命名分组'}</div>
                            <div className="c-dim font-mono">#{id}</div>
                          </div>
                          <div className="text-right shrink-0">
                            <div className="c-dim">{g ? `${g.account_count} 账号` : '账号数未知'}</div>
                            <div className={g?.status === 'active' ? 'text-teal-400' : 'c-dim'}>{g?.status || 'unknown'}</div>
                          </div>
                        </div>
                      );
                    })}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
