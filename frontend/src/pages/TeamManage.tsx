import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { User, ChevronLeft, ChevronRight, Users, Trash2, X, Loader2, Zap, ShieldAlert, UserPlus } from 'lucide-react';

interface TeamOwner { email: string; account_id: string; access_token?: string; member_count?: number }
interface TeamMember { user_id: string; email?: string; name?: string; role: string; created_at?: string }
interface QuotaWindow { used_percent: number; remaining_percent: number; reset_after_seconds: number; window_minutes: number }
interface CodexQuota { five_hour?: QuotaWindow; seven_day?: QuotaWindow; status: string; model_used?: string; error?: string }
interface S2aTeam { name: string; api_base: string }

const PAGE_SIZE = 10;
const MAX_MEMBERS = 4;

function fmtReset(secs: number) {
  if (secs <= 0) return '--';
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (h > 24) { const d = Math.floor(h / 24); return `${d}天${h % 24}h`; }
  if (h > 0) return `${h}h${m}m`;
  return `${m}m`;
}

function quotaColor(remaining: number) {
  if (remaining >= 60) return '#2dd4bf';
  if (remaining >= 30) return '#f59e0b';
  return '#f87171';
}

function QuotaBar({ label, window }: { label: string; window: QuotaWindow }) {
  const pct = Math.round(window.remaining_percent);
  const color = quotaColor(pct);
  return (
    <div className="flex items-center gap-2 text-[.65rem]">
      <span className="c-dim w-5 shrink-0">{label}</span>
      <div className="flex-1 h-1.5 rounded-full overflow-hidden" style={{ background: 'var(--ghost)', minWidth: 40 }}>
        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="font-mono w-7 text-right shrink-0" style={{ color }}>{pct}%</span>
      <span className="c-dim w-12 text-right shrink-0">{fmtReset(window.reset_after_seconds)}</span>
    </div>
  );
}

function MemberQuotaInline({ quota, loading, onLoad }: { quota?: CodexQuota; loading?: boolean; onLoad: () => void }) {
  if (loading) return <div className="flex items-center gap-1 text-[.6rem] c-dim"><Loader2 size={10} className="animate-spin" />查询中</div>;
  if (!quota) return (
    <button onClick={e => { e.stopPropagation(); onLoad(); }} className="text-[.6rem] flex items-center gap-0.5 px-1.5 py-0.5 rounded transition-colors" style={{ color: 'var(--text-dim)', background: 'var(--ghost)' }} title="查额度">
      <Zap size={10} /> 额度
    </button>
  );
  if (quota.status === 'banned') return (
    <span className="flex items-center gap-0.5 text-[.6rem] font-medium px-1.5 py-0.5 rounded" style={{ background: 'rgba(248,113,113,.12)', color: '#f87171' }}>
      <ShieldAlert size={10} />封禁
    </span>
  );
  if (quota.status === 'error') return (
    <span className="text-[.6rem] c-dim" title={quota.error || ''}>错误</span>
  );
  return (
    <div className="flex flex-col gap-0.5 min-w-[120px]">
      {quota.five_hour && <QuotaBar label="5h" window={quota.five_hour} />}
      {quota.seven_day && <QuotaBar label="7d" window={quota.seven_day} />}
    </div>
  );
}

function QuotaBadge({ quota }: { quota?: CodexQuota }) {
  if (!quota) return <span className="text-[.6rem] c-dim">--</span>;
  if (quota.status === 'banned') return (
    <span className="flex items-center gap-1 text-[.6rem] font-medium px-1.5 py-0.5 rounded" style={{ background: 'rgba(248,113,113,.15)', color: '#f87171' }}>
      <ShieldAlert size={10} /> 封禁
    </span>
  );
  if (quota.status === 'error') return <span className="text-[.6rem] c-dim" title={quota.error || ''}>错误</span>;
  return (
    <div className="flex items-center gap-1.5">
      {quota.five_hour && (
        <span className="text-[.6rem] font-mono px-1.5 py-0.5 rounded" style={{ background: `${quotaColor(quota.five_hour.remaining_percent)}15`, color: quotaColor(quota.five_hour.remaining_percent), border: `1px solid ${quotaColor(quota.five_hour.remaining_percent)}30` }}>
          5h {Math.round(quota.five_hour.remaining_percent)}%
        </span>
      )}
      {quota.seven_day && (
        <span className="text-[.6rem] font-mono px-1.5 py-0.5 rounded" style={{ background: `${quotaColor(quota.seven_day.remaining_percent)}15`, color: quotaColor(quota.seven_day.remaining_percent), border: `1px solid ${quotaColor(quota.seven_day.remaining_percent)}30` }}>
          7d {Math.round(quota.seven_day.remaining_percent)}%
        </span>
      )}
    </div>
  );
}

function Pagination({ page, total, onChange }: { page: number; total: number; onChange: (p: number) => void }) {
  const totalPages = Math.ceil(total / PAGE_SIZE);
  if (totalPages <= 1) return null;
  return (
    <div className="flex items-center justify-between mt-3 px-1">
      <span className="text-xs c-dim">第 {page} / {totalPages} 页，共 {total} 条</span>
      <div className="flex items-center gap-1">
        <button onClick={() => onChange(page - 1)} disabled={page <= 1} className="btn btn-ghost p-1.5 disabled:opacity-30"><ChevronLeft size={14} /></button>
        {Array.from({ length: totalPages }, (_, i) => i + 1)
          .filter(p => p === 1 || p === totalPages || Math.abs(p - page) <= 1)
          .reduce<(number | '…')[]>((acc, p, idx, arr) => {
            if (idx > 0 && (p as number) - (arr[idx - 1] as number) > 1) acc.push('…');
            acc.push(p);
            return acc;
          }, [])
          .map((p, i) =>
            p === '…' ? (
              <span key={`ellipsis-${i}`} className="text-xs c-dim px-1">…</span>
            ) : (
              <button
                key={p}
                onClick={() => onChange(p as number)}
                className={`text-xs px-2.5 py-1 rounded-lg font-mono transition-all ${page === p ? 'text-white' : 'btn btn-ghost'}`}
                style={page === p ? { background: 'linear-gradient(135deg, rgba(20,184,166,0.85), rgba(139,92,246,0.85))' } : {}}
              >{p}</button>
            )
          )}
        <button onClick={() => onChange(page + 1)} disabled={page >= totalPages} className="btn btn-ghost p-1.5 disabled:opacity-30"><ChevronRight size={14} /></button>
      </div>
    </div>
  );
}

export default function TeamManage() {
  const { toast } = useToast();
  const [owners, setOwners] = useState<TeamOwner[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [members, setMembers] = useState<TeamMember[]>([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [kickLoading, setKickLoading] = useState<string | null>(null);
  const [kickAllLoading, setKickAllLoading] = useState(false);
  const [kickAllProgress, setKickAllProgress] = useState({ done: 0, total: 0 });
  const [memberCounts, setMemberCounts] = useState<Record<string, number>>({});
  const [ownerQuotas, setOwnerQuotas] = useState<Record<string, CodexQuota>>({});
  const [ownerQuotaLoading, setOwnerQuotaLoading] = useState<Record<string, boolean>>({});
  const [memberQuotas, setMemberQuotas] = useState<Record<string, CodexQuota>>({});
  const [memberQuotaLoading, setMemberQuotaLoading] = useState<Record<string, boolean>>({});

  const [s2aTeams, setS2aTeams] = useState<S2aTeam[]>([]);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [inviteLoading, setInviteLoading] = useState(false);
  const [selectedS2aTeam, setSelectedS2aTeam] = useState('');

  const [ownerPage, setOwnerPage] = useState(1);
  const [memberPage, setMemberPage] = useState(1);

  const pagedOwners = owners.slice((ownerPage - 1) * PAGE_SIZE, ownerPage * PAGE_SIZE);
  const pagedMembers = members.slice((memberPage - 1) * PAGE_SIZE, memberPage * PAGE_SIZE);

  const loadOwners = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.get<{ owners: TeamOwner[] }>('/api/team-manage/owners');
      setOwners(data.owners || []);
      setOwnerPage(1);
    } catch { setOwners([]); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadOwners(); }, [loadOwners]);

  useEffect(() => {
    (async () => {
      try {
        const data = await api.get<{ teams: S2aTeam[] }>('/api/config');
        setS2aTeams((data as any).teams || []);
      } catch {}
    })();
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') setSelected(null); };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, []);

  const loadMembers = async (accountId: string) => {
    setSelected(accountId); setMembers([]); setMembersLoading(true); setMemberPage(1);
    setMemberQuotas({}); setMemberQuotaLoading({});
    try {
      const data = await api.get<{ members: TeamMember[] }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/members`);
      const raw = data.members || [];
      const list = raw.filter(m => m.role !== 'account-owner');
      setMembers(list);
      setMemberCounts(prev => ({ ...prev, [accountId]: list.length }));
      // 自动查询所有成员额度
      setTimeout(() => {
        for (const m of list) {
          if (m.email) loadMemberQuota(m.email);
        }
      }, 100);
    } catch { toast('获取成员失败', 'error'); }
    finally { setMembersLoading(false); }
  };

  const loadOwnerQuota = async (accountId: string) => {
    setOwnerQuotaLoading(prev => ({ ...prev, [accountId]: true }));
    try {
      const data = await api.get<CodexQuota>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/quota`);
      setOwnerQuotas(prev => ({ ...prev, [accountId]: data }));
    } catch { toast('额度查询失败', 'error'); }
    finally { setOwnerQuotaLoading(prev => ({ ...prev, [accountId]: false })); }
  };

  const loadMemberQuota = async (email: string) => {
    setMemberQuotaLoading(prev => ({ ...prev, [email]: true }));
    try {
      const data = await api.post<CodexQuota>('/api/team-manage/member-quota', { email });
      setMemberQuotas(prev => ({ ...prev, [email]: data }));
    } catch { toast(`${email} 额度查询失败`, 'error'); }
    finally { setMemberQuotaLoading(prev => ({ ...prev, [email]: false })); }
  };

  const loadAllMemberQuotas = async () => {
    const toLoad = members.filter(m => m.email && !memberQuotas[m.email!] && !memberQuotaLoading[m.email!]);
    if (toLoad.length === 0) { toast('没有可查询的成员', 'error'); return; }
    toast(`开始查询 ${toLoad.length} 个成员的额度...`, 'success');
    for (const m of toLoad) {
      if (m.email) await loadMemberQuota(m.email);
    }
    toast('全部额度查询完成', 'success');
  };

  const kickMember = async (userId: string) => {
    if (!selected) return;
    if (!confirm('确定要踢除该成员？')) return;
    setKickLoading(userId);
    try {
      await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}/kick`);
      toast('已踢除成员', 'success');
      setMembers(prev => {
        const next = prev.filter(m => m.user_id !== userId);
        setMemberCounts(c => ({ ...c, [selected!]: next.length }));
        return next;
      });
    } catch (e) { toast(`踢除失败: ${e}`, 'error'); }
    finally { setKickLoading(null); }
  };

  const kickAll = async () => {
    if (!selected) return;
    const toKick = members.filter(m => m.role !== 'owner' && m.role !== 'account-owner');
    if (toKick.length === 0) { toast('没有可踢除的成员', 'error'); return; }
    if (!confirm(`确定要踢除全部 ${toKick.length} 个成员？`)) return;

    setKickAllLoading(true);
    setKickAllProgress({ done: 0, total: toKick.length });
    let success = 0;
    let failed = 0;

    for (const m of toKick) {
      try {
        await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(m.user_id)}/kick`);
        success++;
        setMembers(prev => prev.filter(x => x.user_id !== m.user_id));
      } catch { failed++; }
      setKickAllProgress({ done: success + failed, total: toKick.length });
    }

    setKickAllLoading(false);
    setMemberCounts(prev => ({ ...prev, [selected!]: members.length - success }));
    toast(`踢除完成: 成功 ${success}，失败 ${failed}`, failed > 0 ? 'error' : 'success');
  };

  const refreshMembers = async (accountId: string) => {
    try {
      const data = await api.post<{ members: TeamMember[] }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/refresh`);
      toast('已刷新成员列表', 'success');
      const list = data.members || [];
      setMemberCounts(prev => ({ ...prev, [accountId]: list.length }));
      if (selected === accountId) { setMembers(list); setMemberPage(1); }
    } catch (e) { toast(`刷新失败: ${e}`, 'error'); }
  };

  const inviteAndPush = async (s2aTeam: string) => {
    if (!selected) return;
    const availableSlots = MAX_MEMBERS - members.length;
    if (availableSlots <= 0) { toast('已满员，无法邀请', 'error'); return; }
    setInviteLoading(true);
    try {
      const res = await api.post<{ task_id: string; message: string }>(
        `/api/team-manage/owners/${encodeURIComponent(selected)}/invite`,
        { s2a_team: s2aTeam, invite_count: availableSlots }
      );
      toast(`${res.message} (任务ID: ${res.task_id})`, 'success');
      setShowInviteModal(false);
    } catch (e) { toast(`邀请失败: ${e}`, 'error'); }
    finally { setInviteLoading(false); }
  };

  const selectedOwner = owners.find(o => o.account_id === selected);
  const kickableCount = members.filter(m => m.role !== 'owner' && m.role !== 'account-owner').length;
  const availableSlots = Math.max(0, MAX_MEMBERS - members.length);
  const selectedOwnerQuota = selected ? ownerQuotas[selected] : undefined;

  return (
    <div className="space-y-4">
      <div className="card p-5">
        <div className="flex items-center justify-between mb-4">
          <div className="section-title mb-0">Team 管理</div>
          <button onClick={loadOwners} disabled={loading} className="btn btn-ghost text-xs py-1.5">
            {loading ? '加载中...' : '刷新'}
          </button>
        </div>
        <p className="text-xs c-dim mb-4">管理所有 Owner 下的 Team 成员。点击 Owner 查看成员列表，可以踢除成员。</p>

        {loading && owners.length === 0 ? (
          <div className="text-center c-dim py-8">加载中...</div>
        ) : owners.length === 0 ? (
          <div className="text-center c-dim py-8">
            <p className="text-sm">暂无 Owner 数据</p>
            <p className="text-xs mt-1">请先在邀请模块中上传 Owner JSON</p>
          </div>
        ) : (
          <>
            <div className="grid gap-2">
              {pagedOwners.map(o => {
                const count = memberCounts[o.account_id] ?? o.member_count;
                const q = ownerQuotas[o.account_id];
                const qLoading = ownerQuotaLoading[o.account_id];
                return (
                  <div
                    key={o.account_id}
                    className="row-item cursor-pointer transition-all duration-150 hover:scale-[1.005]"
                    onClick={() => loadMembers(o.account_id)}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="flex items-center gap-3 min-w-0">
                        <div className="w-8 h-8 rounded-lg flex items-center justify-center shrink-0" style={{ background: 'var(--ghost)', border: '1px solid var(--border)' }}>
                          <User size={16} className="c-dim" />
                        </div>
                        <div className="min-w-0">
                          <div className="text-sm c-heading font-medium truncate">{o.email || '未知邮箱'}</div>
                          <div className="text-xs font-mono c-dim">{o.account_id.substring(0, 12)}...</div>
                        </div>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        <QuotaBadge quota={q} />
                        {count != null && (
                          <span className="flex items-center gap-1 text-xs font-mono px-2 py-0.5 rounded-md" style={{ background: 'var(--ghost)', border: '1px solid var(--border)' }}>
                            <Users size={12} className="c-dim" />
                            <span className="c-heading">{count}</span>
                          </span>
                        )}
                        <button onClick={e => { e.stopPropagation(); loadOwnerQuota(o.account_id); }} disabled={qLoading} className="btn btn-ghost text-xs py-1 px-2" title="查额度">
                          {qLoading ? <Loader2 size={14} className="animate-spin" /> : <Zap size={14} />}
                        </button>
                        <button onClick={e => { e.stopPropagation(); refreshMembers(o.account_id); }} className="btn btn-ghost text-xs py-1 px-2" title="刷新成员">
                          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-3.5 h-3.5">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.992 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182" />
                          </svg>
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
            <Pagination page={ownerPage} total={owners.length} onChange={setOwnerPage} />
          </>
        )}
      </div>

      {/* ─── Members Modal ─── */}
      {selected && (
        <div className="team-modal" onClick={e => { if (e.target === e.currentTarget) setSelected(null); }}>
          <div className="team-modal-card p-5" style={{ maxWidth: 920, width: '96vw', maxHeight: '85vh', display: 'flex', flexDirection: 'column' }} onClick={e => e.stopPropagation()}>
            {/* Header */}
            <div className="flex items-center justify-between mb-3">
              <div className="min-w-0">
                <div className="section-title mb-0 truncate">
                  成员管理 <span className="text-amber-400">{selectedOwner?.email || selected.substring(0, 12)}</span>
                </div>
                <div className="text-xs font-mono c-dim mt-0.5">{selected}</div>
              </div>
              <div className="flex items-center gap-1.5 shrink-0">
                {!membersLoading && availableSlots > 0 && (
                  <button
                    onClick={() => setShowInviteModal(true)}
                    disabled={inviteLoading}
                    className="btn text-[.65rem] py-1 px-2 flex items-center gap-1"
                    style={{ background: 'linear-gradient(135deg, rgba(20,184,166,0.8), rgba(59,130,246,0.8))', color: '#fff' }}
                  >
                    {inviteLoading ? <Loader2 size={11} className="animate-spin" /> : <UserPlus size={11} />}
                    邀请并入库 ({availableSlots})
                  </button>
                )}
                {!membersLoading && members.length > 0 && (
                  <button onClick={loadAllMemberQuotas} className="btn btn-ghost text-[.65rem] py-1 px-2 flex items-center gap-1">
                    <Zap size={11} /> 刷新额度
                  </button>
                )}
                {!membersLoading && kickableCount > 0 && (
                  <button onClick={kickAll} disabled={kickAllLoading} className="btn btn-danger text-[.65rem] py-1 px-2 flex items-center gap-1">
                    {kickAllLoading ? (
                      <><Loader2 size={11} className="animate-spin" /> {kickAllProgress.done}/{kickAllProgress.total}</>
                    ) : (
                      <><Trash2 size={11} /> 全踢 ({kickableCount})</>
                    )}
                  </button>
                )}
                <button onClick={() => setSelected(null)} className="btn btn-ghost p-1" title="关闭"><X size={14} /></button>
              </div>
            </div>

            {/* Owner quota + stats bar */}
            <div className="flex items-center gap-4 mb-3 flex-wrap text-xs pb-3" style={{ borderBottom: '1px solid var(--border)' }}>
              {!membersLoading && (
                <>
                  <span className="c-dim">共 <span className="font-mono c-heading">{members.length}</span> 个成员</span>
                  <span className="c-dim">可踢除 <span className="font-mono text-red-400">{kickableCount}</span></span>
                  <span className="c-dim">可邀请 <span className="font-mono" style={{ color: availableSlots > 0 ? '#2dd4bf' : '#f87171' }}>{availableSlots}</span></span>
                </>
              )}
              <span className="w-px h-3" style={{ background: 'var(--border)' }} />
              <span className="c-dim">Owner 额度:</span>
              {selectedOwnerQuota ? (
                selectedOwnerQuota.status === 'banned' ? (
                  <span className="flex items-center gap-1 font-medium" style={{ color: '#f87171' }}><ShieldAlert size={12} /> 封禁</span>
                ) : selectedOwnerQuota.status === 'error' ? (
                  <span className="c-dim" title={selectedOwnerQuota.error || ''}>查询失败</span>
                ) : (
                  <>
                    {selectedOwnerQuota.five_hour && (
                      <span style={{ color: quotaColor(selectedOwnerQuota.five_hour.remaining_percent) }}>
                        5h <span className="font-mono font-medium">{Math.round(selectedOwnerQuota.five_hour.remaining_percent)}%</span>
                        <span className="c-dim ml-1">({fmtReset(selectedOwnerQuota.five_hour.reset_after_seconds)})</span>
                      </span>
                    )}
                    {selectedOwnerQuota.seven_day && (
                      <span style={{ color: quotaColor(selectedOwnerQuota.seven_day.remaining_percent) }}>
                        7d <span className="font-mono font-medium">{Math.round(selectedOwnerQuota.seven_day.remaining_percent)}%</span>
                        <span className="c-dim ml-1">({fmtReset(selectedOwnerQuota.seven_day.reset_after_seconds)})</span>
                      </span>
                    )}
                  </>
                )
              ) : (
                <button onClick={() => loadOwnerQuota(selected)} disabled={ownerQuotaLoading[selected] || false} className="btn btn-ghost text-[.65rem] py-0.5 px-2 flex items-center gap-1">
                  {ownerQuotaLoading[selected] ? <Loader2 size={11} className="animate-spin" /> : <Zap size={11} />} 查询
                </button>
              )}
            </div>

            {/* Members list */}
            <div className="flex-1 overflow-y-auto" style={{ minHeight: 0 }}>
              {membersLoading ? (
                <div className="flex items-center justify-center gap-2 c-dim py-12">
                  <Loader2 size={16} className="animate-spin" /> 加载中...
                </div>
              ) : members.length === 0 ? (
                <div className="text-center c-dim py-12">无成员数据</div>
              ) : (
                <div className="space-y-1.5">
                  {pagedMembers.map(m => {
                    const mq = m.email ? memberQuotas[m.email] : undefined;
                    const mqLoading = m.email ? memberQuotaLoading[m.email] : false;
                    return (
                      <div key={m.user_id} className="card-inner px-4 py-3 flex items-center gap-3">
                        {/* 左侧: 用户信息 */}
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-sm c-heading font-medium truncate">{m.name || m.email || '未知'}</span>
                            <span className={`px-1.5 py-0.5 rounded text-[.6rem] font-medium shrink-0 ${m.role === 'owner' ? 'text-amber-400' : 'c-dim'}`} style={{ background: 'var(--ghost)' }}>
                              {m.role}
                            </span>
                          </div>
                          <div className="flex items-center gap-2 text-[.65rem] c-dim mt-0.5">
                            {m.email && <span className="font-mono truncate">{m.email}</span>}
                            {m.created_at && <span className="shrink-0">{new Date(m.created_at).toLocaleDateString('zh-CN')}</span>}
                          </div>
                        </div>

                        {/* 中间: 额度 */}
                        <div className="shrink-0">
                          <MemberQuotaInline
                            quota={mq}
                            loading={mqLoading}
                            onLoad={() => m.email && loadMemberQuota(m.email)}
                          />
                        </div>

                        {/* 右侧: 操作 */}
                        <div className="shrink-0">
                          {m.role !== 'owner' && m.role !== 'account-owner' && (
                            <button
                              onClick={() => kickMember(m.user_id)}
                              disabled={kickLoading === m.user_id || kickAllLoading}
                              className="btn btn-danger text-xs py-1 px-2.5"
                            >
                              {kickLoading === m.user_id ? '...' : '踢除'}
                            </button>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            {members.length > PAGE_SIZE && (
              <div className="pt-2 border-t mt-2" style={{ borderColor: 'var(--border)' }}>
                <Pagination page={memberPage} total={members.length} onChange={setMemberPage} />
              </div>
            )}

            {/* ─── Invite S2A Pool Selector ─── */}
            {showInviteModal && (
              <div className="absolute inset-0 flex items-center justify-center rounded-2xl" style={{ background: 'rgba(0,0,0,.6)', zIndex: 50 }} onClick={() => !inviteLoading && setShowInviteModal(false)}>
                <div className="p-4 rounded-xl" style={{ background: 'var(--card)', border: '1px solid var(--border)', minWidth: 320, maxWidth: 420 }} onClick={e => e.stopPropagation()}>
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-sm c-heading font-medium">选择号池</span>
                    <button onClick={() => setShowInviteModal(false)} disabled={inviteLoading} className="btn btn-ghost p-1"><X size={14} /></button>
                  </div>
                  <p className="text-xs c-dim mb-3">将为该 Owner 邀请 <span className="font-mono text-teal-400">{availableSlots}</span> 个成员并入库到选定号池</p>
                  <div className="space-y-1.5 max-h-48 overflow-y-auto mb-3">
                    {s2aTeams.length === 0 ? (
                      <div className="text-center c-dim text-xs py-4">暂无号池配置</div>
                    ) : s2aTeams.map(t => (
                      <div
                        key={t.name}
                        className="px-3 py-2 rounded-lg cursor-pointer transition-all"
                        style={{
                          background: selectedS2aTeam === t.name ? 'rgba(20,184,166,.15)' : 'var(--ghost)',
                          border: `1px solid ${selectedS2aTeam === t.name ? 'rgba(20,184,166,.5)' : 'var(--border)'}`,
                        }}
                        onClick={() => setSelectedS2aTeam(t.name)}
                      >
                        <div className="text-sm c-heading font-medium">{t.name}</div>
                        <div className="text-[.6rem] font-mono c-dim truncate">{t.api_base}</div>
                      </div>
                    ))}
                  </div>
                  <button
                    onClick={() => inviteAndPush(selectedS2aTeam)}
                    disabled={!selectedS2aTeam || inviteLoading}
                    className="w-full py-2 rounded-lg text-sm font-medium transition-all disabled:opacity-40"
                    style={{ background: 'linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))', color: '#fff' }}
                  >
                    {inviteLoading ? (
                      <span className="flex items-center justify-center gap-1.5"><Loader2 size={14} className="animate-spin" /> 邀请中...</span>
                    ) : (
                      `确认邀请 ${availableSlots} 个到 ${selectedS2aTeam || '...'}`
                    )}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
