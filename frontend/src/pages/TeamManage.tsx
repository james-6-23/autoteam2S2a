import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { User, ChevronLeft, ChevronRight } from 'lucide-react';

interface TeamOwner { email: string; account_id: string; access_token?: string; member_count?: number }
interface TeamMember { user_id: string; email?: string; name?: string; role: string; created_at?: string }

const PAGE_SIZE = 10;

function Pagination({ page, total, onChange }: { page: number; total: number; onChange: (p: number) => void }) {
  const totalPages = Math.ceil(total / PAGE_SIZE);
  if (totalPages <= 1) return null;
  return (
    <div className="flex items-center justify-between mt-3 px-1">
      <span className="text-xs c-dim">第 {page} / {totalPages} 页，共 {total} 条</span>
      <div className="flex items-center gap-1">
        <button
          onClick={() => onChange(page - 1)} disabled={page <= 1}
          className="btn btn-ghost p-1.5 disabled:opacity-30"
        ><ChevronLeft size={14} /></button>
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
        <button
          onClick={() => onChange(page + 1)} disabled={page >= totalPages}
          className="btn btn-ghost p-1.5 disabled:opacity-30"
        ><ChevronRight size={14} /></button>
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

  const loadMembers = async (accountId: string) => {
    setSelected(accountId); setMembers([]); setMembersLoading(true); setMemberPage(1);
    try {
      const data = await api.get<{ members: TeamMember[] }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/members`);
      setMembers(data.members || []);
    } catch { toast('获取成员失败', 'error'); }
    finally { setMembersLoading(false); }
  };

  const kickMember = async (userId: string) => {
    if (!selected) return;
    if (!confirm(`确定要踢除该成员？`)) return;
    setKickLoading(userId);
    try {
      await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}/kick`);
      toast('已踢除成员', 'success');
      setMembers(prev => prev.filter(m => m.user_id !== userId));
    } catch (e) { toast(`踢除失败: ${e}`, 'error'); }
    finally { setKickLoading(null); }
  };

  const refreshMembers = async (accountId: string) => {
    try {
      const data = await api.post<{ members: TeamMember[] }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/refresh`);
      toast('已刷新成员列表', 'success');
      if (selected === accountId) { setMembers(data.members || []); setMemberPage(1); }
      loadOwners();
    } catch (e) { toast(`刷新失败: ${e}`, 'error'); }
  };

  const selectedOwner = owners.find(o => o.account_id === selected);

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
              {pagedOwners.map(o => (
                <div
                  key={o.account_id}
                  className={`row-item cursor-pointer ${selected === o.account_id ? 'ring-1' : ''}`}
                  style={selected === o.account_id ? { borderColor: 'var(--teal)', boxShadow: '0 0 0 1px rgba(45,212,191,.3)' } : {}}
                  onClick={() => loadMembers(o.account_id)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3 min-w-0">
                      <div className="w-8 h-8 rounded-lg flex items-center justify-center shrink-0" style={{ background: 'var(--ghost)', border: '1px solid var(--border)' }}>
                        <User size={18} className="c-dim" />
                      </div>
                      <div className="min-w-0">
                        <div className="text-sm c-heading font-medium truncate">{o.email || '未知邮箱'}</div>
                        <div className="text-xs font-mono c-dim">{o.account_id.substring(0, 12)}...</div>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      {o.member_count != null && (
                        <span className="text-xs font-mono c-dim2">{o.member_count} 成员</span>
                      )}
                      <button onClick={e => { e.stopPropagation(); refreshMembers(o.account_id); }} className="btn btn-ghost text-xs py-1 px-2" title="刷新成员">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-3.5 h-3.5">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.992 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182" />
                        </svg>
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
            <Pagination page={ownerPage} total={owners.length} onChange={setOwnerPage} />
          </>
        )}
      </div>

      {/* Members Panel */}
      {selected && (
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="section-title mb-0">
              成员列表 <span className="text-amber-400">{selectedOwner?.email || selected.substring(0, 12)}</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs font-mono c-dim">{members.length} 个成员</span>
              <button onClick={() => setSelected(null)} className="btn btn-ghost text-xs py-1">关闭</button>
            </div>
          </div>

          {membersLoading ? (
            <div className="text-center c-dim py-4">加载中...</div>
          ) : members.length === 0 ? (
            <div className="text-center c-dim py-4">无成员数据</div>
          ) : (
            <>
              <div className="space-y-2">
                {pagedMembers.map(m => (
                  <div key={m.user_id} className="card-inner px-4 py-3 flex items-center justify-between">
                    <div className="flex items-center gap-3 min-w-0">
                      <div className="min-w-0">
                        <div className="text-sm c-heading font-medium">{m.name || m.email || '未知'}</div>
                        <div className="flex items-center gap-2 text-xs c-dim">
                          <span className="font-mono">{m.user_id.substring(0, 12)}...</span>
                          <span className={`px-1.5 py-0.5 rounded text-[.6rem] ${m.role === 'owner' ? 'text-amber-400' : 'c-dim'}`} style={{ background: 'var(--ghost)' }}>
                            {m.role}
                          </span>
                          {m.created_at && <span>{new Date(m.created_at).toLocaleDateString('zh-CN')}</span>}
                        </div>
                      </div>
                    </div>
                    {m.role !== 'owner' && (
                      <button
                        onClick={() => kickMember(m.user_id)}
                        disabled={kickLoading === m.user_id}
                        className="btn btn-danger text-xs py-1 px-3"
                      >
                        {kickLoading === m.user_id ? '处理中...' : '踢除'}
                      </button>
                    )}
                  </div>
                ))}
              </div>
              <Pagination page={memberPage} total={members.length} onChange={setMemberPage} />
            </>
          )}
        </div>
      )}
    </div>
  );
}
