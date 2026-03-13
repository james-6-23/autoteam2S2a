import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { User } from 'lucide-react';

interface TeamOwner { email: string; account_id: string; access_token?: string; member_count?: number }
interface TeamMember { user_id: string; email?: string; name?: string; role: string; created_at?: string }

export default function TeamManage() {
  const { toast } = useToast();
  const [owners, setOwners] = useState<TeamOwner[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [members, setMembers] = useState<TeamMember[]>([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [kickLoading, setKickLoading] = useState<string | null>(null);

  const loadOwners = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.get<{ owners: TeamOwner[] }>('/api/team-manage/owners');
      setOwners(data.owners || []);
    } catch { setOwners([]); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadOwners(); }, [loadOwners]);

  const loadMembers = async (accountId: string) => {
    setSelected(accountId); setMembers([]); setMembersLoading(true);
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
      if (selected === accountId) setMembers(data.members || []);
      loadOwners(); // refresh counts
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
          <div className="grid gap-2">
            {owners.map(o => (
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
            <div className="space-y-2">
              {members.map(m => (
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
          )}
        </div>
      )}
    </div>
  );
}
