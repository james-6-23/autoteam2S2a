import { useEffect, useState, useCallback, useRef } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';
import { FileUp, ClipboardPaste, Type, Trash2, Info, Check, Loader2 } from 'lucide-react';

interface OwnerPreview { email: string; account_id: string; expires?: string }
interface InviteTaskItem { id: string; status: string; owner_email: string; s2a_team?: string; created_at: string; invited_ok: number; invited_failed: number; reg_ok: number; reg_failed: number; rt_ok: number; rt_failed: number; s2a_ok: number; s2a_failed: number; error?: string }
interface UploadItem { id: string; filename: string; owner_count: number; unused_count: number; created_at: string; owner_emails?: string }
interface TeamItem { name: string }

// ─── Smart JSON parsing (ported from invite.js) ───
function smartParseAccounts(text: string) {
  const raw = text.trim(); if (!raw) return [];
  try { const p = JSON.parse(raw); return unwrap(p); } catch {}
  const blocks = extractJsonBlocks(raw);
  if (blocks.length) { const res: unknown[] = []; for (const b of blocks) { try { res.push(...unwrap(JSON.parse(b))); } catch {} } if (res.length) return res; }
  const results: unknown[] = [];
  for (const chunk of raw.split(/\n\s*\n/)) { const t = chunk.trim(); if (!t) continue; try { results.push(...unwrap(JSON.parse(t))); } catch {} }
  return results;
}
function extractJsonBlocks(text: string) {
  const blocks: string[] = []; let i = 0;
  while (i < text.length) {
    if (text[i] === '{' || text[i] === '[') {
      const open = text[i], close = open === '{' ? '}' : ']';
      let depth = 1, j = i + 1, inStr = false, esc = false;
      while (j < text.length && depth > 0) { const ch = text[j]; if (esc) { esc = false; j++; continue; } if (ch === '\\') { esc = true; j++; continue; } if (ch === '"') { inStr = !inStr; j++; continue; } if (!inStr) { if (ch === open) depth++; else if (ch === close) depth--; } j++; }
      if (depth === 0) { blocks.push(text.substring(i, j)); i = j; continue; }
    }
    i++;
  }
  return blocks;
}
function unwrap(parsed: unknown): unknown[] {
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === 'object') {
    const obj = parsed as Record<string, unknown>;
    if (Array.isArray(obj.accounts)) return obj.accounts;
    if (obj.accessToken || obj.access_token || obj.id) return [obj];
    for (const k of Object.keys(obj)) { if (Array.isArray(obj[k]) && (obj[k] as unknown[]).length > 0) return obj[k] as unknown[]; }
    return [obj];
  }
  return [];
}

export default function Invite() {
  const { toast } = useToast();
  const [mode, setMode] = useState<'file' | 'json'>('file');
  const [owners, setOwners] = useState<OwnerPreview[]>([]);
  const [uploadId, setUploadId] = useState('');
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [tasks, setTasks] = useState<InviteTaskItem[]>([]);
  const [invCount, setInvCount] = useState(6);
  const [pushS2a, setPushS2a] = useState(true);
  const [teams, setTeams] = useState<TeamItem[]>([]);
  const [dist, setDist] = useState<{ team: string; percent: number }[]>([]);
  const [jsonText, setJsonText] = useState('');
  const [jsonCharCount, setJsonCharCount] = useState(0);
  const jsonRef = useRef<HTMLTextAreaElement>(null);
  const [dragOver, setDragOver] = useState(false);
  const [fileName, setFileName] = useState('');
  const [uploading, setUploading] = useState(false);
  const [executing, setExecuting] = useState(false);  const fileRef = useRef<HTMLInputElement>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // SSE log
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logConnected, setLogConnected] = useState(false);
  const [logAutoScroll, setLogAutoScroll] = useState(true);
  const logRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);

  const loadUploads = useCallback(async () => {
    try {
      const data = await api.get<UploadItem[]>('/api/invite/uploads');
      setUploads(Array.isArray(data) ? data : []);
      const available = (Array.isArray(data) ? data : []).filter(u => u.unused_count > 0);
      if (available.length && !uploadId) setUploadId(available[0].id);
    } catch {}
  }, [uploadId]);

  const loadTasks = useCallback(async () => {
    try {
      const data = await api.get<InviteTaskItem[]>('/api/invite/tasks');
      const arr = Array.isArray(data) ? data : [];
      setTasks(arr);
      if (arr.some(t => t.status === 'running' || t.status === 'pending')) {
        if (!pollRef.current) pollRef.current = setInterval(loadTasks, 3000);
      } else { if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; } }
    } catch {}
  }, []);

  const loadTeams = useCallback(async () => {
    try {
      const cfg = await api.fetchConfig() as { teams: TeamItem[] };
      const teamList = cfg.teams || [];
      setTeams(teamList);
      if (!dist.length && teamList.length) {
        // 优先从 localStorage 恢复，过滤掉已不存在的号池
        const names = new Set(teamList.map(t => t.name));
        try {
          const raw = localStorage.getItem('inv-dist');
          if (raw) {
            const saved = JSON.parse(raw) as { team: string; percent: number }[];
            const valid = saved.filter(d => names.has(d.team));
            if (valid.length) { setDist(valid); return; }
          }
        } catch {}
        setDist([{ team: teamList[0].name, percent: 100 }]);
      }
    } catch {}
  }, [dist.length]);

  useEffect(() => { loadUploads(); loadTasks(); loadTeams(); }, [loadUploads, loadTasks, loadTeams]);
  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current); }, []);

  // 分发配置变更时持久化到 localStorage
  useEffect(() => {
    if (dist.length) localStorage.setItem('inv-dist', JSON.stringify(dist));
  }, [dist]);

  // SSE
  useEffect(() => {
    const connect = () => {
      const es = new EventSource('/api/logs/stream');
      esRef.current = es;
      es.onopen = () => setLogConnected(true);
      es.onmessage = (e) => setLogLines(prev => { const n = [...prev, e.data]; return n.length > 500 ? n.slice(-500) : n; });
      es.onerror = () => { setLogConnected(false); es.close(); esRef.current = null; setTimeout(connect, 3000); };
    };
    connect();
    return () => { esRef.current?.close(); };
  }, []);
  useEffect(() => { if (logAutoScroll && logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [logLines, logAutoScroll]);

  const doUpload = async (filename: string, accounts: unknown[]) => {
    setUploading(true);
    try {
      const resp = await api.post<{ upload_id: string; owner_count: number; owners: OwnerPreview[] }>('/api/invite/upload', { filename, accounts });
      setOwners(resp.owners || []);
      toast(`上传成功: ${resp.owner_count} 个 Owner，正在导入...`, 'info');

      // 等待批次列表刷新，确认后端已入库
      const data = await api.get<UploadItem[]>('/api/invite/uploads');
      const arr = Array.isArray(data) ? data : [];
      setUploads(arr);

      // 验证新批次存在且有可用 Owner
      const ready = arr.find(u => u.id === resp.upload_id && u.unused_count > 0);
      if (ready) {
        setUploadId(resp.upload_id);
        toast(`导入完成: ${ready.unused_count} 个 Owner 已就绪`, 'success');
      } else {
        toast('批次导入异常，请刷新重试', 'error');
      }
    } catch {} finally {
      setUploading(false);
    }
  };

  const handleFile = async (file: File) => {
    setFileName(file.name);
    try { const text = await file.text(); const accounts = smartParseAccounts(text); if (!accounts.length) { toast('文件中未找到有效账号', 'error'); return; } await doUpload(file.name, accounts); } catch (e) { toast('解析失败', 'error'); }
  };

  const handleJsonUpload = async () => {
    const text = jsonRef.current?.value || jsonText;
    if (!text.trim()) { toast('请粘贴 JSON', 'error'); return; }
    const accounts = smartParseAccounts(text);
    if (!accounts.length) { toast('未解析到有效账号', 'error'); return; }
    await doUpload('pasted-json', accounts);
  };

  const handleJsonChange = useCallback(() => {
    const val = jsonRef.current?.value || '';
    setJsonCharCount(val.length);
    setJsonText(val);
  }, []);

  const formatJson = useCallback(() => {
    const val = jsonRef.current?.value || '';
    if (!val.trim()) return;
    try {
      const parsed = JSON.parse(val);
      const formatted = JSON.stringify(parsed, null, 2);
      if (jsonRef.current) jsonRef.current.value = formatted;
      setJsonText(formatted);
      setJsonCharCount(formatted.length);
      toast('已格式化', 'success');
    } catch {
      toast('JSON 格式错误，无法格式化', 'error');
    }
  }, [toast]);

  const clearJson = useCallback(() => {
    if (jsonRef.current) jsonRef.current.value = '';
    setJsonText('');
    setJsonCharCount(0);
  }, []);

  const execute = async () => {
    if (!uploadId) { toast('请先选择上传批次', 'error'); return; }
    if (uploading) { toast('正在导入中，请稍候', 'error'); return; }
    const body: Record<string, unknown> = { upload_id: uploadId, invite_count: invCount, push_s2a: pushS2a };
    if (pushS2a) {
      const tot = dist.reduce((s, d) => s + d.percent, 0);
      if (tot !== 100) { toast(`百分比总和必须为100，当前${tot}`, 'error'); return; }
      body.distribution = dist.filter(d => d.team && d.percent > 0);
    }
    setExecuting(true);
    try {
      const resp = await api.post<{ task_count: number }>('/api/invite/execute', body);
      toast(`已创建 ${resp.task_count} 个邀请任务`, 'success');
      await Promise.all([loadTasks(), loadUploads()]);
    } catch {} finally {
      setExecuting(false);
    }
  };

  const available = uploads.filter(u => u.unused_count > 0);
  const fmt = (iso: string) => { try { return new Date(iso).toLocaleString('zh-CN', { hour12: false, month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }); } catch { return iso; } };

  return (
    <div className="space-y-4">
      {/* Upload Area */}
      <div className="card p-5">
        <div className="section-title">上传 Owner</div>
        {/* Tabs */}
        <div className="upload-tabs mb-4">
          <button className={`upload-tab ${mode === 'file' ? 'active' : ''}`} onClick={() => setMode('file')}><FileUp size={14} /> 文件上传</button>
          <button className={`upload-tab ${mode === 'json' ? 'active' : ''}`} onClick={() => setMode('json')}><ClipboardPaste size={14} /> 粘贴 JSON</button>
        </div>

        {mode === 'file' ? (
          <div>
            {!fileName ? (
              <div className={`drop-zone ${dragOver ? 'drag-over' : ''}`}
                onDragEnter={e => { e.preventDefault(); setDragOver(true); }}
                onDragOver={e => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={e => { e.preventDefault(); setDragOver(false); }}
                onDrop={e => { e.preventDefault(); setDragOver(false); if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]); }}
                onClick={() => fileRef.current?.click()}
              >
                <div className="drop-zone-inner">
                  <div className="text-sm c-dim">点击选择或拖拽文件</div>
                  <div className="text-xs c-dim">支持 JSON / TXT</div>
                </div>
              </div>
            ) : (
              <div className="flex items-center gap-3 p-3 rounded-lg" style={{ background: 'var(--ghost)' }}>
                <span className="text-sm c-heading">{fileName}</span>
                <button onClick={() => { setFileName(''); if (fileRef.current) fileRef.current.value = ''; }} className="btn btn-ghost text-xs py-1">清除</button>
              </div>
            )}
            <input ref={fileRef} type="file" accept=".json,.txt" className="hidden" onChange={e => { if (e.target.files?.[0]) handleFile(e.target.files[0]); }} />
          </div>
        ) : (
          <div className="json-editor-wrap">
            {/* macOS 风格头部 */}
            <div className="json-editor-header">
              <div className="flex items-center gap-2">
                <span className="inline-block w-[10px] h-[10px] rounded-full" style={{ background: '#f87171' }} />
                <span className="inline-block w-[10px] h-[10px] rounded-full" style={{ background: '#fbbf24' }} />
                <span className="inline-block w-[10px] h-[10px] rounded-full" style={{ background: '#4ade80' }} />
                <span className="text-[.65rem] c-dim font-mono ml-1.5">accounts.json</span>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="text-[.6rem] c-dim font-mono">{jsonCharCount} 字符</span>
                <button onClick={formatJson} className="json-editor-btn" title="格式化 JSON">
                  <Type size={12} />
                </button>
                <button onClick={clearJson} className="json-editor-btn" title="清空">
                  <Trash2 size={12} />
                </button>
              </div>
            </div>
            <textarea
              ref={jsonRef}
              className="json-editor-textarea"
              rows={9}
              spellCheck={false}
              defaultValue={jsonText}
              onChange={handleJsonChange}
              placeholder={`[\n  {\n    "user": { "email": "..." },\n    "account": { "id": "...", "planType": "team" },\n    "accessToken": "...",\n    "expires": "2026-..."\n  }\n]`}
            />
            <div className="json-editor-footer">
              <div className="flex items-center gap-2">
                <Info size={11} className="c-dim" />
                <span className="text-[.6rem] c-dim">支持单个对象或数组 · 自动提取 email / account / token</span>
              </div>
              <button onClick={handleJsonUpload} disabled={uploading} className="btn btn-teal py-1.5 px-5 text-xs">
                {uploading ? <Loader2 size={12} className="spin" /> : <Check size={12} />}
                {uploading ? '上传中...' : '解析并上传'}
              </button>
            </div>
          </div>
        )}

        {/* Owner preview */}
        {owners.length > 0 && (
          <div className="mt-4">
            <div className="field-label mb-2">已解析 <span className="text-teal-400 font-mono">{owners.length}</span> 个 Owner</div>
            <div className="grid gap-2" style={{ maxHeight: 200, overflowY: 'auto' }}>
              {owners.map((o, i) => {
                const expired = o.expires ? new Date(o.expires) < new Date() : false;
                return (
                  <div key={i} className="owner-card" style={{ animationDelay: `${i * 50}ms` }}>
                    <div className="flex items-center gap-3 min-w-0">
                      <div className="w-[30px] h-[30px] rounded-lg flex items-center justify-center shrink-0" style={{ background: 'var(--ghost)', border: '1px solid var(--border)' }}>
                        <span className="font-mono text-[.6rem] c-dim font-semibold">{i + 1}</span>
                      </div>
                      <div className="min-w-0">
                        <div className="text-xs c-heading font-medium truncate">{o.email || '未知邮箱'}</div>
                        <span className="font-mono text-[.6rem] c-dim">{o.account_id?.substring(0, 8)}…</span>
                      </div>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <div className="w-[5px] h-[5px] rounded-full" style={{ background: expired ? '#f87171' : '#2dd4bf' }} />
                      <span className="text-[.6rem] font-medium" style={{ color: expired ? '#f87171' : '#2dd4bf' }}>{expired ? '已过期' : '有效'}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* Execute */}
      <div className="card p-5">
        <div className="section-title">执行邀请</div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="field-label">选择批次</label>
            <Select
              value={uploadId}
              onChange={setUploadId}
              options={available.length === 0
                ? [{ label: '-- 无 --', value: '' }]
                : available.map(u => ({ label: `${u.filename} (${u.unused_count}/${u.owner_count})`, value: u.id }))
              }
            />
          </div>
          <div><label className="field-label">邀请数</label><input type="number" min={1} max={25} className="field-input" value={invCount} onChange={e => setInvCount(Number(e.target.value))} /></div>
          <div><label className="field-label">推送 S2A</label><Select value={pushS2a ? 'true' : 'false'} onChange={v => setPushS2a(v === 'true')} options={[{ label: '是', value: 'true' }, { label: '否', value: 'false' }]} /></div>
          <div>
            <label className="field-label">&nbsp;</label>
            <button
              onClick={execute}
              disabled={uploading || executing || !uploadId}
              className="btn btn-amber w-full justify-center h-[40px]"
            >
              {uploading ? (<><Loader2 size={14} className="spin" /> 导入中...</>) :
               executing ? (<><Loader2 size={14} className="spin" /> 执行中...</>) :
               '执行'}
            </button>
          </div>
        </div>
        {/* Distribution */}
        {pushS2a && (
          <div className="mt-3">
            <div className="flex items-center justify-between mb-2">
              <label className="field-label mb-0">分发 (总和=100%)</label>
              <button
                onClick={() => {
                  const used = dist.reduce((s, d) => s + d.percent, 0);
                  setDist([...dist, { team: teams[0]?.name || '', percent: Math.max(0, 100 - used) }]);
                }}
                className="text-amber-400 text-xs font-medium hover:text-amber-300 transition-colors cursor-pointer"
              >+ 号池</button>
            </div>
            <div className="space-y-2">
              {dist.map((d, i) => (
                <div key={i} className="flex items-center gap-2">
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <Select
                      value={d.team}
                      onChange={v => { const n = [...dist]; n[i] = { ...n[i], team: v }; setDist(n); }}
                      options={teams.map(t => ({ label: t.name, value: t.name }))}
                    />
                  </div>
                  <input
                    type="number" min={0} max={100}
                    className="field-input"
                    style={{ width: '6rem', flex: 'none' }}
                    placeholder="%"
                    value={d.percent}
                    onChange={e => { const n = [...dist]; n[i] = { ...n[i], percent: Number(e.target.value) }; setDist(n); }}
                  />
                  <button onClick={() => setDist(dist.filter((_, j) => j !== i))} className="btn btn-danger text-xs py-1 px-2">&times;</button>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Tasks */}
      <div className="card p-5">
        <div className="section-title">邀请任务</div>
        <div className="space-y-2">
          {tasks.length === 0 ? <span className="c-dim text-sm">暂无邀请任务</span> :
            tasks.map(t => {
              const sc = t.status === 'completed' ? 'text-teal-400' : t.status === 'running' ? 'text-amber-400' : t.status === 'failed' ? 'text-red-400' : 'c-dim';
              return (
                <div key={t.id} className="py-2.5 px-3 rounded" style={{ background: 'var(--ghost)' }}>
                  <div className="flex items-center gap-4 mb-1.5">
                    <span className="font-mono c-heading text-xs">{t.id}</span>
                    <span className={`${sc} font-medium text-xs`}>{t.status}</span>
                    <span className="c-dim text-xs">{t.owner_email}</span>
                    <span className="c-dim text-xs">{t.s2a_team || '--'}</span>
                    <span className="c-dim text-xs">{fmt(t.created_at)}</span>
                  </div>
                  <div className="flex gap-4 text-[.65rem]">
                    <span>邀请: <span className="text-teal-400">{t.invited_ok}</span>/<span className="text-red-400">{t.invited_failed}</span></span>
                    <span>注册: <span className="text-teal-400">{t.reg_ok}</span>/<span className="text-red-400">{t.reg_failed}</span></span>
                    <span>RT: <span className="text-teal-400">{t.rt_ok}</span>/<span className="text-red-400">{t.rt_failed}</span></span>
                    <span>S2A: <span className="text-teal-400">{t.s2a_ok}</span>/<span className="text-red-400">{t.s2a_failed}</span></span>
                  </div>
                  {t.error && <div className="text-red-400 text-[.65rem] mt-1">{t.error}</div>}
                </div>
              );
            })}
        </div>
      </div>

      {/* Log */}
      <div className="card p-5">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div className="section-title mb-0">实时日志</div>
            <span className={`w-1.5 h-1.5 rounded-full ${logConnected ? 'bg-teal-400' : 'bg-red-400'}`} />
            <span className="text-xs c-dim">{logConnected ? '已连接' : '已断开'}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs font-mono c-dim">{logLines.length} 条</span>
            <button onClick={() => setLogAutoScroll(!logAutoScroll)} className="btn btn-ghost text-xs py-1">自动滚动: {logAutoScroll ? '开' : '关'}</button>
            <button onClick={() => setLogLines([])} className="btn btn-ghost text-xs py-1">清空</button>
          </div>
        </div>
        <div ref={logRef} className="font-mono text-xs leading-relaxed p-4 rounded-lg overflow-y-auto" style={{ background: 'var(--bg-inner)', border: '1px solid var(--border)', height: '40vh', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {logLines.map((line, i) => <span key={i} className="log-line">{line}{'\n'}</span>)}
        </div>
      </div>
    </div>
  );
}
