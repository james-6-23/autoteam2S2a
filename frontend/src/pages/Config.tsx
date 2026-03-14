import { useEffect, useState, useCallback } from 'react';
import { useToast } from '../components/Toast';
import * as api from '../lib/api';
import { Select } from '../components/Select';

interface ConfigData {
  defaults: { target_count?: number; register_workers?: number; rt_workers?: number; rt_retries?: number; team?: Record<string, number>; free?: Record<string, number> };
  register: Record<string, unknown>;
  proxy_pool?: string[];
  email_domains: string[];
  chatgpt_mail_domains?: string[];
  d1_cleanup?: { enabled: boolean; account_id: string; api_key: string; keep_percent: number; batch_size: number; databases: { name: string; id: string }[]; cleanup_timing?: string };
}

export default function Config() {
  const { toast } = useToast();
  const [config, setConfig] = useState<ConfigData | null>(null);

  // Defaults
  const [mode, setMode] = useState('team');
  const [target, setTarget] = useState(4); const [regW, setRegW] = useState(4); const [rtW, setRtW] = useState(4); const [rtR, setRtR] = useState(4);

  // Register
  const [mailBase, setMailBase] = useState(''); const [mailPath, setMailPath] = useState(''); const [mailToken, setMailToken] = useState('');
  const [mailTimeout, setMailTimeout] = useState(20); const [otpRetries, setOtpRetries] = useState(3); const [reqTimeout, setReqTimeout] = useState(30);
  const [mailConc, setMailConc] = useState(50); const [logMode, setLogMode] = useState('verbose'); const [perfMode, setPerfMode] = useState('baseline');
  const [gptMailKey, setGptMailKey] = useState('');

  // Proxy
  const [newProxy, setNewProxy] = useState('');
  const [checking, setChecking] = useState(false);
  const [healthResults, setHealthResults] = useState<{ proxy: string; ok: boolean; reason: string }[]>([]);

  // Domains
  const [newDomain, setNewDomain] = useState(''); const [newGptDomain, setNewGptDomain] = useState('');

  // D1
  const [d1Enabled, setD1Enabled] = useState(false); const [d1AccId, setD1AccId] = useState(''); const [d1ApiKey, setD1ApiKey] = useState('');
  const [d1Keep, setD1Keep] = useState(0.1); const [d1Batch, setD1Batch] = useState(5000);
  const [d1Dbs, setD1Dbs] = useState<{ name: string; id: string }[]>([]);
  const [d1Timing, setD1Timing] = useState<'before_task' | 'after_task'>('after_task');
  const [d1Cleaning, setD1Cleaning] = useState(false);

  const load = useCallback(async () => {
    try {
      const data = await api.fetchConfig() as ConfigData;
      setConfig(data);
      const d = data.defaults; const r = data.register as Record<string, unknown>;
      const md = mode === 'free' ? (d?.free || {}) : (d?.team || {});
      setTarget(Number((md as Record<string, number>).target_count ?? d?.target_count ?? 4));
      setRegW(Number((md as Record<string, number>).register_workers ?? d?.register_workers ?? 4));
      setRtW(Number((md as Record<string, number>).rt_workers ?? d?.rt_workers ?? 4));
      setRtR(Number(d?.rt_retries ?? 4));
      setMailBase(String(r.mail_api_base || '')); setMailPath(String(r.mail_api_path || '')); setMailToken(String(r.mail_api_token || ''));
      setMailTimeout(Number(r.mail_request_timeout_sec || 20)); setOtpRetries(Number(r.otp_max_retries || 3)); setReqTimeout(Number(r.request_timeout_sec || 30));
      setMailConc(Number(r.mail_max_concurrency || 50)); setLogMode(String(r.register_log_mode || 'verbose')); setPerfMode(String(r.register_perf_mode || 'baseline'));
      setGptMailKey(String(r.chatgpt_mail_api_key || ''));
      if (data.d1_cleanup) {
        const c = data.d1_cleanup;
        setD1Enabled(c.enabled); setD1AccId(c.account_id || ''); setD1ApiKey(c.api_key || '');
        setD1Keep(c.keep_percent); setD1Batch(c.batch_size); setD1Dbs(c.databases || []);
        setD1Timing((c.cleanup_timing as 'before_task' | 'after_task') || 'after_task');
      }
    } catch {}
  }, [mode]);

  useEffect(() => { load(); }, [load]);

  const saveDefaults = async () => {
    try { await api.put('/api/config/defaults', { mode, target_count: target, register_workers: regW, rt_workers: rtW, rt_retries: rtR }); toast('参数已保存', 'success'); load(); } catch {}
  };
  const saveRegister = async () => {
    try { await api.put('/api/config/register', { mail_api_base: mailBase, mail_api_path: mailPath, mail_api_token: mailToken, mail_request_timeout_sec: mailTimeout, otp_max_retries: otpRetries, request_timeout_sec: reqTimeout, mail_max_concurrency: mailConc, register_log_mode: logMode, register_perf_mode: perfMode }); toast('注册配置已保存', 'success'); load(); } catch {}
  };
  const saveGptMailFn = async () => { try { await api.put('/api/config/register', { chatgpt_mail_api_key: gptMailKey }); toast('GPTMail Key 已保存', 'success'); load(); } catch {} };
  const handleSaveToFile = async () => { try { const d = await api.post<{ message: string }>('/api/config/save'); toast(d.message, 'success'); } catch {} };
  const addDomain = async () => { if (!newDomain.trim()) return; try { await api.post('/api/config/email_domains', { domain: newDomain.trim() }); setNewDomain(''); toast('域名已添加', 'success'); load(); } catch {} };
  const delDomain = async (d: string) => { try { await api.del('/api/config/email_domains'); await api.post('/api/config/email_domains', {}); } catch {} try { await api.put('/api/config/register', {}); } catch {} load(); };
  const addGptDomain = async () => { if (!newGptDomain.trim()) return; try { await api.post('/api/config/gptmail_domains', { domain: newGptDomain.trim() }); setNewGptDomain(''); toast('域名已添加', 'success'); load(); } catch {} };
  const saveD1 = async () => { try { await api.put('/api/config/d1_cleanup', { enabled: d1Enabled, account_id: d1AccId, api_key: d1ApiKey, keep_percent: d1Keep, batch_size: d1Batch, databases: d1Dbs, cleanup_timing: d1Timing }); toast('D1 配置已保存', 'success'); load(); } catch {} };
  const triggerD1Clean = async () => {
    setD1Cleaning(true);
    try {
      const r = await api.triggerD1Cleanup();
      toast(r.message || 'D1 清理完成', 'success');
    } catch (e: any) {
      toast(e.message || 'D1 清理失败', 'error');
    } finally {
      setD1Cleaning(false);
    }
  };

  const addProxy = async () => { if (!newProxy.trim()) return; try { await api.addProxy(newProxy.trim()); setNewProxy(''); toast('代理已添加', 'success'); load(); } catch (e: any) { toast(e.message || '添加失败', 'error'); } };
  const delProxy = async (p: string) => { try { await api.deleteProxy(p); toast('代理已删除', 'success'); load(); } catch (e: any) { toast(e.message || '删除失败', 'error'); } };
  const checkHealth = async () => { setChecking(true); setHealthResults([]); try { const r = await api.checkProxyHealth(); setHealthResults(r); } catch (e: any) { toast(e.message || '检测失败', 'error'); } finally { setChecking(false); } };

  return (
    <div className="space-y-4">
      {/* Defaults */}
      <div className="card p-5">
        <div className="section-title">运行参数</div>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <div><label className="field-label">模式</label><Select value={mode} onChange={setMode} options={[{ label: 'Team', value: 'team' }, { label: 'Free', value: 'free' }]} /></div>
          <div><label className="field-label">目标数</label><input type="number" className="field-input" value={target} onChange={e => setTarget(Number(e.target.value))} /></div>
          <div><label className="field-label">注册并发</label><input type="number" className="field-input" value={regW} onChange={e => setRegW(Number(e.target.value))} /></div>
          <div><label className="field-label">RT 并发</label><input type="number" className="field-input" value={rtW} onChange={e => setRtW(Number(e.target.value))} /></div>
          <div><label className="field-label">RT 重试</label><input type="number" className="field-input" value={rtR} onChange={e => setRtR(Number(e.target.value))} /></div>
        </div>
        <button onClick={saveDefaults} className="btn btn-amber mt-3">保存</button>
      </div>

      {/* Register */}
      <div className="card p-5">
        <div className="section-title">注册配置</div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <div><label className="field-label">邮箱 API 地址</label><input className="field-input" value={mailBase} onChange={e => setMailBase(e.target.value)} /></div>
          <div><label className="field-label">邮箱 API 路径</label><input className="field-input" value={mailPath} onChange={e => setMailPath(e.target.value)} /></div>
          <div><label className="field-label">邮箱 API Token</label><input className="field-input" value={mailToken} onChange={e => setMailToken(e.target.value)} /></div>
          <div><label className="field-label">邮箱超时(s)</label><input type="number" className="field-input" value={mailTimeout} onChange={e => setMailTimeout(Number(e.target.value))} /></div>
          <div><label className="field-label">OTP 重试</label><input type="number" className="field-input" value={otpRetries} onChange={e => setOtpRetries(Number(e.target.value))} /></div>
          <div><label className="field-label">请求超时(s)</label><input type="number" className="field-input" value={reqTimeout} onChange={e => setReqTimeout(Number(e.target.value))} /></div>
          <div><label className="field-label">邮箱并发</label><input type="number" className="field-input" value={mailConc} onChange={e => setMailConc(Number(e.target.value))} /></div>
          <div><label className="field-label">日志模式</label><Select value={logMode} onChange={setLogMode} options={[{ label: '详细', value: 'verbose' }, { label: '汇总', value: 'summary' }]} /></div>
          <div><label className="field-label">性能模式</label><Select value={perfMode} onChange={setPerfMode} options={[{ label: 'baseline', value: 'baseline' }, { label: 'adaptive', value: 'adaptive' }]} /></div>
        </div>
        <button onClick={saveRegister} className="btn btn-amber mt-3">保存</button>
      </div>

      {/* Proxy Pool */}
      <div className="card p-5">
        <div className="section-title">代理池 <span className="c-dim2 font-mono text-[.65rem]">{config?.proxy_pool?.length ?? 0}</span></div>
        <div className="flex flex-wrap gap-1.5 mb-3">
          {(config?.proxy_pool || []).map(p => (
            <span key={p} className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style={{ background: 'var(--domain-bg)', border: '1px solid var(--domain-border)', color: 'var(--text-dim2)' }}>
              {p}<button onClick={() => delProxy(p)} className="text-red-400 hover:text-red-300 ml-1">&times;</button>
            </span>
          ))}
        </div>
        <div className="flex gap-2 mb-3">
          <input className="field-input flex-1" placeholder="http://127.0.0.1:7890" value={newProxy} onChange={e => setNewProxy(e.target.value)} onKeyDown={e => e.key === 'Enter' && addProxy()} />
          <button onClick={addProxy} className="btn btn-teal text-xs">添加</button>
        </div>
        <button onClick={checkHealth} disabled={checking} className="btn btn-amber text-xs">{checking ? '检测中...' : '健康检测'}</button>
        {healthResults.length > 0 && (
          <div className="mt-3 space-y-1">
            {healthResults.map(r => (
              <div key={r.proxy} className="flex items-center gap-2 text-xs font-mono">
                <span className={r.ok ? 'text-green-400' : 'text-red-400'}>{r.ok ? '✓' : '✗'}</span>
                <span className="c-dim2">{r.proxy}</span>
                <span className="c-dim2 truncate">{r.reason}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Domains */}
      <div className="card p-5">
        <div className="section-title">邮箱域名 <span className="c-dim2 font-mono text-[.65rem]">{config?.email_domains.length ?? 0}</span></div>
        <div className="flex flex-wrap gap-1.5 mb-3">
          {(config?.email_domains || []).map(d => (
            <span key={d} className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style={{ background: 'var(--domain-bg)', border: '1px solid var(--domain-border)', color: 'var(--text-dim2)' }}>
              {d}<button onClick={() => delDomain(d)} className="text-red-400 hover:text-red-300 ml-1">&times;</button>
            </span>
          ))}
        </div>
        <div className="flex gap-2"><input className="field-input flex-1" placeholder="example.com" value={newDomain} onChange={e => setNewDomain(e.target.value)} /><button onClick={addDomain} className="btn btn-teal text-xs">添加</button></div>
      </div>

      {/* GPTMail */}
      <div className="card p-5">
        <div className="section-title">GPTMail</div>
        <div className="flex gap-2 mb-3"><input className="field-input flex-1" placeholder="API Key" value={gptMailKey} onChange={e => setGptMailKey(e.target.value)} /><button onClick={saveGptMailFn} className="btn btn-amber text-xs">保存</button></div>
        <div className="flex flex-wrap gap-1.5 mb-3">
          {(config?.chatgpt_mail_domains || []).map(d => (
            <span key={d} className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style={{ background: 'var(--domain-bg)', border: '1px solid var(--domain-border)', color: 'var(--text-dim2)' }}>{d}</span>
          ))}
        </div>
        <div className="flex gap-2"><input className="field-input flex-1" placeholder="域名" value={newGptDomain} onChange={e => setNewGptDomain(e.target.value)} /><button onClick={addGptDomain} className="btn btn-teal text-xs">添加</button></div>
      </div>

      {/* D1 */}
      <div className="card p-5">
        <div className="section-title">D1 清理</div>
        <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
          <div><label className="switch"><input className="switch-input" type="checkbox" checked={d1Enabled} onChange={e => setD1Enabled(e.target.checked)} /><span className="switch-slider" /><span>启用</span></label></div>
          <div><label className="field-label">Account ID</label><input className="field-input" value={d1AccId} onChange={e => setD1AccId(e.target.value)} /></div>
          <div><label className="field-label">API Key</label><input className="field-input" value={d1ApiKey} onChange={e => setD1ApiKey(e.target.value)} /></div>
          <div><label className="field-label">保留比例</label><input type="number" step="0.01" className="field-input" value={d1Keep} onChange={e => setD1Keep(Number(e.target.value))} /></div>
          <div><label className="field-label">批大小</label><input type="number" className="field-input" value={d1Batch} onChange={e => setD1Batch(Number(e.target.value))} /></div>
          <div><label className="field-label">清理时机</label><Select value={d1Timing} onChange={v => setD1Timing(v as 'before_task' | 'after_task')} options={[{ label: '任务执行后', value: 'after_task' }, { label: '任务执行前', value: 'before_task' }]} /></div>
        </div>
        <div className="mt-3 space-y-2">
          {d1Dbs.map((db, i) => (
            <div key={i} className="flex items-center gap-2">
              <input className="field-input flex-1" placeholder="名称" value={db.name} onChange={e => { const n = [...d1Dbs]; n[i] = { ...n[i], name: e.target.value }; setD1Dbs(n); }} />
              <input className="field-input flex-1" placeholder="ID" value={db.id} onChange={e => { const n = [...d1Dbs]; n[i] = { ...n[i], id: e.target.value }; setD1Dbs(n); }} />
              <button onClick={() => setD1Dbs(d1Dbs.filter((_, j) => j !== i))} className="btn btn-danger text-xs py-1 px-2">&times;</button>
            </div>
          ))}
          <button onClick={() => setD1Dbs([...d1Dbs, { name: '', id: '' }])} className="btn btn-ghost text-xs">+ 数据库</button>
        </div>
        <div className="flex gap-2 mt-3">
          <button onClick={saveD1} className="btn btn-amber">保存 D1</button>
          <button onClick={triggerD1Clean} disabled={d1Cleaning} className="btn btn-teal">{d1Cleaning ? '清理中...' : '清理'}</button>
        </div>
      </div>

      <button onClick={handleSaveToFile} className="btn btn-ghost w-full justify-center">保存到文件</button>
    </div>
  );
}
