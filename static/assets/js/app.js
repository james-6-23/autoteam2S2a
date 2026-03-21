// Theme
function initTheme(){
  const saved=localStorage.getItem('theme');
  const prefer=saved||(window.matchMedia('(prefers-color-scheme:light)').matches?'light':'dark');
  applyTheme(prefer);
}
function applyTheme(t){
  document.documentElement.classList.toggle('light',t==='light');
  document.getElementById('icon-moon').classList.toggle('hidden',t==='light');
  document.getElementById('icon-sun').classList.toggle('hidden',t==='dark');
  localStorage.setItem('theme',t);
}
function toggleTheme(){
  const isLight=document.documentElement.classList.contains('light');
  applyTheme(isLight?'dark':'light');
}
initTheme();

const API='';let configData=null;let taskPollTimer=null;

// Toast
async function animateRefresh(btn,fn){
  btn.classList.add('spinning');btn.disabled=true;
  try{await fn()}finally{setTimeout(()=>{btn.classList.remove('spinning');btn.disabled=false},400)}
}

function toast(msg,type='info'){
  const c=document.getElementById('toast-container');
  const cls={info:'toast-info',success:'toast-success',error:'toast-error'};
  const el=document.createElement('div');
  el.className=`toast ${cls[type]||cls.info}`;
  el.textContent=msg;
  c.appendChild(el);
  setTimeout(()=>{el.style.opacity='0';el.style.transition='opacity .3s';setTimeout(()=>el.remove(),300)},3000);
}

// API
async function api(path,opts={}){
  try{
    const res=await fetch(API+path,{headers:{'Content-Type':'application/json'},...opts,body:opts.body?JSON.stringify(opts.body):undefined});
    const data=await res.json();
    if(!res.ok) throw new Error(data.error||`HTTP ${res.status}`);
    return data;
  }catch(e){toast(e.message,'error');throw e}
}

// Tabs
const ALL_TABS=['dashboard','teams','config','email','tasks','schedules','runs','logs'];
function switchTab(name){
  ALL_TABS.forEach(t=>{
    document.getElementById('panel-'+t).classList.toggle('hidden',t!==name);
    const b=document.getElementById('tab-'+t);
    b.classList.toggle('tab-active',t===name);
  });
  if(name==='tasks'){refreshTasks();startTaskPoll()}else{stopTaskPoll();closeTaskProgress()}
  if(name==='dashboard'||name==='teams'||name==='config'||name==='email') loadConfig();
  if(name==='schedules'){loadSchedules();startSchedulePoll()}else{stopSchedulePoll()}
  if(name==='runs'){loadRunStats();loadRunsFilter();loadRuns(1)}
  if(name==='logs') connectLogStream();
}

// Health
async function checkHealth(){
  try{
    const d=await api('/health');
    document.getElementById('health-dot').className='w-1.5 h-1.5 rounded-full bg-teal-400 animate-pulse-dot';
    document.getElementById('version-badge').textContent='v'+d.version;
    const h=Math.floor(d.uptime_secs/3600),m=Math.floor((d.uptime_secs%3600)/60);
    document.getElementById('uptime-text').textContent=`${h}h ${m}m`;
  }catch{document.getElementById('health-dot').className='w-1.5 h-1.5 rounded-full bg-red-400'}
}

// Config
async function loadConfig(){try{configData=await api('/api/config');renderDashboard();renderTeams();renderConfigForm();applySiteTitle(configData.site_title)}catch{}}
function resolveModeDefaults(mode){
  const d=configData?.defaults||{};
  const common={
    target_count:d.target_count??1,
    register_workers:d.register_workers??15,
    rt_workers:d.rt_workers??10,
    rt_retries:d.rt_retries??4,
  };
  const modeData=mode==='free'?(d.free||{}):(d.team||{});
  return {
    target_count:modeData.target_count??common.target_count,
    register_workers:modeData.register_workers??common.register_workers,
    rt_workers:modeData.rt_workers??common.rt_workers,
    rt_retries:modeData.rt_retries??common.rt_retries,
  };
}
function applyQuickDefaultsByMode(mode){
  if(!configData)return;
  const resolved=resolveModeDefaults(mode);
  document.getElementById('q-target').value=resolved.target_count;
  document.getElementById('q-reg-workers').value=resolved.register_workers;
  document.getElementById('q-rt-workers').value=resolved.rt_workers;
}
function applyConfigDefaultsByMode(mode){
  if(!configData)return;
  const resolved=resolveModeDefaults(mode);
  document.getElementById('cfg-target').value=resolved.target_count;
  document.getElementById('cfg-reg-workers').value=resolved.register_workers;
  document.getElementById('cfg-rt-workers').value=resolved.rt_workers;
  document.getElementById('cfg-rt-retries').value=resolved.rt_retries;
}
function renderDashboard(){
  if(!configData)return;
  document.getElementById('stat-teams').textContent=configData.teams.length;
  document.getElementById('stat-proxies').textContent=configData.proxy_pool.length;
  document.getElementById('stat-domains').textContent=configData.email_domains.length;
  const sel=document.getElementById('q-team');
  sel.innerHTML=configData.teams.map(t=>`<option value="${t.name}">${t.name}</option>`).join('');
  const modeEl=document.getElementById('q-mode');
  const mode=modeEl?.value==='free'?'free':'team';
  applyQuickDefaultsByMode(mode);
}
let teamGroupsCache={};
function groupSummaryText(groupIds){
  const count=Array.isArray(groupIds)?groupIds.length:0;
  return count?`已配 ${count} 组`:'未配置';
}
function escapeHtml(v){
  return String(v??'').replace(/[&<>"']/g,ch=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[ch]));
}
function renderGroupRows(groupIds,allGroups,emptyText){
  if(!groupIds||groupIds.length===0) return `<div class="text-xs text-dim">${emptyText}</div>`;
  return groupIds.map(id=>{
    const g=(allGroups||[]).find(x=>x.id===id);
    const name=g?.name||`未命名分组`;
    const cnt=typeof g?.account_count==='number'?`${g.account_count} 账号`:'账号数未知';
    const status=g?.status||'unknown';
    const statusCls=status==='active'?'text-teal-400':'text-dim';
    return `<div class="card-inner px-3 py-2.5 flex items-center justify-between gap-2 text-xs">
      <div class="min-w-0">
        <div class="c-heading font-medium truncate">${escapeHtml(name)}</div>
        <div class="text-dim font-mono">#${id}</div>
      </div>
      <div class="text-right shrink-0">
        <div class="text-dim">${cnt}</div>
        <div class="${statusCls}">${escapeHtml(status)}</div>
      </div>
    </div>`;
  }).join('');
}
async function fetchTeamGroupsMeta(team){
  const cacheKey=team.api_base;
  let groups=teamGroupsCache[cacheKey];
  if(groups) return groups;
  const res=await fetch(API+'/api/s2a/fetch-groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_base:team.api_base,admin_key:team.admin_key})});
  if(!res.ok) throw new Error('获取分组失败');
  groups=await res.json();
  teamGroupsCache[cacheKey]=groups;
  return groups;
}
async function showTeamGroupsModal(name){
  const team=configData?.teams?.find(t=>t.name===name);
  if(!team) return;
  hideAddTeamForm();
  hideEditTeamForm();
  const modal=document.getElementById('team-groups-modal');
  modal.classList.remove('hidden');
  document.getElementById('tg-team-name').textContent=name;
  const teamList=document.getElementById('tg-team-groups-list');
  const freeList=document.getElementById('tg-free-groups-list');
  teamList.innerHTML='<div class="text-xs text-dim">加载中...</div>';
  freeList.innerHTML='<div class="text-xs text-dim">加载中...</div>';
  try{
    const groups=await fetchTeamGroupsMeta(team);
    teamList.innerHTML=renderGroupRows(team.group_ids||[],groups,'未配置 Team 分组');
    freeList.innerHTML=renderGroupRows(team.free_group_ids||[],groups,'未配置 Free 分组');
  }catch(e){
    teamList.innerHTML=`<div class="text-xs text-red-400">${escapeHtml(e.message||'获取失败')}</div>`;
    freeList.innerHTML='<div class="text-xs text-dim">-</div>';
  }
}
function hideTeamGroupsModal(){
  const modal=document.getElementById('team-groups-modal');
  if(modal) modal.classList.add('hidden');
}
function handleTeamModalBackdrop(evt,modalId,closeFnName){
  if(evt.target.id!==modalId) return;
  const fn=window[closeFnName];
  if(typeof fn==='function') fn();
}
document.addEventListener('keydown',e=>{
  if(e.key!=='Escape') return;
  if(!document.getElementById('team-groups-modal')?.classList.contains('hidden')) hideTeamGroupsModal();
  else if(!document.getElementById('edit-team-modal')?.classList.contains('hidden')) hideEditTeamForm();
  else if(!document.getElementById('add-team-modal')?.classList.contains('hidden')) hideAddTeamForm();
});
function renderTeams(){
  const el=document.getElementById('team-list');
  hideEditTeamForm();
  hideTeamGroupsModal();
  if(!configData||configData.teams.length===0){el.innerHTML='<p class="team-grid-empty text-sm text-dim text-center py-6">暂无号池</p>';return}
  el.innerHTML=configData.teams.map((t,idx)=>`
  <div class="row-item" id="team-card-${t.name||idx}">
    <div class="flex items-start justify-between mb-2">
      <div class="flex-1 grid grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6 gap-2 text-[.8125rem]">
        <div><span class="field-label">名称</span><div class="font-medium c-heading">${t.name||'<span class="text-red-400">(未命名)</span>'}</div></div>
        <div><span class="field-label">API</span><div class="font-mono text-xs text-dim-2 truncate max-w-[200px]">${t.api_base}</div></div>
        <div><span class="field-label">并发</span><div class="font-mono c-heading">${t.concurrency}</div></div>
        <div><span class="field-label">优先级</span><div class="font-mono c-heading">${t.priority}</div></div>
        <div><span class="field-label">分组</span><div class="font-mono c-heading">${groupSummaryText(t.group_ids)}</div></div>
        <div><span class="field-label">Free 分组</span><div class="font-mono ${t.free_group_ids?.length?'text-amber-400':'text-dim'}">${groupSummaryText(t.free_group_ids)}</div></div>
      </div>
      <div class="flex items-center gap-1 ml-3 shrink-0">
        <button onclick="showTeamGroupsModal('${t.name.replace(/'/g,"\\'")}')" class="btn btn-ghost text-xs py-1 px-2">分组详情</button>
        <button onclick="testS2a('${t.name.replace(/'/g,"\\'")}')" class="btn btn-ghost text-xs py-1 px-2" id="test-btn-${t.name||idx}">测试</button>
        <button onclick="editTeam('${t.name.replace(/'/g,"\\'")}',${idx})" class="btn btn-ghost text-xs py-1 px-2">编辑</button>
        <button onclick="deleteTeam('${t.name.replace(/'/g,"\\'")}',${idx})" class="btn btn-danger text-xs py-1 px-2">删除</button>
      </div>
    </div>
    <div class="flex items-center gap-4 text-[.8125rem] mt-1">
      <div><span class="field-label">Free 优先级</span><span class="font-mono ${t.free_group_ids?.length?(t.free_priority?'text-amber-400':'text-dim'):'text-dim'} ml-1">${t.free_group_ids?.length?(t.free_priority||'同 team'):'未启用'}</span></div>
      <div><span class="field-label">Free 并发</span><span class="font-mono ${t.free_group_ids?.length?(t.free_concurrency?'text-amber-400':'text-dim'):'text-dim'} ml-1">${t.free_group_ids?.length?(t.free_concurrency||'同 team'):'未启用'}</span></div>
    </div>
    <div id="s2a-stats-${t.name}" class="flex flex-col gap-1 text-xs text-dim pt-1 border-t mt-2" style="border-color:var(--border)">
      <div class="flex items-center gap-4">
        <span class="text-dim">账号统计</span>
        <span>可用 <span class="font-mono text-teal-400" id="stat-available-${t.name}">-</span></span>
        <span>活跃 <span class="font-mono text-dim-2" id="stat-active-${t.name}">-</span></span>
        <span>限流 <span class="font-mono text-red-400" id="stat-ratelimited-${t.name}">-</span></span>
        <button onclick="animateRefresh(this,()=>loadS2aStats('${t.name.replace(/'/g,"\\'")}'))" class="refresh-btn ml-auto" title="刷新"><svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.992 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg></button>
      </div>
      <div class="flex items-center gap-4 ${t.free_group_ids?.length?'':'hidden'}" id="s2a-free-stats-${t.name}">
        <span class="text-amber-400">Free 统计</span>
        <span>可用 <span class="font-mono text-amber-400" id="stat-free-available-${t.name}">-</span></span>
        <span>活跃 <span class="font-mono text-dim-2" id="stat-free-active-${t.name}">-</span></span>
        <span>限流 <span class="font-mono text-red-400" id="stat-free-ratelimited-${t.name}">-</span></span>
      </div>
    </div>
  </div>`).join('');
  // 自动加载统计；分组详情改为按需弹窗，避免卡片被长列表撑高
  configData.teams.forEach(t=>{loadS2aStats(t.name)});
}
async function testS2a(name){
  const btn=document.getElementById('test-btn-'+name);
  const orig=btn.textContent;btn.textContent='测试中...';btn.disabled=true;
  try{const d=await api(`/api/config/s2a/${encodeURIComponent(name)}/test`,{method:'POST'});toast(`${name}: ${d.message}`,'success')}
  catch(e){toast(`${name}: 连接失败`,'error')}
  finally{btn.textContent=orig;btn.disabled=false}
}
async function loadS2aStats(name){
  try{
    const d=await api(`/api/config/s2a/${encodeURIComponent(name)}/stats`);
    document.getElementById('stat-available-'+name).textContent=d.available;
    document.getElementById('stat-active-'+name).textContent=d.active;
    document.getElementById('stat-ratelimited-'+name).textContent=d.rate_limited;
    const freeEl=document.getElementById('stat-free-available-'+name);
    if(freeEl){
      document.getElementById('stat-free-available-'+name).textContent=d.free_available;
      document.getElementById('stat-free-active-'+name).textContent=d.free_active;
      document.getElementById('stat-free-ratelimited-'+name).textContent=d.free_rate_limited;
    }
  }catch{
    document.getElementById('stat-available-'+name).textContent='?';
    document.getElementById('stat-active-'+name).textContent='?';
    document.getElementById('stat-ratelimited-'+name).textContent='?';
    const freeEl=document.getElementById('stat-free-available-'+name);
    if(freeEl){
      document.getElementById('stat-free-available-'+name).textContent='?';
      document.getElementById('stat-free-active-'+name).textContent='?';
      document.getElementById('stat-free-ratelimited-'+name).textContent='?';
    }
  }
}
function renderConfigForm(){
  if(!configData)return;
  const d=configData.defaults,r=configData.register;
  const modeEl=document.getElementById('cfg-default-mode');
  const mode=modeEl?.value==='free'?'free':'team';
  if(modeEl) modeEl.value=mode;
  applyConfigDefaultsByMode(mode);
  document.getElementById('cfg-mail-base').value=r.mail_api_base;
  document.getElementById('cfg-mail-token').value=r.mail_api_token;
  document.getElementById('cfg-mail-timeout').value=r.mail_request_timeout_sec;
  document.getElementById('cfg-otp-retries').value=r.otp_max_retries;
  document.getElementById('cfg-req-timeout').value=r.request_timeout_sec;
  document.getElementById('cfg-mail-concurrency').value=r.mail_max_concurrency||50;
  document.getElementById('cfg-reg-log-mode').value=r.register_log_mode||'verbose';
  document.getElementById('cfg-reg-perf-mode').value=r.register_perf_mode||'baseline';
  document.getElementById('cfg-gptmail-key').value=r.chatgpt_mail_api_key||'';
  document.getElementById('cfg-tempmail-key').value=r.tempmail_api_key||'';
  document.getElementById('cfg-site-title').value=configData.site_title||'';
  renderEmailDomains();renderGptMailDomains();renderTempMailDomains();renderD1Cleanup();
}
function applySiteTitle(title){
  const t=title||'AutoTeam2S2A';
  document.title=t;
  const el=document.querySelector('header .c-heading');
  if(el) el.textContent=t;
}
async function saveSiteTitle(){
  const title=document.getElementById('cfg-site-title').value.trim();
  try{await api('/api/config/site_title',{method:'PUT',body:{site_title:title}});toast('站点标题已保存','success');loadConfig()}catch{}
}

// D1
function renderD1Cleanup(){
  if(!configData||!configData.d1_cleanup)return;
  const c=configData.d1_cleanup;
  document.getElementById('cfg-d1-enabled').checked=c.enabled;
  document.getElementById('cfg-d1-account-id').value=c.account_id||'';
  document.getElementById('cfg-d1-api-key').value=c.api_key||'';
  document.getElementById('cfg-d1-keep-percent').value=c.keep_percent;
  document.getElementById('cfg-d1-batch-size').value=c.batch_size;
  const rows=document.getElementById('d1-db-rows');rows.innerHTML='';
  (c.databases||[]).forEach(db=>addD1DbRow(db.name,db.id));
}
let d1DbRowCount=0;
function addD1DbRow(name='',id=''){
  const rid=d1DbRowCount++;const rows=document.getElementById('d1-db-rows');
  const row=document.createElement('div');row.className='flex items-center gap-2';row.id=`d1-db-row-${rid}`;
  row.innerHTML=`<input class="d1-db-name field-input flex-1" placeholder="名称" value="${name}"><input class="d1-db-id field-input flex-1" placeholder="ID" value="${id}"><button onclick="document.getElementById('d1-db-row-${rid}').remove()" class="btn btn-danger text-xs py-1 px-2">&times;</button>`;
  rows.appendChild(row);
}
async function saveD1Cleanup(){
  const databases=[];
  document.querySelectorAll('#d1-db-rows > div').forEach(row=>{const n=row.querySelector('.d1-db-name').value.trim(),i=row.querySelector('.d1-db-id').value.trim();if(n&&i) databases.push({name:n,id:i})});
  try{await api('/api/config/d1_cleanup',{method:'PUT',body:{enabled:document.getElementById('cfg-d1-enabled').checked,account_id:document.getElementById('cfg-d1-account-id').value.trim(),api_key:document.getElementById('cfg-d1-api-key').value.trim(),keep_percent:parseFloat(document.getElementById('cfg-d1-keep-percent').value)||0.1,batch_size:parseInt(document.getElementById('cfg-d1-batch-size').value)||5000,databases}});toast('D1 配置已保存','success');loadConfig()}catch{}
}

// Teams
function showAddTeamForm(){
  hideTeamGroupsModal();
  hideEditTeamForm();
  document.getElementById('add-team-modal').classList.remove('hidden');
  fillS2aExtraForm('at');
}
function hideAddTeamForm(){
  document.getElementById('add-team-modal').classList.add('hidden');
  document.getElementById('at-fetch-groups-btn').disabled=false;
  document.getElementById('at-fetch-groups-btn').textContent='获取分组';
  document.getElementById('at-groups-status').textContent='';
  document.getElementById('at-groups-container').innerHTML='<span class="text-xs text-dim">请先填写 API 地址和 Admin Key</span>';
  document.getElementById('at-free-groups-container').innerHTML='<span class="text-xs text-dim">获取分组后可选</span>';
  fillS2aExtraForm('at');
}
let atGroupsData=[];
async function fetchGroups(){
  const apiBase=document.getElementById('at-api').value.trim();
  const adminKey=document.getElementById('at-key').value.trim();
  if(!apiBase||!adminKey){toast('请先填写 API 地址和 Admin Key','error');return}
  const btn=document.getElementById('at-fetch-groups-btn');
  const status=document.getElementById('at-groups-status');
  btn.disabled=true;btn.textContent='获取中...';status.textContent='';
  try{
    const res=await fetch(API+'/api/s2a/fetch-groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_base:apiBase,admin_key:adminKey})});
    const data=await res.json();
    if(!res.ok) throw new Error(data.error||'请求失败');
    atGroupsData=data;
    const container=document.getElementById('at-groups-container');
    if(data.length===0){container.innerHTML='<span class="text-xs text-dim">未找到分组</span>';return}
    const groupHtml=data.map(g=>`<label class="flex items-center gap-2 py-1 cursor-pointer text-xs" style="border-bottom:1px solid var(--border)">
      <input type="checkbox" value="${g.id}" class="at-group-cb" style="accent-color:#14b8a6">
      <span class="c-heading font-medium">${g.name}</span>
      <span class="text-dim font-mono">#${g.id}</span>
      <span class="text-dim">${g.account_count} 账号</span>
      <span class="text-[.6rem] px-1.5 py-0.5 rounded" style="background:${g.status==='active'?'rgba(45,212,191,.12);color:#2dd4bf':'var(--ghost);color:var(--text-dim)'}">${g.status}</span>
    </label>`).join('');
    container.innerHTML=groupHtml;
    // free 分组选择器
    const freeContainer=document.getElementById('at-free-groups-container');
    freeContainer.innerHTML=data.map(g=>`<label class="flex items-center gap-2 py-1 cursor-pointer text-xs" style="border-bottom:1px solid var(--border)">
      <input type="checkbox" value="${g.id}" class="at-free-group-cb" style="accent-color:#f59e0b">
      <span class="c-heading font-medium">${g.name}</span>
      <span class="text-dim font-mono">#${g.id}</span>
    </label>`).join('');
    status.textContent=`共 ${data.length} 个分组`;status.style.color='#2dd4bf';
  }catch(e){status.textContent=e.message;status.style.color='#f87171';toast(e.message,'error')}
  finally{btn.disabled=false;btn.textContent='获取分组'}
}
function autoFetchGroups(){
  const apiBase=document.getElementById('at-api').value.trim();
  const adminKey=document.getElementById('at-key').value.trim();
  if(apiBase&&adminKey) fetchGroups();
}
function getSelectedGroupIds(){
  return Array.from(document.querySelectorAll('.at-group-cb:checked')).map(cb=>parseInt(cb.value));
}
function getSelectedFreeGroupIds(){
  return Array.from(document.querySelectorAll('.at-free-group-cb:checked')).map(cb=>parseInt(cb.value));
}
function syncOpenaiWsModeState(prefix){
  const enabled=document.getElementById(`${prefix}-openai-ws-v2-enabled`).checked;
  const mode=document.getElementById(`${prefix}-openai-ws-v2-mode`);
  mode.disabled=!enabled;
}
function fillS2aExtraForm(prefix,extra){
  const e=extra||{};
  const hasExtra=extra!==undefined&&extra!==null;
  document.getElementById(`${prefix}-openai-passthrough`).checked=hasExtra?e.openai_passthrough===true:true;
  document.getElementById(`${prefix}-openai-ws-v2-enabled`).checked=hasExtra?e.openai_oauth_responses_websockets_v2_enabled===true:true;
  document.getElementById(`${prefix}-openai-ws-v2-mode`).value=e.openai_oauth_responses_websockets_v2_mode||'passthrough';
  syncOpenaiWsModeState(prefix);
}
function buildS2aExtraPayload(prefix){
  const extra={};
  if(document.getElementById(`${prefix}-openai-passthrough`).checked){
    extra.openai_passthrough=true;
  }
  if(document.getElementById(`${prefix}-openai-ws-v2-enabled`).checked){
    extra.openai_oauth_responses_websockets_v2_enabled=true;
    extra.openai_oauth_responses_websockets_v2_mode=document.getElementById(`${prefix}-openai-ws-v2-mode`).value||'passthrough';
  }
  return extra;
}
async function submitAddTeam(){
  const groups=getSelectedGroupIds();
  const freeGroups=getSelectedFreeGroupIds();
  const freePri=parseInt(document.getElementById('at-free-priority').value);
  const freeConc=parseInt(document.getElementById('at-free-concurrency').value);
  try{await api('/api/config/s2a',{method:'POST',body:{name:document.getElementById('at-name').value.trim(),api_base:document.getElementById('at-api').value.trim(),admin_key:document.getElementById('at-key').value.trim(),concurrency:parseInt(document.getElementById('at-concurrency').value)||50,priority:parseInt(document.getElementById('at-priority').value)||30,group_ids:groups,free_group_ids:freeGroups,free_priority:freePri||undefined,free_concurrency:freeConc||undefined,extra:buildS2aExtraPayload('at')}});toast('号池已添加','success');hideAddTeamForm();['at-name','at-api','at-key','at-free-priority','at-free-concurrency'].forEach(id=>document.getElementById(id).value='');atGroupsData=[];loadConfig()}catch{}
}
async function deleteTeam(name,idx){
  const label=name||`#${idx}`;
  if(!confirm(`删除号池 "${label}"?`))return;
  try{
    // 名称为空时按索引删除
    const url=name?`/api/config/s2a/${encodeURIComponent(name)}`:`/api/config/s2a/by-index/${idx}`;
    await api(url,{method:'DELETE'});
    toast(`${label} 已删除`,'success');loadConfig()
  }catch{}
}
let editTeamName=null;
let editTeamIdx=null;
let etGroupsData=[];
function hideEditTeamForm(){
  document.getElementById('edit-team-modal').classList.add('hidden');
  document.getElementById('et-fetch-groups-btn').disabled=false;
  document.getElementById('et-fetch-groups-btn').textContent='获取分组';
  document.getElementById('et-groups-status').textContent='';
  editTeamName=null;editTeamIdx=null;
}
function editTeam(name,idx){
  editTeamName=name;
  editTeamIdx=idx;
  const t=configData?.teams?.[idx];if(!t) return;
  hideAddTeamForm();
  hideTeamGroupsModal();
  document.getElementById('edit-team-modal').classList.remove('hidden');
  document.getElementById('et-name').value=t.name||'';
  document.getElementById('et-api').value=t.api_base||'';
  document.getElementById('et-key').value=t.admin_key||'';
  document.getElementById('et-concurrency').value=t.concurrency||50;
  document.getElementById('et-priority').value=t.priority||30;
  document.getElementById('et-free-priority').value=t.free_priority||'';
  document.getElementById('et-free-concurrency').value=t.free_concurrency||'';
  fillS2aExtraForm('et',t.extra);
  // show current group IDs as text first, user can fetch to get checkboxes
  document.getElementById('et-groups-container').innerHTML=t.group_ids?.length?`<span class="text-xs text-dim">当前: [${t.group_ids.join(', ')}] — 点击获取分组可修改</span>`:'<span class="text-xs text-dim">点击获取分组</span>';
  document.getElementById('et-free-groups-container').innerHTML=t.free_group_ids?.length?`<span class="text-xs text-dim">当前: [${t.free_group_ids.join(', ')}] — 获取分组后可修改</span>`:'<span class="text-xs text-dim">获取分组后可选</span>';
  etGroupsData=[];
  // auto-fetch if api_base and admin_key are present
  if(t.api_base&&t.admin_key) fetchGroupsEdit();
}
async function fetchGroupsEdit(){
  const apiBase=document.getElementById('et-api').value.trim();
  const adminKey=document.getElementById('et-key').value.trim();
  if(!apiBase||!adminKey){toast('请先填写 API 地址和 Admin Key','error');return}
  const btn=document.getElementById('et-fetch-groups-btn');
  const status=document.getElementById('et-groups-status');
  btn.disabled=true;btn.textContent='获取中...';status.textContent='';
  try{
    const res=await fetch(API+'/api/s2a/fetch-groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_base:apiBase,admin_key:adminKey})});
    const data=await res.json();if(!res.ok) throw new Error(data.error||'请求失败');
    etGroupsData=data;
    const t=editTeamIdx!=null?configData?.teams?.[editTeamIdx]:configData?.teams?.find(x=>x.name===editTeamName);
    const curGroups=t?.group_ids||[];const curFree=t?.free_group_ids||[];
    const container=document.getElementById('et-groups-container');
    if(!data.length){container.innerHTML='<span class="text-xs text-dim">未找到分组</span>';return}
    container.innerHTML=data.map(g=>`<label class="flex items-center gap-2 py-1 cursor-pointer text-xs" style="border-bottom:1px solid var(--border)">
      <input type="checkbox" value="${g.id}" class="et-group-cb" ${curGroups.includes(g.id)?'checked':''} style="accent-color:#14b8a6">
      <span class="c-heading font-medium">${g.name}</span><span class="text-dim font-mono">#${g.id}</span>
      <span class="text-dim">${g.account_count} 账号</span>
      <span class="text-[.6rem] px-1.5 py-0.5 rounded" style="background:${g.status==='active'?'rgba(45,212,191,.12);color:#2dd4bf':'var(--ghost);color:var(--text-dim)'}">${g.status}</span>
    </label>`).join('');
    const freeContainer=document.getElementById('et-free-groups-container');
    freeContainer.innerHTML=data.map(g=>`<label class="flex items-center gap-2 py-1 cursor-pointer text-xs" style="border-bottom:1px solid var(--border)">
      <input type="checkbox" value="${g.id}" class="et-free-group-cb" ${curFree.includes(g.id)?'checked':''} style="accent-color:#f59e0b">
      <span class="c-heading font-medium">${g.name}</span><span class="text-dim font-mono">#${g.id}</span>
    </label>`).join('');
    status.textContent=`共 ${data.length} 个分组`;status.style.color='#2dd4bf';
  }catch(e){status.textContent=e.message;status.style.color='#f87171';toast(e.message,'error')}
  finally{btn.disabled=false;btn.textContent='获取分组'}
}
function autoFetchGroupsEdit(){
  const apiBase=document.getElementById('et-api').value.trim();
  const adminKey=document.getElementById('et-key').value.trim();
  if(apiBase&&adminKey) fetchGroupsEdit();
}
async function submitEditTeam(){
  if(editTeamIdx==null&&!editTeamName) return;
  const newName=document.getElementById('et-name').value.trim();
  const body={
    name:newName||undefined,
    api_base:document.getElementById('et-api').value.trim()||undefined,
    admin_key:document.getElementById('et-key').value.trim()||undefined,
    concurrency:parseInt(document.getElementById('et-concurrency').value)||undefined,
    priority:parseInt(document.getElementById('et-priority').value)||undefined,
  };
  // only send group_ids/free_group_ids if user fetched groups (checkboxes exist)
  if(document.querySelector('.et-group-cb')){
    body.group_ids=Array.from(document.querySelectorAll('.et-group-cb:checked')).map(cb=>parseInt(cb.value));
  }
  if(document.querySelector('.et-free-group-cb')){
    body.free_group_ids=Array.from(document.querySelectorAll('.et-free-group-cb:checked')).map(cb=>parseInt(cb.value));
  }
  const fp=document.getElementById('et-free-priority').value;
  const fc=document.getElementById('et-free-concurrency').value;
  body.free_priority=fp?parseInt(fp):null;
  body.free_concurrency=fc?parseInt(fc):null;
  body.extra=buildS2aExtraPayload('et');
  // 名称为空时按索引更新
  const url=editTeamName?`/api/config/s2a/${encodeURIComponent(editTeamName)}`:`/api/config/s2a/by-index/${editTeamIdx}`;
  try{await api(url,{method:'PUT',body});toast(`${newName||editTeamName||'号池'} 已更新`,'success');hideEditTeamForm();loadConfig()}catch{}
}

// Config save
async function saveDefaults(){
  const modeEl=document.getElementById('cfg-default-mode');
  const mode=modeEl?.value==='free'?'free':'team';
  try{await api('/api/config/defaults',{method:'PUT',body:{mode,target_count:parseInt(document.getElementById('cfg-target').value),register_workers:parseInt(document.getElementById('cfg-reg-workers').value),rt_workers:parseInt(document.getElementById('cfg-rt-workers').value),rt_retries:parseInt(document.getElementById('cfg-rt-retries').value)}});toast('参数已保存','success');loadConfig()}catch{}
}
async function saveRegister(){try{await api('/api/config/register',{method:'PUT',body:{mail_api_base:document.getElementById('cfg-mail-base').value,mail_api_token:document.getElementById('cfg-mail-token').value,mail_request_timeout_sec:parseInt(document.getElementById('cfg-mail-timeout').value),otp_max_retries:parseInt(document.getElementById('cfg-otp-retries').value),request_timeout_sec:parseInt(document.getElementById('cfg-req-timeout').value),mail_max_concurrency:parseInt(document.getElementById('cfg-mail-concurrency').value)||50,register_log_mode:document.getElementById('cfg-reg-log-mode').value||'verbose',register_perf_mode:document.getElementById('cfg-reg-perf-mode').value||'baseline'}});toast('配置已保存','success');loadConfig()}catch{}}
async function saveGptMail(){const key=document.getElementById('cfg-gptmail-key').value.trim();try{await api('/api/config/register',{method:'PUT',body:{chatgpt_mail_api_key:key}});toast('GPTMail API Key 已保存','success');loadConfig()}catch{}}
function renderGptMailDomains(){
  if(!configData)return;const doms=configData.chatgpt_mail_domains||[];
  document.getElementById('gptmail-domain-count').textContent=doms.length;
  const el=document.getElementById('gptmail-domain-list');
  if(!doms.length){el.innerHTML='<p class="text-xs text-dim">未配置（使用随机域名）</p>';return}
  el.innerHTML=doms.map(d=>`<span class="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style="background:var(--domain-bg);border:1px solid var(--domain-border);color:var(--text-dim2)">${d}<button onclick="deleteGptMailDomain('${d.replace(/'/g,"\\'")}')" class="text-red-400 hover:text-red-300 ml-1">&times;</button></span>`).join('');
}
async function addGptMailDomain(){const v=document.getElementById('new-gptmail-domain').value.trim();if(!v){toast('请输入域名','error');return}try{await api('/api/config/gptmail_domains',{method:'POST',body:{domain:v}});document.getElementById('new-gptmail-domain').value='';toast('域名已添加','success');loadConfig()}catch{}}
async function deleteGptMailDomain(d){try{await api('/api/config/gptmail_domains',{method:'DELETE',body:{domain:d}});toast('域名已删除','success');loadConfig()}catch{}}
async function testGptMail(){
  const key=document.getElementById('cfg-gptmail-key').value.trim();
  if(!key){toast('请先输入 API Key 并保存','error');return}
  const st=document.getElementById('gptmail-status');const info=document.getElementById('gptmail-info');
  st.textContent='测试中…';st.className='text-xs text-amber-400';info.classList.add('hidden');
  try{
    // 先保存 key，再通过后端代理测试
    await api('/api/config/register',{method:'PUT',body:{chatgpt_mail_api_key:key}});
    const json=await api('/api/test/gptmail',{method:'POST'});
    const d=json.data;
    st.textContent='连接成功';st.className='text-xs text-teal-400';
    info.classList.remove('hidden');
    info.innerHTML=`<span>累计邮件 <span class="c-heading font-mono">${(d.total_emails||0).toLocaleString()}</span></span><span>今日邮件 <span class="c-heading font-mono">${d.today_emails||0}</span></span><span>活跃域名 <span class="c-heading font-mono">${d.active_domains||0}</span></span><span>总域名 <span class="c-heading font-mono">${d.domain_count||0}</span></span>`;
  }catch(e){st.textContent='连接失败';st.className='text-xs text-red-400'}
}
async function saveToFile(){try{const d=await api('/api/config/save',{method:'POST'});toast(d.message,'success')}catch{}}

// TempMail
async function saveTempMail(){const key=document.getElementById('cfg-tempmail-key').value.trim();try{await api('/api/config/register',{method:'PUT',body:{tempmail_api_key:key}});toast('TempMail 配置已保存','success');loadConfig()}catch{}}
function renderTempMailDomains(){
  if(!configData)return;const doms=configData.tempmail_domains||[];
  document.getElementById('tempmail-domain-count').textContent=doms.length;
  const el=document.getElementById('tempmail-domain-list');
  if(!doms.length){el.innerHTML='<p class="text-xs text-dim">未配置（使用自动获取）</p>';return}
  el.innerHTML=doms.map(d=>`<span class="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style="background:var(--domain-bg);border:1px solid var(--domain-border);color:var(--text-dim2)">${d}<button onclick="deleteTempMailDomain('${d.replace(/'/g,"\\\'")}')" class="text-red-400 hover:text-red-300 ml-1">&times;</button></span>`).join('');
}
async function addTempMailDomain(){const v=document.getElementById('new-tempmail-domain').value.trim();if(!v){toast('请输入域名','error');return}try{await api('/api/config/tempmail_domains',{method:'POST',body:{domain:v}});document.getElementById('new-tempmail-domain').value='';toast('域名已添加','success');loadConfig()}catch{}}
async function deleteTempMailDomain(d){try{await api('/api/config/tempmail_domains',{method:'DELETE',body:{domain:d}});toast('域名已删除','success');loadConfig()}catch{}}
async function testTempMail(){
  const key=document.getElementById('cfg-tempmail-key').value.trim();
  if(!key){toast('请先输入 API Key 并保存','error');return}
  const st=document.getElementById('tempmail-status');const info=document.getElementById('tempmail-info');
  st.textContent='测试中…';st.className='text-xs text-amber-400';info.classList.add('hidden');
  try{
    await api('/api/config/register',{method:'PUT',body:{tempmail_api_key:key}});
    const json=await api('/api/test/tempmail',{method:'POST'});
    const d=json.data;
    st.textContent='连接成功';st.className='text-xs text-teal-400';
    info.classList.remove('hidden');
    info.innerHTML=`<span>邮箱数 <span class="c-heading font-mono">${d.mailbox_count||0}</span></span>`;
  }catch(e){st.textContent='连接失败';st.className='text-xs text-red-400'}
}

// Domains
function renderEmailDomains(){
  if(!configData)return;const doms=configData.email_domains||[];
  document.getElementById('domain-count').textContent=doms.length;
  const el=document.getElementById('email-domain-list');
  if(!doms.length){el.innerHTML='<p class="text-xs text-dim">暂无域名</p>';return}
  el.innerHTML=doms.map(d=>`<span class="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md font-mono" style="background:var(--domain-bg);border:1px solid var(--domain-border);color:var(--text-dim2)">${d}<button onclick="deleteEmailDomain('${d.replace(/'/g,"\\'")}')" class="text-red-400 hover:text-red-300 ml-1">&times;</button></span>`).join('');
}
async function addEmailDomain(){const input=document.getElementById('new-domain');const domain=input.value.trim();if(!domain){toast('请输入域名','error');return}try{await api('/api/config/email_domains',{method:'POST',body:{domain}});toast(`${domain} 已添加`,'success');input.value='';loadConfig()}catch{}}
async function deleteEmailDomain(domain){try{await api('/api/config/email_domains',{method:'DELETE',body:{domain}});toast(`${domain} 已删除`,'success');loadConfig()}catch{}}

// Tasks
async function quickCreateTask(){
  const team=document.getElementById('q-team').value;if(!team){toast('请先配置号池','error');return}
  try{const d=await api('/api/tasks',{method:'POST',body:{team,target:parseInt(document.getElementById('q-target').value),register_workers:parseInt(document.getElementById('q-reg-workers').value),rt_workers:parseInt(document.getElementById('q-rt-workers').value),push_s2a:true,mail_provider:document.getElementById('q-mail').value,free_mode:document.getElementById('q-mode').value==='free'}});toast(`任务 ${d.task_id} 已创建`,'success');switchTab('tasks');setTimeout(()=>showTaskDetail(d.task_id),300)}catch{}
}
async function refreshTasks(){
  try{
    const [d, sd]=await Promise.all([api('/api/tasks'), api('/api/schedules').catch(()=>({schedules:[]}))]);
    const el=document.getElementById('task-list');
    const runningTasks=d.tasks.filter(t=>t.status==='running'||t.status==='pending').length;
    const runningScheds=(sd.schedules||[]).filter(s=>s.running);
    document.getElementById('stat-running').textContent=runningTasks+runningScheds.length;
    let html='';
    // running schedules
    if(runningScheds.length){
      html+=runningScheds.map(s=>`<div class="row-item flex items-center justify-between cursor-pointer" onclick="switchTab('logs')">
        <div class="flex items-center gap-3 flex-1 min-w-0">
          <span class="badge badge-run">调度中</span>
          <span class="text-xs font-medium c-heading">${s.name}</span>
          <span class="text-xs text-dim">RT目标 x${s.target_count}/批</span>
          <span class="text-xs text-dim">${s.start_time}-${s.end_time}</span>
          <span class="text-xs text-dim">间隔 ${s.batch_interval_mins}m</span>
          <span class="text-xs text-teal-400">点击查看日志</span>
        </div>
        <button onclick="event.stopPropagation();stopSchedule('${s.name}')" class="btn btn-danger text-xs py-1 px-2">停止</button>
      </div>`).join('');
    }
    // manual tasks
    html+=d.tasks.map(t=>{
      const bc={pending:'badge-warn',running:'badge-run',completed:'badge-ok',failed:'badge-err',cancelled:'badge-off'};
      const lb={pending:'等待',running:'运行',completed:'完成',failed:'失败',cancelled:'取消'};
      const cc=t.status==='pending'||t.status==='running';
      const tm=new Date(t.created_at).toLocaleTimeString('zh-CN');
      return `<div class="row-item flex items-center justify-between cursor-pointer" onclick="showTaskDetail('${t.task_id}')">
        <div class="flex items-center gap-3 flex-1 min-w-0">
          <span class="badge ${bc[t.status]||'badge-off'}">${lb[t.status]||t.status}</span>
          <span class="text-xs font-mono text-dim">${t.task_id}</span>
          <span class="text-xs text-dim-1">${t.team}</span>
          <span class="text-xs text-dim">x${t.target}</span>
          <span class="text-xs text-dim">${tm}</span>
        </div>
        ${cc?`<button onclick="event.stopPropagation();cancelTask('${t.task_id}')" class="btn btn-danger text-xs py-1 px-2">取消</button>`:''}
      </div>`}).join('');
    if(!html) html='<p class="text-sm text-dim text-center py-8">暂无任务</p>';
    el.innerHTML=html;
  }catch{}
}
let _tpTimer=null;
let _tpTaskId=null;
let _tpElapsedTick=null;
let _tpStartedAtMs=0;
let _tpFinalElapsedSec=null;
function formatElapsedSec(sec){
  const v=Math.max(0,Number(sec)||0);
  return `${v.toFixed(1)}s`;
}
function renderTaskElapsed(){
  const el=document.getElementById('tp-elapsed');
  if(!el) return;
  const sec=_tpFinalElapsedSec!=null
    ? Number(_tpFinalElapsedSec)||0
    : (_tpStartedAtMs>0?(Date.now()-_tpStartedAtMs)/1000:0);
  el.textContent=`耗时 ${formatElapsedSec(sec)}`;
}
function startTaskElapsedTick(){
  if(_tpElapsedTick){clearInterval(_tpElapsedTick);_tpElapsedTick=null}
  renderTaskElapsed();
  _tpElapsedTick=setInterval(renderTaskElapsed,100);
}
function stopTaskElapsedTick(){
  if(_tpElapsedTick){clearInterval(_tpElapsedTick);_tpElapsedTick=null}
}
function showTaskDetail(taskId){
  _tpTaskId=taskId;
  _tpStartedAtMs=Date.now();
  _tpFinalElapsedSec=null;
  const panel=document.getElementById('task-progress-panel');
  panel.classList.remove('hidden');
  // reset
  ['tp-reg-ok','tp-rt-ok','tp-s2a-ok','tp-target','tp-reg-ok2','tp-reg-fail','tp-rt-ok2','tp-rt-fail','tp-s2a-ok2','tp-s2a-fail'].forEach(id=>{
    document.getElementById(id).textContent='0';
  });
  ['tp-reg-bar','tp-rt-bar','tp-s2a-bar'].forEach(id=>{
    document.getElementById(id).style.width='0%';
  });
  document.getElementById('tp-id').textContent=taskId;
  document.getElementById('tp-stage').textContent='';
  renderTaskElapsed();
  document.getElementById('tp-error').classList.add('hidden');
  document.getElementById('tp-report').classList.add('hidden');
  document.getElementById('tp-badge').className='badge badge-run tp-pulse';
  document.getElementById('tp-badge').textContent='运行';
  startTaskElapsedTick();
  // 预取任务详情，用 created_at 校准耗时；若已完成则直接显示最终耗时
  api(`/api/tasks/${taskId}`).then(t=>{
    if(_tpTaskId!==taskId) return;
    if(t&&t.created_at){
      const started=Date.parse(t.created_at);
      if(!Number.isNaN(started)) _tpStartedAtMs=started;
    }
    if(t&&t.report&&typeof t.report.elapsed_secs==='number'){
      _tpFinalElapsedSec=t.report.elapsed_secs;
    }
    renderTaskElapsed();
  }).catch(()=>{});
  // start polling
  pollTaskProgress();
  _tpTimer=setInterval(pollTaskProgress,1500);
  panel.scrollIntoView({behavior:'smooth',block:'start'});
}
function closeTaskProgress(){
  if(_tpTimer){clearInterval(_tpTimer);_tpTimer=null}
  stopTaskElapsedTick();
  _tpTaskId=null;
  _tpStartedAtMs=0;
  _tpFinalElapsedSec=null;
  const elapsedEl=document.getElementById('tp-elapsed');
  if(elapsedEl) elapsedEl.textContent='耗时 0.0s';
  document.getElementById('task-progress-panel').classList.add('hidden');
}
async function pollTaskProgress(){
  if(!_tpTaskId)return;
  try{
    const p=await api(`/api/tasks/${_tpTaskId}/progress`);
    const target=p.target||1;
    // badge
    const badge=document.getElementById('tp-badge');
    const bc={pending:'badge-warn',running:'badge-run',completed:'badge-ok',failed:'badge-err',cancelled:'badge-off'};
    const lb={pending:'等待',running:'运行',completed:'完成',failed:'失败',cancelled:'取消'};
    badge.className=`badge ${bc[p.status]||'badge-off'} ${p.status==='running'?'tp-pulse':''}`;
    badge.textContent=lb[p.status]||p.status;
    // stage
    document.getElementById('tp-stage').textContent=p.stage||'';
    // counters
    document.getElementById('tp-target').textContent=target;
    document.getElementById('tp-reg-ok').textContent=p.reg_ok;
    document.getElementById('tp-rt-ok').textContent=p.rt_ok;
    document.getElementById('tp-s2a-ok').textContent=p.s2a_ok;
    document.getElementById('tp-reg-ok2').textContent=p.reg_ok;
    document.getElementById('tp-reg-fail').textContent=p.reg_failed;
    document.getElementById('tp-rt-ok2').textContent=p.rt_ok;
    document.getElementById('tp-rt-fail').textContent=p.rt_failed;
    document.getElementById('tp-s2a-ok2').textContent=p.s2a_ok;
    document.getElementById('tp-s2a-fail').textContent=p.s2a_failed;
    // bars
    const regTotal=p.reg_ok+p.reg_failed;
    const rtTotal=p.rt_ok+p.rt_failed;
    const s2aTotal=p.s2a_ok+p.s2a_failed;
    document.getElementById('tp-reg-bar').style.width=`${Math.min(100,regTotal/target*100).toFixed(1)}%`;
    document.getElementById('tp-rt-bar').style.width=`${Math.min(100,rtTotal/target*100).toFixed(1)}%`;
    document.getElementById('tp-s2a-bar').style.width=`${s2aTotal>0?Math.min(100,s2aTotal/Math.max(1,p.rt_ok)*100).toFixed(1):0}%`;
    // if done, show report
    if(p.status==='completed'||p.status==='failed'||p.status==='cancelled'){
      if(_tpTimer){clearInterval(_tpTimer);_tpTimer=null}
      if(_tpFinalElapsedSec==null&&_tpStartedAtMs>0){
        _tpFinalElapsedSec=(Date.now()-_tpStartedAtMs)/1000;
        renderTaskElapsed();
      }
      // fetch full task detail for report & error
      try{
        const t=await api(`/api/tasks/${_tpTaskId}`);
        if(t.error){
          const errEl=document.getElementById('tp-error');
          errEl.textContent=t.error;
          errEl.classList.remove('hidden');
        }
        if(t.report){
          const r=t.report;
          const rp=document.getElementById('tp-report');
          rp.classList.remove('hidden');
          if(typeof r.elapsed_secs==='number'){
            _tpFinalElapsedSec=r.elapsed_secs;
            renderTaskElapsed();
          }
          const totalS2aOk=Number(r.s2a_ok??0);
          const totalS2aFailed=Number(r.s2a_failed??0);
          const freeS2aOk=Number(r.free_s2a_ok??0);
          const freeS2aFailed=Number(r.free_s2a_failed??0);
          const teamS2aOk=Math.max(0,totalS2aOk-freeS2aOk);
          const teamS2aFailed=Math.max(0,totalS2aFailed-freeS2aFailed);
          let lines=[
            `耗时: ${r.elapsed_secs?.toFixed(1)}s`,
            `注册: ${r.registered_ok} 成功 / ${r.registered_failed} 失败`,
            `RT: ${r.rt_ok} 成功 / ${r.rt_failed} 失败`,
          ];
          if(r.s2a_ok!==undefined){
            lines.push(`S2A 总计: ${totalS2aOk} 成功 / ${totalS2aFailed} 失败`);
            lines.push(`S2A Team分组: ${teamS2aOk} 成功 / ${teamS2aFailed} 失败`);
            lines.push(`S2A Free分组: ${freeS2aOk} 成功 / ${freeS2aFailed} 失败`);
          }
          if(r.output_files?.length) lines.push(`文件: ${r.output_files.join(', ')}`);
          document.getElementById('tp-report-content').innerHTML=lines.map(l=>`<div>${l}</div>`).join('');
        }
      }catch{}
    }
  }catch{}
}
async function cancelTask(id){if(!confirm('取消此任务?'))return;try{await api(`/api/tasks/${id}/cancel`,{method:'POST'});toast('取消请求已发送','success');refreshTasks()}catch{}}
function startTaskPoll(){stopTaskPoll();taskPollTimer=setInterval(refreshTasks,3000)}
function stopTaskPoll(){if(taskPollTimer){clearInterval(taskPollTimer);taskPollTimer=null}}

// Schedules
function normalizeRegisterPerfMode(mode){
  if(mode==='current') return 'baseline';
  if(mode==='optimized') return 'adaptive';
  return mode||'baseline';
}
function currentRegisterPerfMode(){
  return normalizeRegisterPerfMode((configData&&configData.register&&configData.register.register_perf_mode)||'baseline');
}
function scheduleLogModeLabel(mode){
  return mode==='summary'?'汇总（降噪）':'详细（原格式）';
}
function schedulePerfModeLabel(mode){
  return normalizeRegisterPerfMode(mode)==='adaptive'?'adaptive（自适应）':'baseline（基线）';
}
function scheduleLogModeText(mode){
  if(!mode) return `继承全局（${scheduleLogModeLabel(currentRegisterLogMode())}）`;
  return scheduleLogModeLabel(mode);
}
function schedulePerfModeText(mode){
  if(!mode) return `继承全局（${schedulePerfModeLabel(currentRegisterPerfMode())}）`;
  return schedulePerfModeLabel(mode);
}
async function loadSchedules(){
  try{const d=await api('/api/schedules');const el=document.getElementById('schedule-list');
  if(!d.schedules||!d.schedules.length){el.innerHTML='<p class="text-sm text-dim text-center py-8">暂无定时计划</p>';return}
  el.innerHTML=d.schedules.map(s=>{
    const isCooldown=!!s.cooldown;
    const isPending=!!s.pending;
    const bc=s.running?'badge-run':isCooldown?'badge-warn':isPending?'badge-warn':s.enabled?'badge-ok':'badge-off';
    const bt=s.running?'运行中':isCooldown?'准备中':isPending?'等待中':s.enabled?'已启用':'已禁用';
    const dt=s.distribution.map(d=>`${d.team}:${d.percent}%`).join(' / ');
    const logModeTxt=scheduleLogModeText(s.register_log_mode);
    const perfModeTxt=schedulePerfModeText(s.register_perf_mode);
    const esc=n=>String(n).replace(/'/g,"\\'");
    const safeName=esc(s.name);
    const startBtn=(s.running||isCooldown)
      ?`<button onclick="stopSchedule('${safeName}')" class="btn btn-danger text-xs py-1 px-2">停止</button>`
      :isPending
        ?`<button onclick="stopSchedule('${safeName}')" class="btn btn-danger text-xs py-1 px-2">取消等待</button>`
        :`<button onclick="triggerSchedule('${safeName}')" class="btn btn-ghost text-xs py-1 px-2">启动</button>`;
    // 运行信息
    let runHtml='';
    if(s.running&&s.run_info){
      const ri=s.run_info;
      const batchTxt=ri.batch_num>0?`第 ${ri.batch_num} 批`:'执行中';
      const nextTxt=ri.next_batch_at?`下批 ${ri.next_batch_at}`:'执行中...';
      runHtml=`<div class="flex items-center gap-3 mt-1.5 text-xs">
        <span class="text-amber-400 font-medium">${batchTxt}</span>
        <span class="text-dim font-mono">${nextTxt}</span>
      </div>`;
    }else if(isCooldown&&s.run_info){
      const ri=s.run_info;
      const prepTxt=ri.batch_num>0?`第 ${ri.batch_num} 批后冷却中`:'准备中';
      const nextTxt=ri.next_batch_at?`下批 ${ri.next_batch_at}`:'等待调度...';
      runHtml=`<div class="flex items-center gap-3 mt-1.5 text-xs">
        <span class="text-amber-400 font-medium">${prepTxt}</span>
        <span class="text-dim font-mono">${nextTxt}</span>
      </div>`;
    }else if(isPending&&s.pending_info){
      const blockedBy=s.pending_info.blocked_by?`等待 ${s.pending_info.blocked_by}`:'等待调度槽位';
      const sinceTxt=s.pending_info.pending_since?`自 ${s.pending_info.pending_since}`:'等待中...';
      runHtml=`<div class="flex items-center gap-3 mt-1.5 text-xs">
        <span class="text-amber-400 font-medium">${blockedBy}</span>
        <span class="text-dim font-mono">${sinceTxt}</span>
      </div>`;
    }
    const runOnceBtn=(s.running||isPending||isCooldown)?'':`<button id="run-once-${safeName}" onclick="runOnceSchedule('${safeName}')" class="btn btn-ghost text-xs py-1 px-2">运行一次</button>`;
    return `<div class="row-item" style="padding:10px 14px">
      <div class="flex items-center justify-between mb-1.5">
        <div class="flex items-center gap-2.5">
          <span class="text-sm font-medium c-heading">${s.name}</span>
          <span class="badge ${bc}">${bt}</span>
          <code class="text-xs font-mono text-dim px-2 py-0.5 rounded" style="background:var(--ghost)">${s.start_time} - ${s.end_time}</code>
        </div>
        <div class="flex items-center gap-1">
          ${runOnceBtn}
          ${startBtn}
          <button onclick="editSchedule('${esc(s.name)}')" class="btn btn-ghost text-xs py-1 px-2">编辑</button>
          <button onclick="toggleSchedule('${esc(s.name)}')" class="btn btn-ghost text-xs py-1 px-2">${s.enabled?'禁用':'启用'}</button>
          <button onclick="deleteSchedule('${esc(s.name)}')" class="btn btn-danger text-xs py-1 px-2">删除</button>
        </div>
      </div>
      <div class="flex items-center gap-4 text-xs text-dim flex-wrap">
        <span>每批RT目标 <span class="font-mono text-dim-2">${s.target_count}</span></span>
        <span>间隔 <span class="font-mono text-dim-2">${s.batch_interval_mins}分</span></span>
        <span>优先级 <span class="font-mono text-dim-2">${s.priority??100}</span></span>
        <span>注册 <span class="font-mono text-dim-2">${s.register_workers||'默认'}</span></span>
        <span>RT <span class="font-mono text-dim-2">${s.rt_workers||'默认'}</span></span>
        <span>邮箱 <span class="text-dim-2">${s.mail_provider||(s.use_chatgpt_mail?'chatgpt':'kyx')}</span></span>
        <span>模式 <span class="text-dim-2">${s.free_mode?'free':'team'}</span></span>
        <span>日志 <span class="text-dim-2">${logModeTxt}</span></span>
        <span>性能 <span class="text-dim-2">${perfModeTxt}</span></span>
        <span>分发 <span class="text-dim-2 font-mono">${dt||'无'}</span></span>
      </div>${runHtml}
    </div>`}).join('')}catch{}
}
function showAddScheduleForm(){
  document.getElementById('add-schedule-form').classList.remove('hidden');
  document.getElementById('sched-reg-log-mode').value='inherit';
  document.getElementById('sched-reg-perf-mode').value='inherit';
  const r=document.getElementById('sched-dist-rows');
  if(!r.children.length) addDistRow();
}
function hideAddScheduleForm(){document.getElementById('add-schedule-form').classList.add('hidden')}
let distRowCount=0;
function addDistRow(){
  const id=distRowCount++;const rows=document.getElementById('sched-dist-rows');
  const opts=(configData?.teams||[]).map(t=>`<option value="${t.name}">${t.name}</option>`).join('');
  const row=document.createElement('div');row.className='flex items-center gap-2';row.id=`dist-row-${id}`;
  row.innerHTML=`<select class="dist-team field-input flex-1">${opts}</select><input class="dist-percent field-input w-24" type="number" min="1" max="100" placeholder="%" value="100"><button onclick="document.getElementById('dist-row-${id}').remove()" class="btn btn-danger text-xs py-1 px-2">&times;</button>`;
  rows.appendChild(row);
}
async function submitAddSchedule(){
  const name=document.getElementById('sched-name').value.trim(),startTime=document.getElementById('sched-start').value,endTime=document.getElementById('sched-end').value,target=parseInt(document.getElementById('sched-target').value),interval=parseInt(document.getElementById('sched-interval').value)||30;
  const priorityValue=parseInt(document.getElementById('sched-priority').value,10);
  const priority=Number.isNaN(priorityValue)?100:priorityValue;
  if(!name||!startTime||!endTime||!target){toast('请填写必填字段','error');return}
  if(startTime===endTime){toast('开始和结束时间不能相同','error');return}
  const distribution=[];
  document.querySelectorAll('#sched-dist-rows > div').forEach(r=>{const t=r.querySelector('.dist-team').value,p=parseInt(r.querySelector('.dist-percent').value);if(t&&p>0) distribution.push({team:t,percent:p})});
  const tot=distribution.reduce((s,d)=>s+d.percent,0);
  if(tot!==100){toast(`百分比总和必须为100，当前${tot}`,'error');return}
  const isFree=document.getElementById('sched-mode').value==='free';
  if(isFree&&distribution.length>0){
    const noFreeGroups=distribution.filter(d=>{const t=configData?.teams?.find(x=>x.name===d.team);return!t||!t.free_group_ids||!t.free_group_ids.length});
    if(noFreeGroups.length){toast(`Free 模式下以下号池未配置 Free 分组: ${noFreeGroups.map(d=>d.team).join(', ')}，请先在号池配置中添加 Free 分组`,'error');return}
  }
  const body={name,start_time:startTime,end_time:endTime,target_count:target,batch_interval_mins:interval,priority,distribution,enabled:document.getElementById('sched-enabled').checked,push_s2a:document.getElementById('sched-push-s2a').checked,mail_provider:document.getElementById('sched-mail').value,free_mode:isFree};
  const addLogMode=document.getElementById('sched-reg-log-mode').value;
  const addPerfMode=normalizeRegisterPerfMode(document.getElementById('sched-reg-perf-mode').value);
  body.register_log_mode=addLogMode==='inherit'?null:addLogMode;
  body.register_perf_mode=addPerfMode==='inherit'?null:addPerfMode;
  const rw=document.getElementById('sched-reg-workers').value,rtw=document.getElementById('sched-rt-workers').value,rtr=document.getElementById('sched-rt-retries').value;
  if(rw) body.register_workers=parseInt(rw);if(rtw) body.rt_workers=parseInt(rtw);if(rtr) body.rt_retries=parseInt(rtr);
  try{await api('/api/schedules',{method:'POST',body});toast(`${name} 已创建`,'success');hideAddScheduleForm();document.getElementById('sched-name').value='';loadSchedules()}catch{}
}
async function toggleSchedule(n){try{const d=await api(`/api/schedules/${encodeURIComponent(n)}/toggle`,{method:'POST'});toast(d.message,'success');loadSchedules()}catch{}}
async function runOnceSchedule(n){
  if(!confirm(`运行一次 "${n}"？将执行单个批次（注册+RT+S2A），完成后自动结束。`))return;
  const btn=document.getElementById(`run-once-${n}`);
  if(btn){btn.disabled=true;btn.textContent='执行中...';}
  try{
    const d=await api(`/api/schedules/${encodeURIComponent(n)}/run-once`,{method:'POST'});
    toast(`运行一次完成 | RT: ${d.rt_ok} | S2A: ${d.total_s2a_ok} | 耗时: ${d.elapsed_secs.toFixed(1)}s`,'success');
  }catch{}finally{
    if(btn){btn.disabled=false;btn.textContent='运行一次';}
    loadSchedules();
  }
}
async function triggerSchedule(n){if(!confirm(`手动启动 "${n}" 的批次循环?`))return;try{const d=await api(`/api/schedules/${encodeURIComponent(n)}/trigger`,{method:'POST'});toast(d.message,'success');startSchedulePoll();loadSchedules()}catch{}}
async function stopSchedule(n){if(!confirm(`停止 "${n}"?`))return;try{const d=await api(`/api/schedules/${encodeURIComponent(n)}/stop`,{method:'POST'});toast(d.message,'success');loadSchedules()}catch{}}
async function deleteSchedule(n){if(!confirm(`删除 "${n}"?`))return;try{await api(`/api/schedules/${encodeURIComponent(n)}`,{method:'DELETE'});toast(`${n} 已删除`,'success');loadSchedules()}catch{}}
// Edit schedule
let editSchedName=null;
async function editSchedule(name){
  editSchedName=name;
  try{const d=await api('/api/schedules');
  const s=d.schedules.find(x=>x.name===name);if(!s) return;
  const f=document.getElementById('edit-schedule-form');
  f.classList.remove('hidden');
  document.getElementById('es-name').value=name;
  document.getElementById('es-start').value=s.start_time;
  document.getElementById('es-end').value=s.end_time;
  document.getElementById('es-target').value=s.target_count;
  document.getElementById('es-interval').value=s.batch_interval_mins;
  document.getElementById('es-priority').value=s.priority??100;
  document.getElementById('es-reg-workers').value=s.register_workers||'';
  document.getElementById('es-rt-workers').value=s.rt_workers||'';
  document.getElementById('es-rt-retries').value=s.rt_retries||'';
  document.getElementById('es-mail').value=s.mail_provider||(s.use_chatgpt_mail?'chatgpt':'kyx');
  document.getElementById('es-mode').value=s.free_mode?'free':'team';
  document.getElementById('es-reg-log-mode').value=s.register_log_mode||'inherit';
  document.getElementById('es-reg-perf-mode').value=normalizeRegisterPerfMode(s.register_perf_mode||'inherit');
  document.getElementById('es-push-s2a').checked=s.push_s2a;
  document.getElementById('es-enabled').checked=s.enabled;
  // dist rows
  const rows=document.getElementById('es-dist-rows');rows.innerHTML='';
  editDistCount=0;
  s.distribution.forEach(d=>addEditDistRow(d.team,d.percent));
  f.scrollIntoView({behavior:'smooth'});}catch{}
}
let editDistCount=0;
function addEditDistRow(team,pct){
  const id=editDistCount++;const rows=document.getElementById('es-dist-rows');
  const opts=(configData?.teams||[]).map(t=>`<option value="${t.name}" ${t.name===team?'selected':''}>${t.name}</option>`).join('');
  const row=document.createElement('div');row.className='flex items-center gap-2';row.id=`es-dist-row-${id}`;
  row.innerHTML=`<select class="es-dist-team field-input flex-1">${opts}</select><input class="es-dist-percent field-input w-24" type="number" min="1" max="100" value="${pct||100}"><button onclick="document.getElementById('es-dist-row-${id}').remove()" class="btn btn-danger text-xs py-1 px-2">&times;</button>`;
  rows.appendChild(row);
}
async function submitEditSchedule(){
  if(!editSchedName) return;
  const newName=document.getElementById('es-name').value.trim();
  const priorityValue=parseInt(document.getElementById('es-priority').value,10);
  const priority=Number.isNaN(priorityValue)?100:priorityValue;
  if(!newName){toast('名称不能为空','error');return}
  const body={
    name:newName,
    start_time:document.getElementById('es-start').value,
    end_time:document.getElementById('es-end').value,
    target_count:parseInt(document.getElementById('es-target').value),
    batch_interval_mins:parseInt(document.getElementById('es-interval').value)||30,
    priority,
    push_s2a:document.getElementById('es-push-s2a').checked,
    mail_provider:document.getElementById('es-mail').value,
    free_mode:document.getElementById('es-mode').value==='free',
    enabled:document.getElementById('es-enabled').checked
  };
  const editLogMode=document.getElementById('es-reg-log-mode').value;
  const editPerfMode=normalizeRegisterPerfMode(document.getElementById('es-reg-perf-mode').value);
  body.register_log_mode=editLogMode==='inherit'?null:editLogMode;
  body.register_perf_mode=editPerfMode==='inherit'?null:editPerfMode;
  const rw=document.getElementById('es-reg-workers').value,rtw=document.getElementById('es-rt-workers').value,rtr=document.getElementById('es-rt-retries').value;
  if(rw) body.register_workers=parseInt(rw);if(rtw) body.rt_workers=parseInt(rtw);if(rtr) body.rt_retries=parseInt(rtr);
  const distribution=[];
  document.querySelectorAll('#es-dist-rows > div').forEach(r=>{const t=r.querySelector('.es-dist-team').value,p=parseInt(r.querySelector('.es-dist-percent').value);if(t&&p>0) distribution.push({team:t,percent:p})});
  const tot=distribution.reduce((s,d)=>s+d.percent,0);
  if(tot!==100){toast(`百分比总和必须为100，当前${tot}`,'error');return}
  const isFree=document.getElementById('es-mode').value==='free';
  if(isFree&&distribution.length>0){
    const noFreeGroups=distribution.filter(d=>{const t=configData?.teams?.find(x=>x.name===d.team);return!t||!t.free_group_ids||!t.free_group_ids.length});
    if(noFreeGroups.length){toast(`Free 模式下以下号池未配置 Free 分组: ${noFreeGroups.map(d=>d.team).join(', ')}，请先在号池配置中添加 Free 分组`,'error');return}
  }
  body.distribution=distribution;
  try{await api(`/api/schedules/${encodeURIComponent(editSchedName)}`,{method:'PUT',body});toast('已更新','success');document.getElementById('edit-schedule-form').classList.add('hidden');loadSchedules()}catch{}
}
let schedPollTimer=null;
function startSchedulePoll(){stopSchedulePoll();schedPollTimer=setInterval(loadSchedules,5000)}
function stopSchedulePoll(){if(schedPollTimer){clearInterval(schedPollTimer);schedPollTimer=null}}

// Runs
let currentRunPage=1;
let runScheduleModeMap={};
function normalizeDistTeamName(name){
  const raw=String(name||'');
  return raw.endsWith('-free')?raw.slice(0,-5):raw;
}
function splitRunDistributionRows(run,distributions){
  const rows=Array.isArray(distributions)?distributions:[];
  const hasFreeSuffix=rows.some(ds=>String(ds.team_name||'').endsWith('-free'));
  if(hasFreeSuffix){
    return {
      mode:'mixed',
      teamRows:rows.filter(ds=>!String(ds.team_name||'').endsWith('-free')),
      freeRows:rows.filter(ds=>String(ds.team_name||'').endsWith('-free')),
      inferredBySchedule:false,
    };
  }
  const scheduleName=String(run?.schedule_name||'');
  const scheduleIsFree=!!(scheduleName&&runScheduleModeMap[scheduleName]===true);
  return scheduleIsFree
    ? {mode:'free',teamRows:[],freeRows:rows,inferredBySchedule:true}
    : {mode:'team',teamRows:rows,freeRows:[],inferredBySchedule:false};
}
async function loadRunStats(){
  try{
    const s=await api('/api/runs/stats');
    document.getElementById('rs-total').textContent=s.total_runs;
    document.getElementById('rs-ok').textContent=s.completed;
    document.getElementById('rs-fail').textContent=s.failed;
    document.getElementById('rs-reg').textContent=s.total_reg_ok;
    const regAll=s.total_reg_ok+s.total_reg_failed;
    document.getElementById('rs-reg-rate').textContent=regAll>0?` ${Math.round(s.total_reg_ok/regAll*100)}%`:'';
    document.getElementById('rs-rt').textContent=s.total_rt_ok;
    const rtAll=s.total_rt_ok+s.total_rt_failed;
    document.getElementById('rs-rt-rate').textContent=rtAll>0?` ${Math.round(s.total_rt_ok/rtAll*100)}%`:'';
    document.getElementById('rs-s2a').textContent=s.total_s2a_ok;
    const s2aAll=s.total_s2a_ok+s.total_s2a_failed;
    document.getElementById('rs-s2a-rate').textContent=s2aAll>0?` ${Math.round(s.total_s2a_ok/s2aAll*100)}%`:'';
    const regTotal=s.total_reg_ok+s.total_reg_failed;
    const regRate=regTotal>0?Math.round(s.total_reg_ok/regTotal*100):0;
    document.getElementById('rs-reg-total').textContent=regTotal;
    document.getElementById('rs-reg-ok').textContent=s.total_reg_ok;
    document.getElementById('rs-reg-success-rate').textContent=`${regRate}%`;
    document.getElementById('rs-avg').textContent=s.avg_secs_per_account>0?s.avg_secs_per_account.toFixed(1)+'s/个':'-';
    document.getElementById('rs-target').textContent=s.total_target;
    const h=Math.floor(s.total_elapsed_secs/3600),m=Math.floor((s.total_elapsed_secs%3600)/60);
    document.getElementById('rs-elapsed').textContent=h>0?`${h}h${m}m`:`${m}m`;
  }catch{}
}
async function loadRunsFilter(){
  try{
    const d=await api('/api/schedules');
    runScheduleModeMap={};
    (d.schedules||[]).forEach(item=>{runScheduleModeMap[item.name]=!!item.free_mode});
    const s=document.getElementById('runs-filter');
    const p=s.value;
    const baseOptions='<option value="">全部</option><option value="__manual__">手动任务</option>';
    const scheduleOptions=(d.schedules||[]).map(item=>`<option value="sched:${encodeURIComponent(item.name)}">${item.name}</option>`).join('');
    s.innerHTML=baseOptions+scheduleOptions;
    const hasPrev=Array.from(s.options).some(opt=>opt.value===p);
    s.value=hasPrev?p:'';
  }catch{}
}
function fmtBjTime(iso){
  try{const d=new Date(iso);const utc=d.getTime()+d.getTimezoneOffset()*60000;const bj=new Date(utc+8*3600000);
    const mm=String(bj.getMonth()+1).padStart(2,'0'),dd=String(bj.getDate()).padStart(2,'0');
    const hh=String(bj.getHours()).padStart(2,'0'),mi=String(bj.getMinutes()).padStart(2,'0'),ss=String(bj.getSeconds()).padStart(2,'0');
    return `${mm}-${dd} ${hh}:${mi}:${ss}`}catch{return iso||'-'}
}
async function loadRuns(page){
  if(page<1) page=1;currentRunPage=page;_openRunId=null;
  const f=document.getElementById('runs-filter').value;
  const params=new URLSearchParams({page,per_page:15});
  if(f==='__manual__'){
    params.set('trigger','manual');
  }else if(f.startsWith('sched:')){
    params.set('schedule',decodeURIComponent(f.slice(6)));
  }else if(f){
    // 兼容历史 value 直接为计划名
    params.set('schedule',f);
  }
  try{const d=await api(`/api/runs?${params}`);const el=document.getElementById('run-list');const pg=document.getElementById('run-pagination');
  if(!d.runs||!d.runs.length){el.innerHTML='<p class="text-sm text-dim text-center py-8">暂无记录</p>';pg.classList.add('hidden');return}
  el.innerHTML=d.runs.map(r=>{
    const bc={running:'badge-run',completed:'badge-ok',failed:'badge-err'};
    const lb={running:'运行中',completed:'完成',failed:'失败'};
    const elapsed=r.elapsed_secs?`${r.elapsed_secs.toFixed(1)}s`:'-';
    const avgSec=(r.elapsed_secs&&r.target_count>0)?(r.elapsed_secs/r.target_count).toFixed(1)+'s/个':'-';
    const trigLb={manual:'手动任务',manual_task:'手动任务',scheduled:'定时调度'};
    const regTotal=r.registered_ok+r.registered_failed;
    const regPct=regTotal>0?Math.round(r.registered_ok/regTotal*100):0;
    const rtTotal=r.rt_ok+r.rt_failed;
    const rtPct=rtTotal>0?Math.round(r.rt_ok/rtTotal*100):0;
    const s2aTotal=r.total_s2a_ok+r.total_s2a_failed;
    const s2aPct=s2aTotal>0?Math.round(r.total_s2a_ok/s2aTotal*100):0;
    const regSummary=`注册${regTotal}次 成功${r.registered_ok} 成功率${regPct}%`;
    const errHtml=r.error?`<div class="text-[.7rem] text-red-400 mt-1.5 truncate" title="${r.error}">${r.error}</div>`:'';
    return `<div class="row-item cursor-pointer" id="run-row-${r.id}" onclick="showRunDetail('${r.id}')" style="padding:10px 14px">
      <div class="flex items-center justify-between mb-2">
        <div class="flex items-center gap-2">
          <span class="badge ${bc[r.status]||'badge-off'}">${lb[r.status]||r.status}</span>
          <span class="text-xs font-mono text-dim">#${r.id}</span>
          <span class="text-xs" style="color:var(--text-dim2)">${r.schedule_name||trigLb[r.trigger_type]||r.trigger_type}</span>
        </div>
        <div class="flex items-center gap-2 text-[.7rem] text-dim">
          <span title="开始时间">${fmtBjTime(r.started_at)}</span>
          <span style="color:var(--border)">|</span>
          <span title="总耗时">${elapsed}</span>
          <span style="color:var(--border)">|</span>
          <span title="平均耗时" class="text-amber-400">${avgSec}</span>
          <span class="text-[.65rem] text-dim" title="目标数">[${r.target_count}]</span>
        </div>
      </div>
      <div class="grid grid-cols-3 gap-2">
        <div class="flex items-center gap-2">
          <span class="text-[.7rem] text-dim w-8">注册</span>
          <div class="progress-bar flex-1"><div class="progress-fill" style="width:${regPct}%"></div></div>
          <span class="text-[.7rem] font-mono min-w-[56px] text-right"><span class="c-heading">${r.registered_ok}</span><span class="text-dim">/${regTotal}</span></span>
        </div>
        <div class="flex items-center gap-2">
          <span class="text-[.7rem] text-dim w-8">RT</span>
          <div class="progress-bar flex-1"><div class="progress-fill" style="width:${rtPct}%"></div></div>
          <span class="text-[.7rem] font-mono min-w-[56px] text-right"><span class="c-heading">${r.rt_ok}</span><span class="text-dim">/${rtTotal}</span></span>
        </div>
        <div class="flex items-center gap-2">
          <span class="text-[.7rem] text-teal-400 w-8">S2A</span>
          <div class="progress-bar flex-1"><div class="progress-fill" style="width:${s2aPct}%;background:#14b8a6"></div></div>
          <span class="text-[.7rem] font-mono min-w-[56px] text-right"><span class="text-teal-400">${r.total_s2a_ok}</span><span class="text-dim">/${s2aTotal}</span></span>
        </div>
      </div>
      <div class="text-[.68rem] text-dim mt-1">${regSummary}</div>${errHtml}
    </div>`}).join('');
  const tp=Math.ceil(d.total/d.per_page);
  if(tp>1){pg.classList.remove('hidden');document.getElementById('runs-prev').disabled=page<=1;document.getElementById('runs-next').disabled=page>=tp;document.getElementById('runs-page-info').textContent=`第 ${page}/${tp} 页 (共 ${d.total} 条)`;document.getElementById('runs-total-pages').value=tp;document.getElementById('runs-last').disabled=page>=tp}else{pg.classList.add('hidden')}
  }catch{}
}
let _openRunId=null;
async function showRunDetail(runId){
  if(_openRunId===runId){hideRunDetail();return}
  hideRunDetail(true);
  _openRunId=runId;
  const rowEl=document.getElementById('run-row-'+runId);
  if(!rowEl) return;
  rowEl.style.borderColor='var(--border-hover)';
  // 立即插入加载骨架
  const panel=document.createElement('div');
  panel.id='run-detail-inline';
  panel.className='card run-detail-inline p-5 mt-1';
  panel.innerHTML='<div class="flex items-center gap-2 text-xs text-dim"><svg class="animate-spin h-4 w-4" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" fill="none" opacity=".25"/><path fill="currentColor" d="M4 12a8 8 0 018-8v3a5 5 0 00-5 5H4z"/></svg>加载中…</div>';
  rowEl.insertAdjacentElement('afterend',panel);
  try{const d=await api(`/api/runs/${runId}`);const r=d.run;
  if(_openRunId!==runId) return;
  const detailAvg=(r.elapsed_secs&&r.target_count>0)?(r.elapsed_secs/r.target_count).toFixed(1)+'s':'-';
  const detailRegTotal=r.registered_ok+r.registered_failed;
  const detailRegRate=detailRegTotal>0?Math.round(r.registered_ok/detailRegTotal*100):0;
  const detailRtTotal=r.rt_ok+r.rt_failed;
  const detailRtRate=detailRtTotal>0?Math.round(r.rt_ok/detailRtTotal*100):0;
  const detailS2aTotal=r.total_s2a_ok+r.total_s2a_failed;
  const detailS2aRate=detailS2aTotal>0?Math.round(r.total_s2a_ok/detailS2aTotal*100):0;
  let html=`<div class="flex items-center justify-between mb-4">
    <div class="section-title mb-0">运行详情</div>
    <button onclick="hideRunDetail()" class="btn btn-ghost text-xs py-1">关闭</button>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div class="card-inner p-3"><div class="field-label">注册成功</div><div class="stat-num text-lg c-heading">${r.registered_ok}</div></div>
    <div class="card-inner p-3"><div class="field-label">RT 成功</div><div class="stat-num text-lg c-heading">${r.rt_ok}</div></div>
    <div class="card-inner p-3"><div class="field-label">S2A 成功</div><div class="stat-num text-lg text-teal-400">${r.total_s2a_ok}</div></div>
    <div class="card-inner p-3"><div class="field-label">总耗时</div><div class="stat-num text-lg c-heading">${r.elapsed_secs?r.elapsed_secs.toFixed(1)+'s':'-'}</div></div>
    <div class="card-inner p-3"><div class="field-label">平均耗时</div><div class="stat-num text-lg text-amber-400">${detailAvg}<span class="text-xs text-dim">/个</span></div></div>
  </div>
  <div class="card-inner p-3 mb-4 text-xs text-dim grid grid-cols-2 md:grid-cols-4 gap-2">
    <span>ID <span class="font-mono c-heading">${r.id}</span></span>
    <span>计划 <span class="c-heading">${r.schedule_name||'-'}</span></span>
    <span>触发 <span class="c-heading">${r.trigger_type}</span></span>
    <span>状态 <span class="c-heading">${r.status}</span></span>
    <span>目标 <span class="c-heading">${r.target_count}</span></span>
    <span>注册总次数 <span class="c-heading">${detailRegTotal}</span></span>
    <span>注册成功 <span class="text-teal-400">${r.registered_ok}</span></span>
    <span>注册成功率 <span class="text-amber-400">${detailRegRate}%</span></span>
    <span>RT成功率 <span class="text-amber-400">${detailRtRate}%</span></span>
    <span>S2A成功率 <span class="text-amber-400">${detailS2aRate}%</span></span>
  </div>`;
  if(r.error) html+=`<div class="card-inner p-3 mb-4 text-xs text-red-400" style="border-color:rgba(248,113,113,.15)">${r.error}</div>`;
  if(d.distributions&&d.distributions.length){
    const splitRows=splitRunDistributionRows(r,d.distributions);
    const teamRows=splitRows.teamRows;
    const freeRows=splitRows.freeRows;
    const sumRows=rows=>rows.reduce((acc,ds)=>{
      acc.ok+=Number(ds.s2a_ok||0);
      acc.fail+=Number(ds.s2a_failed||0);
      acc.assigned+=Number(ds.assigned_count||0);
      return acc;
    },{ok:0,fail:0,assigned:0});
    const teamAgg=sumRows(teamRows);
    const freeAgg=sumRows(freeRows);
    const teamTot=teamAgg.ok+teamAgg.fail;
    const freeTot=freeAgg.ok+freeAgg.fail;
    const modeHint=splitRows.inferredBySchedule
      ? `<div class="text-dim md:col-span-2">统计口径: 当前记录未写入 -free 标记，按计划模式（Free）聚合展示</div>`
      : '';
    html+=`<div class="card-inner p-3 mb-4 text-xs grid grid-cols-1 md:grid-cols-2 gap-2">
      <div>Team分组推送: <span class="text-teal-400 font-mono">${teamAgg.ok}</span> 成功 / <span class="text-red-400 font-mono">${teamAgg.fail}</span> 失败 <span class="text-dim">（入队 ${teamAgg.assigned}）</span></div>
      <div>Free分组推送: <span class="text-teal-400 font-mono">${freeAgg.ok}</span> 成功 / <span class="text-red-400 font-mono">${freeAgg.fail}</span> 失败 <span class="text-dim">（入队 ${freeAgg.assigned}）</span></div>
      <div class="text-dim">Team成功率: <span class="c-heading font-mono">${teamTot>0?Math.round(teamAgg.ok/teamTot*100):0}%</span></div>
      <div class="text-dim">Free成功率: <span class="c-heading font-mono">${freeTot>0?Math.round(freeAgg.ok/freeTot*100):0}%</span></div>
      ${modeHint}
    </div>`;
    html+=`<div class="section-title">分发详情</div><div class="space-y-2">`;
    d.distributions.forEach(ds=>{const tot=ds.s2a_ok+ds.s2a_failed;const pct=tot>0?Math.round(ds.s2a_ok/tot*100):0;
      const isFreeTagged=String(ds.team_name||'').endsWith('-free');
      const rowMode=splitRows.mode==='free'||isFreeTagged?'Free':'Team';
      const rowModeCls=rowMode==='Free'?'text-amber-400':'text-dim';
      html+=`<div class="card-inner p-3 flex items-center justify-between">
        <div class="flex items-center gap-3"><span class="text-sm font-medium c-heading">${normalizeDistTeamName(ds.team_name)}</span><span class="text-xs ${rowModeCls} font-mono">${rowMode}</span><span class="text-xs text-dim font-mono">${ds.percent}%</span><span class="text-xs text-dim">x${ds.assigned_count}</span></div>
        <div class="flex items-center gap-3 text-xs font-mono">
          <span class="text-teal-400">${ds.s2a_ok}</span><span class="text-red-400">${ds.s2a_failed}</span>
          <div class="progress-bar w-20"><div class="progress-fill" style="width:${pct}%"></div></div>
        </div></div>`});
    html+='</div>'}
  panel.innerHTML=html;
  }catch{_openRunId=null;panel.remove()}
}
function hideRunDetail(instant){
  const el=document.getElementById('run-detail-inline');
  if(_openRunId){const row=document.getElementById('run-row-'+_openRunId);if(row) row.style.borderColor=''}
  _openRunId=null;
  if(!el) return;
  if(instant){el.remove();return}
  el.classList.add('collapsing');
  el.addEventListener('animationend',()=>el.remove(),{once:true});
}

// Logs SSE — text buffer + batched RAF for performance
let logEs=null;let logAutoScroll=true;let logLineCount=0;
const LOG_MAX_LINES=800;
let logLines=[];let logPending=[];let logRafId=null;
function currentRegisterLogMode(){
  return (configData&&configData.register&&configData.register.register_log_mode)||'verbose';
}
function shouldKeepSummaryLog(line){
  if(!line) return false;
  if(line.includes('[ERR]')) return true;
  if(line.includes('[SIG-END]')) return true;
  if(line.includes('[REG]')||line.includes('[REG-SUM]')||line.includes('[REG-END]')) return true;
  if(line.includes('阶段1+2')||line.includes('补注册')||line.includes('本轮结束')) return true;
  if(line.includes('已收到中断信号')) return true;
  return false;
}
function escapeHtml(s){
  return String(s)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}
function detectLogLineLevel(line){
  if(!line) return 'log-line-normal';
  if(line.includes('[SIG-END][ERR]')||line.includes('[ERR]')||line.includes('失败')) return 'log-line-error';
  if(line.includes('[SIG-END][OK]')||line.includes('[OK]')) return 'log-line-success';
  return 'log-line-normal';
}
function renderLogLines(lines){
  return lines
    .map(line=>`<span class="log-line ${detectLogLineLevel(line)}">${escapeHtml(line)}</span>`)
    .join('');
}
function flushLogs(){
  logRafId=null;
  if(!logPending.length) return;
  const batch=logPending.splice(0);
  const mode=currentRegisterLogMode();
  const rows=mode==='summary'?batch.filter(shouldKeepSummaryLog):batch;
  if(!rows.length) return;
  logLines.push(...rows);
  logLineCount+=rows.length;
  // trim oldest
  if(logLines.length>LOG_MAX_LINES){logLines=logLines.slice(-LOG_MAX_LINES)}
  const container=document.getElementById('log-container');
  container.innerHTML=renderLogLines(logLines);
  document.getElementById('log-count').textContent=logLineCount+' 条';
  if(logAutoScroll) container.scrollTop=container.scrollHeight;
}
function connectLogStream(){
  if(logEs&&logEs.readyState!==EventSource.CLOSED) return;
  const status=document.getElementById('log-status');
  status.textContent='连接中...';status.style.color='var(--text-dim)';
  logEs=new EventSource(API+'/api/logs/stream');
  logEs.onopen=()=>{status.textContent='已连接';status.style.color='#2dd4bf'};
  logEs.onmessage=(e)=>{
    logPending.push(e.data);
    if(!logRafId) logRafId=requestAnimationFrame(flushLogs);
  };
  logEs.onerror=()=>{
    status.textContent='已断开';status.style.color='#f87171';
    logEs.close();logEs=null;
    setTimeout(()=>{if(document.getElementById('panel-logs')&&!document.getElementById('panel-logs').classList.contains('hidden'))connectLogStream()},3000);
  };
}
function toggleLogScroll(){
  logAutoScroll=!logAutoScroll;
  document.getElementById('log-scroll-btn').textContent='自动滚动: '+(logAutoScroll?'开':'关');
  if(logAutoScroll){const c=document.getElementById('log-container');c.scrollTop=c.scrollHeight}
}
function clearLogs(){
  document.getElementById('log-container').innerHTML='';
  logLines=[];logPending=[];logLineCount=0;document.getElementById('log-count').textContent='0 条';
}

// Beijing clock (UTC+8)
function updateBeijingClock(){
  const now=new Date();
  const utc=now.getTime()+now.getTimezoneOffset()*60000;
  const bj=new Date(utc+8*3600000);
  const h=String(bj.getHours()).padStart(2,'0');
  const m=String(bj.getMinutes()).padStart(2,'0');
  const s=String(bj.getSeconds()).padStart(2,'0');
  document.getElementById('beijing-clock').textContent=`${h}:${m}:${s} CST`;
}
updateBeijingClock();setInterval(updateBeijingClock,1000);
const qModeEl=document.getElementById('q-mode');
if(qModeEl){
  qModeEl.addEventListener('change',e=>applyQuickDefaultsByMode(e.target.value==='free'?'free':'team'));
}
const cfgModeEl=document.getElementById('cfg-default-mode');
if(cfgModeEl){
  cfgModeEl.addEventListener('change',e=>applyConfigDefaultsByMode(e.target.value==='free'?'free':'team'));
}

// Init
checkHealth();loadConfig();setInterval(checkHealth,15000);
