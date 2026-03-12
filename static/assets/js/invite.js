// ─── 主题 ──────────────────────────────────────────────────────────────────
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

// ─── 基础工具 ──────────────────────────────────────────────────────────────

const API='';

function toast(msg,type='info'){
  const c=document.getElementById('toast-container');
  const cls={info:'toast-info',success:'toast-success',error:'toast-error'};
  const el=document.createElement('div');
  el.className=`toast ${cls[type]||cls.info}`;
  el.textContent=msg;
  c.appendChild(el);
  setTimeout(()=>{el.style.opacity='0';el.style.transition='opacity .3s';setTimeout(()=>el.remove(),300)},3000);
}

async function api(path,opts={}){
  try{
    const body=opts.body!=null?(typeof opts.body==='string'?opts.body:JSON.stringify(opts.body)):undefined;
    const {body:_,...restOpts}=opts;
    const res=await fetch(API+path,{headers:{'Content-Type':'application/json'},...restOpts,body});
    const data=await res.json();
    if(!res.ok) throw new Error(data.error||`HTTP ${res.status}`);
    return data;
  }catch(e){toast(e.message,'error');throw e}
}

function esc(s){
  const d=document.createElement('div');
  d.textContent=String(s);
  return d.innerHTML;
}

function formatTime(iso){
  if(!iso)return'--';
  try{const d=new Date(iso);return d.toLocaleString('zh-CN',{hour12:false,month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'})}catch{return iso}
}

// ─── SSE 实时日志 ──────────────────────────────────────────────────────────

let logEs=null;let logAutoScroll=true;let logLineCount=0;
const LOG_MAX_LINES=500;
let logLines=[];let logPending=[];let logRafId=null;

function flushLogs(){
  logRafId=null;
  if(!logPending.length) return;
  const container=document.getElementById('log-container');
  const frag=document.createDocumentFragment();
  for(const line of logPending){
    const span=document.createElement('span');
    span.textContent=line+'\n';
    frag.appendChild(span);
    logLines.push(span);
    logLineCount++;
  }
  logPending=[];
  // 超出上限时裁剪
  while(logLines.length>LOG_MAX_LINES){
    const old=logLines.shift();
    old.remove();
  }
  container.appendChild(frag);
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
    setTimeout(connectLogStream,3000);
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

// ─── 邀请功能 ──────────────────────────────────────────────────────────────

let inviteCurrentUploadId=null;
let inviteTaskPollTimer=null;
let currentInputMode='file';

// ─── 分页状态 ───
const PAGE_SIZE=5;
let allInviteTasks=[];let inviteTasksPage=1;
let allInviteUploads=[];let inviteUploadsPage=1;

function paginationHtml(total,page,perPage,fnName){
  const pages=Math.ceil(total/perPage);
  if(pages<=1) return '';
  const prevAttr=page>1?`onclick="${fnName}(${page-1})"`:'disabled style="opacity:.35;pointer-events:none"';
  const nextAttr=page<pages?`onclick="${fnName}(${page+1})"`:'disabled style="opacity:.35;pointer-events:none"';
  return `<div class="flex items-center justify-center gap-3 mt-3 pt-3" style="border-top:1px solid var(--border)">
    <button ${prevAttr} class="btn btn-ghost text-xs py-1 px-2">‹ 上一页</button>
    <span class="text-[.65rem] font-mono text-dim">${page} / ${pages}</span>
    <button ${nextAttr} class="btn btn-ghost text-xs py-1 px-2">下一页 ›</button>
  </div>`;
}

// ─── Tab 切换 + 滑块动画 ───
function updateTabIndicator(){
  const tabs=document.getElementById('upload-tabs');
  const indicator=document.getElementById('tab-indicator');
  const active=tabs.querySelector('.upload-tab.active');
  if(!tabs||!indicator||!active) return;
  const tabsRect=tabs.getBoundingClientRect();
  const activeRect=active.getBoundingClientRect();
  indicator.style.width=activeRect.width+'px';
  indicator.style.transform='translateX('+(activeRect.left-tabsRect.left-3)+'px)';
}

function switchInputMode(mode){
  currentInputMode=mode;
  document.getElementById('input-mode-file').classList.toggle('hidden',mode!=='file');
  document.getElementById('input-mode-json').classList.toggle('hidden',mode!=='json');
  document.querySelectorAll('.upload-tab').forEach(t=>{
    t.classList.toggle('active',t.id==='tab-'+mode);
  });
  updateTabIndicator();
}

// ─── 拖拽上传 ───
function initDropZone(){
  const zone=document.getElementById('drop-zone');
  const input=document.getElementById('invite-file');
  if(!zone||!input) return;

  ['dragenter','dragover'].forEach(ev=>zone.addEventListener(ev,e=>{
    e.preventDefault();e.stopPropagation();zone.classList.add('drag-over');
  }));
  ['dragleave','drop'].forEach(ev=>zone.addEventListener(ev,e=>{
    e.preventDefault();e.stopPropagation();zone.classList.remove('drag-over');
  }));
  zone.addEventListener('drop',e=>{
    const files=e.dataTransfer.files;
    if(files.length>0){input.files=files;handleFileSelected()}
  });
  input.addEventListener('change',handleFileSelected);
}

function handleFileSelected(){
  const input=document.getElementById('invite-file');
  if(!input.files||!input.files[0]) return;
  const file=input.files[0];
  document.getElementById('file-name').textContent=file.name;
  document.getElementById('file-size').textContent=formatFileSize(file.size);
  document.getElementById('file-selected').classList.remove('hidden');
  document.getElementById('drop-zone').style.display='none';
}

function clearFileSelection(){
  const input=document.getElementById('invite-file');
  input.value='';
  document.getElementById('file-selected').classList.add('hidden');
  document.getElementById('drop-zone').style.display='';
}

function formatFileSize(bytes){
  if(bytes<1024) return bytes+' B';
  if(bytes<1048576) return (bytes/1024).toFixed(1)+' KB';
  return (bytes/1048576).toFixed(1)+' MB';
}

// ─── JSON 编辑器 ───
function initJsonEditor(){
  const ta=document.getElementById('invite-json-input');
  if(!ta) return;
  ta.addEventListener('input',()=>{
    const len=ta.value.length;
    const el=document.getElementById('json-char-count');
    el.textContent=len>=1000?(len/1000).toFixed(1)+'K 字符':len+' 字符';
  });
}

function formatJsonInput(){
  const ta=document.getElementById('invite-json-input');
  try{
    const parsed=JSON.parse(ta.value.trim());
    ta.value=JSON.stringify(parsed,null,2);
    ta.dispatchEvent(new Event('input'));
    toast('JSON 已格式化','success');
  }catch(e){toast('JSON 格式错误: '+e.message,'error')}
}

function clearJsonInput(){
  const ta=document.getElementById('invite-json-input');
  ta.value='';ta.dispatchEvent(new Event('input'));
}

// ─── 上传逻辑 ───
async function uploadFromJsonInput(){
  const ta=document.getElementById('invite-json-input');
  const raw=ta.value.trim();
  if(!raw){toast('请粘贴 JSON 数据','error');return}
  try{
    let parsed=JSON.parse(raw);
    if(!Array.isArray(parsed)) parsed=[parsed];
    await doUpload('pasted-json',parsed);
  }catch(e){toast('JSON 解析失败: '+e.message,'error')}
}

async function uploadInviteFile(){
  const input=document.getElementById('invite-file');
  if(!input.files||!input.files[0]){toast('请选择 JSON 文件','error');return}
  const file=input.files[0];
  try{
    const text=await file.text();
    let accounts=JSON.parse(text);
    if(!Array.isArray(accounts)) accounts=[accounts];
    await doUpload(file.name,accounts);
  }catch(e){toast('文件解析失败: '+e.message,'error')}
}

async function doUpload(filename,accounts){
  try{
    const resp=await api('/api/invite/upload',{method:'POST',body:{filename,accounts}});
    if(resp.error){toast(resp.error,'error');return}
    toast(`上传成功: ${resp.owner_count} 个 Owner`,'success');
    showOwnersPreview(resp.owners,resp.owner_count);
    inviteCurrentUploadId=resp.upload_id;
    loadInviteUploads();
  }catch(e){/* api() 已 toast */}
}

// ─── 丰富的数据预览 ───
function showOwnersPreview(owners,count){
  const preview=document.getElementById('invite-owners-preview');
  const badge=document.getElementById('owner-count-badge');
  badge.textContent=count+' 个 Owner';
  preview.innerHTML=owners.map((o,i)=>{
    const expired=o.expires?new Date(o.expires)<new Date():false;
    const statusDot=expired?'background:#f87171;box-shadow:0 0 6px rgba(248,113,113,.5)':'background:#2dd4bf;box-shadow:0 0 6px rgba(45,212,191,.5)';
    const statusText=expired?'已过期':'有效';
    const expDate=o.expires?o.expires.substring(0,10):'--';
    const idShort=o.account_id?o.account_id.substring(0,8):'--';
    return `<div class="owner-card" style="animation-delay:${i*50}ms">
      <div class="flex items-center gap-3 min-w-0">
        <div style="width:30px;height:30px;border-radius:8px;background:var(--ghost);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;flex-shrink:0">
          <span class="font-mono text-[.6rem] c-dim font-semibold">${i+1}</span>
        </div>
        <div class="min-w-0">
          <div class="text-xs c-heading font-medium truncate">${esc(o.email||'未知邮箱')}</div>
          <div class="flex items-center gap-2 mt-0.5">
            <span class="font-mono text-[.6rem] text-dim">${esc(idShort)}…</span>
            <span class="text-[.6rem] text-dim">·</span>
            <span class="text-[.6rem] text-dim">${expDate}</span>
          </div>
        </div>
      </div>
      <div class="flex items-center gap-1.5">
        <div style="width:5px;height:5px;border-radius:50%;${statusDot}"></div>
        <span class="text-[.6rem] font-medium" style="color:${expired?'#f87171':'#2dd4bf'}">${statusText}</span>
      </div>
    </div>`;
  }).join('');
  document.getElementById('invite-upload-result').classList.remove('hidden');
}

async function loadInviteUploads(){
  try{
    const uploads=await api('/api/invite/uploads');
    allInviteUploads=uploads||[];
    inviteUploadsPage=1;
    renderInviteUploadsPage(1);
    // 更新下拉选择器
    const select=document.getElementById('inv-upload-id');
    if(!allInviteUploads.length){
      select.innerHTML='<option value="">-- 无 --</option>';
      return;
    }
    const available=allInviteUploads.filter(u=>u.unused_count>0);
    if(!available.length){
      select.innerHTML='<option value="">-- 全部已使用 --</option>';
      loadInviteUploadDetail();
      return;
    }
    select.innerHTML=available.map(u=>`<option value="${u.id}"${u.id===inviteCurrentUploadId?' selected':''}>${esc(u.filename)} (${u.unused_count}/${u.owner_count} 可用, ${formatTime(u.created_at)})</option>`).join('');
    loadInviteUploadDetail();
  }catch(e){console.error('loadInviteUploads',e)}
}

function renderInviteUploadsPage(page){
  inviteUploadsPage=page;
  const container=document.getElementById('invite-uploads-list');
  if(!allInviteUploads.length){
    container.innerHTML='<span class="text-dim">暂无上传记录</span>';return;
  }
  const start=(page-1)*PAGE_SIZE;
  const slice=allInviteUploads.slice(start,start+PAGE_SIZE);
  container.innerHTML=`<div class="grid gap-2">${slice.map(u=>`<div class="flex items-center gap-4 py-2 px-3 rounded" style="background:var(--ghost)"><span class="c-heading font-mono">${esc(u.id)}</span><span>${esc(u.filename)}</span><span class="text-dim">${u.owner_count} 个 Owner</span><span class="text-dim">${formatTime(u.created_at)}</span></div>`).join('')}</div>${paginationHtml(allInviteUploads.length,page,PAGE_SIZE,'renderInviteUploadsPage')}`;
}

async function loadInviteUploadDetail(){
  const uploadId=document.getElementById('inv-upload-id').value;
  const container=document.getElementById('invite-owners-table');
  const wrapper=document.getElementById('invite-owners-list');
  if(!uploadId){wrapper.classList.add('hidden');return}
  try{
    const detail=await api(`/api/invite/uploads/${uploadId}`);
    if(!detail||!detail.owners){wrapper.classList.add('hidden');return}
    wrapper.classList.remove('hidden');
    container.innerHTML=`<div class="grid gap-1">${detail.owners.map(o=>`<div class="flex items-center gap-4 py-1.5 px-3 rounded" style="background:var(--ghost)"><span class="c-heading">${esc(o.email)}</span><span class="font-mono text-dim">${esc(o.account_id.substring(0,12))}...</span><span>${o.used?'<span class="text-amber-400">已使用</span>':'<span class="text-teal-400">可用</span>'}</span></div>`).join('')}</div>`;
  }catch(e){console.error('loadInviteUploadDetail',e)}
}

async function executeInvite(){
  const uploadId=document.getElementById('inv-upload-id').value;
  if(!uploadId){toast('请先选择上传批次','error');return}
  const inviteCount=parseInt(document.getElementById('inv-count').value)||6;
  const s2aTeam=document.getElementById('inv-s2a-team').value||undefined;
  const pushS2a=document.getElementById('inv-push-s2a').value==='true';
  try{
    const resp=await api('/api/invite/execute',{method:'POST',body:{upload_id:uploadId,invite_count:inviteCount,s2a_team:s2aTeam,push_s2a:pushS2a}});
    if(resp.error){toast(resp.error,'error');return}
    toast(`已创建 ${resp.task_count} 个邀请任务`,'success');
    loadInviteTasks();
    startInviteTaskPoll();
    loadInviteUploadDetail();
  }catch(e){toast('执行失败: '+e.message,'error')}
}

async function loadInviteTasks(resetPage){
  try{
    const tasks=await api('/api/invite/tasks');
    allInviteTasks=tasks||[];
    if(resetPage!==false) inviteTasksPage=1;
    // 页码越界修正
    const totalPages=Math.max(1,Math.ceil(allInviteTasks.length/PAGE_SIZE));
    if(inviteTasksPage>totalPages) inviteTasksPage=totalPages;
    renderInviteTasksPage(inviteTasksPage);
    if(allInviteTasks.some(t=>t.status==='running'||t.status==='pending')){
      startInviteTaskPoll();
    }else{
      stopInviteTaskPoll();
    }
  }catch(e){console.error('loadInviteTasks',e)}
}

function renderInviteTasksPage(page){
  inviteTasksPage=page;
  const container=document.getElementById('invite-tasks-list');
  if(!allInviteTasks.length){
    container.innerHTML='<span class="text-dim">暂无邀请任务</span>';return;
  }
  const start=(page-1)*PAGE_SIZE;
  const slice=allInviteTasks.slice(start,start+PAGE_SIZE);
  container.innerHTML=`<div class="grid gap-2">${slice.map(t=>{
    const statusColor=t.status==='completed'?'text-teal-400':t.status==='running'?'text-amber-400':t.status==='failed'?'text-red-400':'text-dim';
    return `<div class="py-2.5 px-3 rounded" style="background:var(--ghost)">
      <div class="flex items-center gap-4 mb-1.5">
        <span class="font-mono c-heading">${esc(t.id)}</span>
        <span class="${statusColor} font-medium">${esc(t.status)}</span>
        <span class="text-dim">${esc(t.owner_email)}</span>
        <span class="text-dim">${t.s2a_team||'--'}</span>
        <span class="text-dim">${formatTime(t.created_at)}</span>
      </div>
      <div class="flex gap-4 text-[.65rem]">
        <span>邀请: <span class="text-teal-400">${t.invited_ok}</span>/<span class="text-red-400">${t.invited_failed}</span></span>
        <span>注册: <span class="text-teal-400">${t.reg_ok}</span>/<span class="text-red-400">${t.reg_failed}</span></span>
        <span>RT: <span class="text-teal-400">${t.rt_ok}</span>/<span class="text-red-400">${t.rt_failed}</span></span>
        <span>S2A: <span class="text-teal-400">${t.s2a_ok}</span>/<span class="text-red-400">${t.s2a_failed}</span></span>
      </div>
      ${t.error?`<div class="text-red-400 text-[.65rem] mt-1">${esc(t.error)}</div>`:''}
    </div>`;
  }).join('')}</div>${paginationHtml(allInviteTasks.length,page,PAGE_SIZE,'renderInviteTasksPage')}`;
}

function startInviteTaskPoll(){
  if(inviteTaskPollTimer)return;
  inviteTaskPollTimer=setInterval(()=>loadInviteTasks(false),3000);
}
function stopInviteTaskPoll(){
  if(inviteTaskPollTimer){clearInterval(inviteTaskPollTimer);inviteTaskPollTimer=null}
}

async function loadInviteConfig(){
  try{
    const cfg=await api('/api/invite/config');
    if(cfg&&cfg.default_invite_count){
      const el=document.getElementById('inv-count');
      if(el&&!el.dataset.userChanged)el.value=cfg.default_invite_count;
    }
  }catch(e){/* ignore */}
}

async function loadTeamsForSelect(){
  try{
    const config=await api('/api/config');
    if(config&&config.teams){
      const sel=document.getElementById('inv-s2a-team');
      sel.innerHTML=config.teams.map(t=>`<option value="${esc(t.name)}">${esc(t.name)}</option>`).join('');
    }
  }catch(e){/* ignore */}
}

// ─── 页面初始化 ────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded',()=>{
  initDropZone();
  initJsonEditor();
  updateTabIndicator();
  loadInviteConfig();
  loadInviteUploads();
  loadInviteTasks();
  loadTeamsForSelect();
  connectLogStream();
});
// 窗口大小变化时重新计算 tab 滑块位置
window.addEventListener('resize',updateTabIndicator);
