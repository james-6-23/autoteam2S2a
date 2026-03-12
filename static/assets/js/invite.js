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
    const text=await res.text();
    const data=text?JSON.parse(text):null;
    if(!res.ok) throw new Error((data&&data.error)||`HTTP ${res.status}`);
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

// ─── 自定义下拉组件 ──────────────────────────────────────────────────────

let customSelectValue={};

function toggleCustomSelect(id){
  const el=document.getElementById(id);
  if(!el)return;
  const wasOpen=el.classList.contains('open');
  // 关闭所有
  document.querySelectorAll('.custom-select.open').forEach(s=>s.classList.remove('open'));
  if(!wasOpen) el.classList.add('open');
}

function selectCustomOption(id,value){
  customSelectValue[id]=value;
  const el=document.getElementById(id);
  if(!el)return;
  el.classList.remove('open');
  // 更新选中状态
  el.querySelectorAll('.cs-option').forEach(o=>{
    o.classList.toggle('selected',o.dataset.value===value);
  });
  // 更新 trigger 文本
  const opt=el.querySelector(`.cs-option[data-value="${value}"]`);
  const label=el.querySelector('.cs-label');
  if(opt&&label){
    label.textContent=opt.querySelector('.cs-opt-title').textContent;
    label.classList.remove('cs-placeholder');
  }
  // 触发回调
  if(id==='inv-upload-id') loadInviteUploadDetail();
}

function getCustomSelectValue(id){
  return customSelectValue[id]||'';
}

function renderCustomSelectOptions(id,options,selectedVal){
  const el=document.getElementById(id);
  if(!el)return;
  const panel=el.querySelector('.custom-select-panel');
  const label=el.querySelector('.cs-label');
  if(!options.length){
    panel.innerHTML='<div class="cs-option" style="cursor:default;color:var(--text-dim)">暂无可用批次</div>';
    label.textContent='-- 无 --';
    label.classList.add('cs-placeholder');
    customSelectValue[id]='';
    return;
  }
  panel.innerHTML=options.map(o=>`<div class="cs-option${o.value===selectedVal?' selected':''}" data-value="${esc(o.value)}" onclick="selectCustomOption('${id}','${esc(o.value)}')">
    <div class="cs-opt-dot" style="background:${o.dotColor};box-shadow:0 0 6px ${o.dotColor}44"></div>
    <div class="cs-opt-info">
      <div class="cs-opt-title">${esc(o.title)}</div>
      <div class="cs-opt-meta">${esc(o.meta)}</div>
    </div>
    <span class="cs-opt-badge ${o.badgeCls}">${esc(o.badge)}</span>
  </div>`).join('');
  // 设置选中项文本
  const sel=options.find(o=>o.value===selectedVal)||options[0];
  if(sel){
    customSelectValue[id]=sel.value;
    label.textContent=sel.title;
    label.classList.remove('cs-placeholder');
  }
}

// 点击外部关闭下拉
document.addEventListener('click',e=>{
  if(!e.target.closest('.custom-select')){
    document.querySelectorAll('.custom-select.open').forEach(s=>s.classList.remove('open'));
  }
});

// ─── 万能 JSON 解析 ─────────────────────────────────────────────────────
// 兼容：
//   1. 标准数组 [{ ... }, { ... }]
//   2. 包装对象 { "accounts": [{ ... }] }
//   3. 单个对象 { "accessToken": "..." }
//   4. 多个对象换行/空行分隔（无逗号、无数组括号）
//   5. TXT 混合文本中夹杂 JSON 块
//   6. 字符串中包含字段（纯文本 key=value 不处理，仅 JSON）

function smartParseAccounts(text){
  const raw=text.trim();
  if(!raw) return [];

  // ── 策略 1: 整体 JSON.parse ──
  try{
    const parsed=JSON.parse(raw);
    return unwrapAccounts(parsed);
  }catch{}

  // ── 策略 2: 全文大括号配对扫描（处理 JSON 中夹杂空行的情况）──
  const extracted=extractJsonBlocks(raw);
  if(extracted.length>0){
    const results=[];
    for(const block of extracted){
      try{
        const parsed=JSON.parse(block);
        results.push(...unwrapAccounts(parsed));
      }catch{}
    }
    if(results.length>0) return results;
  }

  // ── 策略 3: 按空行分块，逐块解析 ──
  const results=[];
  const chunks=raw.split(/\n\s*\n/);
  for(const chunk of chunks){
    const trimmed=chunk.trim();
    if(!trimmed) continue;
    try{
      const parsed=JSON.parse(trimmed);
      results.push(...unwrapAccounts(parsed));
    }catch{}
  }
  return results;
}

// 从混合文本中提取所有顶层 JSON 块（{ } 或 [ ] 配对）
function extractJsonBlocks(text){
  const blocks=[];
  let i=0;
  while(i<text.length){
    // 找到第一个 { 或 [
    if(text[i]==='{'||text[i]==='['){
      const open=text[i];
      const close=open==='{'?'}':']';
      let depth=1;let j=i+1;let inStr=false;let esc=false;
      while(j<text.length&&depth>0){
        const ch=text[j];
        if(esc){esc=false;j++;continue}
        if(ch==='\\'){esc=true;j++;continue}
        if(ch==='"'){inStr=!inStr;j++;continue}
        if(!inStr){
          if(ch===open) depth++;
          else if(ch===close) depth--;
        }
        j++;
      }
      if(depth===0){
        blocks.push(text.substring(i,j));
        i=j;
        continue;
      }
    }
    i++;
  }
  return blocks;
}

// 统一解包：数组 / 包装对象 / 单个对象 → 扁平数组
function unwrapAccounts(parsed){
  if(Array.isArray(parsed)) return parsed;
  if(parsed&&typeof parsed==='object'){
    // { "accounts": [...] }
    if(Array.isArray(parsed.accounts)) return parsed.accounts;
    // 单个对象（有 accessToken / access_token / id 等关键字段）
    if(parsed.accessToken||parsed.access_token||parsed.id) return [parsed];
    // 尝试遍历所有 key，找第一个数组值
    for(const key of Object.keys(parsed)){
      if(Array.isArray(parsed[key])&&parsed[key].length>0&&typeof parsed[key][0]==='object'){
        return parsed[key];
      }
    }
    return [parsed];
  }
  return [];
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
  const accounts=smartParseAccounts(raw);
  if(!accounts.length){toast('未解析到有效账号数据','error');return}
  await doUpload('pasted-json',accounts);
}

async function uploadInviteFile(){
  const input=document.getElementById('invite-file');
  if(!input.files||!input.files[0]){toast('请选择文件','error');return}
  const file=input.files[0];
  try{
    const text=await file.text();
    const accounts=smartParseAccounts(text);
    if(!accounts.length){toast('文件中未找到有效账号数据','error');return}
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
    // 更新自定义下拉
    const available=allInviteUploads.filter(u=>u.unused_count>0);
    const selectedVal=inviteCurrentUploadId||(available[0]&&available[0].id)||'';
    renderCustomSelectOptions('inv-upload-id',available.map(u=>{
      const emails=u.owner_emails||'';
      const emailShort=emails.length>30?emails.substring(0,30)+'…':emails;
      return {
        value:u.id,
        title:`${u.filename} — ${emailShort||u.id.substring(0,8)}`,
        meta:`${u.unused_count}/${u.owner_count} 可用 · ${formatTime(u.created_at)}`,
        dotColor:u.unused_count===u.owner_count?'#2dd4bf':'#fbbf24',
        badge:`${u.unused_count} 可用`,
        badgeCls:'cs-badge-ok',
      };
    }),selectedVal);
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
  const uploadId=getCustomSelectValue('inv-upload-id');
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
  const uploadId=getCustomSelectValue('inv-upload-id');
  if(!uploadId){toast('请先选择上传批次','error');return}
  const inviteCount=parseInt(document.getElementById('inv-count').value)||6;
  const pushS2a=document.getElementById('inv-push-s2a').value==='true';

  const body={upload_id:uploadId,invite_count:inviteCount,push_s2a:pushS2a};

  if(pushS2a){
    // 收集分发配置
    const distribution=[];
    document.querySelectorAll('#inv-dist-rows > div').forEach(r=>{
      const t=r.querySelector('.inv-dist-team').value;
      const p=parseInt(r.querySelector('.inv-dist-percent').value);
      if(t&&p>0) distribution.push({team:t,percent:p});
    });
    if(!distribution.length){toast('请至少添加一个号池','error');return}
    const tot=distribution.reduce((s,d)=>s+d.percent,0);
    if(tot!==100){toast(`百分比总和必须为100，当前${tot}`,'error');return}
    body.distribution=distribution;
  }

  try{
    const resp=await api('/api/invite/execute',{method:'POST',body});
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
  // 邀请数固定为 4（team 最大席位），不从后端覆盖
}

// ─── 号池分发行管理 ─────────────────────────────────────────────────────

let invDistRowCount=0;
let inviteTeams=[];

function getInvDistUsedPercent(){
  let sum=0;
  document.querySelectorAll('#inv-dist-rows .inv-dist-percent').forEach(el=>{sum+=parseInt(el.value)||0});
  return sum;
}

function addInvDistRow(team,pct,skipSave){
  const id=invDistRowCount++;
  const rows=document.getElementById('inv-dist-rows');
  // 未指定百分比时，自动填充剩余值
  if(pct==null) pct=Math.max(0,100-getInvDistUsedPercent());
  const opts=inviteTeams.map(t=>`<option value="${esc(t.name)}" ${t.name===team?'selected':''}>${esc(t.name)}</option>`).join('');
  const row=document.createElement('div');
  row.className='flex items-center gap-2';
  row.id=`inv-dist-row-${id}`;
  row.innerHTML=`<select class="inv-dist-team field-input flex-1">${opts}</select><input class="inv-dist-percent field-input w-24" type="number" min="0" max="100" placeholder="%" value="${pct}"><button onclick="removeInvDistRow('inv-dist-row-${id}')" class="btn btn-danger text-xs py-1 px-2">&times;</button>`;
  // 选择/百分比变更时自动保存
  row.querySelector('.inv-dist-team').addEventListener('change',saveInvDist);
  row.querySelector('.inv-dist-percent').addEventListener('change',saveInvDist);
  rows.appendChild(row);
  if(!skipSave) saveInvDist();
}

function removeInvDistRow(rowId){
  const el=document.getElementById(rowId);
  if(el) el.remove();
  saveInvDist();
}

function saveInvDist(){
  const dist=[];
  document.querySelectorAll('#inv-dist-rows > div').forEach(r=>{
    const t=r.querySelector('.inv-dist-team').value;
    const p=parseInt(r.querySelector('.inv-dist-percent').value)||0;
    dist.push({team:t,percent:p});
  });
  localStorage.setItem('inv-dist',JSON.stringify(dist));
}

function restoreInvDist(){
  const raw=localStorage.getItem('inv-dist');
  if(!raw) return false;
  try{
    const dist=JSON.parse(raw);
    if(!Array.isArray(dist)||!dist.length) return false;
    // 过滤掉已不存在的号池
    const names=new Set(inviteTeams.map(t=>t.name));
    const valid=dist.filter(d=>names.has(d.team));
    if(!valid.length) return false;
    valid.forEach(d=>addInvDistRow(d.team,d.percent,true));
    return true;
  }catch{return false}
}

function toggleInvDistRows(){
  const pushS2a=document.getElementById('inv-push-s2a').value==='true';
  const section=document.getElementById('inv-dist-section');
  if(section) section.style.display=pushS2a?'':'none';
}

async function loadTeamsForSelect(){
  try{
    const config=await api('/api/config');
    if(config&&config.teams){
      inviteTeams=config.teams;
      // 恢复上次保存的分发配置，无则默认一行
      if(!restoreInvDist()) addInvDistRow();
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
