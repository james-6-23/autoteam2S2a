import type {
  CodexQuota,
  InviteTaskDetail,
  OwnerHealth,
  S2aTeam,
  TeamManageBatchCheckResponse,
  TeamManageBatchInviteRequest,
  TeamManageBatchInviteResponse,
  TeamManageBatchJobDetail,
  TeamManageBatchJobItem,
  TeamManageBatchRetryRequest,
  TeamManageBatchRetryResponse,
  TeamManageBatchJobSummary,
  TeamManageBatchOwnerStateRequest,
  TeamManageBatchOwnerStateResponse,
  TeamManageBatchRefreshMembersResponse,
  TeamManageDashboardSummary,
  TeamManageOwnerAuditPageResponse,
  TeamManageOwnerAuditQuery,
  TeamManageOwnerPageParams,
  TeamManageOwnerPageResponse,
  TeamMember,
} from "./team-manage-types";

const BASE = '';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

function get<T>(path: string) {
  return request<T>(path);
}

function post<T>(path: string, body?: unknown) {
  return request<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined });
}

function put<T>(path: string, body?: unknown) {
  return request<T>(path, { method: 'PUT', body: body ? JSON.stringify(body) : undefined });
}

function del<T>(path: string) {
  return request<T>(path, { method: 'DELETE' });
}

// ─── Health ──
export const fetchHealth = () => get<{ status: string; version: string; uptime_secs: number }>('/api/health');

// ─── Teams ──
export const fetchTeams = () => get<{ teams: unknown[] }>('/api/teams');
export const addTeam = (body: unknown) => post('/api/teams', body);
export const updateTeam = (name: string, body: unknown) => put(`/api/teams/${encodeURIComponent(name)}`, body);
export const deleteTeam = (name: string) => del(`/api/teams/${encodeURIComponent(name)}`);
export const fetchTeamStats = (name: string) => get<unknown>(`/api/teams/${encodeURIComponent(name)}/stats`);
export const fetchTeamGroups = (apiUrl: string, adminKey: string) =>
  post<{ groups: string[] }>('/api/teams/groups', { api_url: apiUrl, admin_key: adminKey });

// ─── Config ──
export const fetchConfig = () => get<unknown>('/api/config');
export const saveDefaults = (body: unknown) => put('/api/config/defaults', body);
export const saveRegister = (body: unknown) => put('/api/config/register', body);
export const saveGptMail = (body: unknown) => put('/api/config/gptmail', body);
export const testGptMail = () => get<unknown>('/api/config/gptmail/test');
export const saveD1Cleanup = (body: unknown) => put('/api/config/d1-cleanup', body);
export const triggerD1Cleanup = () => post<{ message: string }>('/api/d1_cleanup/trigger');
export const saveToFile = () => post('/api/config/save');
export const setProxyEnabled = (enabled: boolean) => put('/api/config/proxy_enabled', { enabled });
export const setProxyRefreshUrl = (proxy_url: string, refresh_url: string) => put<{ message: string }>('/api/config/proxy_refresh_url', { proxy_url, refresh_url });
export const refreshProxyIp = (proxy_url?: string) => post<{ success: boolean; message: string; results?: Array<{ proxy: string; success: boolean; message: string }> }>('/api/proxy/refresh-ip', proxy_url ? { proxy_url } : {});
export const addProxy = (proxy: string) => post<{ message: string }>('/api/config/proxy_pool', { proxy });
export const batchAddProxy = (proxies: string[]) => post<{ added: number; skipped: number; message: string }>('/api/config/proxy_pool/batch', { proxies });
export const deleteProxy = (proxy: string) => request<{ message: string }>('/api/config/proxy_pool', { method: 'DELETE', body: JSON.stringify({ proxy }), headers: { 'Content-Type': 'application/json' } });
export const checkProxyHealth = () => post<{ proxy: string; ok: boolean; reason: string }[]>('/api/proxy/health-check');
export const batchTestProxy = (proxy_urls: string[], concurrency?: number) => post<{
  results: Array<{
    proxy_url: string;
    success: boolean;
    message: string;
    latency_ms?: number;
    ip_address?: string;
    city?: string;
    region?: string;
    country?: string;
    country_code?: string;
  }>;
}>('/api/proxy/batch-test', { proxy_urls, concurrency: concurrency ?? 5 });

export const testProxy = (proxy_url: string) => post<{
  success: boolean;
  message: string;
  latency_ms?: number;
  ip_address?: string;
  city?: string;
  region?: string;
  country?: string;
  country_code?: string;
}>('/api/proxy/test', { proxy_url });

export const checkProxyQuality = (proxy_url: string) => post<{
  score: number;
  grade: string;
  summary: string;
  exit_ip?: string;
  country?: string;
  country_code?: string;
  base_latency_ms?: number;
  passed_count: number;
  warn_count: number;
  failed_count: number;
  challenge_count: number;
  checked_at: number;
  items: Array<{
    target: string;
    status: string;
    http_status?: number;
    latency_ms?: number;
    message?: string;
  }>;
}>('/api/proxy/quality-check', { proxy_url });

// ─── Tasks ──
export const fetchTasks = () => get<{ tasks: unknown[] }>('/api/tasks');
export const createTask = (body: unknown) => post('/api/tasks', body);
export const cancelTask = (id: string) => del(`/api/tasks/${id}`);
export const fetchTaskProgress = (id: string) => get<unknown>(`/api/tasks/${id}/progress`);

// ─── Schedules ──
export const fetchSchedules = () => get<{ schedules: unknown[] }>('/api/schedules');
export const addSchedule = (body: unknown) => post('/api/schedules', body);
export const updateSchedule = (id: string, body: unknown) => put(`/api/schedules/${id}`, body);
export const deleteSchedule = (id: string) => del(`/api/schedules/${id}`);

// ─── Runs ──
export const fetchRuns = (page: number, team?: string) => {
  const params = new URLSearchParams({ page: String(page), page_size: '20' });
  if (team) params.set('team', team);
  return get<unknown>(`/api/runs?${params}`);
};

// ─── Invite ──
export const uploadOwners = (body: unknown) => post<unknown>('/api/invite/upload', body);
export const fetchInviteUploads = () => get<unknown>('/api/invite/uploads');
export const fetchInviteTasks = () => get<unknown>('/api/invite/tasks');
export const fetchInviteTaskDetail = (id: string) =>
  get<InviteTaskDetail>(`/api/invite/tasks/${encodeURIComponent(id)}`);
export const executeInvite = (body: unknown) => post<unknown>('/api/invite/execute', body);

// ─── Team Management ──
export const fetchTeamMembers = (accountId: string, accessToken: string) =>
  post<unknown>('/api/team/members', { account_id: accountId, access_token: accessToken });

export const kickTeamMember = (accountId: string, accessToken: string, userId: string) =>
  post<unknown>('/api/team/kick', { account_id: accountId, access_token: accessToken, user_id: userId });

function buildQueryString(params: TeamManageOwnerPageParams) {
  const query = new URLSearchParams();
  if (typeof params.page === "number") query.set("page", String(params.page));
  if (typeof params.page_size === "number") query.set("page_size", String(params.page_size));
  if (params.search) query.set("search", params.search);
  if (params.state) query.set("state", params.state);
  if (typeof params.has_slots === "boolean") query.set("has_slots", String(params.has_slots));
  if (typeof params.has_banned_member === "boolean") query.set("has_banned_member", String(params.has_banned_member));
  if (params.sort) query.set("sort", params.sort);
  if (params.order) query.set("order", params.order);
  return query.toString();
}

export async function fetchTeamManageOwnersPage(
  params: TeamManageOwnerPageParams = {},
): Promise<TeamManageOwnerPageResponse> {
  const page = params.page ?? 1;
  const pageSize = params.page_size ?? 10;
  const query = buildQueryString(params);
  const path = query ? `/api/team-manage/owners?${query}` : "/api/team-manage/owners";
  const raw = await get<Record<string, unknown>>(path);
  const hasPagedItems = Array.isArray(raw.items);
  const rawItems = hasPagedItems
    ? raw.items
    : Array.isArray(raw.owners)
      ? raw.owners
      : [];
  const total = typeof raw.total === "number" ? raw.total : rawItems.length;
  const totalPages = typeof raw.total_pages === "number"
    ? raw.total_pages
    : Math.max(1, Math.ceil(total / pageSize));
  const offset = (page - 1) * pageSize;
  const items = hasPagedItems ? rawItems : rawItems.slice(offset, offset + pageSize);

  return {
    items: items as TeamManageOwnerPageResponse["items"],
    page: typeof raw.page === "number" ? raw.page : page,
    page_size: typeof raw.page_size === "number" ? raw.page_size : pageSize,
    total,
    total_pages: totalPages,
    summary: raw.summary as TeamManageOwnerPageResponse["summary"],
  };
}

export const fetchTeamManageDashboard = () => get<TeamManageDashboardSummary>('/api/team-manage/dashboard');
export const fetchTeamManageHealth = () => get<{ records: OwnerHealth[] }>('/api/team-manage/health');
export const fetchTeamManageConfigTeams = () => get<{ teams: S2aTeam[] }>('/api/config');
export const fetchTeamManageOwnerMembers = (accountId: string, options?: { force_refresh?: boolean }) =>
  get<{ members: TeamMember[] }>(
    `/api/team-manage/owners/${encodeURIComponent(accountId)}/members${
      options?.force_refresh ? "?force_refresh=true" : ""
    }`,
  );
export const fetchTeamManageOwnerQuota = (accountId: string) =>
  get<CodexQuota>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/quota`);
export const fetchTeamManageMemberQuota = (email: string) =>
  post<CodexQuota>('/api/team-manage/member-quota', { email });
export const refreshTeamManageOwnerMembers = (accountId: string) =>
  post<{ members: TeamMember[] }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/refresh`);
export const inviteTeamManageOwner = (accountId: string, body: { s2a_team: string; invite_count: number }) =>
  post<{ task_id: string; message: string }>(`/api/team-manage/owners/${encodeURIComponent(accountId)}/invite`, body);
export const batchCheckTeamManageOwners = (body: {
  account_ids: string[];
  concurrency: number;
  force_refresh?: boolean;
  prefer_cache?: boolean;
  scope?: string;
  filters?: TeamManageBatchOwnerStateRequest["filters"];
}) =>
  post<TeamManageBatchCheckResponse>('/api/team-manage/batch-check', body);
export const batchInviteTeamManageOwners = (body: TeamManageBatchInviteRequest) =>
  post<TeamManageBatchInviteResponse>('/api/team-manage/batch-invite', body);
export const batchRefreshTeamManageOwnerMembers = (body: {
  account_ids: string[];
  concurrency: number;
  scope?: string;
  filters?: TeamManageBatchOwnerStateRequest["filters"];
}) =>
  post<TeamManageBatchRefreshMembersResponse>('/api/team-manage/batch-refresh-members', body);
export const batchDisableTeamManageOwners = (body: TeamManageBatchOwnerStateRequest) =>
  post<TeamManageBatchOwnerStateResponse>('/api/team-manage/owners/batch-disable', body);
export const batchRestoreTeamManageOwners = (body: TeamManageBatchOwnerStateRequest) =>
  post<TeamManageBatchOwnerStateResponse>('/api/team-manage/owners/batch-restore', body);
export const batchArchiveTeamManageOwners = (body: TeamManageBatchOwnerStateRequest) =>
  post<TeamManageBatchOwnerStateResponse>('/api/team-manage/owners/batch-archive', body);
export const fetchTeamManageBatchJobs = () =>
  get<{ jobs: TeamManageBatchJobSummary[] }>('/api/team-manage/batches');
export const fetchTeamManageBatchJob = (jobId: string) =>
  get<TeamManageBatchJobDetail>(`/api/team-manage/batches/${encodeURIComponent(jobId)}`);
export const fetchTeamManageBatchJobItems = (jobId: string) =>
  get<{ items: TeamManageBatchJobItem[] }>(`/api/team-manage/batches/${encodeURIComponent(jobId)}/items`);
export const retryFailedTeamManageBatchItems = (jobId: string, body?: TeamManageBatchRetryRequest) =>
  post<TeamManageBatchRetryResponse>(`/api/team-manage/batches/${encodeURIComponent(jobId)}/retry-failed`, body ?? {});
export const fetchTeamManageOwnerAudits = (params?: TeamManageOwnerAuditQuery) => {
  const query = new URLSearchParams();
  if (params?.account_id) query.set("account_id", params.account_id);
  if (params?.action) query.set("action", params.action);
  if (params?.batch_job_id) query.set("batch_job_id", params.batch_job_id);
  if (typeof params?.page === "number") query.set("page", String(params.page));
  if (typeof params?.page_size === "number") query.set("page_size", String(params.page_size));
  const suffix = query.toString();
  return get<TeamManageOwnerAuditPageResponse>(
    suffix ? `/api/team-manage/owners/audits?${suffix}` : "/api/team-manage/owners/audits",
  );
};

export { get, post, put, del };
