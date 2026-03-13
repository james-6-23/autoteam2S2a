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
export const saveToFile = () => post('/api/config/save');

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
export const fetchInviteTaskDetail = (id: string) => get<unknown>(`/api/invite/tasks/${id}`);
export const executeInvite = (body: unknown) => post<unknown>('/api/invite/execute', body);

// ─── Team Management ──
export const fetchTeamMembers = (accountId: string, accessToken: string) =>
  post<unknown>('/api/team/members', { account_id: accountId, access_token: accessToken });

export const kickTeamMember = (accountId: string, accessToken: string, userId: string) =>
  post<unknown>('/api/team/kick', { account_id: accountId, access_token: accessToken, user_id: userId });

export { get, post, put, del };
