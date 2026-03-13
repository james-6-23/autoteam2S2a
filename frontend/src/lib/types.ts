// ─── API Types ──────────────────────────────────────────────────────────────

export interface HealthResponse {
  status: string;
  version: string;
  uptime_secs: number;
}

// ── Teams / S2A ──
export interface S2aTeam {
  name: string;
  api_url: string;
  admin_key: string;
  groups: string[];
  concurrency: number;
  priority: number;
  free_groups?: string[];
  free_priority?: number;
  free_concurrency?: number;
  openai_passthrough?: boolean;
  openai_ws_v2_enabled?: boolean;
  openai_ws_v2_mode?: string;
}

export interface S2aStats {
  total: number;
  active: number;
}

// ── Config ──
export interface AppConfig {
  default_mode: string;
  target: number;
  reg_workers: number;
  rt_workers: number;
  rt_retries: number;
  mail_api_base: string;
  mail_api_path: string;
  mail_api_token: string;
  mail_request_timeout_sec: number;
  otp_retries: number;
  request_timeout_sec: number;
  mail_max_concurrency: number;
  email_domains: string[];
  reg_log_mode: string;
  reg_perf_mode: string;
  gptmail_api_key?: string;
  gptmail_domains?: string[];
  d1_cleanup?: D1CleanupConfig;
}

export interface D1CleanupConfig {
  enabled: boolean;
  account_id: string;
  api_key: string;
  keep_percent: number;
  batch_size: number;
  databases: { name: string; id: string }[];
}

// ── Tasks ──
export interface Task {
  id: string;
  team: string;
  target: number;
  mode: string;
  status: string;
  priority: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  error?: string;
  reg_ok: number;
  reg_failed: number;
  rt_ok: number;
  rt_failed: number;
  s2a_ok: number;
  s2a_failed: number;
  elapsed_secs?: number;
  stage?: string;
}

// ── Schedules ──
export interface Schedule {
  id: string;
  name: string;
  enabled: boolean;
  start_time: string;
  end_time: string;
  target: number;
  interval_mins: number;
  priority: number;
  distribution: { team: string; percent: number }[];
  reg_workers?: number;
  rt_workers?: number;
  rt_retries?: number;
  use_gptmail: boolean;
  mode: string;
  reg_log_mode?: string;
  reg_perf_mode?: string;
  push_s2a: boolean;
}

// ── Runs ──
export interface Run {
  id: number;
  task_id: string;
  team: string;
  target: number;
  mode: string;
  status: string;
  started_at: string;
  finished_at?: string;
  elapsed_secs?: number;
  reg_ok: number;
  reg_failed: number;
  rt_ok: number;
  rt_failed: number;
  s2a_ok: number;
  s2a_failed: number;
  error?: string;
}

export interface RunStats {
  total: number;
  ok: number;
  failed: number;
  total_target: number;
  total_reg_ok: number;
  total_reg_failed: number;
  total_rt_ok: number;
  total_rt_failed: number;
  total_s2a_ok: number;
  total_s2a_failed: number;
  avg_elapsed: number;
  total_elapsed: number;
}

export interface RunsPage {
  items: Run[];
  total: number;
  page: number;
  page_size: number;
  stats: RunStats;
}

// ── Invite ──
export interface InviteOwner {
  id: number;
  email: string;
  account_id: string;
  access_token: string;
  expires?: string;
  used: boolean;
}

export interface InviteUpload {
  id: string;
  created_at: string;
  count: number;
  owners: InviteOwner[];
}

export interface InviteTask {
  id: string;
  upload_id: string;
  status: string;
  created_at: string;
  started_at?: string;
  finished_at?: string;
  invited_ok: number;
  invited_failed: number;
  reg_ok: number;
  reg_failed: number;
  rt_ok: number;
  rt_failed: number;
  s2a_ok: number;
  s2a_failed: number;
}

// ── Team Management ──
export interface TeamMember {
  id: string;
  account_user_id: string;
  email: string;
  role: string;
  seat_type: string | null;
  name: string;
  created_time: string;
  is_scim_managed: boolean;
  deactivated_time: string | null;
}

export interface TeamMembersResponse {
  items: TeamMember[];
  total: number;
  limit: number;
  offset: number;
}
