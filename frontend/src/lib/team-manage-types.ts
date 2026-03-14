export interface TeamOwner {
  email: string;
  account_id: string;
  access_token?: string;
  member_count?: number;
  state?: string;
  disabled_reason?: string;
  owner_status?: string;
  checked_at?: string;
  cache_status?: string;
  has_banned_member?: boolean;
  available_slots?: number;
  available_slots_cached?: number;
  last_health_checked_at?: string;
  last_health_status?: string;
}

export interface TeamMember {
  user_id: string;
  email?: string;
  name?: string;
  role: string;
  created_at?: string;
}

export interface QuotaWindow {
  used_percent: number;
  remaining_percent: number;
  reset_after_seconds: number;
  window_minutes: number;
}

export interface CodexQuota {
  five_hour?: QuotaWindow;
  seven_day?: QuotaWindow;
  status: string;
  model_used?: string;
  error?: string;
}

export interface S2aTeam {
  name: string;
  api_base: string;
}

export interface MemberHealth {
  email: string;
  name?: string;
  status: string;
  seven_day_pct: number | null;
}

export interface OwnerHealth {
  account_id: string;
  owner_status: string;
  members: MemberHealth[];
  checked_at: string;
  cache_status?: string;
  expires_at?: string;
  source?: string;
  last_error?: string;
}

export interface TeamManageOwnerPageSummary {
  total_owners?: number;
  active_owners?: number;
  banned_owners?: number;
  expired_owners?: number;
  quarantined_owners?: number;
  owners_with_slots?: number;
  owners_with_banned_members?: number;
  fresh_cache_owners?: number;
  stale_cache_owners?: number;
  running_batch_jobs?: number;
}

export interface TeamManageOwnerPageResponse {
  items: TeamOwner[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
  summary?: TeamManageOwnerPageSummary;
}

export interface TeamManageDashboardSummary {
  total_owners: number;
  active_owners: number;
  banned_owners: number;
  owners_with_slots: number;
  owners_with_banned_members: number;
  expired_owners?: number;
  quarantined_owners?: number;
  fresh_cache_owners?: number;
  stale_cache_owners?: number;
  running_batch_jobs?: number;
  checked_recently?: number;
}

export interface TeamManageBatchInviteRequest {
  account_ids: string[];
  s2a_team: string;
  strategy?: "fill_to_limit" | "fixed_count";
  fixed_count?: number;
  scope?: "manual" | "filtered";
  filters?: TeamManageBatchFilters;
  skip_banned?: boolean;
  skip_expired?: boolean;
  skip_quarantined?: boolean;
  only_with_slots?: boolean;
}

export interface TeamManageBatchInviteResponse {
  job_id: string;
  accepted: number;
  skipped: number;
  message: string;
}

export interface TeamManageBatchJobSummary {
  job_id: string;
  job_type: string;
  status: string;
  scope: string;
  s2a_team?: string;
  total_count: number;
  success_count: number;
  failed_count: number;
  skipped_count: number;
  created_at: string;
  started_at?: string;
  finished_at?: string;
}

export interface TeamManageBatchJobItem {
  account_id: string;
  status: string;
  invite_count: number;
  child_task_id?: string;
  message?: string;
  error?: string;
  updated_at: string;
}

export interface TeamManageBatchJobDetail extends TeamManageBatchJobSummary {
  items: TeamManageBatchJobItem[];
}

export type TeamManageBatchRetryMode = "all" | "network" | "recoverable";

export interface TeamManageBatchRetryRequest {
  retry_mode?: TeamManageBatchRetryMode;
}

export interface TeamManageBatchRetryResponse {
  job_id: string;
  retried: number;
  retry_mode: TeamManageBatchRetryMode;
  message: string;
}

export interface TeamManageOwnerPageParams {
  page?: number;
  page_size?: number;
  search?: string;
  state?: string;
  has_slots?: boolean;
  has_banned_member?: boolean;
  sort?: string;
  order?: "asc" | "desc";
}

export interface TeamManageBatchFilters {
  search?: string;
  state?: string;
  has_slots?: boolean;
  has_banned_member?: boolean;
}

export interface TeamManageBatchCheckResponse {
  results: OwnerHealth[];
  cache_hits?: number;
  cache_misses?: number;
  stale_returns?: number;
  scheduled_refreshes?: number;
}

export interface TeamManageBatchRefreshMembersResponse {
  total: number;
  success: number;
  failed: number;
  refreshed: Array<{
    account_id: string;
    member_count: number;
  }>;
}

export interface TeamManageBatchOwnerStateRequest {
  account_ids: string[];
  scope?: "manual" | "filtered";
  filters?: TeamManageBatchFilters;
  reason?: string;
}

export interface TeamManageBatchOwnerStateResponse {
  affected: number;
  state: string;
  message: string;
}

export interface TeamManageOwnerAuditRecord {
  id: number;
  account_id: string;
  action: string;
  from_state?: string;
  to_state: string;
  reason?: string;
  scope?: string;
  batch_job_id?: string;
  created_at: string;
}

export interface TeamManageOwnerAuditQuery {
  account_id?: string;
  action?: string;
  batch_job_id?: string;
  page?: number;
  page_size?: number;
}

export interface TeamManageOwnerAuditPageResponse {
  records: TeamManageOwnerAuditRecord[];
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
}

export interface InviteTaskRecord {
  id: string;
  upload_id: string;
  owner_email: string;
  owner_account_id: string;
  s2a_team?: string;
  invite_count: number;
  status: string;
  invited_ok: number;
  invited_failed: number;
  reg_ok: number;
  reg_failed: number;
  rt_ok: number;
  rt_failed: number;
  s2a_ok: number;
  s2a_failed: number;
  error?: string;
  created_at: string;
  finished_at?: string;
}

export interface InviteEmailRecord {
  id: number;
  task_id: string;
  email: string;
  invite_status: string;
  reg_status: string;
  rt_status: string;
  s2a_status: string;
  error?: string;
}

export interface InviteTaskDetail {
  task: InviteTaskRecord;
  emails: InviteEmailRecord[];
}

export const TEAM_MANAGE_PAGE_SIZE = 10;
export const TEAM_MANAGE_MAX_MEMBERS = 4;

export function quotaColor(remaining: number) {
  if (remaining >= 60) return "#2dd4bf";
  if (remaining >= 30) return "#f59e0b";
  return "#f87171";
}

export function fmtReset(secs: number) {
  if (secs <= 0) return "--";
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (h > 24) {
    const d = Math.floor(h / 24);
    return `${d}天${h % 24}h`;
  }
  if (h > 0) return `${h}h${m}m`;
  return `${m}m`;
}

export function buildFallbackDashboardSummary(
  owners: TeamOwner[],
  healthMap: Record<string, OwnerHealth>,
  memberCounts: Record<string, number>,
) {
  let activeOwners = 0;
  let bannedOwners = 0;
  let expiredOwners = 0;
  let quarantinedOwners = 0;
  let ownersWithSlots = 0;
  let ownersWithBannedMembers = 0;
  let freshCacheOwners = 0;
  let staleCacheOwners = 0;
  const now = Date.now();

  for (const owner of owners) {
    const state = owner.state ?? "active";
    const health = healthMap[owner.account_id];
    const count = memberCounts[owner.account_id] ?? owner.member_count ?? 0;
    const availableSlots = owner.available_slots_cached ?? Math.max(0, TEAM_MANAGE_MAX_MEMBERS - count);
    const hasBannedMember = Boolean(health?.members.some(member => member.status === "banned"));
    const isBanned = health?.owner_status === "banned" || hasBannedMember;
    const isExpired = state === "expired";
    const isQuarantined = state === "quarantined";
    const checkedAt = (owner.checked_at ?? health?.checked_at) ? Date.parse(owner.checked_at ?? health?.checked_at ?? "") : Number.NaN;
    const cacheStatus = owner.cache_status ?? health?.cache_status ?? (
      !Number.isNaN(checkedAt) && now - checkedAt <= 10 * 60 * 1000
        ? "fresh"
        : !Number.isNaN(checkedAt) && now - checkedAt <= 30 * 60 * 1000
          ? "stale"
          : "miss"
    );

    if (state === "active") activeOwners += 1;
    if (isBanned) bannedOwners += 1;
    if (isExpired) expiredOwners += 1;
    if (isQuarantined) quarantinedOwners += 1;
    if (availableSlots > 0 && state !== "seat_limited") ownersWithSlots += 1;
    if (hasBannedMember) ownersWithBannedMembers += 1;
    if (cacheStatus === "fresh") freshCacheOwners += 1;
    if (cacheStatus === "stale" || cacheStatus === "expired") staleCacheOwners += 1;
  }

  return {
    total_owners: owners.length,
    active_owners: activeOwners,
    banned_owners: bannedOwners,
    expired_owners: expiredOwners,
    quarantined_owners: quarantinedOwners,
    owners_with_slots: ownersWithSlots,
    owners_with_banned_members: ownersWithBannedMembers,
    fresh_cache_owners: freshCacheOwners,
    stale_cache_owners: staleCacheOwners,
    checked_recently: freshCacheOwners,
  } satisfies TeamManageDashboardSummary;
}
