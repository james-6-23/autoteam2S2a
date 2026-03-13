import { useCallback, useEffect, useMemo, useState } from "react";
import { Loader2, Search, ShieldAlert, Trash2, UserPlus, X, Zap } from "lucide-react";

import { useToast } from "../components/Toast";
import { OwnerDashboard } from "../components/team-manage/OwnerDashboard";
import { OwnerList } from "../components/team-manage/OwnerList";
import { TeamManagePagination } from "../components/team-manage/TeamManagePagination";
import * as api from "../lib/api";
import type {
  CodexQuota,
  InviteTaskDetail,
  OwnerHealth,
  S2aTeam,
  TeamManageBatchJobDetail,
  TeamManageBatchRetryMode,
  TeamManageBatchJobSummary,
  TeamManageDashboardSummary,
  TeamMember,
  TeamOwner,
} from "../lib/team-manage-types";
import {
  TEAM_MANAGE_MAX_MEMBERS,
  TEAM_MANAGE_PAGE_SIZE,
  buildFallbackDashboardSummary,
  fmtReset,
  quotaColor,
} from "../lib/team-manage-types";

function QuotaBar({ label, window }: { label: string; window: NonNullable<CodexQuota["five_hour"]> }) {
  const pct = Math.round(window.remaining_percent);
  const color = quotaColor(pct);
  return (
    <div className="flex items-center gap-2 text-[.65rem]">
      <span className="c-dim w-5 shrink-0">{label}</span>
      <div className="h-1.5 min-w-[40px] flex-1 overflow-hidden rounded-full" style={{ background: "var(--ghost)" }}>
        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="w-7 shrink-0 text-right font-mono" style={{ color }}>{pct}%</span>
      <span className="c-dim w-12 shrink-0 text-right">{fmtReset(window.reset_after_seconds)}</span>
    </div>
  );
}

function MemberQuotaInline({ quota, loading, onLoad }: { quota?: CodexQuota; loading?: boolean; onLoad: () => void }) {
  if (loading) {
    return <div className="flex items-center gap-1 text-[.6rem] c-dim"><Loader2 size={10} className="animate-spin" />查询中</div>;
  }
  if (!quota) {
    return (
      <button
        type="button"
        onClick={event => {
          event.stopPropagation();
          onLoad();
        }}
        className="rounded px-1.5 py-0.5 text-[.6rem] transition-colors"
        style={{ color: "var(--text-dim)", background: "var(--ghost)" }}
        title="查额度"
      >
        <span className="inline-flex items-center gap-0.5"><Zap size={10} /> 额度</span>
      </button>
    );
  }
  if (quota.status === "banned") {
    return (
      <span className="flex items-center gap-0.5 rounded px-1.5 py-0.5 text-[.6rem] font-medium" style={{ background: "rgba(248,113,113,.12)", color: "#f87171" }}>
        <ShieldAlert size={10} />封禁
      </span>
    );
  }
  if (quota.status === "error") {
    return <span className="text-[.6rem] c-dim" title={quota.error || ""}>错误</span>;
  }
  return (
    <div className="flex min-w-[120px] flex-col gap-0.5">
      {quota.five_hour && <QuotaBar label="5h" window={quota.five_hour} />}
      {quota.seven_day && <QuotaBar label="7d" window={quota.seven_day} />}
    </div>
  );
}

const INVITE_TASK_EMAIL_PREVIEW = 12;

export default function TeamManage() {
  const { toast } = useToast();
  const [owners, setOwners] = useState<TeamOwner[]>([]);
  const [dashboard, setDashboard] = useState<TeamManageDashboardSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [ownerPage, setOwnerPage] = useState(1);
  const [ownerTotal, setOwnerTotal] = useState(0);
  const [ownerTotalPages, setOwnerTotalPages] = useState(1);
  const [selectedOwnerIds, setSelectedOwnerIds] = useState<string[]>([]);
  const [selectionScope, setSelectionScope] = useState<"manual" | "filtered">("manual");
  const [searchInput, setSearchInput] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [onlyWithSlots, setOnlyWithSlots] = useState(false);
  const [onlyWithBannedMembers, setOnlyWithBannedMembers] = useState(false);
  const [forceRefreshHealth, setForceRefreshHealth] = useState(false);

  const [selected, setSelected] = useState<string | null>(null);
  const [members, setMembers] = useState<TeamMember[]>([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [kickLoading, setKickLoading] = useState<string | null>(null);
  const [kickAllLoading, setKickAllLoading] = useState(false);
  const [kickAllProgress, setKickAllProgress] = useState({ done: 0, total: 0 });

  const [memberCounts, setMemberCounts] = useState<Record<string, number>>({});
  const [ownerQuotas, setOwnerQuotas] = useState<Record<string, CodexQuota>>({});
  const [ownerQuotaLoading, setOwnerQuotaLoading] = useState<Record<string, boolean>>({});
  const [memberQuotas, setMemberQuotas] = useState<Record<string, CodexQuota>>({});
  const [memberQuotaLoading, setMemberQuotaLoading] = useState<Record<string, boolean>>({});
  const [healthMap, setHealthMap] = useState<Record<string, OwnerHealth>>({});

  const [s2aTeams, setS2aTeams] = useState<S2aTeam[]>([]);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [inviteLoading, setInviteLoading] = useState(false);
  const [selectedS2aTeam, setSelectedS2aTeam] = useState("");
  const [showBatchInviteModal, setShowBatchInviteModal] = useState(false);
  const [batchInviteLoading, setBatchInviteLoading] = useState(false);
  const [batchRefreshLoading, setBatchRefreshLoading] = useState(false);
  const [batchJobs, setBatchJobs] = useState<TeamManageBatchJobSummary[]>([]);
  const [activeBatchJobId, setActiveBatchJobId] = useState<string | null>(null);
  const [activeBatchJob, setActiveBatchJob] = useState<TeamManageBatchJobDetail | null>(null);
  const [batchJobLoading, setBatchJobLoading] = useState(false);
  const [retryFailedLoading, setRetryFailedLoading] = useState(false);
  const [retryMode, setRetryMode] = useState<TeamManageBatchRetryMode>("all");
  const [activeInviteTaskId, setActiveInviteTaskId] = useState<string | null>(null);
  const [activeInviteTask, setActiveInviteTask] = useState<InviteTaskDetail | null>(null);
  const [inviteTaskLoading, setInviteTaskLoading] = useState(false);
  const [batchInviteForm, setBatchInviteForm] = useState({
    s2a_team: "",
    strategy: "fill_to_limit" as "fill_to_limit" | "fixed_count",
    fixed_count: 1,
    skip_banned: true,
    skip_expired: false,
    skip_quarantined: false,
    only_with_slots: true,
  });

  const [batchCheckLoading, setBatchCheckLoading] = useState(false);
  const [batchCheckProgress, setBatchCheckProgress] = useState({ done: 0, total: 0 });
  const [checkConcurrency, setCheckConcurrency] = useState(() =>
    parseInt(localStorage.getItem("team-check-concurrency") || "5", 10),
  );
  const [memberPage, setMemberPage] = useState(1);

  const pagedMembers = useMemo(
    () => members.slice((memberPage - 1) * TEAM_MANAGE_PAGE_SIZE, memberPage * TEAM_MANAGE_PAGE_SIZE),
    [memberPage, members],
  );

  const dashboardSummary = useMemo(
    () => dashboard ?? buildFallbackDashboardSummary(owners, healthMap, memberCounts),
    [dashboard, healthMap, memberCounts, owners],
  );
  const visibleInviteEmails = useMemo(
    () => activeInviteTask?.emails.slice(0, INVITE_TASK_EMAIL_PREVIEW) ?? [],
    [activeInviteTask],
  );

  const selectedOwnerCount = selectionScope === "filtered" ? ownerTotal : selectedOwnerIds.length;
  const selectedOwnerSet = useMemo(
    () => new Set(selectionScope === "filtered" ? owners.map(owner => owner.account_id) : selectedOwnerIds),
    [owners, selectedOwnerIds, selectionScope],
  );

  const ownerQuery = useMemo(
    () => ({
      page: ownerPage,
      page_size: TEAM_MANAGE_PAGE_SIZE,
      search: searchInput.trim() || undefined,
      state: stateFilter || undefined,
      has_slots: onlyWithSlots ? true : undefined,
      has_banned_member: onlyWithBannedMembers ? true : undefined,
    }),
    [onlyWithBannedMembers, onlyWithSlots, ownerPage, searchInput, stateFilter],
  );

  const loadOwners = useCallback(async (page = ownerPage) => {
    setLoading(true);
    try {
      const data = await api.fetchTeamManageOwnersPage({
        ...ownerQuery,
        page,
      });
      setOwners(data.items || []);
      setOwnerPage(data.page);
      setOwnerTotal(data.total);
      setOwnerTotalPages(data.total_pages);
    } catch (error) {
      setOwners([]);
      toast(`获取 Owner 失败: ${error}`, "error");
    } finally {
      setLoading(false);
    }
  }, [ownerPage, ownerQuery, toast]);

  const loadDashboard = useCallback(async () => {
    setDashboardLoading(true);
    try {
      const data = await api.fetchTeamManageDashboard();
      setDashboard(data);
    } catch {
      setDashboard(null);
    } finally {
      setDashboardLoading(false);
    }
  }, []);

  const loadBatchJobs = useCallback(async () => {
    try {
      const data = await api.fetchTeamManageBatchJobs();
      setBatchJobs(data.jobs || []);
    } catch {
      setBatchJobs([]);
    }
  }, []);

  const loadBatchJobDetail = useCallback(async (
    jobId: string,
    options?: { preserveInviteTask?: boolean },
  ) => {
    setBatchJobLoading(true);
    if (!options?.preserveInviteTask) {
      setActiveInviteTask(null);
      setActiveInviteTaskId(null);
    }
    try {
      const data = await api.fetchTeamManageBatchJob(jobId);
      setActiveBatchJob(data);
      setActiveBatchJobId(jobId);
    } catch (error) {
      toast(`获取批量任务详情失败: ${error}`, "error");
    } finally {
      setBatchJobLoading(false);
    }
  }, [toast]);

  const loadInviteTaskDetail = useCallback(async (taskId: string) => {
    setInviteTaskLoading(true);
    try {
      const data = await api.fetchInviteTaskDetail(taskId);
      setActiveInviteTask(data);
      setActiveInviteTaskId(taskId);
    } catch (error) {
      toast(`获取邀请任务详情失败: ${error}`, "error");
    } finally {
      setInviteTaskLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    void loadOwners(ownerPage);
  }, [loadOwners, ownerPage]);

  useEffect(() => {
    setOwnerPage(1);
  }, [onlyWithBannedMembers, onlyWithSlots, searchInput, stateFilter]);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    void loadBatchJobs();
  }, [loadBatchJobs]);

  useEffect(() => {
    if (!batchJobs.some(job => job.status === "pending" || job.status === "running")) {
      return;
    }
    const timer = window.setInterval(() => {
      void loadBatchJobs();
      if (activeBatchJobId) void loadBatchJobDetail(activeBatchJobId, { preserveInviteTask: true });
    }, 3000);
    return () => window.clearInterval(timer);
  }, [activeBatchJobId, batchJobs, loadBatchJobDetail, loadBatchJobs]);

  useEffect(() => {
    if (!activeInviteTaskId || !activeInviteTask) {
      return;
    }
    if (activeInviteTask.task.status !== "pending" && activeInviteTask.task.status !== "running") {
      return;
    }
    const timer = window.setInterval(() => {
      void loadInviteTaskDetail(activeInviteTaskId);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [activeInviteTask, activeInviteTaskId, loadInviteTaskDetail]);

  useEffect(() => {
    void (async () => {
      try {
        const data = await api.fetchTeamManageHealth();
        const map: Record<string, OwnerHealth> = {};
        for (const record of data.records || []) {
          map[record.account_id] = record;
        }
        setHealthMap(map);
      } catch {
        setHealthMap({});
      }
    })();
  }, []);

  useEffect(() => {
    void (async () => {
      try {
        const data = await api.fetchTeamManageConfigTeams();
        setS2aTeams(data.teams || []);
      } catch {
        setS2aTeams([]);
      }
    })();
  }, []);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (event.key === "Escape") setSelected(null);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);

  const loadMembers = async (accountId: string) => {
    setSelected(accountId);
    setMembers([]);
    setMembersLoading(true);
    setMemberPage(1);
    setMemberQuotas({});
    setMemberQuotaLoading({});
    try {
      const data = await api.fetchTeamManageOwnerMembers(accountId);
      const list = (data.members || []).filter(member => member.role !== "account-owner");
      setMembers(list);
      setMemberCounts(prev => ({ ...prev, [accountId]: list.length }));
      setTimeout(() => {
        for (const member of list) {
          if (member.email) void loadMemberQuota(member.email);
        }
      }, 100);
    } catch {
      toast("获取成员失败", "error");
    } finally {
      setMembersLoading(false);
    }
  };

  const loadOwnerQuota = async (accountId: string) => {
    setOwnerQuotaLoading(prev => ({ ...prev, [accountId]: true }));
    try {
      const data = await api.fetchTeamManageOwnerQuota(accountId);
      setOwnerQuotas(prev => ({ ...prev, [accountId]: data }));
    } catch {
      toast("额度查询失败", "error");
    } finally {
      setOwnerQuotaLoading(prev => ({ ...prev, [accountId]: false }));
    }
  };

  const loadMemberQuota = async (email: string) => {
    setMemberQuotaLoading(prev => ({ ...prev, [email]: true }));
    try {
      const data = await api.fetchTeamManageMemberQuota(email);
      setMemberQuotas(prev => ({ ...prev, [email]: data }));
    } catch {
      toast(`${email} 额度查询失败`, "error");
    } finally {
      setMemberQuotaLoading(prev => ({ ...prev, [email]: false }));
    }
  };

  const loadAllMemberQuotas = async () => {
    const toLoad = members.filter(member => member.email && !memberQuotas[member.email] && !memberQuotaLoading[member.email]);
    if (toLoad.length === 0) {
      toast("没有可查询的成员", "error");
      return;
    }
    toast(`开始查询 ${toLoad.length} 个成员的额度...`, "success");
    for (const member of toLoad) {
      if (member.email) await loadMemberQuota(member.email);
    }
    toast("全部额度查询完成", "success");
  };

  const kickMember = async (userId: string) => {
    if (!selected) return;
    if (!confirm("确定要踢除该成员？")) return;
    setKickLoading(userId);
    try {
      await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(userId)}/kick`);
      toast("已踢除成员", "success");
      setMembers(prev => {
        const next = prev.filter(member => member.user_id !== userId);
        setMemberCounts(counts => ({ ...counts, [selected]: next.length }));
        return next;
      });
      void loadDashboard();
    } catch (error) {
      toast(`踢除失败: ${error}`, "error");
    } finally {
      setKickLoading(null);
    }
  };

  const kickAll = async () => {
    if (!selected) return;
    const toKick = members.filter(member => member.role !== "owner" && member.role !== "account-owner");
    if (toKick.length === 0) {
      toast("没有可踢除的成员", "error");
      return;
    }
    if (!confirm(`确定要踢除全部 ${toKick.length} 个成员？`)) return;
    setKickAllLoading(true);
    setKickAllProgress({ done: 0, total: toKick.length });
    let success = 0;
    let failed = 0;
    for (const member of toKick) {
      try {
        await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(member.user_id)}/kick`);
        success += 1;
        setMembers(prev => prev.filter(item => item.user_id !== member.user_id));
      } catch {
        failed += 1;
      }
      setKickAllProgress({ done: success + failed, total: toKick.length });
    }
    setKickAllLoading(false);
    setMemberCounts(prev => ({ ...prev, [selected]: Math.max(0, members.length - success) }));
    toast(`踢除完成: 成功 ${success}，失败 ${failed}`, failed > 0 ? "error" : "success");
    void loadDashboard();
  };

  const refreshMembers = async (accountId: string) => {
    try {
      const data = await api.refreshTeamManageOwnerMembers(accountId);
      const list = data.members || [];
      toast("已刷新成员列表", "success");
      setMemberCounts(prev => ({ ...prev, [accountId]: list.length }));
      if (selected === accountId) {
        setMembers(list);
        setMemberPage(1);
      }
      void loadDashboard();
    } catch (error) {
      toast(`刷新失败: ${error}`, "error");
    }
  };

  const inviteAndPush = async (s2aTeam: string) => {
    if (!selected) return;
    const availableSlots = Math.max(0, TEAM_MANAGE_MAX_MEMBERS - members.length);
    if (availableSlots <= 0) {
      toast("已满员，无法邀请", "error");
      return;
    }
    setInviteLoading(true);
    try {
      const res = await api.inviteTeamManageOwner(selected, { s2a_team: s2aTeam, invite_count: availableSlots });
      toast(`${res.message} (任务ID: ${res.task_id})`, "success");
      setShowInviteModal(false);
      void loadDashboard();
    } catch (error) {
      toast(`邀请失败: ${error}`, "error");
    } finally {
      setInviteLoading(false);
    }
  };

  const batchCheck = async () => {
    const accountIds = selectionScope === "filtered"
      ? []
      : selectedOwnerIds.length > 0
        ? selectedOwnerIds
        : owners.map(owner => owner.account_id);
    const targetTotal = selectionScope === "filtered" ? ownerTotal : accountIds.length;
    if (targetTotal === 0) return;
    setBatchCheckLoading(true);
    setBatchCheckProgress({ done: 0, total: targetTotal });
    try {
      const res = await api.batchCheckTeamManageOwners({
        account_ids: accountIds,
        concurrency: checkConcurrency,
        force_refresh: forceRefreshHealth,
        prefer_cache: !forceRefreshHealth,
        scope: selectionScope === "filtered" ? "filtered" : (selectedOwnerIds.length > 0 ? "manual" : "page"),
        filters: selectionScope === "filtered" ? {
          search: ownerQuery.search,
          state: ownerQuery.state,
          has_slots: ownerQuery.has_slots,
          has_banned_member: ownerQuery.has_banned_member,
        } : undefined,
      });
      const nextMap = { ...healthMap };
      for (const record of res.results || []) {
        nextMap[record.account_id] = record;
        setMemberCounts(prev => ({ ...prev, [record.account_id]: record.members.length }));
      }
      setHealthMap(nextMap);
      setBatchCheckProgress({ done: targetTotal, total: targetTotal });
      void loadDashboard();
      toast(
        `检查完成: 命中缓存 ${res.cache_hits ?? 0}，重算 ${res.cache_misses ?? 0}`,
        "success",
      );
    } catch (error) {
      toast(`检查失败: ${error}`, "error");
    } finally {
      setBatchCheckLoading(false);
    }
  };

  const toggleOwnerSelected = (accountId: string) => {
    setSelectionScope("manual");
    setSelectedOwnerIds(prev =>
      prev.includes(accountId)
        ? prev.filter(id => id !== accountId)
        : [...prev, accountId],
    );
  };

  const toggleSelectPage = () => {
    setSelectionScope("manual");
    const pageIds = owners.map(owner => owner.account_id);
    const allSelected = pageIds.length > 0 && pageIds.every(id => selectedOwnerIds.includes(id));
    setSelectedOwnerIds(prev => {
      if (allSelected) {
        return prev.filter(id => !pageIds.includes(id));
      }
      return Array.from(new Set([...prev, ...pageIds]));
    });
  };

  const selectAllFilteredOwners = async () => {
    setSelectionScope("filtered");
    setSelectedOwnerIds([]);
    toast(`已切换为筛选结果作用域，共 ${ownerTotal} 个 Owner`, "success");
  };

  const batchRefreshSelectedMembers = async () => {
    if (selectionScope !== "filtered" && selectedOwnerIds.length === 0) {
      toast("请先选择 Owner", "error");
      return;
    }
    setBatchRefreshLoading(true);
    try {
      const result = await api.batchRefreshTeamManageOwnerMembers({
        account_ids: selectionScope === "filtered" ? [] : selectedOwnerIds,
        concurrency: checkConcurrency,
        scope: selectionScope,
        filters: selectionScope === "filtered" ? {
          search: ownerQuery.search,
          state: ownerQuery.state,
          has_slots: ownerQuery.has_slots,
          has_banned_member: ownerQuery.has_banned_member,
        } : undefined,
      });
      setMemberCounts(prev => {
        const next = { ...prev };
        for (const item of result.refreshed || []) {
          if (typeof item.member_count === "number") {
            next[item.account_id] = item.member_count;
          }
        }
        return next;
      });
      toast(
        `批量刷新完成: 成功 ${result.success}，失败 ${result.failed}`,
        result.failed > 0 ? "error" : "success",
      );
      void loadOwners(ownerPage);
      void loadDashboard();
    } catch (error) {
      toast(`批量刷新成员失败: ${error}`, "error");
    } finally {
      setBatchRefreshLoading(false);
    }
  };

  const batchUpdateOwnerState = useCallback(async (
    action: "disable" | "restore" | "archive",
    reason?: string,
  ) => {
    if (selectionScope !== "filtered" && selectedOwnerIds.length === 0) {
      toast("请先选择 Owner", "error");
      return;
    }
    const run = action === "disable"
      ? api.batchDisableTeamManageOwners
      : action === "restore"
        ? api.batchRestoreTeamManageOwners
        : api.batchArchiveTeamManageOwners;
    try {
      const result = await run({
        account_ids: selectionScope === "filtered" ? [] : selectedOwnerIds,
        scope: selectionScope,
        filters: selectionScope === "filtered" ? {
          search: ownerQuery.search,
          state: ownerQuery.state,
          has_slots: ownerQuery.has_slots,
          has_banned_member: ownerQuery.has_banned_member,
        } : undefined,
        reason,
      });
      toast(result.message, "success");
      setSelectedOwnerIds([]);
      setSelectionScope("manual");
      void loadOwners(1);
      void loadDashboard();
    } catch (error) {
      toast(`批量更新 Owner 状态失败: ${error}`, "error");
    }
  }, [loadDashboard, loadOwners, ownerQuery.has_banned_member, ownerQuery.has_slots, ownerQuery.search, ownerQuery.state, selectedOwnerIds, selectionScope, toast]);

  const retryFailedBatchItems = async (jobId: string) => {
    setRetryFailedLoading(true);
    try {
      const result = await api.retryFailedTeamManageBatchItems(jobId, { retry_mode: retryMode });
      toast(result.message, "success");
      void loadBatchJobs();
      void loadBatchJobDetail(result.job_id);
    } catch (error) {
      toast(`重试失败子项失败: ${error}`, "error");
    } finally {
      setRetryFailedLoading(false);
    }
  };

  const submitBatchInvite = async () => {
    if (selectionScope !== "filtered" && selectedOwnerIds.length === 0) {
      toast("请先选择 Owner", "error");
      return;
    }
    if (!batchInviteForm.s2a_team) {
      toast("请选择号池", "error");
      return;
    }
    setBatchInviteLoading(true);
    try {
      const result = await api.batchInviteTeamManageOwners({
        account_ids: selectionScope === "filtered" ? [] : selectedOwnerIds,
        s2a_team: batchInviteForm.s2a_team,
        strategy: batchInviteForm.strategy,
        fixed_count: batchInviteForm.strategy === "fixed_count" ? batchInviteForm.fixed_count : undefined,
        scope: selectionScope,
        filters: selectionScope === "filtered" ? {
          search: ownerQuery.search,
          state: ownerQuery.state,
          has_slots: ownerQuery.has_slots,
          has_banned_member: ownerQuery.has_banned_member,
        } : undefined,
        skip_banned: batchInviteForm.skip_banned,
        skip_expired: batchInviteForm.skip_expired,
        skip_quarantined: batchInviteForm.skip_quarantined,
        only_with_slots: batchInviteForm.only_with_slots,
      });
      toast(`${result.message} (Job: ${result.job_id})`, "success");
      setShowBatchInviteModal(false);
      setSelectedOwnerIds([]);
      setSelectionScope("manual");
      void loadBatchJobs();
    } catch (error) {
      toast(`批量邀请失败: ${error}`, "error");
    } finally {
      setBatchInviteLoading(false);
    }
  };

  const selectedOwner = owners.find(owner => owner.account_id === selected);
  const kickableCount = members.filter(member => member.role !== "owner" && member.role !== "account-owner").length;
  const availableSlots = Math.max(0, TEAM_MANAGE_MAX_MEMBERS - members.length);
  const selectedOwnerQuota = selected ? ownerQuotas[selected] : undefined;
  const isSelectedOwnerBanned = Boolean(
    selected && (
      selectedOwner?.state === "banned"
      || healthMap[selected]?.owner_status === "banned"
    ),
  );
  const allPageSelected = selectionScope === "filtered"
    || (owners.length > 0 && owners.every(owner => selectedOwnerIds.includes(owner.account_id)));

  return (
    <div className="space-y-4">
      <div className="card p-5">
        <div className="mb-4 flex flex-col gap-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="section-title mb-0">Team 管理</div>
              <p className="mt-1 text-xs c-dim">当前已接入 owner_registry、缓存健康检查、批量邀请和批量任务面板。</p>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1">
                <span className="text-[.65rem] c-dim">并发:</span>
                <input
                  type="number"
                  min={1}
                  max={20}
                  value={checkConcurrency}
                  onChange={event => {
                    const value = Math.max(1, Math.min(20, Number(event.target.value) || 1));
                    setCheckConcurrency(value);
                    localStorage.setItem("team-check-concurrency", String(value));
                  }}
                  className="field-input w-10 px-1 py-1 text-center text-xs"
                />
              </div>
              <label className="inline-flex items-center gap-2 text-xs c-dim">
                <input
                  type="checkbox"
                  checked={forceRefreshHealth}
                  onChange={event => setForceRefreshHealth(event.target.checked)}
                />
                强制刷新
              </label>
              <button type="button" onClick={batchCheck} disabled={batchCheckLoading || loading} className="btn btn-ghost flex items-center gap-1 py-1.5 text-xs">
                {batchCheckLoading ? (
                  <><Loader2 size={12} className="animate-spin" /> 检查中 {batchCheckProgress.done}/{batchCheckProgress.total}</>
                ) : (
                  <><Search size={12} /> 一键检查</>
                )}
              </button>
              <button type="button" onClick={() => void loadOwners(1)} disabled={loading} className="btn btn-ghost py-1.5 text-xs">
                {loading ? "加载中..." : "刷新"}
              </button>
            </div>
          </div>
          <div className="team-manage-filterbar">
            <input
              type="text"
              value={searchInput}
              onChange={event => setSearchInput(event.target.value)}
              className="field-input team-manage-filterbar__search"
              placeholder="搜索邮箱 / account_id"
            />
            <select value={stateFilter} onChange={event => setStateFilter(event.target.value)} className="field-input team-manage-filterbar__select">
              <option value="">全部状态</option>
              <option value="active">活跃</option>
              <option value="banned">封禁</option>
              <option value="expired">过期</option>
              <option value="quarantined">隔离</option>
            </select>
            <label className="inline-flex items-center gap-2 text-xs c-dim">
              <input type="checkbox" checked={onlyWithSlots} onChange={event => setOnlyWithSlots(event.target.checked)} />
              仅有空位
            </label>
            <label className="inline-flex items-center gap-2 text-xs c-dim">
              <input type="checkbox" checked={onlyWithBannedMembers} onChange={event => setOnlyWithBannedMembers(event.target.checked)} />
              仅有封禁成员
            </label>
          </div>
          <OwnerDashboard summary={dashboardSummary} loading={dashboardLoading} />
          <div className="team-manage-bulkbar">
            <div className="team-manage-bulkbar__meta">
              已选 <span className="font-mono c-heading">{selectedOwnerCount}</span> 个 Owner
              <span className="c-dim"> · 当前筛选共 {ownerTotal} 个</span>
            </div>
            <div className="team-manage-bulkbar__actions">
              <button type="button" onClick={toggleSelectPage} className="btn btn-ghost py-1.5 text-xs">
                {allPageSelected ? "取消本页" : "全选本页"}
              </button>
              <button type="button" onClick={() => void selectAllFilteredOwners()} disabled={ownerTotal === 0} className="btn btn-ghost py-1.5 text-xs">
                全选筛选结果
              </button>
              <button
                type="button"
                onClick={() => {
                  setSelectedOwnerIds([]);
                  setSelectionScope("manual");
                }}
                disabled={selectedOwnerCount === 0}
                className="btn btn-ghost py-1.5 text-xs"
              >
                清空选择
              </button>
              <button type="button" onClick={() => void batchRefreshSelectedMembers()} disabled={selectedOwnerCount === 0 || batchRefreshLoading} className="btn btn-ghost py-1.5 text-xs">
                {batchRefreshLoading ? "刷新中..." : "批量刷新成员"}
              </button>
              <button
                type="button"
                onClick={() => {
                  if (confirm(`确定要下线当前作用域内的 ${selectedOwnerCount} 个 Owner？`)) {
                    void batchUpdateOwnerState("disable", "manual_disable");
                  }
                }}
                disabled={selectedOwnerCount === 0}
                className="btn btn-ghost py-1.5 text-xs"
              >
                批量下线
              </button>
              <button
                type="button"
                onClick={() => {
                  if (confirm(`确定要恢复当前作用域内的 ${selectedOwnerCount} 个 Owner？`)) {
                    void batchUpdateOwnerState("restore", "manual_restore");
                  }
                }}
                disabled={selectedOwnerCount === 0}
                className="btn btn-ghost py-1.5 text-xs"
              >
                批量恢复
              </button>
              <button
                type="button"
                onClick={() => {
                  if (confirm(`确定要归档当前作用域内的 ${selectedOwnerCount} 个 Owner？`)) {
                    void batchUpdateOwnerState("archive", "manual_archive");
                  }
                }}
                disabled={selectedOwnerCount === 0}
                className="btn btn-ghost py-1.5 text-xs"
              >
                批量归档
              </button>
              <button
                type="button"
                onClick={() => {
                  setBatchInviteForm(prev => ({
                    ...prev,
                    s2a_team: prev.s2a_team || s2aTeams[0]?.name || "",
                  }));
                  setShowBatchInviteModal(true);
                }}
                disabled={selectedOwnerCount === 0}
                className="btn py-1.5 text-xs"
                style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.84), rgba(59,130,246,0.82))", color: "#fff" }}
              >
                批量邀请入库
              </button>
              <button type="button" onClick={() => void loadBatchJobs()} className="btn btn-ghost py-1.5 text-xs">
                刷新任务
              </button>
            </div>
          </div>
        </div>

        <OwnerList
          owners={owners}
          loading={loading}
          memberCounts={memberCounts}
          ownerQuotas={ownerQuotas}
          ownerQuotaLoading={ownerQuotaLoading}
          healthMap={healthMap}
          selectedOwnerIds={selectedOwnerSet}
          onOpenMembers={loadMembers}
          onToggleSelected={toggleOwnerSelected}
          onLoadOwnerQuota={accountId => { void loadOwnerQuota(accountId); }}
          onRefreshMembers={accountId => { void refreshMembers(accountId); }}
        />

        {batchJobs.length > 0 && (
          <div className="team-manage-jobs">
            <div className="team-manage-jobs__header">
              <span className="text-sm font-medium c-heading">批量任务</span>
              <span className="text-xs c-dim">展示最近 {batchJobs.length} 条</span>
            </div>
            <div className="space-y-2">
              {batchJobs.slice(0, 6).map(job => (
                <button
                  key={job.job_id}
                  type="button"
                  className={`team-manage-jobs__item ${activeBatchJobId === job.job_id ? "team-manage-jobs__item--active" : ""}`}
                  onClick={() => {
                    void loadBatchJobDetail(job.job_id);
                  }}
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium c-heading">{job.job_type}</span>
                      <span className="badge badge-off">{job.status}</span>
                    </div>
                    <div className="mt-1 text-[.7rem] c-dim font-mono">
                      {job.job_id} · {job.created_at}
                    </div>
                  </div>
                  <div className="text-right text-[.75rem]">
                    <div className="font-mono c-heading">
                      {job.success_count}/{job.total_count}
                    </div>
                    <div className="c-dim">
                      失败 {job.failed_count} · 跳过 {job.skipped_count}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        )}

        {(batchJobLoading || activeBatchJob) && (
          <div className="team-manage-job-detail">
            <div className="team-manage-jobs__header">
              <span className="text-sm font-medium c-heading">
                {batchJobLoading ? "任务详情加载中..." : `任务详情 · ${activeBatchJob?.job_id}`}
              </span>
              {activeBatchJob && activeBatchJob.failed_count > 0 && activeBatchJob.job_type === "batch_invite" && (
                <div className="team-manage-job-detail__toolbar">
                  <select
                    value={retryMode}
                    onChange={event => setRetryMode(event.target.value as TeamManageBatchRetryMode)}
                    className="input py-1.5 text-xs"
                    style={{ minWidth: 140 }}
                  >
                    <option value="all">重试全部失败项</option>
                    <option value="network">仅网络类错误</option>
                    <option value="recoverable">仅可恢复错误</option>
                  </select>
                  <button
                    type="button"
                    onClick={() => void retryFailedBatchItems(activeBatchJob.job_id)}
                    disabled={retryFailedLoading}
                    className="btn btn-ghost py-1.5 text-xs"
                  >
                    {retryFailedLoading ? "重试中..." : "重试失败子项"}
                  </button>
                </div>
              )}
            </div>
            {activeBatchJob && (
              <div className="space-y-2">
                {activeBatchJob.items.map(item => (
                  <div key={`${activeBatchJob.job_id}-${item.account_id}`} className="team-manage-jobs__detail-item">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-xs c-heading">{item.account_id}</span>
                        <span className="badge badge-off">{item.status}</span>
                      </div>
                      <div className="mt-1 text-[.72rem] c-dim">
                        invite_count: {item.invite_count}
                        {item.child_task_id ? (
                          <>
                            {" · "}
                            <button
                              type="button"
                              className="team-manage-link-button"
                              onClick={() => void loadInviteTaskDetail(item.child_task_id!)}
                            >
                              task: {item.child_task_id}
                            </button>
                          </>
                        ) : null}
                      </div>
                      {item.message && <div className="mt-1 text-[.72rem] c-dim">{item.message}</div>}
                      {item.error && <div className="mt-1 text-[.72rem] text-red-400">{item.error}</div>}
                    </div>
                    <div className="text-[.72rem] c-dim">{item.updated_at}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {(inviteTaskLoading || activeInviteTask) && (
          <div className="team-manage-job-detail">
            <div className="team-manage-jobs__header">
              <span className="text-sm font-medium c-heading">
                {inviteTaskLoading ? "邀请任务详情加载中..." : `邀请任务详情 · ${activeInviteTask?.task.id}`}
              </span>
              {activeInviteTask && (
                <button
                  type="button"
                  onClick={() => {
                    setActiveInviteTaskId(null);
                    setActiveInviteTask(null);
                  }}
                  className="btn btn-ghost py-1.5 text-xs"
                >
                  关闭
                </button>
              )}
            </div>
            {activeInviteTask && (
              <div className="space-y-3">
                <div className="team-manage-invite-task__summary">
                  <div>
                    <div className="text-[.72rem] c-dim">Owner</div>
                    <div className="font-mono text-xs c-heading">{activeInviteTask.task.owner_account_id}</div>
                    <div className="text-[.72rem] c-dim">{activeInviteTask.task.owner_email}</div>
                  </div>
                  <div>
                    <div className="text-[.72rem] c-dim">状态</div>
                    <div className="flex items-center gap-2">
                      <span className="badge badge-off">{activeInviteTask.task.status}</span>
                      <span className="text-[.72rem] c-dim">{activeInviteTask.task.s2a_team || "--"}</span>
                    </div>
                  </div>
                  <div>
                    <div className="text-[.72rem] c-dim">进度</div>
                    <div className="text-[.78rem] c-heading">
                      邀请 {activeInviteTask.task.invited_ok}/{activeInviteTask.task.invite_count}
                      {" · "}
                      注册 {activeInviteTask.task.reg_ok}/{activeInviteTask.task.invite_count}
                    </div>
                    <div className="text-[.72rem] c-dim">
                      RT {activeInviteTask.task.rt_ok} · 入库 {activeInviteTask.task.s2a_ok}
                    </div>
                  </div>
                </div>
                {activeInviteTask.task.error && (
                  <div className="rounded-xl border border-red-500/20 bg-red-500/8 px-3 py-2 text-[.75rem] text-red-300">
                    {activeInviteTask.task.error}
                  </div>
                )}
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-sm font-medium c-heading">邮箱明细</span>
                    <span className="text-[.72rem] c-dim">
                      展示前 {Math.min(INVITE_TASK_EMAIL_PREVIEW, activeInviteTask.emails.length)} / {activeInviteTask.emails.length} 条
                    </span>
                  </div>
                  {visibleInviteEmails.map(email => (
                    <div key={email.id} className="team-manage-jobs__detail-item">
                      <div className="min-w-0">
                        <div className="font-mono text-xs c-heading">{email.email}</div>
                        <div className="mt-1 text-[.72rem] c-dim">
                          invite:{email.invite_status} · reg:{email.reg_status} · rt:{email.rt_status} · s2a:{email.s2a_status}
                        </div>
                        {email.error && <div className="mt-1 text-[.72rem] text-red-400">{email.error}</div>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        <TeamManagePagination
          page={ownerPage}
          totalPages={ownerTotalPages}
          totalItems={ownerTotal}
          onChange={page => { setOwnerPage(page); }}
        />
      </div>

      {selected && (
        <div className="team-modal" onClick={event => { if (event.target === event.currentTarget) setSelected(null); }}>
          <div
            className="team-modal-card p-5"
            style={{ maxWidth: 920, width: "96vw", maxHeight: "85vh", display: "flex", flexDirection: "column" }}
            onClick={event => event.stopPropagation()}
          >
            <div className="mb-3">
              <div className="flex items-center justify-between">
                <div className="min-w-0">
                  <div className="section-title mb-0 truncate">
                    成员管理 <span className="text-amber-400">{selectedOwner?.email || selected.substring(0, 12)}</span>
                  </div>
                  <div className="mt-0.5 text-xs font-mono c-dim">{selected}</div>
                </div>
                <button type="button" onClick={() => setSelected(null)} className="btn btn-ghost shrink-0 p-1" title="关闭">
                  <X size={14} />
                </button>
              </div>
              {!membersLoading && !isSelectedOwnerBanned && (
                <div className="mt-2 flex flex-wrap items-center gap-1.5">
                  {availableSlots > 0 && (
                    <button
                      type="button"
                      onClick={() => setShowInviteModal(true)}
                      disabled={inviteLoading}
                      className="btn flex items-center gap-1 px-2 py-1 text-[.65rem]"
                      style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.8), rgba(59,130,246,0.8))", color: "#fff" }}
                    >
                      {inviteLoading ? <Loader2 size={11} className="animate-spin" /> : <UserPlus size={11} />}
                      邀请并入库 ({availableSlots})
                    </button>
                  )}
                  {members.length > 0 && (
                    <button type="button" onClick={() => void loadAllMemberQuotas()} className="btn btn-ghost flex items-center gap-1 px-2 py-1 text-[.65rem]">
                      <Zap size={11} /> 刷新额度
                    </button>
                  )}
                  {kickableCount > 0 && (
                    <button type="button" onClick={() => void kickAll()} disabled={kickAllLoading} className="btn btn-danger flex items-center gap-1 px-2 py-1 text-[.65rem]">
                      {kickAllLoading ? (
                        <><Loader2 size={11} className="animate-spin" /> {kickAllProgress.done}/{kickAllProgress.total}</>
                      ) : (
                        <><Trash2 size={11} /> 全踢 ({kickableCount})</>
                      )}
                    </button>
                  )}
                </div>
              )}
            </div>

            {isSelectedOwnerBanned ? (
              <div
                className="flex flex-1 flex-col items-center justify-center gap-2 rounded-xl border px-4 py-12 text-center"
                style={{ borderColor: "rgba(248,113,113,.35)", background: "rgba(248,113,113,.08)", color: "#fca5a5" }}
              >
                <ShieldAlert size={20} />
                <div className="text-sm font-semibold">Owner 封禁</div>
                <div className="text-xs" style={{ color: "#fda4af" }}>该 Owner 已被封禁，无法进行成员管理操作</div>
              </div>
            ) : (
              <>
                <div className="mb-3 flex flex-wrap items-center gap-4 border-b pb-3 text-xs" style={{ borderColor: "var(--border)" }}>
                  {!membersLoading && (
                    <>
                      <span className="c-dim">共 <span className="font-mono c-heading">{members.length}</span> 个成员</span>
                      <span className="c-dim">可踢除 <span className="font-mono text-red-400">{kickableCount}</span></span>
                      <span className="c-dim">可邀请 <span className="font-mono" style={{ color: availableSlots > 0 ? "#2dd4bf" : "#f87171" }}>{availableSlots}</span></span>
                    </>
                  )}
                  <span className="h-3 w-px" style={{ background: "var(--border)" }} />
                  <span className="c-dim">Owner 额度:</span>
                  {selectedOwnerQuota ? (
                    selectedOwnerQuota.status === "banned" ? (
                      <span className="flex items-center gap-1 font-medium" style={{ color: "#f87171" }}>
                        <ShieldAlert size={12} /> 封禁
                      </span>
                    ) : selectedOwnerQuota.status === "error" ? (
                      <span className="c-dim" title={selectedOwnerQuota.error || ""}>查询失败</span>
                    ) : (
                      <>
                        {selectedOwnerQuota.five_hour && (
                          <span style={{ color: quotaColor(selectedOwnerQuota.five_hour.remaining_percent) }}>
                            5h <span className="font-mono font-medium">{Math.round(selectedOwnerQuota.five_hour.remaining_percent)}%</span>
                            <span className="c-dim ml-1">({fmtReset(selectedOwnerQuota.five_hour.reset_after_seconds)})</span>
                          </span>
                        )}
                        {selectedOwnerQuota.seven_day && (
                          <span style={{ color: quotaColor(selectedOwnerQuota.seven_day.remaining_percent) }}>
                            7d <span className="font-mono font-medium">{Math.round(selectedOwnerQuota.seven_day.remaining_percent)}%</span>
                            <span className="c-dim ml-1">({fmtReset(selectedOwnerQuota.seven_day.reset_after_seconds)})</span>
                          </span>
                        )}
                      </>
                    )
                  ) : (
                    <button
                      type="button"
                      onClick={() => { if (selected) void loadOwnerQuota(selected); }}
                      disabled={selected ? ownerQuotaLoading[selected] || false : true}
                      className="btn btn-ghost flex items-center gap-1 px-2 py-0.5 text-[.65rem]"
                    >
                      {selected && ownerQuotaLoading[selected] ? <Loader2 size={11} className="animate-spin" /> : <Zap size={11} />} 查询
                    </button>
                  )}
                </div>

                <div className="flex-1 overflow-y-auto" style={{ minHeight: 0 }}>
                  {membersLoading ? (
                    <div className="flex items-center justify-center gap-2 py-12 c-dim">
                      <Loader2 size={16} className="animate-spin" /> 加载中...
                    </div>
                  ) : members.length === 0 ? (
                    <div className="py-12 text-center c-dim">无成员数据</div>
                  ) : (
                    <div className="space-y-1.5">
                      {pagedMembers.map(member => {
                        const quota = member.email ? memberQuotas[member.email] : undefined;
                        const quotaLoading = member.email ? memberQuotaLoading[member.email] : false;
                        return (
                          <div key={member.user_id} className="card-inner flex items-center gap-3 px-4 py-3">
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-2">
                                <span className="truncate text-sm font-medium c-heading">{member.name || member.email || "未知"}</span>
                                <span className={`shrink-0 rounded px-1.5 py-0.5 text-[.6rem] font-medium ${member.role === "owner" ? "text-amber-400" : "c-dim"}`} style={{ background: "var(--ghost)" }}>
                                  {member.role}
                                </span>
                              </div>
                              <div className="mt-0.5 flex items-center gap-2 text-[.65rem] c-dim">
                                {member.email && <span className="truncate font-mono">{member.email}</span>}
                                {member.created_at && <span className="shrink-0">{new Date(member.created_at).toLocaleDateString("zh-CN")}</span>}
                              </div>
                            </div>

                            <div className="shrink-0">
                              <MemberQuotaInline
                                quota={quota}
                                loading={quotaLoading}
                                onLoad={() => { if (member.email) void loadMemberQuota(member.email); }}
                              />
                            </div>

                            <div className="shrink-0">
                              {member.role !== "owner" && member.role !== "account-owner" && (
                                <button
                                  type="button"
                                  onClick={() => void kickMember(member.user_id)}
                                  disabled={kickLoading === member.user_id || kickAllLoading}
                                  className="btn btn-danger px-2.5 py-1 text-xs"
                                >
                                  {kickLoading === member.user_id ? "..." : "踢除"}
                                </button>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>

                <TeamManagePagination
                  page={memberPage}
                  totalPages={Math.max(1, Math.ceil(members.length / TEAM_MANAGE_PAGE_SIZE))}
                  totalItems={members.length}
                  onChange={page => setMemberPage(page)}
                />

                {showInviteModal && (
                  <div
                    className="absolute inset-0 flex items-center justify-center rounded-2xl"
                    style={{ background: "rgba(0,0,0,.6)", zIndex: 50 }}
                    onClick={() => !inviteLoading && setShowInviteModal(false)}
                  >
                    <div
                      className="rounded-xl p-4"
                      style={{ background: "var(--card)", border: "1px solid var(--border)", minWidth: 320, maxWidth: 420 }}
                      onClick={event => event.stopPropagation()}
                    >
                      <div className="mb-3 flex items-center justify-between">
                        <span className="text-sm font-medium c-heading">选择号池</span>
                        <button type="button" onClick={() => setShowInviteModal(false)} disabled={inviteLoading} className="btn btn-ghost p-1">
                          <X size={14} />
                        </button>
                      </div>
                      <p className="mb-3 text-xs c-dim">将为该 Owner 邀请 <span className="font-mono text-teal-400">{availableSlots}</span> 个成员并入库到选定号池</p>
                      <div className="mb-3 max-h-48 space-y-1.5 overflow-y-auto">
                        {s2aTeams.length === 0 ? (
                          <div className="py-4 text-center text-xs c-dim">暂无号池配置</div>
                        ) : s2aTeams.map(team => (
                          <div
                            key={team.name}
                            className="cursor-pointer rounded-lg px-3 py-2 transition-all"
                            style={{
                              background: selectedS2aTeam === team.name ? "rgba(20,184,166,.15)" : "var(--ghost)",
                              border: `1px solid ${selectedS2aTeam === team.name ? "rgba(20,184,166,.5)" : "var(--border)"}`,
                            }}
                            onClick={() => setSelectedS2aTeam(team.name)}
                          >
                            <div className="text-sm font-medium c-heading">{team.name}</div>
                            <div className="truncate text-[.6rem] font-mono c-dim">{team.api_base}</div>
                          </div>
                        ))}
                      </div>
                      <button
                        type="button"
                        onClick={() => void inviteAndPush(selectedS2aTeam)}
                        disabled={!selectedS2aTeam || inviteLoading}
                        className="w-full rounded-lg py-2 text-sm font-medium transition-all disabled:opacity-40"
                        style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.85), rgba(59,130,246,0.85))", color: "#fff" }}
                      >
                        {inviteLoading ? (
                          <span className="flex items-center justify-center gap-1.5"><Loader2 size={14} className="animate-spin" /> 邀请中...</span>
                        ) : (
                          `确认邀请 ${availableSlots} 个到 ${selectedS2aTeam || "..."}`
                        )}
                      </button>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}

      {showBatchInviteModal && (
        <div className="team-modal" onClick={event => { if (event.target === event.currentTarget) setShowBatchInviteModal(false); }}>
          <div className="team-modal-card p-5" style={{ maxWidth: 520, width: "96vw" }} onClick={event => event.stopPropagation()}>
            <div className="mb-4 flex items-center justify-between">
              <div>
                <div className="section-title mb-0">批量邀请入库</div>
                <div className="mt-1 text-xs c-dim">当前已选 {selectedOwnerCount} 个 Owner</div>
              </div>
              <button type="button" onClick={() => setShowBatchInviteModal(false)} className="btn btn-ghost p-1">
                <X size={14} />
              </button>
            </div>

            <div className="space-y-3">
              <label className="block">
                <span className="field-label">目标号池</span>
                <select
                  value={batchInviteForm.s2a_team}
                  onChange={event => setBatchInviteForm(prev => ({ ...prev, s2a_team: event.target.value }))}
                  className="field-input w-full"
                >
                  <option value="">请选择号池</option>
                  {s2aTeams.map(team => (
                    <option key={team.name} value={team.name}>{team.name}</option>
                  ))}
                </select>
              </label>

              <label className="block">
                <span className="field-label">邀请策略</span>
                <select
                  value={batchInviteForm.strategy}
                  onChange={event => setBatchInviteForm(prev => ({
                    ...prev,
                    strategy: event.target.value as "fill_to_limit" | "fixed_count",
                  }))}
                  className="field-input w-full"
                >
                  <option value="fill_to_limit">补满到上限</option>
                  <option value="fixed_count">每个 Owner 固定数量</option>
                </select>
              </label>

              {batchInviteForm.strategy === "fixed_count" && (
                <label className="block">
                  <span className="field-label">固定邀请数</span>
                  <input
                    type="number"
                    min={1}
                    max={25}
                    value={batchInviteForm.fixed_count}
                    onChange={event => setBatchInviteForm(prev => ({
                      ...prev,
                      fixed_count: Math.max(1, Math.min(25, Number(event.target.value) || 1)),
                    }))}
                    className="field-input w-full"
                  />
                </label>
              )}

              <div className="grid gap-2 text-sm c-dim">
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={batchInviteForm.skip_banned} onChange={event => setBatchInviteForm(prev => ({ ...prev, skip_banned: event.target.checked }))} />
                  跳过封禁 Owner
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={batchInviteForm.skip_expired} onChange={event => setBatchInviteForm(prev => ({ ...prev, skip_expired: event.target.checked }))} />
                  跳过过期 Owner
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={batchInviteForm.skip_quarantined} onChange={event => setBatchInviteForm(prev => ({ ...prev, skip_quarantined: event.target.checked }))} />
                  跳过隔离 Owner
                </label>
                <label className="inline-flex items-center gap-2">
                  <input type="checkbox" checked={batchInviteForm.only_with_slots} onChange={event => setBatchInviteForm(prev => ({ ...prev, only_with_slots: event.target.checked }))} />
                  仅处理有空位的 Owner
                </label>
              </div>
            </div>

            <div className="mt-5 flex justify-end gap-2">
              <button type="button" onClick={() => setShowBatchInviteModal(false)} className="btn btn-ghost">
                取消
              </button>
              <button
                type="button"
                onClick={() => void submitBatchInvite()}
                disabled={batchInviteLoading || selectedOwnerCount === 0}
                className="btn"
                style={{ background: "linear-gradient(135deg, rgba(20,184,166,0.84), rgba(59,130,246,0.82))", color: "#fff" }}
              >
                {batchInviteLoading ? <span className="inline-flex items-center gap-2"><Loader2 size={14} className="animate-spin" /> 提交中</span> : "创建批量任务"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
