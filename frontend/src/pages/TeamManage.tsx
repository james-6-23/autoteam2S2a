import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Loader2, Search, ShieldAlert, Trash2, UserPlus, X, Zap } from "lucide-react";

import { useToast } from "../components/Toast";
import { HSelect } from "../components/ui/HSelect";
import { HSwitch } from "../components/ui/HSwitch";
import { OwnerDashboard } from "../components/team-manage/OwnerDashboard";
import { OwnerList } from "../components/team-manage/OwnerList";
import { TeamManagePagination } from "../components/team-manage/TeamManagePagination";
import * as api from "../lib/api";
import type {
  CodexQuota,
  OwnerHealth,
  S2aTeam,
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
  if (quota.status === "rate_limited") {
    return (
      <div className="flex min-w-[120px] flex-col gap-0.5">
        <span className="flex items-center gap-0.5 rounded px-1.5 py-0.5 text-[.6rem] font-medium" style={{ background: "rgba(245,158,11,.12)", color: "#f59e0b" }}>
          限流中
        </span>
        {quota.five_hour && <QuotaBar label="5h" window={quota.five_hour} />}
        {quota.seven_day && <QuotaBar label="7d" window={quota.seven_day} />}
      </div>
    );
  }
  if (quota.status === "error") {
    const err = quota.error || "";
    const isExternal = err.includes("外部域名");
    const isPoolNotFound = err.includes("号池域名") || err.includes("已移除");
    const label = isExternal ? "外部域名" : isPoolNotFound ? "号池未找到" : "错误";
    const color = isExternal ? "#94a3b8" : isPoolNotFound ? "#f59e0b" : "var(--text-dim)";
    return <span className="text-[.6rem]" style={{ color }} title={err}>{label}</span>;
  }
  return (
    <div className="flex min-w-[120px] flex-col gap-0.5">
      {quota.five_hour && <QuotaBar label="5h" window={quota.five_hour} />}
      {quota.seven_day && <QuotaBar label="7d" window={quota.seven_day} />}
    </div>
  );
}

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
  const [kickBannedLoading, setKickBannedLoading] = useState(false);

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
  const [batchInviteForm, setBatchInviteForm] = useState({
    s2a_team: "",
    strategy: "fill_to_limit" as "fill_to_limit" | "fixed_count",
    fixed_count: 1,
    skip_banned: true,
    skip_expired: false,
    skip_quarantined: false,
    only_with_slots: true,
  });

  const [showFillSlotsModal, setShowFillSlotsModal] = useState(false);
  const [fillSlotsLoading, setFillSlotsLoading] = useState(false);
  const [fillSlotsPools, setFillSlotsPools] = useState<Array<{ team: string; weight: number }>>([]);

  const [batchCheckLoading, setBatchCheckLoading] = useState(false);
  const [batchCheckProgress, setBatchCheckProgress] = useState({ done: 0, total: 0 });
  const batchCheckCancelledRef = useRef(false);
  const batchCheckLoadingRef = useRef(false);

  // 同步 ref + sessionStorage，防止刷新后丢失状态
  const updateBatchCheckState = useCallback((loading: boolean, progress?: { done: number; total: number }) => {
    batchCheckLoadingRef.current = loading;
    setBatchCheckLoading(loading);
    if (progress) setBatchCheckProgress(progress);
    if (loading) {
      sessionStorage.setItem("batch-check-state", JSON.stringify({
        loading: true,
        ...(progress ?? { done: 0, total: 0 }),
        ts: Date.now(),
      }));
    } else {
      sessionStorage.removeItem("batch-check-state");
    }
  }, []);

  // 刷新浏览器时警告
  useEffect(() => {
    const handler = (e: BeforeUnloadEvent) => {
      if (batchCheckLoadingRef.current) {
        e.preventDefault();
      }
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, []);

  // 组件挂载时检测中断的批量检查
  useEffect(() => {
    const raw = sessionStorage.getItem("batch-check-state");
    if (raw) {
      try {
        const state = JSON.parse(raw);
        // 超过 5 分钟认为过期
        if (state.loading && Date.now() - state.ts < 5 * 60 * 1000) {
          toast(`上次批量检查被中断（已完成 ${state.done}/${state.total}），请重新开始`, "error");
        }
      } catch { /* ignore */ }
      sessionStorage.removeItem("batch-check-state");
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
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

  const selectedOwnerCount = selectionScope === "filtered" ? ownerTotal : selectedOwnerIds.length;
  const selectedOwnerSet = useMemo(
    () => new Set(selectionScope === "filtered" ? owners.map(owner => owner.account_id) : selectedOwnerIds),
    [owners, selectedOwnerIds, selectionScope],
  );

  const ownerQuery = useMemo(
    () => ({
      page_size: TEAM_MANAGE_PAGE_SIZE,
      search: searchInput.trim() || undefined,
      state: stateFilter || undefined,
      has_slots: onlyWithSlots ? true : undefined,
      has_banned_member: onlyWithBannedMembers ? true : undefined,
    }),
    [onlyWithBannedMembers, onlyWithSlots, searchInput, stateFilter],
  );

  const loadOwners = useCallback(async (page: number) => {
    setLoading(true);
    try {
      const data = await api.fetchTeamManageOwnersPage({
        ...ownerQuery,
        page,
      });
      setOwners(data.items || []);
      setOwnerTotal(data.total);
      setOwnerTotalPages(data.total_pages);
    } catch (error) {
      setOwners([]);
      toast(`获取 Owner 失败: ${error}`, "error");
    } finally {
      setLoading(false);
    }
  }, [ownerQuery, toast]);

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
      void loadOwnerQuota(accountId);
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
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const isNotFound = msg.includes("404") || msg.includes("找不到");
      setMemberQuotas(prev => ({
        ...prev,
        [email]: { status: "error", error: isNotFound ? "凭证未找到" : msg } as CodexQuota,
      }));
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
    const concurrency = Math.min(toKick.length, 20);
    let cursor = 0;
    const kickOne = async () => {
      while (cursor < toKick.length) {
        const idx = cursor++;
        const member = toKick[idx];
        try {
          await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(member.user_id)}/kick`);
          success += 1;
          setMembers(prev => prev.filter(item => item.user_id !== member.user_id));
        } catch {
          failed += 1;
        }
        setKickAllProgress({ done: success + failed, total: toKick.length });
      }
    };
    await Promise.all(Array.from({ length: concurrency }, () => kickOne()));
    setKickAllLoading(false);
    setMemberCounts(prev => ({ ...prev, [selected]: Math.max(0, members.length - success) }));
    toast(`踢除完成: 成功 ${success}，失败 ${failed}`, failed > 0 ? "error" : "success");
    void loadDashboard();
  };

  const kickBanned = async () => {
    if (!selected) return;
    const toKick = members.filter(member =>
      member.role !== "owner"
      && member.role !== "account-owner"
      && member.email
      && memberQuotas[member.email]?.status === "banned",
    );
    if (toKick.length === 0) {
      toast("没有封禁成员可踢除", "error");
      return;
    }
    if (!confirm(`确定要踢除全部 ${toKick.length} 个封禁成员？`)) return;
    setKickBannedLoading(true);
    let success = 0;
    let failed = 0;
    const concurrency = Math.min(toKick.length, 20);
    let cursor = 0;
    const kickOne = async () => {
      while (cursor < toKick.length) {
        const idx = cursor++;
        const member = toKick[idx];
        try {
          await api.post(`/api/team-manage/owners/${encodeURIComponent(selected)}/members/${encodeURIComponent(member.user_id)}/kick`);
          success += 1;
          setMembers(prev => prev.filter(item => item.user_id !== member.user_id));
        } catch {
          failed += 1;
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, () => kickOne()));
    setKickBannedLoading(false);
    setMemberCounts(prev => ({ ...prev, [selected]: Math.max(0, members.length - success) }));
    toast(`踢除封禁成员完成: 成功 ${success}，失败 ${failed}`, failed > 0 ? "error" : "success");
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
    let accountIds: string[];
    if (selectionScope === "filtered") {
      const allIds: string[] = [];
      let page = 1;
      const pageSize = 500;
      updateBatchCheckState(true, { done: 0, total: 0 });
      try {
        // 第一页获取 total_pages 信息
        const firstPage = await api.fetchTeamManageOwnersPage({
          ...ownerQuery,
          page: 1,
          page_size: pageSize,
        });
        for (const owner of firstPage.items || []) {
          allIds.push(owner.account_id);
        }
        const totalPages = firstPage.total_pages;
        // 估算总数用于显示进度
        const estimatedTotal = (firstPage as { total?: number }).total
          ?? totalPages * pageSize;
        updateBatchCheckState(true, { done: 0, total: estimatedTotal });

        // 剩余页并行获取（最多 5 页同时）
        if (totalPages > 1) {
          const remainingPages = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);
          const PARALLEL_FETCH = 5;
          for (let i = 0; i < remainingPages.length; i += PARALLEL_FETCH) {
            const batch = remainingPages.slice(i, i + PARALLEL_FETCH);
            const results = await Promise.all(
              batch.map(p =>
                api.fetchTeamManageOwnersPage({
                  ...ownerQuery,
                  page: p,
                  page_size: pageSize,
                }),
              ),
            );
            for (const data of results) {
              for (const owner of data.items || []) {
                allIds.push(owner.account_id);
              }
            }
          }
        }
      } catch (error) {
        toast(`获取 Owner 列表失败: ${error}`, "error");
        updateBatchCheckState(false);
        return;
      }
      accountIds = allIds;
    } else {
      accountIds = selectedOwnerIds.length > 0
        ? selectedOwnerIds
        : owners.map(owner => owner.account_id);
    }

    if (accountIds.length === 0) {
      updateBatchCheckState(false);
      return;
    }

    // chunk 大小跟随并发设置，确保后端 semaphore 被充分利用
    const chunkSize = Math.max(checkConcurrency, 50);
    const chunks: string[][] = [];
    for (let i = 0; i < accountIds.length; i += chunkSize) {
      chunks.push(accountIds.slice(i, i + chunkSize));
    }

    updateBatchCheckState(true, { done: 0, total: accountIds.length });
    batchCheckCancelledRef.current = false;
    const checkStartTime = Date.now();

    let totalCacheHits = 0;
    let totalCacheMisses = 0;
    let doneCount = 0;
    let failedChunks = 0;
    let processedChunks = 0;
    let nextChunkIndex = 0;
    // 并行请求数：减少 HTTP 往返开销
    const chunkRequestConcurrency = 5;

    const runChunkWorker = async () => {
      while (true) {
        if (batchCheckCancelledRef.current) {
          return;
        }

        const chunkIndex = nextChunkIndex;
        if (chunkIndex >= chunks.length) {
          return;
        }
        nextChunkIndex += 1;

        try {
          const res = await api.batchCheckTeamManageOwners({
            account_ids: chunks[chunkIndex],
            concurrency: checkConcurrency,
            force_refresh: forceRefreshHealth,
            prefer_cache: !forceRefreshHealth,
            scope: "manual",
          });

          setHealthMap(prev => {
            const next = { ...prev };
            for (const record of res.results || []) {
              next[record.account_id] = record;
            }
            return next;
          });

          for (const record of res.results || []) {
            setMemberCounts(prev => ({ ...prev, [record.account_id]: record.members.length }));
          }

          totalCacheHits += res.cache_hits ?? 0;
          totalCacheMisses += res.cache_misses ?? 0;
        } catch {
          failedChunks += 1;
        }

        doneCount += chunks[chunkIndex].length;
        processedChunks += 1;
        updateBatchCheckState(true, { done: doneCount, total: accountIds.length });

        if (processedChunks % 5 === 0) {
          void loadDashboard();
        }
      }
    };

    const workerCount = Math.min(chunkRequestConcurrency, chunks.length);
    await Promise.all(Array.from({ length: workerCount }, () => runChunkWorker()));

    if (batchCheckCancelledRef.current) {
      toast(`检查已取消，已完成 ${doneCount}/${accountIds.length}`, "success");
    }

    const elapsed = ((Date.now() - checkStartTime) / 1000).toFixed(1);
    void loadDashboard();
    if (!batchCheckCancelledRef.current) {
      const parts = [`${accountIds.length} 个 owner`, `耗时 ${elapsed}s`];
      if (totalCacheHits > 0) parts.push(`缓存 ${totalCacheHits}`);
      parts.push(`实查 ${totalCacheMisses}`);
      if (failedChunks > 0) parts.push(`失败 ${failedChunks} 批`);
      toast(
        `检查完成: ${parts.join("，")}`,
        failedChunks > 0 ? "error" : "success",
      );
    }
    updateBatchCheckState(false);
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
    } catch (error) {
      toast(`批量邀请失败: ${error}`, "error");
    } finally {
      setBatchInviteLoading(false);
    }
  };

  const submitFillSlots = async () => {
    const activePools = fillSlotsPools.filter(p => p.team && p.weight > 0);
    if (activePools.length === 0) {
      toast("请至少添加一个号池", "error");
      return;
    }

    setFillSlotsLoading(true);

    // 单号池：直接让后端筛选，无需前端分页拉取
    if (activePools.length === 1) {
      try {
        const result = await api.batchInviteTeamManageOwners({
          account_ids: [],
          s2a_team: activePools[0].team,
          strategy: "fill_to_limit",
          scope: "filtered",
          filters: { has_slots: true },
          skip_banned: true,
          only_with_slots: true,
        });
        setFillSlotsLoading(false);
        setShowFillSlotsModal(false);
        toast(`补位任务已创建: 接受 ${result.accepted}，跳过 ${result.skipped}`, "success");
      } catch (error) {
        setFillSlotsLoading(false);
        toast(`补位任务创建失败: ${error}`, "error");
      }
      return;
    }

    // 多号池：先拉取所有有空位 owner，再按权重分配到不同号池
    const allIds: string[] = [];
    let page = 1;
    try {
      while (true) {
        const data = await api.fetchTeamManageOwnersPage({
          page, page_size: 200, has_slots: true,
        });
        for (const owner of data.items || []) allIds.push(owner.account_id);
        if (page >= data.total_pages) break;
        page += 1;
      }
    } catch (error) {
      setFillSlotsLoading(false);
      toast(`获取 Owner 列表失败: ${error}`, "error");
      return;
    }

    if (allIds.length === 0) {
      setFillSlotsLoading(false);
      toast("没有可补位的 Owner", "error");
      return;
    }

    // 按权重切分 owner 到各号池
    const totalWeight = activePools.reduce((sum, p) => sum + p.weight, 0);
    let offset = 0;
    const poolAssignments: Array<{ team: string; ids: string[] }> = [];
    for (let i = 0; i < activePools.length; i++) {
      const pool = activePools[i];
      const count = i === activePools.length - 1
        ? allIds.length - offset
        : Math.round((pool.weight / totalWeight) * allIds.length);
      poolAssignments.push({ team: pool.team, ids: allIds.slice(offset, offset + count) });
      offset += count;
    }

    let totalAccepted = 0;
    let totalSkipped = 0;
    let failedPools = 0;

    for (const assignment of poolAssignments) {
      if (assignment.ids.length === 0) continue;
      try {
        const result = await api.batchInviteTeamManageOwners({
          account_ids: assignment.ids,
          s2a_team: assignment.team,
          strategy: "fill_to_limit",
          scope: "manual",
          skip_banned: true,
          only_with_slots: true,
        });
        totalAccepted += result.accepted;
        totalSkipped += result.skipped;
      } catch {
        failedPools += 1;
      }
    }

    setFillSlotsLoading(false);
    setShowFillSlotsModal(false);
    toast(
      failedPools > 0
        ? `补位任务已创建: 接受 ${totalAccepted}，跳过 ${totalSkipped}，${failedPools} 个号池失败`
        : `补位任务已创建: 接受 ${totalAccepted}，跳过 ${totalSkipped}`,
      failedPools > 0 ? "error" : "success",
    );
  };

  const selectedOwner = owners.find(owner => owner.account_id === selected);
  const kickableCount = members.filter(member => member.role !== "owner" && member.role !== "account-owner").length;
  const kickBannedCount = members.filter(member =>
    member.role !== "owner"
    && member.role !== "account-owner"
    && member.email
    && memberQuotas[member.email]?.status === "banned",
  ).length;
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
              <p className="mt-1 text-xs c-dim">管理 Owner 账号、成员配额与批量操作</p>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1">
                <span className="text-[.65rem] c-dim">并发:</span>
                <input
                  type="number"
                  min={1}
                  value={checkConcurrency}
                  onChange={event => {
                    const value = Math.max(1, Number(event.target.value) || 1);
                    setCheckConcurrency(value);
                    localStorage.setItem("team-check-concurrency", String(value));
                  }}
                  className="field-input w-14 px-1 py-1 text-center text-xs"
                />
              </div>
              <HSwitch
                checked={forceRefreshHealth}
                onChange={setForceRefreshHealth}
                label="强制刷新"
              />
              <span className="h-5 w-px shrink-0" style={{ background: "var(--border)" }} />
              <button
                type="button"
                onClick={batchCheck}
                disabled={batchCheckLoading || loading}
                className="btn btn-ghost flex items-center gap-1 py-1.5 text-xs"
              >
                {batchCheckLoading ? (
                  <><Loader2 size={12} className="animate-spin" /> 检查中 {batchCheckProgress.done}/{batchCheckProgress.total}</>
                ) : (
                  <><Search size={12} /> 一键检查</>
                )}
              </button>
              {batchCheckLoading && (
                <button
                  type="button"
                  onClick={() => { batchCheckCancelledRef.current = true; }}
                  className="btn btn-ghost py-1.5 text-xs"
                  style={{ color: "#f87171" }}
                >
                  取消
                </button>
              )}
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
            <HSelect
              value={stateFilter}
              onChange={setStateFilter}
              className="team-manage-filterbar__select"
              options={[
                { value: "", label: "全部状态" },
                { value: "active", label: "活跃" },
                { value: "banned", label: "封禁" },
                { value: "expired", label: "过期" },
                { value: "quarantined", label: "隔离" },
                { value: "seat_limited", label: "席位已满" },
              ]}
            />
            <HSwitch
              checked={onlyWithSlots}
              onChange={setOnlyWithSlots}
              label="仅有空位"
            />
            <HSwitch
              checked={onlyWithBannedMembers}
              onChange={setOnlyWithBannedMembers}
              label="仅有封禁成员"
            />
          </div>
          <OwnerDashboard summary={dashboardSummary} loading={dashboardLoading} />
          <div className="team-manage-bulkbar">
            <div className="team-manage-bulkbar__meta">
              已选 <span className="font-mono c-heading">{selectedOwnerCount}</span> 个 Owner
              <span className="c-dim"> · 当前筛选共 {ownerTotal} 个</span>
            </div>
            <div className="team-manage-bulkbar__actions">
              {/* 选择操作 */}
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
              <span className="h-5 w-px shrink-0" style={{ background: "var(--border)" }} />
              {/* 批量操作 */}
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
              <span className="h-5 w-px shrink-0" style={{ background: "var(--border)" }} />
              {/* 邀请操作 */}
              <button
                type="button"
                onClick={() => {
                  setFillSlotsPools(s2aTeams.length > 0 ? [{ team: s2aTeams[0].name, weight: 100 }] : []);
                  setShowFillSlotsModal(true);
                }}
                className="btn py-1.5 text-xs"
                style={{ background: "linear-gradient(135deg, rgba(139,92,246,0.84), rgba(20,184,166,0.82))", color: "#fff" }}
              >
                一键补位
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


        <TeamManagePagination
          page={ownerPage}
          totalPages={ownerTotalPages}
          totalItems={ownerTotal}
          onChange={page => { setOwnerPage(page); }}
        />
      </div>

      {selected && createPortal(
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
                  {kickBannedCount > 0 && (
                    <button type="button" onClick={() => void kickBanned()} disabled={kickBannedLoading || kickAllLoading} className="btn btn-danger flex items-center gap-1 px-2 py-1 text-[.65rem]">
                      {kickBannedLoading ? (
                        <><Loader2 size={11} className="animate-spin" /> 踢除中...</>
                      ) : (
                        <><ShieldAlert size={11} /> 踢除封禁 ({kickBannedCount})</>
                      )}
                    </button>
                  )}
                  {kickableCount > 0 && (
                    <button type="button" onClick={() => void kickAll()} disabled={kickAllLoading || kickBannedLoading} className="btn btn-danger flex items-center gap-1 px-2 py-1 text-[.65rem]">
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
                    <>
                      {/* 成员列表表头 */}
                      <div
                        className="flex items-center gap-3 px-4 py-2 text-[.62rem] font-semibold c-dim"
                        style={{ borderBottom: "1px solid var(--border)", background: "rgba(255,255,255,0.015)" }}
                      >
                        <div className="min-w-0 flex-1">成员信息</div>
                        <div className="shrink-0" style={{ width: 130 }}>额度状态</div>
                        <div className="shrink-0 text-center" style={{ width: 52 }}>操作</div>
                      </div>
                      <div>
                      {pagedMembers.map(member => {
                        const quota = member.email ? memberQuotas[member.email] : undefined;
                        const quotaLoading = member.email ? memberQuotaLoading[member.email] : false;
                        return (
                          <div
                            key={member.user_id}
                            className="flex items-center gap-3 px-4 py-3 transition-colors hover:bg-[rgba(255,255,255,0.025)]"
                            style={{ borderBottom: "1px solid rgba(255,255,255,0.03)" }}
                          >
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-2">
                                <span className="truncate text-sm font-medium c-heading">{member.name || member.email || "未知"}</span>
                                <span
                                  className={`shrink-0 rounded-md px-1.5 py-0.5 text-[.58rem] font-semibold ${member.role === "owner" ? "text-amber-400" : "c-dim"}`}
                                  style={{ background: "var(--ghost)" }}
                                >
                                  {member.role}
                                </span>
                              </div>
                              <div className="mt-0.5 flex items-center gap-2 text-[.65rem] c-dim">
                                {member.email && <span className="truncate font-mono">{member.email}</span>}
                                {member.created_at && <span className="shrink-0">{new Date(member.created_at).toLocaleDateString("zh-CN")}</span>}
                              </div>
                            </div>

                            <div className="shrink-0" style={{ width: 130 }}>
                              <MemberQuotaInline
                                quota={quota}
                                loading={quotaLoading}
                                onLoad={() => { if (member.email) void loadMemberQuota(member.email); }}
                              />
                            </div>

                            <div className="shrink-0 text-center" style={{ width: 52 }}>
                              {member.role !== "owner" && member.role !== "account-owner" && (
                                <button
                                  type="button"
                                  onClick={() => void kickMember(member.user_id)}
                                  disabled={kickLoading === member.user_id || kickAllLoading}
                                  className="btn btn-danger px-2 py-1 text-[.65rem]"
                                >
                                  {kickLoading === member.user_id ? "..." : "踢除"}
                                </button>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                    </>
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
      , document.body)}

      {showBatchInviteModal && createPortal(
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
              <div>
                <span className="field-label">目标号池</span>
                <HSelect
                  value={batchInviteForm.s2a_team}
                  onChange={value => setBatchInviteForm(prev => ({ ...prev, s2a_team: value }))}
                  placeholder="请选择号池"
                  className="w-full"
                  options={[
                    { value: "", label: "请选择号池" },
                    ...s2aTeams.map(team => ({ value: team.name, label: team.name })),
                  ]}
                />
              </div>

              <div>
                <span className="field-label">邀请策略</span>
                <HSelect
                  value={batchInviteForm.strategy}
                  onChange={value => setBatchInviteForm(prev => ({
                    ...prev,
                    strategy: value as "fill_to_limit" | "fixed_count",
                  }))}
                  className="w-full"
                  options={[
                    { value: "fill_to_limit", label: "补满到上限" },
                    { value: "fixed_count", label: "每个 Owner 固定数量" },
                  ]}
                />
              </div>

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

              <div className="grid gap-3">
                <HSwitch
                  checked={batchInviteForm.skip_banned}
                  onChange={v => setBatchInviteForm(prev => ({ ...prev, skip_banned: v }))}
                  label="跳过封禁 Owner"
                />
                <HSwitch
                  checked={batchInviteForm.skip_expired}
                  onChange={v => setBatchInviteForm(prev => ({ ...prev, skip_expired: v }))}
                  label="跳过过期 Owner"
                />
                <HSwitch
                  checked={batchInviteForm.skip_quarantined}
                  onChange={v => setBatchInviteForm(prev => ({ ...prev, skip_quarantined: v }))}
                  label="跳过隔离 Owner"
                />
                <HSwitch
                  checked={batchInviteForm.only_with_slots}
                  onChange={v => setBatchInviteForm(prev => ({ ...prev, only_with_slots: v }))}
                  label="仅处理有空位的 Owner"
                />
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
      , document.body)}

      {showFillSlotsModal && createPortal(
        <div className="team-modal" onClick={event => { if (event.target === event.currentTarget) setShowFillSlotsModal(false); }}>
          <div className="team-modal-card p-6" style={{ maxWidth: 640, width: "96vw" }} onClick={event => event.stopPropagation()}>
            {/* 标题区 */}
            <div className="mb-5 flex items-start justify-between">
              <div>
                <div className="section-title mb-0">一键补位</div>
                <div className="mt-1.5 text-xs c-dim">
                  自动补位所有有空位的 Owner，按权重分配到号池
                </div>
              </div>
              <button type="button" onClick={() => setShowFillSlotsModal(false)} className="btn btn-ghost p-1.5 -mr-1 -mt-1">
                <X size={16} />
              </button>
            </div>

            {/* 号池列表 */}
            <div className="space-y-2.5">
              {fillSlotsPools.map((pool, index) => {
                const totalWeight = fillSlotsPools.reduce((s, p) => s + p.weight, 0);
                const pct = totalWeight > 0 ? Math.round((pool.weight / totalWeight) * 100) : 0;
                return (
                  <div
                    key={index}
                    className="flex items-center gap-2 rounded-lg px-3 py-2.5"
                    style={{ background: "var(--ghost)" }}
                  >
                    <div className="min-w-[140px] flex-1">
                      <HSelect
                        value={pool.team}
                        onChange={v => {
                          const next = [...fillSlotsPools];
                          next[index] = { ...next[index], team: v };
                          setFillSlotsPools(next);
                        }}
                        className="w-full"
                        placeholder="选择号池"
                        options={[
                          { value: "", label: "请选择号池" },
                          ...s2aTeams.map(t => ({ value: t.name, label: t.name })),
                        ]}
                      />
                    </div>
                    <div className="flex items-center gap-1.5 shrink-0">
                      <span className="text-[.65rem] c-dim">权重</span>
                      <input
                        type="number"
                        min={1}
                        value={pool.weight}
                        onChange={event => {
                          const next = [...fillSlotsPools];
                          next[index] = { ...next[index], weight: Math.max(1, Number(event.target.value) || 1) };
                          setFillSlotsPools(next);
                        }}
                        className="field-input w-14 px-1.5 py-1 text-center text-xs"
                      />
                      <span
                        className="w-10 shrink-0 text-right text-xs font-mono font-semibold"
                        style={{ color: pct >= 50 ? "var(--accent)" : "var(--text-dim)" }}
                      >
                        {pct}%
                      </span>
                    </div>
                    {fillSlotsPools.length > 1 && (
                      <button
                        type="button"
                        onClick={() => setFillSlotsPools(prev => prev.filter((_, i) => i !== index))}
                        className="btn btn-ghost p-1 shrink-0 opacity-50 hover:opacity-100"
                      >
                        <X size={12} />
                      </button>
                    )}
                  </div>
                );
              })}
            </div>

            {/* 添加按钮 */}
            <button
              type="button"
              onClick={() => setFillSlotsPools(prev => [...prev, { team: "", weight: 1 }])}
              className="btn btn-ghost mt-2.5 w-full py-2 text-xs"
              style={{ borderStyle: "dashed" }}
            >
              + 添加号池
            </button>

            {/* 底部操作 */}
            <div className="mt-6 flex justify-end gap-2">
              <button type="button" onClick={() => setShowFillSlotsModal(false)} className="btn btn-ghost">
                取消
              </button>
              <button
                type="button"
                onClick={() => void submitFillSlots()}
                disabled={fillSlotsLoading || fillSlotsPools.filter(p => p.team && p.weight > 0).length === 0}
                className="btn"
                style={{ background: "linear-gradient(135deg, rgba(139,92,246,0.84), rgba(20,184,166,0.82))", color: "#fff" }}
              >
                {fillSlotsLoading ? <span className="inline-flex items-center gap-2"><Loader2 size={14} className="animate-spin" /> 提交中</span> : "确认补位"}
              </button>
            </div>
          </div>
        </div>
      , document.body)}
    </div>
  );
}
