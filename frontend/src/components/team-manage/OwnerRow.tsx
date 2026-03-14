import { Loader2, RefreshCw, ShieldAlert, Users, Zap } from "lucide-react";

import type { CodexQuota, OwnerHealth, TeamOwner } from "../../lib/team-manage-types";
import { quotaColor } from "../../lib/team-manage-types";

interface OwnerRowProps {
  owner: TeamOwner;
  memberCount?: number;
  quota?: CodexQuota;
  quotaLoading?: boolean;
  health?: OwnerHealth;
  selected?: boolean;
  onOpenMembers: (accountId: string) => void;
  onToggleSelected: (accountId: string) => void;
  onLoadOwnerQuota: (accountId: string) => void;
  onRefreshMembers: (accountId: string) => void;
}

function MiniBar({ percent }: { percent: number }) {
  const color = quotaColor(percent);
  return (
    <div className="h-1.5 w-full max-w-[48px] overflow-hidden rounded-full" style={{ background: "var(--ghost)" }}>
      <div className="h-full rounded-full transition-all duration-300" style={{ width: `${percent}%`, background: color }} />
    </div>
  );
}

function QuotaBadge({ quota }: { quota?: CodexQuota }) {
  if (!quota) return <span className="text-[.6rem] c-dim">--</span>;
  if (quota.status === "banned") {
    return (
      <span
        className="inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[.6rem] font-semibold"
        style={{ background: "rgba(248,113,113,.12)", color: "#f87171" }}
      >
        <ShieldAlert size={9} /> 封禁
      </span>
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
    <div className="flex flex-col gap-1">
      {quota.five_hour && (
        <span
          className="inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[.58rem] font-mono tabular-nums"
          style={{
            background: `${quotaColor(quota.five_hour.remaining_percent)}12`,
            color: quotaColor(quota.five_hour.remaining_percent),
            border: `1px solid ${quotaColor(quota.five_hour.remaining_percent)}25`,
          }}
        >
          5h {Math.round(quota.five_hour.remaining_percent)}%
        </span>
      )}
      {quota.seven_day && (
        <span
          className="inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[.58rem] font-mono tabular-nums"
          style={{
            background: `${quotaColor(quota.seven_day.remaining_percent)}12`,
            color: quotaColor(quota.seven_day.remaining_percent),
            border: `1px solid ${quotaColor(quota.seven_day.remaining_percent)}25`,
          }}
        >
          7d {Math.round(quota.seven_day.remaining_percent)}%
        </span>
      )}
    </div>
  );
}

function stateToLabel(state: string): string {
  switch (state) {
    case "active": return "活跃";
    case "banned": return "封禁";
    case "expired": return "过期";
    case "quarantined": return "隔离";
    case "archived": return "归档";
    case "seat_limited": return "席位已满";
    default: return state;
  }
}

export function OwnerRow({
  owner,
  memberCount,
  quota,
  quotaLoading,
  health,
  selected,
  onOpenMembers,
  onToggleSelected,
  onLoadOwnerQuota,
  onRefreshMembers,
}: OwnerRowProps) {
  const isOwnerBanned = health?.owner_status === "banned";
  const isCheckError = health?.owner_status === "check_failed" || health?.owner_status === "timeout" || health?.owner_status === "lock_conflict";
  const hasBannedMember = health?.members.some(member => member.status === "banned");
  const stateLabel = owner.state ?? "active";
  const isSeatLimited = stateLabel === "seat_limited";

  return (
    <div
      className={`team-manage-owner-row team-manage-table-grid ${
        isOwnerBanned ? "team-manage-owner-row--owner-banned"
        : hasBannedMember ? "team-manage-owner-row--alert"
        : ""
      }`}
      onClick={() => onOpenMembers(owner.account_id)}
    >
      {/* 列1: 复选框 */}
      <button
        type="button"
        className={`team-manage-owner-row__check ${selected ? "team-manage-owner-row__check--selected" : ""}`}
        onClick={event => {
          event.stopPropagation();
          onToggleSelected(owner.account_id);
        }}
        aria-pressed={selected}
        title={selected ? "取消选择" : "选择该 Owner"}
      >
        <span className="team-manage-owner-row__check-dot" />
      </button>

      {/* 列2: Owner 身份 */}
      <div className="min-w-0">
        <div
          className="truncate text-[.82rem] font-medium c-heading"
          style={{ fontFamily: "var(--font-mono)", letterSpacing: "-0.02em" }}
        >
          {owner.email || "未知邮箱"}
        </div>
        <div
          className="mt-0.5 truncate text-[.65rem] c-dim"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {owner.account_id}
        </div>
      </div>

      {/* 列3: 状态 */}
      <div className="flex flex-col items-start gap-1">
        <span className={`badge ${
          isOwnerBanned ? "badge-err"
          : hasBannedMember ? "badge-warn"
          : isSeatLimited ? "badge-warn"
          : stateLabel === "active" ? "badge-ok"
          : "badge-off"
        }`}>
          {isOwnerBanned ? "Owner封禁" : hasBannedMember ? "成员封禁" : stateToLabel(stateLabel)}
        </span>
        {health?.checked_at && (
          <span className="text-[.52rem] c-dim whitespace-nowrap">
            {new Date(health.checked_at).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}
          </span>
        )}
      </div>

      {/* 列4: 成员额度 */}
      <div className="flex items-center gap-2.5 overflow-hidden">
        {health ? (
          isCheckError ? (
            <span className="text-[.6rem]" style={{ color: health.owner_status === "timeout" ? "#f59e0b" : "#94a3b8" }}>
              {health.owner_status === "check_failed" ? "检查失败" : health.owner_status === "timeout" ? "超时" : "锁冲突"}
            </span>
          ) : health.members.length > 0 ? health.members.map((member, index) => {
            const pct = member.seven_day_pct;
            const memberBanned = member.status === "banned";
            const isExternal = member.status === "external_domain";
            const isPoolNotFound = member.status === "pool_not_found";
            const color = memberBanned ? "#f87171"
              : isExternal ? "#94a3b8"
              : isPoolNotFound ? "#f59e0b"
              : pct != null ? quotaColor(pct) : "var(--text-dim)";
            return (
              <div key={member.email} className="flex flex-col items-center gap-0.5" style={{ minWidth: 44 }} title={member.email}>
                <span className="text-[.58rem] font-mono tabular-nums" style={{ color }}>
                  {memberBanned ? "封" : isExternal ? "外" : isPoolNotFound ? "未" : pct != null ? `${Math.round(pct)}%` : "--"}
                </span>
                {!memberBanned && !isExternal && !isPoolNotFound && pct != null && <MiniBar percent={pct} />}
              </div>
            );
          }) : <span className="text-[.58rem] c-dim">无成员</span>
        ) : (
          <span className="text-[.58rem] c-dim">未检查</span>
        )}
      </div>

      {/* 列5: Owner 额度 */}
      <div className="flex items-center justify-start">
        <QuotaBadge quota={quota} />
      </div>

      {/* 列6: 成员数 */}
      <div className="flex items-center justify-center">
        {typeof memberCount === "number" && (
          <span
            className="inline-flex items-center gap-1 text-[.7rem] font-mono tabular-nums"
            style={{ color: "var(--text-heading)" }}
          >
            <Users size={11} className="c-dim" />
            {memberCount}
          </span>
        )}
      </div>

      {/* 列7: 操作按钮 */}
      <div className="flex items-center justify-end gap-1">
        <button
          type="button"
          onClick={event => {
            event.stopPropagation();
            onLoadOwnerQuota(owner.account_id);
          }}
          disabled={quotaLoading}
          className="inline-flex items-center justify-center rounded-md p-1.5 transition-colors hover:bg-[rgba(255,255,255,0.06)]"
          style={{ color: "var(--text-dim)" }}
          title="查额度"
        >
          {quotaLoading ? <Loader2 size={13} className="animate-spin" /> : <Zap size={13} />}
        </button>
        <button
          type="button"
          onClick={event => {
            event.stopPropagation();
            onRefreshMembers(owner.account_id);
          }}
          className="inline-flex items-center justify-center rounded-md p-1.5 transition-colors hover:bg-[rgba(255,255,255,0.06)]"
          style={{ color: "var(--text-dim)" }}
          title="刷新成员"
        >
          <RefreshCw size={13} />
        </button>
      </div>
    </div>
  );
}
