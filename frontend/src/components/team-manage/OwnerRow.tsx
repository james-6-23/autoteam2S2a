import { Loader2, ShieldAlert, Users, Zap } from "lucide-react";

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
    <div className="h-1 w-12 overflow-hidden rounded-full" style={{ background: "var(--ghost)" }}>
      <div className="h-full rounded-full transition-all" style={{ width: `${percent}%`, background: color }} />
    </div>
  );
}

function QuotaBadge({ quota }: { quota?: CodexQuota }) {
  if (!quota) return <span className="text-[.6rem] c-dim">--</span>;
  if (quota.status === "banned") {
    return (
      <span
        className="flex items-center gap-1 rounded px-1.5 py-0.5 text-[.6rem] font-medium"
        style={{ background: "rgba(248,113,113,.15)", color: "#f87171" }}
      >
        <ShieldAlert size={10} /> 封禁
      </span>
    );
  }
  if (quota.status === "error") return <span className="text-[.6rem] c-dim" title={quota.error || ""}>错误</span>;

  return (
    <div className="flex items-center gap-1.5">
      {quota.five_hour && (
        <span
          className="rounded px-1.5 py-0.5 text-[.6rem] font-mono"
          style={{
            background: `${quotaColor(quota.five_hour.remaining_percent)}15`,
            color: quotaColor(quota.five_hour.remaining_percent),
            border: `1px solid ${quotaColor(quota.five_hour.remaining_percent)}30`,
          }}
        >
          5h {Math.round(quota.five_hour.remaining_percent)}%
        </span>
      )}
      {quota.seven_day && (
        <span
          className="rounded px-1.5 py-0.5 text-[.6rem] font-mono"
          style={{
            background: `${quotaColor(quota.seven_day.remaining_percent)}15`,
            color: quotaColor(quota.seven_day.remaining_percent),
            border: `1px solid ${quotaColor(quota.seven_day.remaining_percent)}30`,
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
  const hasBannedMember = health?.members.some(member => member.status === "banned");
  const stateLabel = owner.state ?? "active";

  return (
    <div
      className={`row-item team-manage-owner-row cursor-pointer transition-all duration-150 hover:scale-[1.005] ${isOwnerBanned ? "team-manage-owner-row--owner-banned" : hasBannedMember ? "team-manage-owner-row--alert" : ""}`}
      onClick={() => onOpenMembers(owner.account_id)}
    >
      <div className="flex items-center justify-between gap-3">
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

        <div className="flex min-w-0 shrink-0 flex-col gap-0.5 team-manage-owner-row__identity">
          <div className="text-sm font-medium c-heading" style={{ fontFamily: "'FiraCode Nerd Font Mono', 'FiraCode Nerd Font', 'Fira Code', var(--font-mono)", letterSpacing: "-0.02em" }}>
            {owner.email || "未知邮箱"}
          </div>
          <div className="text-[.68rem] c-dim" style={{ fontFamily: "'FiraCode Nerd Font Mono', 'FiraCode Nerd Font', 'Fira Code', var(--font-mono)" }}>
            {owner.account_id}
          </div>
        </div>

        <div className="team-manage-owner-row__status">
          <span className={`badge ${isOwnerBanned ? "badge-err" : hasBannedMember ? "badge-warn" : "badge-off"}`}>
            {isOwnerBanned ? "Owner封禁" : hasBannedMember ? "成员封禁" : stateToLabel(stateLabel)}
          </span>
          {health?.checked_at && (
            <span className="text-[.55rem] c-dim">
              {new Date(health.checked_at).toLocaleString("zh-CN")}
            </span>
          )}
        </div>

        <div className="flex min-w-0 flex-1 gap-3 mx-3">
          {health ? (
            health.members.length > 0 ? health.members.map((member, index) => {
              const pct = member.seven_day_pct;
              const memberBanned = member.status === "banned";
              const color = memberBanned ? "#f87171" : pct != null ? quotaColor(pct) : "var(--text-dim)";
              return (
                <div key={member.email} className="flex items-center gap-1.5" style={{ width: 90 }} title={member.email}>
                  <span className="text-[.6rem] font-mono shrink-0" style={{ color, width: 14 }}>#{index + 1}</span>
                  {memberBanned ? (
                    <span
                      className="inline-flex items-center gap-0.5 rounded px-1 py-0.5 text-[.55rem]"
                      style={{ background: "rgba(248,113,113,.12)", color: "#f87171" }}
                    >
                      <ShieldAlert size={8} /> 封禁
                    </span>
                  ) : (
                    <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                      <span className="text-right text-[.6rem] font-mono" style={{ color }}>
                        {pct != null ? `${Math.round(pct)}%` : "--"}
                      </span>
                      {pct != null && <MiniBar percent={pct} />}
                    </div>
                  )}
                </div>
              );
            }) : <span className="text-[.6rem] c-dim">无成员</span>
          ) : (
            <span className="text-[.6rem] c-dim">未检查</span>
          )}
        </div>

        <div className="flex shrink-0 items-center gap-2">
          <QuotaBadge quota={quota} />
          {typeof memberCount === "number" && (
            <span
              className="flex items-center gap-1 rounded-md px-2 py-0.5 text-xs font-mono"
              style={{ background: "var(--ghost)", border: "1px solid var(--border)" }}
            >
              <Users size={12} className="c-dim" />
              <span className="c-heading">{memberCount}</span>
            </span>
          )}
          <button
            type="button"
            onClick={event => {
              event.stopPropagation();
              onLoadOwnerQuota(owner.account_id);
            }}
            disabled={quotaLoading}
            className="btn btn-ghost px-2 py-1 text-xs"
            title="查额度"
          >
            {quotaLoading ? <Loader2 size={14} className="animate-spin" /> : <Zap size={14} />}
          </button>
          <button
            type="button"
            onClick={event => {
              event.stopPropagation();
              onRefreshMembers(owner.account_id);
            }}
            className="btn btn-ghost px-2 py-1 text-xs"
            title="刷新成员"
          >
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="h-3.5 w-3.5">
              <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.992 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
}
