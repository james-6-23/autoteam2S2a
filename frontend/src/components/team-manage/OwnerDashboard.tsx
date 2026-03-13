import { Activity, Archive, Ban, Layers3, ShieldAlert, UserPlus } from "lucide-react";

import type { TeamManageDashboardSummary } from "../../lib/team-manage-types";

interface OwnerDashboardProps {
  summary: TeamManageDashboardSummary;
  loading?: boolean;
}

const DASHBOARD_CARDS = [
  { key: "total_owners", label: "Owner 总数", tone: "neutral", icon: Layers3 },
  { key: "active_owners", label: "活跃 Owner", tone: "good", icon: Activity },
  { key: "banned_owners", label: "封禁风险", tone: "danger", icon: Ban },
  { key: "owners_with_slots", label: "可补位", tone: "accent", icon: UserPlus },
  { key: "checked_recently", label: "10 分钟内已检查", tone: "neutral", icon: ShieldAlert },
  { key: "quarantined_owners", label: "待治理隔离", tone: "warn", icon: Archive },
] as const;

export function OwnerDashboard({ summary, loading }: OwnerDashboardProps) {
  return (
    <div className="team-manage-dashboard">
      {DASHBOARD_CARDS.map(card => {
        const Icon = card.icon;
        const value = summary[card.key];
        return (
          <div key={card.key} className={`team-manage-dashboard__card team-manage-dashboard__card--${card.tone}`}>
            <div className="team-manage-dashboard__icon">
              <Icon size={16} />
            </div>
            <div className="team-manage-dashboard__body">
              <div className="team-manage-dashboard__label">{card.label}</div>
              <div className="team-manage-dashboard__value">{loading ? "--" : value}</div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
