import type { CodexQuota, OwnerHealth, TeamOwner } from "../../lib/team-manage-types";
import { OwnerRow } from "./OwnerRow";

interface OwnerListProps {
  owners: TeamOwner[];
  loading?: boolean;
  memberCounts: Record<string, number>;
  ownerQuotas: Record<string, CodexQuota>;
  ownerQuotaLoading: Record<string, boolean>;
  healthMap: Record<string, OwnerHealth>;
  selectedOwnerIds: Set<string>;
  onOpenMembers: (accountId: string) => void;
  onToggleSelected: (accountId: string) => void;
  onLoadOwnerQuota: (accountId: string) => void;
  onRefreshMembers: (accountId: string) => void;
}

export function OwnerList({
  owners,
  loading,
  memberCounts,
  ownerQuotas,
  ownerQuotaLoading,
  healthMap,
  selectedOwnerIds,
  onOpenMembers,
  onToggleSelected,
  onLoadOwnerQuota,
  onRefreshMembers,
}: OwnerListProps) {
  if (loading && owners.length === 0) {
    return <div className="py-8 text-center c-dim">加载中...</div>;
  }

  if (owners.length === 0) {
    return (
      <div className="py-8 text-center c-dim">
        <p className="text-sm">暂无 Owner 数据</p>
        <p className="mt-1 text-xs">请先在邀请模块中上传 Owner JSON</p>
      </div>
    );
  }

  return (
    <div className="grid gap-2">
      {owners.map(owner => (
        <OwnerRow
          key={owner.account_id}
          owner={owner}
          memberCount={memberCounts[owner.account_id] ?? owner.member_count}
          quota={ownerQuotas[owner.account_id]}
          quotaLoading={ownerQuotaLoading[owner.account_id]}
          health={healthMap[owner.account_id]}
          selected={selectedOwnerIds.has(owner.account_id)}
          onOpenMembers={onOpenMembers}
          onToggleSelected={onToggleSelected}
          onLoadOwnerQuota={onLoadOwnerQuota}
          onRefreshMembers={onRefreshMembers}
        />
      ))}
    </div>
  );
}
