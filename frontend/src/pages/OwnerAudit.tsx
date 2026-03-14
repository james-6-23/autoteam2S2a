import { useCallback, useEffect, useMemo, useState } from "react";

import { useToast } from "../components/Toast";
import { TeamManagePagination } from "../components/team-manage/TeamManagePagination";
import { HSelect } from "../components/ui/HSelect";
import { fetchTeamManageOwnerAudits } from "../lib/api";
import type { TeamManageOwnerAuditRecord } from "../lib/team-manage-types";

const OWNER_AUDIT_PAGE_SIZE = 8;

function actionToLabel(action: string): string {
  switch (action) {
    case "batch_quarantined": return "批量下线";
    case "batch_active": return "批量恢复";
    case "batch_archived": return "批量归档";
    case "manual_quarantined": return "手动下线";
    case "manual_active": return "手动恢复";
    case "manual_archived": return "手动归档";
    default: return action;
  }
}

function stateToLabel(state: string): string {
  switch (state) {
    case "active": return "活跃";
    case "banned": return "封禁";
    case "expired": return "过期";
    case "quarantined": return "隔离";
    case "archived": return "归档";
    case "unknown": return "未知";
    default: return state;
  }
}

export default function OwnerAudit() {
  const { toast } = useToast();
  const [ownerAudits, setOwnerAudits] = useState<TeamManageOwnerAuditRecord[]>([]);
  const [ownerAuditsLoading, setOwnerAuditsLoading] = useState(false);
  const [auditAccountInput, setAuditAccountInput] = useState("");
  const [auditAccountQuery, setAuditAccountQuery] = useState("");
  const [auditActionFilter, setAuditActionFilter] = useState("");
  const [auditBatchJobInput, setAuditBatchJobInput] = useState("");
  const [auditBatchJobQuery, setAuditBatchJobQuery] = useState("");
  const [auditPage, setAuditPage] = useState(1);
  const [auditTotal, setAuditTotal] = useState(0);
  const [auditTotalPages, setAuditTotalPages] = useState(1);

  const ownerAuditQuery = useMemo(
    () => ({
      account_id: auditAccountQuery.trim() || undefined,
      action: auditActionFilter || undefined,
      batch_job_id: auditBatchJobQuery.trim() || undefined,
      page: auditPage,
      page_size: OWNER_AUDIT_PAGE_SIZE,
    }),
    [auditAccountQuery, auditActionFilter, auditBatchJobQuery, auditPage],
  );

  const loadOwnerAudits = useCallback(async () => {
    setOwnerAuditsLoading(true);
    try {
      const data = await fetchTeamManageOwnerAudits(ownerAuditQuery);
      setOwnerAudits(data.records || []);
      setAuditPage(data.page || 1);
      setAuditTotal(data.total || 0);
      setAuditTotalPages(data.total_pages || 1);
    } catch (error) {
      setOwnerAudits([]);
      setAuditTotal(0);
      setAuditTotalPages(1);
      toast(`获取 Owner 审计失败: ${error}`, "error");
    } finally {
      setOwnerAuditsLoading(false);
    }
  }, [ownerAuditQuery, toast]);

  const submitOwnerAuditFilter = useCallback(() => {
    const normalizedAccount = auditAccountInput.trim();
    const normalizedBatchJobId = auditBatchJobInput.trim();
    let hasStateChange = false;
    if (auditPage !== 1) {
      setAuditPage(1);
      hasStateChange = true;
    }
    if (
      auditAccountQuery !== normalizedAccount
      || auditBatchJobQuery !== normalizedBatchJobId
    ) {
      setAuditAccountQuery(normalizedAccount);
      setAuditBatchJobQuery(normalizedBatchJobId);
      hasStateChange = true;
    }
    if (hasStateChange) return;
    void loadOwnerAudits();
  }, [auditAccountInput, auditAccountQuery, auditBatchJobInput, auditBatchJobQuery, auditPage, loadOwnerAudits]);

  const resetOwnerAuditFilter = useCallback(() => {
    let hasStateChange = false;
    setAuditAccountInput("");
    setAuditBatchJobInput("");
    if (auditActionFilter) {
      setAuditActionFilter("");
      hasStateChange = true;
    }
    if (auditPage !== 1) {
      setAuditPage(1);
      hasStateChange = true;
    }
    if (auditAccountQuery || auditBatchJobQuery) {
      setAuditAccountQuery("");
      setAuditBatchJobQuery("");
      hasStateChange = true;
    }
    if (hasStateChange) return;
    void loadOwnerAudits();
  }, [auditAccountQuery, auditActionFilter, auditBatchJobQuery, auditPage, loadOwnerAudits]);

  useEffect(() => {
    void loadOwnerAudits();
  }, [loadOwnerAudits]);

  return (
    <div className="card p-5">
      <div className="mb-4">
        <div className="section-title mb-0">Owner 审计日志</div>
      </div>

      <div className="team-manage-jobs">
        <div className="team-manage-jobs__header">
          <span className="text-sm font-medium c-heading">Owner 审计</span>
          <span className="text-xs c-dim">共 {auditTotal} 条</span>
        </div>
        <div className="team-manage-audit__toolbar">
          <input
            value={auditAccountInput}
            onChange={event => setAuditAccountInput(event.target.value)}
            onKeyDown={event => {
              if (event.key === "Enter") {
                submitOwnerAuditFilter();
              }
            }}
            placeholder="搜索邮箱 / account_id"
            className="input team-manage-audit__search"
          />
          <HSelect
            value={auditActionFilter}
            onChange={value => {
              setAuditActionFilter(value);
              setAuditPage(1);
            }}
            className="team-manage-audit__select"
            options={[
              { value: "", label: "全部动作" },
              { value: "batch_quarantined", label: "批量下线" },
              { value: "batch_active", label: "批量恢复" },
              { value: "batch_archived", label: "批量归档" },
            ]}
          />
          <input
            value={auditBatchJobInput}
            onChange={event => setAuditBatchJobInput(event.target.value)}
            onKeyDown={event => {
              if (event.key === "Enter") {
                submitOwnerAuditFilter();
              }
            }}
            placeholder="按 batch_job_id 查询"
            className="input team-manage-audit__search"
          />
          <button
            type="button"
            onClick={submitOwnerAuditFilter}
            className="btn btn-ghost py-1.5 text-xs"
          >
            查询
          </button>
          <button
            type="button"
            onClick={resetOwnerAuditFilter}
            className="btn btn-ghost py-1.5 text-xs"
          >
            重置
          </button>
        </div>
        {ownerAuditsLoading ? (
          <div className="py-6 text-sm c-dim">审计加载中...</div>
        ) : ownerAudits.length === 0 ? (
          <div className="py-6 text-sm c-dim">当前查询条件下暂无审计记录</div>
        ) : (
          <div className="space-y-2">
            {ownerAudits.map(record => (
              <div key={record.id} className="team-manage-jobs__detail-item">
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    {record.owner_email && (
                      <span className="text-xs font-medium c-heading">{record.owner_email}</span>
                    )}
                    <span className="font-mono text-[.68rem] c-dim">{record.account_id}</span>
                    <span className="badge badge-off">{actionToLabel(record.action)}</span>
                  </div>
                  <div className="mt-1 text-[.72rem] c-dim">
                    {stateToLabel(record.from_state || "unknown")} → {stateToLabel(record.to_state)}
                    {record.reason ? ` · ${record.reason}` : ""}
                  </div>
                  <div className="mt-1 text-[.72rem] c-dim">
                    {record.scope ? `scope: ${record.scope}` : "scope: --"}
                    {record.batch_job_id ? (
                      <>
                        {" · "}
                        <a
                          href={`/team-manage?batch_job_id=${encodeURIComponent(record.batch_job_id)}`}
                          className="team-manage-link-button"
                        >
                          batch: {record.batch_job_id}
                        </a>
                      </>
                    ) : null}
                  </div>
                </div>
                <div className="text-[.72rem] c-dim">{record.created_at}</div>
              </div>
            ))}
          </div>
        )}
        <TeamManagePagination
          page={auditPage}
          totalPages={auditTotalPages}
          totalItems={auditTotal}
          onChange={page => { setAuditPage(page); }}
        />
      </div>
    </div>
  );
}
