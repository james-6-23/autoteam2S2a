import { useCallback, useEffect, useMemo, useState } from "react";

import { useToast } from "../components/Toast";
import { TeamManagePagination } from "../components/team-manage/TeamManagePagination";
import { fetchTeamManageOwnerAudits } from "../lib/api";
import type { TeamManageOwnerAuditRecord } from "../lib/team-manage-types";

const OWNER_AUDIT_PAGE_SIZE = 8;

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
            placeholder="按 account_id 查询审计"
            className="input team-manage-audit__search"
          />
          <select
            value={auditActionFilter}
            onChange={event => {
              setAuditActionFilter(event.target.value);
              setAuditPage(1);
            }}
            className="input team-manage-audit__select"
          >
            <option value="">全部动作</option>
            <option value="batch_quarantined">批量下线</option>
            <option value="batch_active">批量恢复</option>
            <option value="batch_archived">批量归档</option>
          </select>
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
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs c-heading">{record.account_id}</span>
                    <span className="badge badge-off">{record.action}</span>
                  </div>
                  <div className="mt-1 text-[.72rem] c-dim">
                    {record.from_state || "unknown"} → {record.to_state}
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
