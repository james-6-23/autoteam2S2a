import { ChevronLeft, ChevronRight } from "lucide-react";

interface TeamManagePaginationProps {
  page: number;
  totalPages: number;
  totalItems?: number;
  onChange: (page: number) => void;
}

type PaginationToken = number | "…" | null;

function buildTokens(page: number, totalPages: number): PaginationToken[] {
  const maxSlots = 6;
  if (totalPages <= 0) return Array.from({ length: maxSlots }, () => null);

  const visibleCount = Math.min(5, totalPages);
  let start = Math.max(1, page - 2);
  let end = start + visibleCount - 1;
  if (end > totalPages) {
    end = totalPages;
    start = Math.max(1, end - visibleCount + 1);
  }

  const tokens: PaginationToken[] = [];
  for (let current = start; current <= end; current += 1) {
    tokens.push(current);
  }
  if (end < totalPages) tokens.push("…");
  while (tokens.length < maxSlots) tokens.push(null);
  return tokens;
}

export function TeamManagePagination({
  page,
  totalPages,
  totalItems,
  onChange,
}: TeamManagePaginationProps) {
  if (totalPages <= 1) return null;

  const tokens = buildTokens(page, totalPages);

  return (
    <div className="team-manage-pagination">
      <div className="team-manage-pagination__meta">
        第 <span className="font-mono c-heading">{page}</span> / <span className="font-mono">{totalPages}</span> 页
        {typeof totalItems === "number" ? <span className="c-dim"> · 共 {totalItems} 条</span> : ""}
      </div>
      <div className="team-manage-pagination__rail">
        <button
          type="button"
          onClick={() => onChange(page - 1)}
          disabled={page <= 1}
          className="btn btn-ghost team-manage-pagination__edge flex items-center gap-1.5 disabled:opacity-30"
        >
          <ChevronLeft size={14} />
          上一页
        </button>

        <div className="team-manage-pagination__pages">
          {tokens.map((token, index) => {
            if (token === null) {
              return <span key={`placeholder-${index}`} className="team-manage-pagination__placeholder" aria-hidden="true" />;
            }
            if (token === "…") {
              return (
                <span key={`ellipsis-${index}`} className="team-manage-pagination__ellipsis">
                  …
                </span>
              );
            }
            const isActive = page === token;
            return (
              <button
                key={token}
                type="button"
                onClick={() => onChange(token)}
                className="team-manage-pagination__page"
                style={isActive ? {
                  background: "linear-gradient(135deg, rgba(20,184,166,0.88), rgba(59,130,246,0.84))",
                  color: "#fff",
                  border: "1px solid rgba(59,130,246,0.35)",
                  boxShadow: "0 6px 20px -8px rgba(59,130,246,0.55)",
                } : {
                  background: "var(--ghost)",
                  color: "var(--text-heading)",
                  border: "1px solid var(--border)",
                }}
              >
                {token}
              </button>
            );
          })}
        </div>

        <button
          type="button"
          onClick={() => onChange(page + 1)}
          disabled={page >= totalPages}
          className="btn btn-ghost team-manage-pagination__edge flex items-center gap-1.5 disabled:opacity-30"
        >
          下一页
          <ChevronRight size={14} />
        </button>
      </div>
    </div>
  );
}
