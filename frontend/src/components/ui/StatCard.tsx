import type { ReactNode } from 'react';

interface StatCardProps {
  label: string;
  value: ReactNode;
  className?: string;
  valueClassName?: string;
}

function StatCard({ label, value, className = '', valueClassName = 'c-heading' }: StatCardProps) {
  return (
    <div className={`card p-4 ${className}`}>
      <div className="field-label">{label}</div>
      <div className={`stat-num ${valueClassName}`}>{value}</div>
    </div>
  );
}

export { StatCard };
export type { StatCardProps };
