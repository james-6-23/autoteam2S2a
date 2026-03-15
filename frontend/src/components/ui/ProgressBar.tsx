interface ProgressBarProps {
  value: number;
  max?: number;
  className?: string;
  thick?: boolean;
}

function ProgressBar({ value, max = 100, className = '', thick }: ProgressBarProps) {
  const pct = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;

  return (
    <div className={`progress-bar ${thick ? 'h-2' : ''} ${className}`}>
      <div className="progress-fill" style={{ width: `${pct}%` }} />
    </div>
  );
}

export { ProgressBar };
export type { ProgressBarProps };
