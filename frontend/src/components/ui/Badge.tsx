type BadgeVariant = 'ok' | 'warn' | 'run' | 'err' | 'off';

interface BadgeProps {
  variant?: BadgeVariant;
  className?: string;
  children: React.ReactNode;
}

function Badge({ variant = 'off', className = '', children }: BadgeProps) {
  return (
    <span className={`badge badge-${variant} ${className}`}>
      {children}
    </span>
  );
}

export { Badge };
export type { BadgeProps, BadgeVariant };
