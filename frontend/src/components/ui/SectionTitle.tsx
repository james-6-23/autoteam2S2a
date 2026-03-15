import type { ReactNode } from 'react';

interface SectionTitleProps {
  children: ReactNode;
  className?: string;
}

function SectionTitle({ children, className = '' }: SectionTitleProps) {
  return <div className={`section-title ${className}`}>{children}</div>;
}

export { SectionTitle };
export type { SectionTitleProps };
