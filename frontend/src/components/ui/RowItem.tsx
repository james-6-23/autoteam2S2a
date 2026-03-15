import { type HTMLAttributes, forwardRef } from 'react';

interface RowItemProps extends HTMLAttributes<HTMLDivElement> {}

const RowItem = forwardRef<HTMLDivElement, RowItemProps>(
  ({ className = '', children, ...props }, ref) => {
    return (
      <div ref={ref} className={`row-item ${className}`} {...props}>
        {children}
      </div>
    );
  },
);
RowItem.displayName = 'RowItem';

export { RowItem };
export type { RowItemProps };
