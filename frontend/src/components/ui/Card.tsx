import { type HTMLAttributes, forwardRef } from 'react';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  inner?: boolean;
}

const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ inner, className = '', children, ...props }, ref) => {
    return (
      <div
        ref={ref}
        className={`${inner ? 'card-inner' : 'card'} ${className}`}
        {...props}
      >
        {children}
      </div>
    );
  },
);
Card.displayName = 'Card';

export { Card };
export type { CardProps };
