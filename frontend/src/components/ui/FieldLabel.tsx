import type { LabelHTMLAttributes } from 'react';

interface FieldLabelProps extends LabelHTMLAttributes<HTMLLabelElement> {}

function FieldLabel({ className = '', children, ...props }: FieldLabelProps) {
  return (
    <label className={`field-label ${className}`} {...props}>
      {children}
    </label>
  );
}

export { FieldLabel };
export type { FieldLabelProps };
