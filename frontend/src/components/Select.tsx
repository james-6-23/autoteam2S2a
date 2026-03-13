import { useState, useRef, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown } from 'lucide-react';

interface SelectOption {
  label: string;
  value: string;
  meta?: string;
}

interface SelectProps {
  value: string;
  onChange: (value: string) => void;
  options: SelectOption[];
  className?: string;
}

export function Select({ value, onChange, options, className = '' }: SelectProps) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState({ top: 0, left: 0, width: 0 });

  const updatePos = useCallback(() => {
    if (triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 4, left: rect.left, width: rect.width });
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    updatePos();
    const handleClickOutside = (e: MouseEvent) => {
      if (
        triggerRef.current && !triggerRef.current.contains(e.target as Node) &&
        dropdownRef.current && !dropdownRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    };
    const handleScroll = (e: Event) => {
      if (dropdownRef.current && dropdownRef.current.contains(e.target as Node)) return;
      setOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    window.addEventListener('scroll', handleScroll, true);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      window.removeEventListener('scroll', handleScroll, true);
    };
  }, [open, updatePos]);

  const selectedOption = options.find((opt) => opt.value === String(value)) || options[0];

  return (
    <div className={`relative ${className}`}>
      <div
        ref={triggerRef}
        className="field-input flex items-center justify-between cursor-pointer select-none"
        onClick={() => setOpen(!open)}
      >
        <span className="truncate">{selectedOption?.label || '\u00A0'}</span>
        <ChevronDown size={14} className={`flex-shrink-0 text-gray-500 transition-transform duration-200 ${open ? 'rotate-180' : ''}`} />
      </div>

      {open && createPortal(
        <div
          ref={dropdownRef}
          className="fixed z-[9999] rounded-xl shadow-2xl overflow-hidden"
          style={{
            top: pos.top,
            left: pos.left,
            width: pos.width,
            background: 'var(--modal-bg)',
            border: '1px solid var(--border-hover)',
            backdropFilter: 'blur(20px) saturate(150%)',
            animation: 'slide-up 0.15s ease-out',
          }}
        >
          <div className="max-h-60 overflow-y-auto py-1">
            {options.map((opt) => (
              <div
                key={opt.value}
                className="px-3 py-2 cursor-pointer text-sm transition-colors"
                style={{
                  color: String(value) === opt.value ? '#2dd4bf' : 'var(--text)',
                  background: String(value) === opt.value ? 'rgba(45,212,191,0.1)' : 'transparent',
                  fontWeight: String(value) === opt.value ? 500 : 400,
                }}
                onMouseEnter={e => {
                  if (String(value) !== opt.value) {
                    (e.currentTarget as HTMLElement).style.background = 'var(--ghost-hover)';
                  }
                }}
                onMouseLeave={e => {
                  (e.currentTarget as HTMLElement).style.background =
                    String(value) === opt.value ? 'rgba(45,212,191,0.1)' : 'transparent';
                }}
                onClick={() => {
                  onChange(opt.value);
                  setOpen(false);
                }}
              >
                <div className="truncate">{opt.label}</div>
                {opt.meta && (
                  <div className="text-xs truncate" style={{ color: 'var(--text-dim)', opacity: 0.65, marginTop: 1 }}>
                    {opt.meta}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
