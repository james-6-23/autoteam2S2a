import { Switch } from "@headlessui/react";

interface HSwitchProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
  className?: string;
}

export function HSwitch({ checked, onChange, label, className = "" }: HSwitchProps) {
  return (
    <label
      className={`inline-flex cursor-pointer select-none items-center gap-2.5 text-xs font-medium ${className}`}
      style={{ color: "var(--text-dim)" }}
    >
      <Switch
        checked={checked}
        onChange={onChange}
        className="relative inline-flex shrink-0 cursor-pointer rounded-full border transition-all duration-300 focus:outline-none"
        style={{
          width: 42,
          height: 24,
          background: checked
            ? "linear-gradient(135deg, #2dd4bf, #14b8a6)"
            : "rgba(255,255,255,0.08)",
          borderColor: checked ? "rgba(45,212,191,0.3)" : "var(--border)",
          boxShadow: checked
            ? "0 0 12px rgba(45,212,191,0.2)"
            : "inset 0 2px 4px rgba(0,0,0,0.15)",
        }}
      >
        <span
          aria-hidden
          className="pointer-events-none inline-block rounded-full shadow transition-transform duration-300"
          style={{
            width: 18,
            height: 18,
            marginTop: 2,
            background: checked ? "#fff" : "#e5e7eb",
            transform: checked ? "translateX(20px)" : "translateX(2px)",
          }}
        />
      </Switch>
      {label && <span>{label}</span>}
    </label>
  );
}
