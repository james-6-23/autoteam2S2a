import { Listbox, ListboxButton, ListboxOption, ListboxOptions } from "@headlessui/react";
import { Check, ChevronDown } from "lucide-react";

export interface SelectOption {
  value: string;
  label: string;
}

interface HSelectProps {
  value: string;
  onChange: (value: string) => void;
  options: SelectOption[];
  className?: string;
  style?: React.CSSProperties;
  placeholder?: string;
}

export function HSelect({ value, onChange, options, className = "", style, placeholder }: HSelectProps) {
  const selected = options.find(o => o.value === value);

  return (
    <Listbox value={value} onChange={onChange}>
      <div className={`relative ${className}`} style={style}>
        <ListboxButton
          className="field-input flex w-full cursor-pointer items-center justify-between gap-2 text-left"
          style={{ height: 40 }}
        >
          <span className={selected ? "c-heading" : "c-dim"}>
            {selected ? selected.label : (placeholder ?? "请选择")}
          </span>
          <ChevronDown size={14} className="c-dim shrink-0 transition-transform ui-open:rotate-180" />
        </ListboxButton>

        <ListboxOptions
          anchor="bottom start"
          transition
          className="z-[200] mt-1.5 min-w-[var(--button-width)] origin-top overflow-hidden rounded-xl border shadow-xl outline-none transition duration-150 ease-out data-[closed]:scale-95 data-[closed]:opacity-0"
          style={{
            background: "var(--bg-card)",
            border: "1px solid var(--border-hover)",
            backdropFilter: "blur(24px) saturate(150%)",
            boxShadow: "0 12px 40px -8px rgba(0,0,0,0.35), 0 4px 16px -4px rgba(0,0,0,0.2)",
          }}
        >
          <div className="p-1.5">
            {options.map(option => (
              <ListboxOption
                key={option.value}
                value={option.value}
                className="group flex cursor-pointer items-center justify-between gap-3 rounded-lg px-3 py-2 text-sm transition-colors duration-100 data-[focus]:bg-white/6 data-[selected]:font-semibold"
                style={{ color: "var(--text-heading)" }}
              >
                <span>{option.label}</span>
                <Check
                  size={13}
                  className="shrink-0 opacity-0 group-data-[selected]:opacity-100"
                  style={{ color: "#2dd4bf" }}
                />
              </ListboxOption>
            ))}
          </div>
        </ListboxOptions>
      </div>
    </Listbox>
  );
}
