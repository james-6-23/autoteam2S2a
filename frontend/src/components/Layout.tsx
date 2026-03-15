import { NavLink, Outlet } from 'react-router-dom';
import { useTheme } from '../hooks/useTheme';
import { useEffect, useState, useRef } from 'react';
import { fetchHealth } from '../lib/api';
import { ClipboardList, Globe, LayoutDashboard, Users, Settings, Rocket, Clock, History, ScrollText, MailPlus, UserCog, MoreHorizontal, X } from 'lucide-react';

const NAV_ITEMS = [
  { to: '/', label: '概览', icon: <LayoutDashboard size={15} />, primary: true },
  { to: '/teams', label: '号池', icon: <Users size={15} />, primary: true },
  { to: '/proxy', label: '代理', icon: <Globe size={15} /> },
  { to: '/config', label: '配置', icon: <Settings size={15} />, primary: true },
  { to: '/team-manage', label: 'Team', icon: <UserCog size={15} />, primary: true },
  { to: '/owner-audit', label: '审计', icon: <ClipboardList size={15} /> },
  { to: '/tasks', label: '任务', icon: <Rocket size={15} />, primary: true },
  { to: '/schedules', label: '定时', icon: <Clock size={15} /> },
  { to: '/runs', label: '记录', icon: <History size={15} /> },
  { to: '/logs', label: '日志', icon: <ScrollText size={15} /> },
  { to: '/invite', label: '邀请', icon: <MailPlus size={15} /> },
];

const PRIMARY_ITEMS = NAV_ITEMS.filter(i => i.primary);
const SECONDARY_ITEMS = NAV_ITEMS.filter(i => !i.primary);

export default function Layout() {
  const { theme, toggleTheme } = useTheme();
  const [version, setVersion] = useState('--');
  const [uptime, setUptime] = useState('--');
  const [healthy, setHealthy] = useState(false);
  const [clock, setClock] = useState('--:--:--');
  const [moreOpen, setMoreOpen] = useState(false);
  const navScrollRef = useRef<HTMLDivElement>(null);
  const [canScrollLeft, setCanScrollLeft] = useState(false);
  const [canScrollRight, setCanScrollRight] = useState(false);

  // 检测导航栏滚动状态
  useEffect(() => {
    const el = navScrollRef.current;
    if (!el) return;
    const check = () => {
      setCanScrollLeft(el.scrollLeft > 2);
      setCanScrollRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 2);
    };
    check();
    el.addEventListener('scroll', check, { passive: true });
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => { el.removeEventListener('scroll', check); ro.disconnect(); };
  }, []);

  // 北京时间时钟
  useEffect(() => {
    const tick = () => {
      const now = new Date();
      const bj = new Date(now.getTime() + (8 - (-now.getTimezoneOffset() / 60)) * 3600000);
      setClock(bj.toTimeString().slice(0, 8));
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  // 健康检查轮询
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await fetchHealth();
        if (cancelled) return;
        setVersion(`v${data.version}`);
        setHealthy(true);
        const hrs = Math.floor(data.uptime_secs / 3600);
        const mins = Math.floor((data.uptime_secs % 3600) / 60);
        setUptime(hrs > 0 ? `${hrs}h${mins}m` : `${mins}m`);
      } catch {
        if (cancelled) return;
        setHealthy(false);
        setUptime('离线');
      }
    };
    poll();
    const id = setInterval(poll, 15000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const navLinkClass = ({ isActive }: { isActive: boolean }) =>
    `relative flex items-center gap-1 px-3 py-[6px] rounded-xl text-[.8rem] font-semibold whitespace-nowrap transition-all duration-300 ${
      isActive
        ? 'text-white'
        : 'c-dim hover:text-[var(--text-heading)] hover:bg-[var(--ghost)]'
    }`;

  const navLinkStyle = ({ isActive }: { isActive: boolean }) =>
    isActive
      ? {
          background: 'linear-gradient(135deg, rgba(20,184,166,0.85), rgba(139,92,246,0.85))',
          boxShadow: '0 3px 12px -2px rgba(20,184,166,0.35), inset 0 1px 1px rgba(255,255,255,0.15)',
        }
      : {};

  return (
    <div className="min-h-screen flex flex-col pt-[60px] pb-[68px] md:pb-0">
      {/* Header */}
      <header className="fixed top-0 left-0 right-0 z-50">
        <div className="relative flex items-center justify-between w-full px-4 sm:px-5 py-2 gap-3">

          {/* 左：品牌 */}
          <div className="flex shrink-0 items-center gap-3 z-10">
            <div className="relative group cursor-pointer">
              <div className="absolute -inset-1 bg-gradient-to-tr from-teal-400/40 to-primary/40 rounded-xl blur-md opacity-0 group-hover:opacity-100 transition-all duration-500" />
              <img
                src="/codex-app.png"
                alt="logo"
                className="relative w-8 h-8 rounded-xl object-cover shadow-lg"
              />
            </div>
            <div className="flex items-center gap-2">
              <span className="text-[.95rem] font-bold tracking-tight c-heading font-display whitespace-nowrap">
                Team-<span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-400 via-teal-300 to-primary">codex</span><span className="hidden sm:inline">自动上号系统</span>
              </span>
              <span
                className="text-[.6rem] font-mono px-2 py-0.5 rounded-md font-semibold"
                style={{ background: 'var(--ghost)', color: 'var(--text-dim)', border: '1px solid var(--border)' }}
              >
                {version}
              </span>
            </div>
          </div>

          {/* 中：胶囊导航 — 居中，溢出时可滚动 */}
          <div className="absolute left-1/2 -translate-x-1/2 max-w-[calc(100vw-340px)]">
            <div className="relative">
              {/* 左渐隐遮罩 */}
              {canScrollLeft && (
                <div className="absolute left-0 top-0 bottom-0 w-8 z-10 pointer-events-none rounded-l-2xl"
                  style={{ background: 'linear-gradient(to right, var(--bg-inner), transparent)' }} />
              )}
              <div
                ref={navScrollRef}
                className="header-nav flex items-center gap-0.5 rounded-2xl p-1 overflow-x-auto scrollbar-none"
                style={{
                  background: 'var(--bg-inner)',
                  border: '1px solid var(--border)',
                  backdropFilter: 'blur(16px) saturate(140%)',
                  WebkitBackdropFilter: 'blur(16px) saturate(140%)',
                  boxShadow: '0 4px 18px -8px rgba(0,0,0,0.18)',
                  scrollbarWidth: 'none',
                }}
              >
                {NAV_ITEMS.map(item => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.to === '/'}
                    className={navLinkClass}
                    style={navLinkStyle}
                  >
                    <span className="flex items-center">{item.icon}</span>
                    <span>{item.label}</span>
                  </NavLink>
                ))}
              </div>
              {/* 右渐隐遮罩 */}
              {canScrollRight && (
                <div className="absolute right-0 top-0 bottom-0 w-8 z-10 pointer-events-none rounded-r-2xl"
                  style={{ background: 'linear-gradient(to left, var(--bg-inner), transparent)' }} />
              )}
            </div>
          </div>

          {/* 右：状态 + 主题切换 */}
          <div className="flex shrink-0 items-center gap-2.5 z-10">
            <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-lg" style={{ background: 'var(--ghost)', border: '1px solid var(--border)' }}>
              <span className="text-[.65rem] font-mono c-dim tabular-nums" title="北京时间 (UTC+8)">{clock}</span>
              <span className="w-px h-3" style={{ background: 'var(--border)' }} />
              <span className="flex items-center gap-1.5 text-[.65rem] font-mono c-dim">
                <span
                  className="w-1.5 h-1.5 rounded-full"
                  style={{
                    background: healthy ? '#2dd4bf' : '#f87171',
                    boxShadow: healthy ? '0 0 6px rgba(45,212,191,0.5)' : '0 0 6px rgba(248,113,113,0.5)',
                  }}
                />
                {uptime}
              </span>
            </div>
            <button onClick={toggleTheme} className="theme-toggle" title="切换主题">
              {theme === 'dark' ? (
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M21.752 15.002A9.72 9.72 0 0 1 18 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 0 0 3 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 0 0 9.002-5.998Z" /></svg>
              ) : (
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 3v2.25m6.364.386-1.591 1.591M21 12h-2.25m-.386 6.364-1.591-1.591M12 18.75V21m-4.773-4.227-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0Z" /></svg>
              )}
            </button>
          </div>

        </div>
      </header>

      {/* Mobile Bottom Tab Bar */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 z-50"
        style={{
          background: 'var(--header-bg)',
          backdropFilter: 'saturate(180%) blur(24px)',
          WebkitBackdropFilter: 'saturate(180%) blur(24px)',
          borderTop: '1px solid var(--nav-border)',
          boxShadow: '0 -1px 24px -1px rgba(0,0,0,0.06)',
        }}
      >
        <div className="flex items-center justify-around px-1 py-1.5">
          {PRIMARY_ITEMS.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              className={({ isActive }) =>
                `flex flex-col items-center gap-0.5 px-2 py-1 rounded-xl min-w-[52px] transition-all duration-200 ${
                  isActive ? 'text-teal-400' : 'c-dim'
                }`
              }
            >
              <span className="flex items-center">{item.icon}</span>
              <span className="text-[.6rem] font-semibold">{item.label}</span>
            </NavLink>
          ))}
          {/* 更多按钮 */}
          <button
            onClick={() => setMoreOpen(!moreOpen)}
            className={`flex flex-col items-center gap-0.5 px-2 py-1 rounded-xl min-w-[52px] transition-all duration-200 ${moreOpen ? 'text-teal-400' : 'c-dim'}`}
          >
            {moreOpen ? <X size={15} /> : <MoreHorizontal size={15} />}
            <span className="text-[.6rem] font-semibold">更多</span>
          </button>
        </div>

        {/* 更多展开面板 */}
        {moreOpen && (
          <div
            className="absolute bottom-full left-0 right-0 p-3"
            style={{
              background: 'var(--modal-bg)',
              borderTop: '1px solid var(--border)',
              backdropFilter: 'blur(24px) saturate(150%)',
              WebkitBackdropFilter: 'blur(24px) saturate(150%)',
              boxShadow: '0 -8px 32px -8px rgba(0,0,0,0.3)',
              animation: 'slide-up 0.25s cubic-bezier(0.16, 1, 0.3, 1)',
            }}
          >
            <div className="grid grid-cols-3 gap-2">
              {SECONDARY_ITEMS.map(item => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  onClick={() => setMoreOpen(false)}
                  className={({ isActive }) =>
                    `flex flex-col items-center gap-1.5 py-3 rounded-xl transition-all duration-200 ${
                      isActive
                        ? 'text-teal-400 bg-teal-400/8'
                        : 'c-dim hover:text-[var(--text-heading)] hover:bg-[var(--ghost)]'
                    }`
                  }
                  style={{ border: '1px solid var(--border)' }}
                >
                  <span className="flex items-center">{item.icon}</span>
                  <span className="text-[.72rem] font-semibold">{item.label}</span>
                </NavLink>
              ))}
            </div>
          </div>
        )}
      </nav>

      <div className="flex-1 w-full max-w-6xl mx-auto px-4 sm:px-6 flex flex-col pb-10 pt-4 sm:pt-5">
        {/* 内容路由 */}
        <main className="flex-1 w-full animate-enter">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
