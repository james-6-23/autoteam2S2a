import { NavLink, Outlet } from 'react-router-dom';
import { useTheme } from '../hooks/useTheme';
import { useEffect, useState } from 'react';
import { fetchHealth } from '../lib/api';
import { ClipboardList, LayoutDashboard, Users, Settings, Rocket, Clock, History, ScrollText, MailPlus, UserCog } from 'lucide-react';

const NAV_ITEMS = [
  { to: '/', label: '概览', icon: <LayoutDashboard size={15} /> },
  { to: '/teams', label: '号池', icon: <Users size={15} /> },
  { to: '/config', label: '配置', icon: <Settings size={15} /> },
  { to: '/team-manage', label: 'Team', icon: <UserCog size={15} /> },
  { to: '/owner-audit', label: '审计', icon: <ClipboardList size={15} /> },
  { to: '/tasks', label: '任务', icon: <Rocket size={15} /> },
  { to: '/schedules', label: '定时', icon: <Clock size={15} /> },
  { to: '/runs', label: '记录', icon: <History size={15} /> },
  { to: '/logs', label: '日志', icon: <ScrollText size={15} /> },
  { to: '/invite', label: '邀请', icon: <MailPlus size={15} /> },
];

export default function Layout() {
  const { theme, toggleTheme } = useTheme();
  const [version, setVersion] = useState('--');
  const [uptime, setUptime] = useState('--');
  const [healthy, setHealthy] = useState(false);
  const [clock, setClock] = useState('--:--:--');

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

  return (
    <div className="min-h-screen flex flex-col pt-32 xl:pt-20">
      {/* Header */}
      <header className="fixed top-0 left-0 right-0 z-50">
        <div className="mx-auto grid w-full max-w-[1400px] grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-3 px-4 py-2.5 sm:px-5 xl:grid-cols-[auto_minmax(0,1fr)_auto] xl:gap-x-5">
        <div className="flex min-w-0 items-center gap-3">
          {/* Logo */}
          <div className="relative group cursor-pointer">
            <div className="absolute -inset-1 bg-gradient-to-tr from-teal-400/40 to-primary/40 rounded-xl blur-md opacity-0 group-hover:opacity-100 transition-all duration-500" />
            <div className="relative w-8 h-8 rounded-xl bg-gradient-to-br from-teal-500 to-primary flex items-center justify-center shadow-lg">
              <span className="text-white font-bold text-sm font-display tracking-tighter">GC</span>
            </div>
          </div>
          {/* Brand */}
          <div className="flex items-center gap-2">
            <span className="text-[.95rem] font-bold tracking-tight c-heading font-display">
              GMN-<span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-400 via-teal-300 to-primary">codex</span>自动上号系统
            </span>
            <span
              className="text-[.6rem] font-mono px-2 py-0.5 rounded-md font-semibold"
              style={{ background: 'var(--ghost)', color: 'var(--text-dim)', border: '1px solid var(--border)' }}
            >
              {version}
            </span>
          </div>
        </div>

        {/* 右侧状态 */}
        <div className="flex items-center gap-2.5 justify-self-end xl:col-start-3">
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

        {/* 胶囊导航 */}
        <div className="col-span-2 min-w-0 xl:col-span-1 xl:col-start-2 xl:row-start-1">
          <nav
            className="header-nav mx-auto flex max-w-full items-center gap-1 overflow-x-auto rounded-2xl p-1"
            style={{
              background: 'var(--bg-inner)',
              border: '1px solid var(--border)',
              width: 'fit-content',
              maxWidth: '100%',
              backdropFilter: 'blur(16px) saturate(140%)',
              WebkitBackdropFilter: 'blur(16px) saturate(140%)',
              boxShadow: '0 4px 18px -8px rgba(0,0,0,0.18)',
            }}
          >
            {NAV_ITEMS.map(item => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) =>
                  `relative flex items-center gap-1.5 px-3.5 py-[7px] rounded-xl text-[.82rem] font-semibold whitespace-nowrap transition-all duration-300 ${
                    isActive
                      ? 'text-white'
                      : 'c-dim hover:text-[var(--text-heading)] hover:bg-[var(--ghost)]'
                  }`
                }
                style={({ isActive }) =>
                  isActive
                    ? {
                        background: 'linear-gradient(135deg, rgba(20,184,166,0.85), rgba(139,92,246,0.85))',
                        boxShadow: '0 3px 12px -2px rgba(20,184,166,0.35), inset 0 1px 1px rgba(255,255,255,0.15)',
                      }
                    : {}
                }
              >
                <span className="flex items-center">{item.icon}</span>
                <span>{item.label}</span>
              </NavLink>
            ))}
          </nav>
        </div>
        </div>
      </header>

      <div className="flex-1 w-full max-w-6xl mx-auto px-4 sm:px-6 flex flex-col pb-10 pt-4 sm:pt-5">
        {/* 内容路由 */}
        <main className="flex-1 w-full animate-enter">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
