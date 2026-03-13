import { Outlet } from 'react-router-dom';
import { useTheme } from '../hooks/useTheme';

export default function InviteLayout() {
  const { theme, toggleTheme } = useTheme();

  return (
    <div className="min-h-screen flex flex-col pt-14">
      {/* 精简头部 */}
      <header className="fixed top-0 left-0 right-0 z-50 flex items-center justify-between px-5 py-2.5">
        <div className="flex items-center gap-3">
          {/* Logo */}
          <div className="relative group cursor-pointer">
            <div className="absolute -inset-1 bg-gradient-to-tr from-teal-400/40 to-primary/40 rounded-xl blur-md opacity-0 group-hover:opacity-100 transition-all duration-500" />
            <div className="relative w-8 h-8 rounded-xl bg-gradient-to-br from-teal-500 to-primary flex items-center justify-center shadow-lg">
              <span className="text-white font-bold text-sm font-display tracking-tighter">AT</span>
            </div>
          </div>
          {/* 品牌 */}
          <div className="flex items-center gap-2">
            <span className="text-[.95rem] font-bold tracking-tight c-heading font-display">
              Auto<span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-400 via-teal-300 to-primary">Team</span>
            </span>
            <span
              className="text-[.6rem] font-mono px-2 py-0.5 rounded-md font-semibold"
              style={{ background: 'var(--ghost)', color: 'var(--text-dim)', border: '1px solid var(--border)' }}
            >
              邀请
            </span>
          </div>
        </div>

        {/* 主题切换 */}
        <button onClick={toggleTheme} className="theme-toggle" title="切换主题">
          {theme === 'dark' ? (
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M21.752 15.002A9.72 9.72 0 0 1 18 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 0 0 3 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 0 0 9.002-5.998Z" /></svg>
          ) : (
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 3v2.25m6.364.386-1.591 1.591M21 12h-2.25m-.386 6.364-1.591-1.591M12 18.75V21m-4.773-4.227-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0Z" /></svg>
          )}
        </button>
      </header>

      {/* 内容区域 */}
      <div className="flex-1 w-full max-w-4xl mx-auto px-4 sm:px-6 flex flex-col pb-10 mt-6">
        <main className="flex-1 w-full animate-enter">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
