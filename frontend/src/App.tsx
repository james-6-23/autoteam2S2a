import { lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import InviteLayout from './components/InviteLayout';
import { ToastProvider } from './components/Toast';

const Dashboard = lazy(() => import('./pages/Dashboard'));
const Teams = lazy(() => import('./pages/Teams'));
const Config = lazy(() => import('./pages/Config'));
const Tasks = lazy(() => import('./pages/Tasks'));
const Schedules = lazy(() => import('./pages/Schedules'));
const Runs = lazy(() => import('./pages/Runs'));
const Logs = lazy(() => import('./pages/Logs'));
const Invite = lazy(() => import('./pages/Invite'));
const TeamManage = lazy(() => import('./pages/TeamManage'));
const OwnerAudit = lazy(() => import('./pages/OwnerAudit'));
const Proxy = lazy(() => import('./pages/Proxy'));

function RouteLoadingFallback() {
  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div
        className="w-full max-w-md rounded-[28px] p-8 sm:p-10 animate-enter"
        style={{
          background: 'var(--bg-card)',
          border: '1px solid var(--border)',
          boxShadow: '0 24px 80px -36px rgba(20, 184, 166, 0.35)',
          backdropFilter: 'blur(24px) saturate(150%)',
          WebkitBackdropFilter: 'blur(24px) saturate(150%)',
        }}
      >
        <div className="flex flex-col items-center text-center gap-5">
          <div className="relative flex items-center justify-center">
            <div
              className="absolute inset-0 rounded-full"
              style={{
                background: 'radial-gradient(circle, rgba(45, 212, 191, 0.22) 0%, rgba(45, 212, 191, 0) 72%)',
                transform: 'scale(1.9)',
              }}
            />
            <div
              className="spin relative h-14 w-14 rounded-full border-2 border-solid"
              style={{
                borderColor: 'rgba(45, 212, 191, 0.2)',
                borderTopColor: '#2dd4bf',
                borderRightColor: 'rgba(139, 92, 246, 0.8)',
                boxShadow: '0 0 24px rgba(45, 212, 191, 0.18)',
              }}
            />
            <div
              className="absolute h-2.5 w-2.5 rounded-full"
              style={{
                background: '#2dd4bf',
                boxShadow: '0 0 14px rgba(45, 212, 191, 0.6)',
              }}
            />
          </div>

          <div className="space-y-2">
            <div className="text-[0.72rem] font-semibold uppercase tracking-[0.32em] text-teal-300/90">
              Loading workspace
            </div>
            <h2 className="font-display text-2xl font-semibold c-heading">
              正在加载页面资源
            </h2>
            <p className="mx-auto max-w-sm text-sm leading-6 c-muted">
              已启用路由懒加载，正在按需准备当前页面内容，请稍候片刻。
            </p>
          </div>

          <div className="grid w-full grid-cols-3 gap-3 pt-2">
            {[0, 1, 2].map((item) => (
              <div
                key={item}
                className="h-2.5 rounded-full skeleton"
                style={{ animationDelay: `${item * 0.12}s` }}
              />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <ToastProvider>
      <BrowserRouter>
        <Suspense fallback={<RouteLoadingFallback />}>
          <Routes>
            {/* 独立邀请页面（无管理导航） */}
            <Route element={<InviteLayout />}>
              <Route path="inv" element={<Invite />} />
            </Route>

            {/* 管理后台 */}
            <Route element={<Layout />}>
              <Route index element={<Dashboard />} />
              <Route path="teams" element={<Teams />} />
              <Route path="config" element={<Config />} />
              <Route path="tasks" element={<Tasks />} />
              <Route path="schedules" element={<Schedules />} />
              <Route path="runs" element={<Runs />} />
              <Route path="logs" element={<Logs />} />
              <Route path="invite" element={<Invite />} />
              <Route path="team-manage" element={<TeamManage />} />
              <Route path="owner-audit" element={<OwnerAudit />} />
              <Route path="proxy" element={<Proxy />} />
            </Route>
          </Routes>
        </Suspense>
      </BrowserRouter>
    </ToastProvider>
  );
}
