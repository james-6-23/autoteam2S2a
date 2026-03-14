import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import InviteLayout from './components/InviteLayout';
import { ToastProvider } from './components/Toast';
import Dashboard from './pages/Dashboard';
import Teams from './pages/Teams';
import Config from './pages/Config';
import Tasks from './pages/Tasks';
import Schedules from './pages/Schedules';
import Runs from './pages/Runs';
import Logs from './pages/Logs';
import Invite from './pages/Invite';
import TeamManage from './pages/TeamManage';
import OwnerAudit from './pages/OwnerAudit';
import Proxy from './pages/Proxy';

export default function App() {
  return (
    <ToastProvider>
      <BrowserRouter>
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
      </BrowserRouter>
    </ToastProvider>
  );
}
