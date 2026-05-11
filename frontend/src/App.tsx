import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import './App.css'
import AppLayout from './pages/AppLayout'

const AsinHomePage = lazy(() => import('./pages/home'))
const GroupAPage = lazy(() => import('./pages/group/A'))
const GroupBPageRoute = lazy(() => import('./pages/group/B'))
const GroupFPage = lazy(() => import('./pages/group/F'))
const MonitorPage = lazy(() => import('./pages/monitor'))
const AdSalesPage = lazy(() => import('./pages/ads/ad-sales'))
const AdsProfitPage = lazy(() => import('./pages/ads/profit'))
const TrendNewListingEmbeddedPage = lazy(() => import('./pages/trend/new-listing'))
const TrendPage = lazy(() => import('./pages/trend/weekly'))
const TrendSessionImpressionEmbeddedPage = lazy(() => import('./pages/trend/session-impression'))
const TasksPageRoute = lazy(() => import('./pages/tasks'))

function RouteFallback() {
  return <p className="loading-hint" style={{ padding: '1.5rem' }}>Loading…</p>
}

export default function App() {
  return (
    <Suspense fallback={<RouteFallback />}>
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<AsinHomePage />} />
        <Route path="/group/A" element={<GroupAPage />} />
          <Route path="/group/B" element={<GroupBPageRoute />} />
        <Route path="/group/F" element={<GroupFPage />} />
        <Route path="/grpup/A" element={<Navigate to="/group/A" replace />} />
          <Route path="/tasks" element={<TasksPageRoute />} />
        <Route path="/monitor" element={<MonitorPage />} />
        <Route path="/ads/ad-sales" element={<AdSalesPage />} />
        <Route path="/ads/profit" element={<AdsProfitPage />} />
        <Route path="/trend" element={<TrendPage />} />
        <Route path="/trend/session-impression" element={<TrendSessionImpressionEmbeddedPage />} />
        <Route path="/trend/session&impression" element={<Navigate to="/trend/session-impression" replace />} />
        <Route path="/trend/New Listing" element={<TrendNewListingEmbeddedPage />} />
      </Route>
    </Routes>
    </Suspense>
  )
}
