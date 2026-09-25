import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Dashboard } from './pages/Dashboard'
import { QueryDetail } from './pages/QueryDetail'
import { HourlyMaxRecent } from './pages/HourlyMaxRecent'
import { RawRecent } from './pages/RawRecent'
import { RangeExplorer } from './pages/RangeExplorer'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="query/:encodedQuery" element={<QueryDetail />} />
          <Route path="explore" element={<RangeExplorer />} />
          <Route path="hourly-max" element={<HourlyMaxRecent />} />
          <Route path="raw" element={<RawRecent />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
