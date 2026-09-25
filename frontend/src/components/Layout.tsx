import { Link, Outlet } from 'react-router-dom'
import { StatusBar } from './StatusBar'

export function Layout() {
  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-700 bg-slate-950">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
          <Link to="/" className="text-lg font-semibold text-white hover:text-sky-300">
            SentinelOne Metrics
          </Link>
          <nav className="flex gap-4 text-sm">
            <Link to="/" className="text-slate-300 hover:text-white">
              Dashboard
            </Link>
            <Link to="/explore" className="text-slate-300 hover:text-white">
              Range explorer
            </Link>
          </nav>
        </div>
        <StatusBar />
      </header>
      <main className="mx-auto max-w-6xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  )
}
