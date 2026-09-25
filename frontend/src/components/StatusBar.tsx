import { useQuery } from '@tanstack/react-query'
import { fetchHealth } from '../api/client'

function StatusDot({ ok }: { ok: boolean }) {
  return (
    <span
      className={`inline-block h-2.5 w-2.5 rounded-full ${ok ? 'bg-emerald-400' : 'bg-red-500'}`}
      aria-hidden
    />
  )
}

export function StatusBar() {
  const { data, error, isLoading } = useQuery({
    queryKey: ['health'],
    queryFn: fetchHealth,
    refetchInterval: 45_000,
  })

  if (isLoading && !data) {
    return (
      <div className="border-b border-slate-700 bg-slate-900/80 px-4 py-2 text-sm text-slate-400">
        Checking collector status…
      </div>
    )
  }

  if (error && !data) {
    return (
      <div className="border-b border-red-900/60 bg-red-950/40 px-4 py-2 text-sm text-red-200">
        Cannot reach /healthz: {error instanceof Error ? error.message : 'unknown error'}
      </div>
    )
  }

  if (!data) {
    return null
  }

  const overallOk = data.ok
  return (
    <div
      className={`border-b px-4 py-2 text-sm ${
        overallOk
          ? 'border-slate-700 bg-slate-900/80 text-slate-200'
          : 'border-amber-900/60 bg-amber-950/30 text-amber-100'
      }`}
    >
      <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-x-6 gap-y-1">
        <span className="flex items-center gap-2 font-medium">
          <StatusDot ok={overallOk} />
          {overallOk ? 'Collector healthy' : 'Collector degraded'}
        </span>
        <span className="flex items-center gap-2">
          <StatusDot ok={data.db_ok} />
          Database {data.db_ok ? 'OK' : 'unreachable'}
        </span>
        <span className="flex items-center gap-2">
          <StatusDot ok={data.collector_thread_alive} />
          Collector thread {data.collector_thread_alive ? 'running' : 'stopped'}
        </span>
        {data.last_success_at && (
          <span className="text-slate-400">Last success: {data.last_success_at}</span>
        )}
        {data.last_error && (
          <span className="text-amber-200">Last error: {data.last_error}</span>
        )}
      </div>
    </div>
  )
}
