export type HealthResponse = {
  ok: boolean
  db_ok: boolean
  collector_thread_alive: boolean
  last_collect_at: string | null
  last_success_at: string | null
  last_error: string | null
}

export type LatestMetric = {
  timestamp: string
  query: string
  result: number | null
}

export type DailyMaxRow = {
  day: string
  query: string
  max_result: number | null
}

export type HourlyMaxRow = {
  hour: string
  query: string
  max_result: number | null
}

export type RangeRow = {
  hour: string
  query: string
  min_result: number | null
  avg_result: number | null
  max_result: number | null
  sample_count: number
}

export type ApiError = {
  error: string
}
