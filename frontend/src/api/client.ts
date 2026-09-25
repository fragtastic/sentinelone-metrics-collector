import { getAccessToken } from '../auth/accessToken'
import type {
  ApiError,
  DailyMaxRow,
  HealthResponse,
  HourlyMaxRow,
  LatestMetric,
  RangeRow,
} from './types'

const apiBase = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? ''

function authHeaders(): HeadersInit {
  const token = getAccessToken()
  if (!token) {
    return {}
  }
  return { Authorization: `Bearer ${token}` }
}

async function apiFetch(input: string): Promise<Response> {
  return fetch(input, { headers: authHeaders() })
}

async function parseJson<T>(response: Response): Promise<T> {
  const text = await response.text()
  if (!text) {
    throw new Error(`Empty response (${response.status})`)
  }
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    throw new Error(`Invalid JSON (${response.status})`)
  }
  if (!response.ok) {
    const err = data as ApiError
    throw new Error(err.error ?? `Request failed (${response.status})`)
  }
  return data as T
}

function buildUrl(path: string, params?: Record<string, string | number | undefined>): string {
  const url = new URL(path, apiBase || window.location.origin)
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== '') {
        url.searchParams.set(key, String(value))
      }
    }
  }
  return url.toString()
}

export async function fetchHealth(): Promise<HealthResponse> {
  const response = await apiFetch(buildUrl('/healthz'))
  const text = await response.text()
  if (!text) {
    throw new Error(`Empty health response (${response.status})`)
  }
  return JSON.parse(text) as HealthResponse
}

export async function fetchLatest(limit = 200, query?: string): Promise<LatestMetric[]> {
  const response = await apiFetch(buildUrl('/metrics/latest', { limit, query }))
  return parseJson<LatestMetric[]>(response)
}

export async function fetchDailyMax(days: number): Promise<DailyMaxRow[]> {
  const response = await apiFetch(buildUrl('/metrics/daily-max', { days }))
  return parseJson<DailyMaxRow[]>(response)
}

export async function fetchHourlyMax(
  options: { days: number; query?: string } | { hours: number; query?: string },
): Promise<HourlyMaxRow[]> {
  const params =
    'hours' in options
      ? { hours: options.hours, query: options.query }
      : { days: options.days, query: options.query }
  const response = await apiFetch(buildUrl('/metrics/hourly-max', params))
  return parseJson<HourlyMaxRow[]>(response)
}

export async function fetchRaw(hours: number, query?: string): Promise<LatestMetric[]> {
  const response = await apiFetch(buildUrl('/metrics/raw', { hours, query }))
  return parseJson<LatestMetric[]>(response)
}

export async function fetchRange(
  from: string,
  to: string,
  options?: { query?: string; limit?: number },
): Promise<RangeRow[]> {
  const response = await apiFetch(
    buildUrl('/metrics/range', {
      from,
      to,
      query: options?.query,
      limit: options?.limit,
    }),
  )
  return parseJson<RangeRow[]>(response)
}

export function distinctQueries(rows: { query: string }[]): string[] {
  return [...new Set(rows.map((r) => r.query))].sort()
}
