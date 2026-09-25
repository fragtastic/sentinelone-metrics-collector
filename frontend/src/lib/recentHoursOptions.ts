export const RECENT_HOUR_OPTIONS = [6, 12, 24, 48, 72, 168] as const

export type RecentHours = (typeof RECENT_HOUR_OPTIONS)[number]
