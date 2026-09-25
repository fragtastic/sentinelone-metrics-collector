import { describe, expect, it } from 'vitest'
import { pivotRawSamples, truncateIsoToMinuteUtc } from './chartData'

describe('truncateIsoToMinuteUtc', () => {
  it('truncates sub-minute precision in UTC', () => {
    expect(truncateIsoToMinuteUtc('2026-09-25T17:59:52.935293+00:00')).toBe(
      '2026-09-25T17:59',
    )
  })
})

describe('pivotRawSamples', () => {
  it('groups queries on shared timestamps', () => {
    const { queries, points } = pivotRawSamples([
      { timestamp: '2026-01-01T12:00:00Z', query: 'a', result: 1 },
      { timestamp: '2026-01-01T12:00:00Z', query: 'b', result: 2 },
      { timestamp: '2026-01-01T12:01:00Z', query: 'a', result: 3 },
    ])
    expect(queries).toEqual(['a', 'b'])
    expect(points).toHaveLength(2)
    expect(points[0].a).toBe(1)
    expect(points[0].b).toBe(2)
    expect(points[1].a).toBe(3)
  })

  it('merges samples in the same UTC minute when displayMinuteBuckets is set', () => {
    const { points } = pivotRawSamples(
      [
        { timestamp: '2026-09-25T17:59:10+00:00', query: 'a', result: 1 },
        { timestamp: '2026-09-25T17:59:52.935293+00:00', query: 'a', result: 2 },
      ],
      undefined,
      { displayMinuteBuckets: true },
    )
    expect(points).toHaveLength(1)
    expect(points[0].time).toBe('2026-09-25T17:59')
    expect(points[0].a).toBe(2)
  })
})
