import { describe, expect, it } from 'vitest'
import { formatQueryLabel } from './formatQueryLabel'

describe('formatQueryLabel', () => {
  it('formats known boolean params', () => {
    expect(formatQueryLabel('isDecommissioned=False&isActive=True')).toBe(
      'Not decommissioned, active',
    )
  })

  it('passes through unknown segments', () => {
    expect(formatQueryLabel('custom=value')).toBe('Custom=value')
  })
})
