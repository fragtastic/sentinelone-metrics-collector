import { describe, expect, it } from 'vitest'
import { scrollTargetFromWheel } from './legendWheelScroll'

describe('scrollTargetFromWheel', () => {
  const pageInfo = { pageNextDataIndex: 3, pagePrevDataIndex: 0 }

  it('uses pageNextDataIndex when scrolling down', () => {
    expect(scrollTargetFromWheel(pageInfo, 100)).toBe(3)
  })

  it('uses pagePrevDataIndex when scrolling up', () => {
    expect(scrollTargetFromWheel(pageInfo, -100)).toBe(0)
  })

  it('returns null when no further page exists', () => {
    expect(scrollTargetFromWheel({ pageNextDataIndex: null, pagePrevDataIndex: 0 }, 100)).toBe(
      null,
    )
  })
})
