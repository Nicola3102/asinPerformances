/** 仅用于收集 candidates 的轻量结构，避免与 trendShared 循环依赖 */
type NlPayloadForOrderFlags = {
  views?: {
    all?: {
      cohortTable?: Array<{
        daySessionAsins?: Array<
          Array<{ asin?: string; storeId?: number; sessions?: number }>
        >
      }>
    }
  }
}
export const NL_ORDER_POSITIVE_LS_KEY = 'asinPerformances.v1.nlOrderPositiveKeys'
/** 当日已发起的 order-flags POST 次数（每日重置） */
export const NL_ORDER_FLAGS_DAILY_LS_KEY = 'asinPerformances.v1.nlOrderFlagsDailyCount'

export const NL_ORDER_FLAGS_BATCH_MAX = 400
export const NL_ORDER_FLAGS_MAX_POSTS_PER_DAY = 2
const POSITIVE_KEYS_MAX = 12_000

function todayYmdLocal(): string {
  const d = new Date()
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

export function loadNlOrderPositiveKeySet(): Set<string> {
  try {
    const raw = localStorage.getItem(NL_ORDER_POSITIVE_LS_KEY)
    if (!raw) return new Set()
    const arr = JSON.parse(raw) as string[]
    return new Set(Array.isArray(arr) ? arr : [])
  } catch {
    return new Set()
  }
}

/** 合并写入「有订单」键；超长时保留末尾一段避免撑爆 localStorage */
export function persistNlOrderPositiveKeys(newKeys: string[]): void {
  if (!newKeys.length) return
  const s = loadNlOrderPositiveKeySet()
  for (const k of newKeys) {
    if (k) s.add(k)
  }
  let arr = [...s]
  if (arr.length > POSITIVE_KEYS_MAX) {
    arr = arr.slice(-POSITIVE_KEYS_MAX)
  }
  try {
    localStorage.setItem(NL_ORDER_POSITIVE_LS_KEY, JSON.stringify(arr))
  } catch {
    /* quota */
  }
}

export function readNlOrderFlagsPostsToday(): number {
  try {
    const raw = localStorage.getItem(NL_ORDER_FLAGS_DAILY_LS_KEY)
    const today = todayYmdLocal()
    if (!raw) return 0
    const o = JSON.parse(raw) as { date?: string; count?: number }
    if (o.date !== today) return 0
    return Math.min(
      NL_ORDER_FLAGS_MAX_POSTS_PER_DAY,
      Math.max(0, Number(o.count) || 0),
    )
  } catch {
    return 0
  }
}

/** 返回写入后的当日计数（封顶 NL_ORDER_FLAGS_MAX_POSTS_PER_DAY） */
export function incrementNlOrderFlagsDailyCount(): number {
  const today = todayYmdLocal()
  let next = 1
  try {
    const raw = localStorage.getItem(NL_ORDER_FLAGS_DAILY_LS_KEY)
    if (raw) {
      const o = JSON.parse(raw) as { date?: string; count?: number }
      if (o.date === today) {
        next = Math.min(
          NL_ORDER_FLAGS_MAX_POSTS_PER_DAY,
          (Number(o.count) || 0) + 1,
        )
      }
    }
    localStorage.setItem(
      NL_ORDER_FLAGS_DAILY_LS_KEY,
      JSON.stringify({ date: today, count: next }),
    )
  } catch {
    /* ignore */
  }
  return next
}

export function remainingNlOrderFlagPostsToday(): number {
  return Math.max(0, NL_ORDER_FLAGS_MAX_POSTS_PER_DAY - readNlOrderFlagsPostsToday())
}

/** 从 views.all 的 cohort 明细收集所有 (asin, store_id)，用于批量 order-flags */
export function collectNlOrderFlagCandidates(
  payload: NlPayloadForOrderFlags,
): Array<{ asin: string; store_id: number }> {
  const view = payload.views?.all
  const cohortTable = view?.cohortTable
  if (!Array.isArray(cohortTable)) return []
  const seen = new Set<string>()
  const out: Array<{ asin: string; store_id: number }> = []
  for (const row of cohortTable) {
    const days = row.daySessionAsins
    if (!Array.isArray(days)) continue
    for (const dayArr of days) {
      if (!Array.isArray(dayArr)) continue
      for (const it of dayArr) {
        const asin = String(it?.asin ?? '').trim()
        const sid = Number(it?.storeId ?? NaN)
        if (!asin || !Number.isFinite(sid)) continue
        const k = `${asin}||${sid}`
        if (seen.has(k)) continue
        seen.add(k)
        out.push({ asin, store_id: sid })
      }
    }
  }
  return out
}
