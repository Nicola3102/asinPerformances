export function formatParentOrderTotal(v: string | number | null | undefined): string {
  if (v == null || v === '') return '–'
  const n = Number(v)
  if (Number.isNaN(n)) return '–'
  return String(Math.round(n))
}

export function formatCreatedAt(v: string | null | undefined): string {
  if (v == null || v === '') return '–'
  try {
    const d = new Date(v)
    if (Number.isNaN(d.getTime())) return v
    return d.toLocaleString('zh-CN', { dateStyle: 'short', timeStyle: 'short' })
  } catch {
    return v
  }
}

export function formatNum(v: number | null | undefined): string {
  if (v == null) return '–'
  return String(v)
}

export function formatDecimal(v: number | null | undefined, fractionDigits = 2): string {
  if (v == null || Number.isNaN(v)) return '–'
  return v.toLocaleString('zh-CN', {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })
}

export function formatPermyriadUntilVisible(v: number | null | undefined, minDigits = 2, maxDigits = 4): string {
  if (v == null || Number.isNaN(v)) return '–'
  const permyriad = Number(v) * 10000
  if (!Number.isFinite(permyriad)) return '–'
  if (permyriad === 0) return `0.${'0'.repeat(Math.max(0, minDigits))}‱`
  const lower = Math.max(0, Math.trunc(minDigits))
  const upper = Math.max(lower, Math.trunc(maxDigits))
  for (let digits = lower; digits <= upper; digits += 1) {
    const text = permyriad.toLocaleString('zh-CN', {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    })
    if (Number(text.replace(/,/g, '')) !== 0 || digits === upper) return `${text}‱`
  }
  return `${permyriad.toLocaleString('zh-CN', {
    minimumFractionDigits: upper,
    maximumFractionDigits: upper,
  })}‱`
}
