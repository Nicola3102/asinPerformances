/**
 * 统一 `/api` 请求：错误解析、网络失败文案与 JSON 解析。
 */

export const API_BASE = '/api'

/** FastAPI HTTPException / validation error body → 可读字符串 */
export function parseErrorResponse(text: string, status: number): string {
  try {
    const err = text ? JSON.parse(text) : {}
    const d = err.detail
    if (typeof d === 'string') return d
    if (Array.isArray(d)) return d.map((x: { msg?: string }) => x?.msg || JSON.stringify(x)).join('; ')
    if (d != null) return JSON.stringify(d)
  } catch {
    /* ignore */
  }
  if (status === 502 || status === 504) return '后端超时或未就绪'
  if (status >= 500) return `后端错误 (${status})，请查看后端日志`
  return `请求失败 (${status})`
}

export class ApiError extends Error {
  readonly status: number
  readonly bodyText: string

  constructor(message: string, status: number, bodyText: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.bodyText = bodyText
  }
}

function buildUrl(path: string): string {
  const p = path.startsWith('/') ? path : `/${path}`
  return `${API_BASE}${p}`
}

function connectionError(e: unknown): Error {
  const msg = e instanceof Error ? e.message : '网络错误'
  return new Error(`无法连接后端：${msg}。请确认后端已启动（如 docker compose up）。`)
}

/**
 * JSON API：`path` 为 `/api` 之后的路径，例如 `/asin-performances/stats`。
 * 成功时 `res.json()`；失败时抛出 `ApiError`（含 `status`）。
 */
export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(buildUrl(path), init)
  } catch (e) {
    throw connectionError(e)
  }
  if (!res.ok) {
    const text = await res.text()
    throw new ApiError(parseErrorResponse(text, res.status) || `请求失败 (${res.status})`, res.status, text)
  }
  return res.json() as Promise<T>
}

/**
 * 与 `apiFetch` 相同的前缀与错误处理，返回 `Blob`（如 CSV 导出）。
 */
export async function apiFetchBlob(path: string, init?: RequestInit): Promise<Blob> {
  let res: Response
  try {
    res = await fetch(buildUrl(path), init)
  } catch (e) {
    throw connectionError(e)
  }
  if (!res.ok) {
    const text = await res.text()
    throw new ApiError(parseErrorResponse(text, res.status) || `请求失败 (${res.status})`, res.status, text)
  }
  return res.blob()
}
