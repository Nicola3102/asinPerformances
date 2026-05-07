import { useEffect, useState } from "react"

import './trendRoutes.css'

/** SPA 内保留上次成功的 embed HTML，路由切回时先用内存展示，不必等 fetch */
let sessionImpressionCachedHtml: string | null = null

const SESSION_IMPRESSION_HTML_LS_KEY = 'asinPerformances.v1.sessionImpressionHtml'
/** localStorage 单 key 上限附近，避免配额爆掉 */
const SESSION_IMPRESSION_HTML_LS_MAX = 4_500_000

const SESSION_IMPRESSION_FIRST_BUILD_STUB = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>生成中</title><style>body{margin:0;font-family:system-ui,"PingFang SC",sans-serif;padding:2rem;background:#0f1419;color:#94a3b8;line-height:1.6}</style></head><body><p>正在生成 session &amp; impression 报表（首次访问或后端内存缓存为空），请稍候…</p></body></html>`

function readSessionImpressionHtmlLs(): string | null {
  try {
    const s = localStorage.getItem(SESSION_IMPRESSION_HTML_LS_KEY)
    if (!s || s.length < 200) return null
    return s
  } catch {
    return null
  }
}

function writeSessionImpressionHtmlLs(html: string): void {
  try {
    if (!html || html.length < 200) return
    if (html.length > SESSION_IMPRESSION_HTML_LS_MAX) return
    localStorage.setItem(SESSION_IMPRESSION_HTML_LS_KEY, html)
  } catch {
    /* quota / 隐私模式 */
  }
}

/** 路由切换：仅 embed=1 读服务端/浏览器缓存；浏览器刷新（reload）时再 rebuild=1 拉最新（用 timeOrigin 避免 SPA 返回页误触发） */
function sessionImpressionRebuildStorageKey(): string {
  if (typeof performance === 'undefined') return 'si-rebuilt-unknown'
  const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined
  const id =
    typeof performance.timeOrigin === 'number' && performance.timeOrigin > 0
      ? String(performance.timeOrigin)
      : nav
        ? `nav-${nav.startTime}-${nav.loadEventEnd}`
        : `fallback-${Date.now()}`
  return `si-rebuilt-${id}`
}

function shouldRunSessionImpressionRebuildAfterEmbed(): boolean {
  if (typeof performance === 'undefined') return false
  const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined
  if (!nav || nav.type !== 'reload') return false
  try {
    return !sessionStorage.getItem(sessionImpressionRebuildStorageKey())
  } catch {
    return true
  }
}

function markSessionImpressionRebuildDone(): void {
  try {
    sessionStorage.setItem(sessionImpressionRebuildStorageKey(), '1')
  } catch {
    /* ignore */
  }
}

function getInitialSessionImpressionHtml(): string | null {
  if (sessionImpressionCachedHtml) return sessionImpressionCachedHtml
  return readSessionImpressionHtmlLs()
}

export function TrendSessionImpressionEmbeddedPage() {
  const [html, setHtml] = useState<string | null>(() => getInitialSessionImpressionHtml())
  const [err, setErr] = useState<string | null>(null)
  const [bgRefreshing, setBgRefreshing] = useState(false)
  const [bgNotice, setBgNotice] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setErr(null)
    setBgNotice(null)
    setBgRefreshing(false)

    const runRebuild = shouldRunSessionImpressionRebuildAfterEmbed()

    ;(async () => {
      try {
        const r1 = await fetch('/api/trend/session-impression?embed=1', {
          cache: 'default',
        })
        const embedCacheHdr = (r1.headers.get('X-Session-Impression-Cache') || '').toLowerCase()
        const t1 = await r1.text()
        if (!r1.ok) {
          throw new Error(`HTTP ${r1.status}: ${t1.slice(0, 200)}`)
        }

        const lsHtml = readSessionImpressionHtmlLs()
        let display = t1
        /** 无本地 HTML 且服务端 miss 时不在 iframe 里塞 stub，避免 srcDoc 先 stub 再正式报表的整页闪烁；用外层加载态直到 rebuild 完成 */
        let skipIframeUntilRebuild = false
        if (embedCacheHdr === 'miss') {
          if (lsHtml) {
            display = lsHtml
            if (!cancelled) {
              setBgNotice('展示浏览器本地缓存，正在向服务器同步最新报表…')
            }
          } else {
            skipIframeUntilRebuild = true
            display = SESSION_IMPRESSION_FIRST_BUILD_STUB
          }
        }

        if (!cancelled) {
          if (skipIframeUntilRebuild) {
            sessionImpressionCachedHtml = null
            setHtml(null)
          } else {
            sessionImpressionCachedHtml = display
            setHtml(display)
          }
        }

        if (embedCacheHdr === 'hit' && t1.length > 200) {
          writeSessionImpressionHtmlLs(t1)
        }

        // 服务端内存无缓存时必须 rebuild；浏览器刷新时亦 rebuild 拉最新（原逻辑）
        const needRebuild = runRebuild || embedCacheHdr === 'miss'
        if (!needRebuild || cancelled) return

        if (!cancelled) setBgRefreshing(true)
        const r2 = await fetch('/api/trend/session-impression?rebuild=1')
        const t2 = await r2.text()
        if (!cancelled) setBgRefreshing(false)

        if (!r2.ok) {
          if (!cancelled) {
            setBgNotice(
              embedCacheHdr === 'miss' && !lsHtml
                ? '报表生成失败，请稍后刷新页面重试，或新标签打开 /api/trend/session-impression?rebuild=1'
                : '刷新后后台同步失败，仍显示缓存内容。可再次刷新重试。',
            )
            if (skipIframeUntilRebuild) {
              setHtml(SESSION_IMPRESSION_FIRST_BUILD_STUB)
              sessionImpressionCachedHtml = SESSION_IMPRESSION_FIRST_BUILD_STUB
            }
          }
          return
        }
        markSessionImpressionRebuildDone()
        writeSessionImpressionHtmlLs(t2)
        if (!cancelled) {
          sessionImpressionCachedHtml = t2
          setHtml(t2)
          setBgNotice(null)
          const cacheHdr2 = r2.headers.get('X-Session-Impression-Cache')
          if (cacheHdr2 === 'stale-fallback') {
            setBgNotice('后台刷新失败，已保留上次成功缓存。')
          }
        }
      } catch (e: unknown) {
        if (!cancelled) {
          setBgRefreshing(false)
          setErr(
            e instanceof Error
              ? e.message
              : '请求失败。请确认后端已启动（如 :9090），且 dev 时 Vite 已代理 /api 到后端；Network 面板勿用会过滤掉该请求的关键字（例如 022）。',
          )
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [])

  if (err) {
    return (
      <div className="trend-embed-page trend-embed-page--message">
        <h2 className="trend-embed-error-title">报表加载失败</h2>
        <pre className="trend-embed-error-body">{err}</pre>
        <p className="trend-embed-hint">
          可直接访问{' '}
          <a href="/api/trend/session-impression?rebuild=1" target="_blank" rel="noreferrer">
            /api/trend/session-impression?rebuild=1
          </a>{' '}
          强制全量重算。进入本页会在服务端无缓存时自动触发重建；亦可<strong>刷新浏览器</strong>拉最新。
        </p>
      </div>
    )
  }
  if (html === null) {
    const showBar = bgRefreshing || Boolean(bgNotice)
    return (
      <div
        className={`trend-embed-page trend-embed-page--message${showBar ? ' trend-embed-page--with-bar' : ''}`}
      >
        {showBar ? (
          <div className="trend-embed-bgbar" role="status">
            {bgRefreshing ? '正在后台与线上库同步最新报表…' : bgNotice}
          </div>
        ) : null}
        <p className="trend-embed-loading">正在加载 session &amp; impression 报表…</p>
        <p className="trend-embed-hint">
          正在请求报表。服务端有内存缓存时较快；无缓存时会自动全量生成并写入浏览器本地缓存，下次进入可秒开。
        </p>
      </div>
    )
  }
  const showBar = bgRefreshing || Boolean(bgNotice)
  return (
    <div className={`trend-embed-page${showBar ? ' trend-embed-page--with-bar' : ''}`}>
      {showBar ? (
        <div className="trend-embed-bgbar" role="status">
          {bgRefreshing ? '正在后台与线上库同步最新报表…' : bgNotice}
        </div>
      ) : null}
      <iframe
        className="trend-embed-frame"
        title="session & impression 报表"
        srcDoc={html}
      />
    </div>
  )
}
