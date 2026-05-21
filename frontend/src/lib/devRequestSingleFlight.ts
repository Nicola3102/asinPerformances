const inflightRequests = new Map<string, Promise<unknown>>()

/**
 * StrictMode 在开发态会双挂载组件；对同一请求 key 复用进行中的 Promise，
 * 避免首屏 effect 在短时间内重复打相同接口。
 */
export function devRequestSingleFlight<T>(key: string, factory: () => Promise<T>): Promise<T> {
  if (!import.meta.env.DEV) return factory()

  const existing = inflightRequests.get(key)
  if (existing) return existing as Promise<T>

  const promise = factory().finally(() => {
    if (inflightRequests.get(key) === promise) inflightRequests.delete(key)
  })
  inflightRequests.set(key, promise)
  return promise
}
