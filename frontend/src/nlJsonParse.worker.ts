/// <reference lib="webworker" />

type OutMsg = { ok: true; data: unknown } | { ok: false; error: string }

const g = self as unknown as DedicatedWorkerGlobalScope

g.onmessage = (e: MessageEvent<string>) => {
  try {
    const data = JSON.parse(e.data) as unknown
    g.postMessage({ ok: true, data } satisfies OutMsg)
  } catch (err) {
    g.postMessage({ ok: false, error: String(err) } satisfies OutMsg)
  }
}

export {}
