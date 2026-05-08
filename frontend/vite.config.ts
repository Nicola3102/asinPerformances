import type { IncomingMessage, ServerResponse } from 'node:http'
import type { ViteDevServer } from 'vite'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 本机 npm run dev 时后端在 localhost:9090；Docker 内用 VITE_API_PROXY_TARGET=http://backend:9090
const apiTarget = process.env.VITE_API_PROXY_TARGET || 'http://localhost:9090'
console.log('[Vite] API proxy target:', apiTarget)

/** Docker healthcheck：与后端 FastAPI `/health` 对齐返回 JSON（不经代理） */
function viteHealthPlugin() {
  return {
    name: 'vite-health-endpoint',
    configureServer(server: ViteDevServer) {
      server.middlewares.use((req: IncomingMessage, res: ServerResponse, next: () => void) => {
        if (req.url === '/health' || req.url?.startsWith('/health?')) {
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ status: 'ok' }))
          return
        }
        next()
      })
    },
  }
}

/**
 * Dockerfile / shell 脚本在项目根目录时，若浏览器或某请求命中 `/Dockerfile`（无扩展名），
 * Vite 会走模块转换链并把内容当 JS 解析 → import-analysis 报错。开发服务器不应对外暴露这些路径。
 */
function viteIgnoreDockerArtifactsPlugin() {
  const denyPath = new Set(['/Dockerfile', '/docker-entrypoint.sh'])
  return {
    name: 'vite-ignore-docker-artifacts',
    configureServer(server: ViteDevServer) {
      server.middlewares.use((req: IncomingMessage, res: ServerResponse, next: () => void) => {
        const pathOnly = req.url?.split('?')[0] ?? ''
        if (denyPath.has(pathOnly)) {
          res.statusCode = 404
          res.end()
          return
        }
        next()
      })
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [viteIgnoreDockerArtifactsPlugin(), react(), viteHealthPlugin()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    watch: {
      usePolling: true,
      ignored: ['**/Dockerfile', '**/docker-entrypoint.sh'],
    },
    proxy: {
      '/api': {
        target: apiTarget,
        changeOrigin: true,
        // New Listing json_views=all 大区间冷算可达数十分钟；过短会中断连接 → 浏览器 Failed to fetch、Response 头为空
        timeout: 3_600_000,
        proxyTimeout: 3_600_000,
      },
    },
  },
})
