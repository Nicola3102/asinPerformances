import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 本机 npm run dev 时后端在 localhost:9090；Docker 内用 VITE_API_PROXY_TARGET=http://backend:9090
const apiTarget = process.env.VITE_API_PROXY_TARGET || 'http://localhost:9090'
console.log('[Vite] API proxy target:', apiTarget)

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    watch: { usePolling: true },
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
