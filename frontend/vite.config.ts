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
        // Group F / traffic 报表等可能数分钟；timeout=客户端 socket，proxyTimeout=等后端首包/全程（过短会红字失败且 Response 空）
        timeout: 900000,
        proxyTimeout: 900000,
      },
    },
  },
})
