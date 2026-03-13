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
        // Group F 查询实际耗时约 5–6 分钟（Step1~109s, Step3~203s），代理需足够长
        timeout: 420000,
      },
    },
  },
})
