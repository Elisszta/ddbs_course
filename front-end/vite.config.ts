import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000, // 前端运行在 3000
    proxy: {
      // 代理配置：凡是 /api 开头的请求，都转发给后端 8000
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        // 如果后端路径不带 /api 前缀需要 rewrite，但你的后端好像带了，所以保留
        // rewrite: (path) => path.replace(/^\/api/, ''), 
      },
    }
  }
})
