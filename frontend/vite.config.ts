import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const devToken = env.VITE_DEV_API_TOKEN ?? ''
  const proxyHeaders: Record<string, string> = {}
  if (devToken) {
    proxyHeaders.Authorization = `Bearer ${devToken}`
  }

  return {
    plugins: [react(), tailwindcss()],
    test: {
      environment: 'jsdom',
      include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
    },
    server: {
      proxy: {
        '/healthz': {
          target: 'http://127.0.0.1:8080',
          changeOrigin: true,
        },
        '/metrics': {
          target: 'http://127.0.0.1:8080',
          changeOrigin: true,
          headers: proxyHeaders,
        },
      },
    },
  }
})
