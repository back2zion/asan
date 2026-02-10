import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        secure: false,
      },
      '/jupyter': {
        target: 'http://localhost:18888',
        changeOrigin: true,
        ws: true,
        rewrite: (path) => path.replace(/^\/jupyter/, ''),
      },
    },
    watch: {
      // node_modules 감시 제외 — 메모리 절약
      ignored: ['**/node_modules/**', '**/.git/**'],
    },
    // HMR 안정성
    hmr: {
      overlay: true,
      timeout: 5000,
    },
  },
  optimizeDeps: {
    include: [
      'react', 'react-dom', 'react-router-dom',
      'antd', '@ant-design/icons',
      'recharts',
      '@tanstack/react-query', 'axios',
    ],
    // 무거운 라이브러리는 사전 번들링에서 제외 (lazy import로 처리)
    exclude: ['three'],
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true,
        drop_debugger: true,
      },
    },
    rollupOptions: {
      output: {
        manualChunks: {
          'vendor-react': ['react', 'react-dom', 'react-router-dom'],
          'vendor-antd': ['antd', '@ant-design/icons'],
          'vendor-charts': ['echarts', 'echarts-for-react', 'recharts'],
          'vendor-graph': ['reactflow', 'react-force-graph-2d'],
          'vendor-3d': ['three', 'react-force-graph-3d'],
          'vendor-query': ['@tanstack/react-query', 'axios'],
          'vendor-editor': ['codemirror', '@codemirror/lang-sql', '@uiw/react-codemirror'],
        },
      },
    },
    chunkSizeWarningLimit: 1500,
  },
})
