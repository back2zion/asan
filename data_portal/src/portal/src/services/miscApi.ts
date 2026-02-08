/**
 * Vector DB API + MCP Server API + Health Check + Migration API
 */

import { apiClient, sanitizeText } from './apiUtils';

// ==================== Vector DB API ====================

export const vectorApi = {
  // 벡터 검색
  search: async (query: string, collection = 'default', topK = 10) => {
    const response = await apiClient.post('/vector/search', {
      query: sanitizeText(query),
      collection: sanitizeText(collection),
      top_k: topK,
    });
    return response.data;
  },

  // 컬렉션 목록
  getCollections: async () => {
    const response = await apiClient.get('/vector/collections');
    return response.data;
  },
};

// ==================== MCP Server API ====================

export const mcpApi = {
  // 매니페스트 조회
  getManifest: async () => {
    const response = await apiClient.get('/mcp/manifest');
    return response.data;
  },

  // 도구 목록
  getTools: async (category?: string) => {
    const response = await apiClient.get('/mcp/tools', {
      params: category ? { category: sanitizeText(category) } : undefined,
    });
    return response.data;
  },

  // 도구 호출
  callTool: async (toolName: string, args: Record<string, unknown> = {}) => {
    const response = await apiClient.post(`/mcp/tools/${encodeURIComponent(toolName)}`, args);
    return response.data;
  },
};

// ==================== Health Check ====================

export const healthApi = {
  check: async () => {
    const response = await apiClient.get('/health');
    return response.data;
  },
};

// ==================== Migration Verification API (DMR-002) ====================

export const migrationApi = {
  verify: async () => {
    const response = await apiClient.get('/migration/verify');
    return response.data;
  },

  benchmark: async () => {
    const response = await apiClient.post('/migration/benchmark');
    return response.data;
  },

  deidentBenchmark: async () => {
    const response = await apiClient.post('/migration/deident-benchmark');
    return response.data;
  },

  history: async (runType?: string) => {
    const response = await apiClient.get('/migration/history', {
      params: runType ? { run_type: runType } : undefined,
    });
    return response.data;
  },
};
