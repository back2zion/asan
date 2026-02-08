/**
 * Catalog Extension API (DPR-001: 샘플 데이터, 커뮤니티, 스냅샷/레시피)
 */

import { apiClient } from './apiUtils';

export const catalogExtApi = {
  // Sample Data
  getSampleData: async (tableName: string, limit = 10) => {
    const response = await apiClient.get(`/catalog-ext/tables/${tableName}/sample-data`, { params: { limit } });
    return response.data;
  },

  // Community Comments
  getComments: async (tableName: string, limit = 50) => {
    const response = await apiClient.get(`/catalog-ext/tables/${tableName}/comments`, { params: { limit } });
    return response.data;
  },
  createComment: async (tableName: string, data: { author?: string; content: string }) => {
    const response = await apiClient.post(`/catalog-ext/tables/${tableName}/comments`, data);
    return response.data;
  },
  deleteComment: async (tableName: string, commentId: number) => {
    const response = await apiClient.delete(`/catalog-ext/tables/${tableName}/comments/${commentId}`);
    return response.data;
  },

  // Snapshots / Recipes
  getSnapshots: async (params?: { table_name?: string; creator?: string; shared_only?: boolean }) => {
    const response = await apiClient.get('/catalog-ext/snapshots', { params });
    return response.data;
  },
  getSnapshot: async (snapshotId: string) => {
    const response = await apiClient.get(`/catalog-ext/snapshots/${snapshotId}`);
    return response.data;
  },
  createSnapshot: async (data: {
    name: string;
    description?: string;
    creator?: string;
    table_name?: string;
    query_logic?: string;
    filters?: Record<string, any>;
    columns?: string[];
    shared?: boolean;
    share_scope?: 'private' | 'group' | 'public';
  }) => {
    const response = await apiClient.post('/catalog-ext/snapshots', data);
    return response.data;
  },
  deleteSnapshot: async (snapshotId: string) => {
    const response = await apiClient.delete(`/catalog-ext/snapshots/${snapshotId}`);
    return response.data;
  },

  // Dashboard Lakehouse Overview
  getLakehouseOverview: async () => {
    const response = await apiClient.get('/catalog-ext/lakehouse-overview');
    return response.data;
  },

  // Version History
  getTableVersions: async (tableName: string) => {
    const response = await apiClient.get(`/catalog-ext/tables/${tableName}/versions`);
    return response.data;
  },

  // Recent Searches
  getRecentSearches: async () => {
    const response = await apiClient.get('/catalog-ext/recent-searches');
    return response.data;
  },

  // Search Suggest (오타 보정 + AI 요약)
  getSearchSuggest: async (query: string) => {
    const response = await apiClient.get('/catalog-ext/search-suggest', { params: { q: query } });
    return response.data;
  },
};
