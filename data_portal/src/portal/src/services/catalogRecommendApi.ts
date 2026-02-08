/**
 * Catalog Recommend API (DPR-002: 지능형 추천)
 */

import { apiClient } from './apiUtils';

export interface Recommendation {
  table_name: string;
  label: string;
  domain: string;
  relevance_score: number;
  reasons: string[];
}

export const catalogRecommendApi = {
  getForUser: async (userId = 'anonymous', limit = 6) => {
    const response = await apiClient.get(`/catalog-recommend/for-user/${userId}`, { params: { limit } });
    return response.data;
  },

  getForTable: async (tableName: string, limit = 5) => {
    const response = await apiClient.get(`/catalog-recommend/for-table/${tableName}`, { params: { limit } });
    return response.data;
  },

  getTrending: async () => {
    const response = await apiClient.get('/catalog-recommend/trending');
    return response.data;
  },
};
