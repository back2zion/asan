/**
 * Catalog Analytics API (DPR-002: 쿼리 패턴 분석 + 마스터 모델)
 */

import { apiClient } from './apiUtils';

export interface QueryLogPayload {
  user_id?: string;
  query_text?: string;
  query_type?: string;
  tables_accessed?: string[];
  columns_accessed?: string[];
  filters_used?: Record<string, unknown>;
  response_time_ms?: number;
  result_count?: number;
}

export interface MasterModel {
  model_id: string;
  name: string;
  description: string | null;
  creator: string;
  model_type: string;
  base_tables: string[];
  query_template: string | null;
  parameters: Record<string, unknown>;
  usage_count: number;
  shared: boolean;
  created_at: string | null;
}

export interface MasterModelCreate {
  name: string;
  description?: string;
  creator?: string;
  model_type?: string;
  base_tables?: string[];
  query_template?: string;
  parameters?: Record<string, unknown>;
  shared?: boolean;
}

export const catalogAnalyticsApi = {
  // Query logging
  logQuery: async (payload: QueryLogPayload) => {
    const response = await apiClient.post('/catalog-analytics/query-log', payload);
    return response.data;
  },

  // Query patterns
  getQueryPatterns: async (days = 7) => {
    const response = await apiClient.get('/catalog-analytics/query-patterns', { params: { days } });
    return response.data;
  },

  // Trending
  getTrending: async () => {
    const response = await apiClient.get('/catalog-analytics/trending');
    return response.data;
  },

  // Column lineage
  getColumnLineage: async (tableName: string, columnName: string) => {
    const response = await apiClient.get(`/catalog-analytics/column-lineage/${tableName}/${columnName}`);
    return response.data;
  },

  // Master models CRUD
  getMasterModels: async () => {
    const response = await apiClient.get('/catalog-analytics/master-models');
    return response.data;
  },

  getMasterModel: async (modelId: string) => {
    const response = await apiClient.get(`/catalog-analytics/master-models/${modelId}`);
    return response.data;
  },

  createMasterModel: async (data: MasterModelCreate) => {
    const response = await apiClient.post('/catalog-analytics/master-models', data);
    return response.data;
  },

  updateMasterModel: async (modelId: string, data: Partial<MasterModelCreate>) => {
    const response = await apiClient.put(`/catalog-analytics/master-models/${modelId}`, data);
    return response.data;
  },

  deleteMasterModel: async (modelId: string) => {
    const response = await apiClient.delete(`/catalog-analytics/master-models/${modelId}`);
    return response.data;
  },
};
