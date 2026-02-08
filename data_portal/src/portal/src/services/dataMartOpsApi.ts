/**
 * Data Mart Ops API
 */

import { apiClient } from './apiUtils';

export const dataMartOpsApi = {
  // Marts
  getMarts: async (params?: { zone?: string; status?: string }) => {
    const response = await apiClient.get('/data-mart-ops/marts', { params });
    return response.data;
  },
  getMart: async (martId: number) => {
    const response = await apiClient.get(`/data-mart-ops/marts/${martId}`);
    return response.data;
  },
  createMart: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/marts', data);
    return response.data;
  },
  updateMart: async (martId: number, data: any) => {
    const response = await apiClient.put(`/data-mart-ops/marts/${martId}`, data);
    return response.data;
  },
  deleteMart: async (martId: number) => {
    const response = await apiClient.delete(`/data-mart-ops/marts/${martId}`);
    return response.data;
  },
  checkDuplicates: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/marts/check-duplicates', data);
    return response.data;
  },
  // Schema Changes
  getSchemaChanges: async (params?: { mart_id?: number; status?: string }) => {
    const response = await apiClient.get('/data-mart-ops/schema-changes', { params });
    return response.data;
  },
  analyzeSchemaChange: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/schema-changes/analyze', data);
    return response.data;
  },
  applySchemaChange: async (changeId: number) => {
    const response = await apiClient.put(`/data-mart-ops/schema-changes/${changeId}/apply`);
    return response.data;
  },
  rollbackSchemaChange: async (changeId: number) => {
    const response = await apiClient.put(`/data-mart-ops/schema-changes/${changeId}/rollback`);
    return response.data;
  },
  // Dimensions
  getDimensions: async () => {
    const response = await apiClient.get('/data-mart-ops/dimensions');
    return response.data;
  },
  createDimension: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/dimensions', data);
    return response.data;
  },
  updateDimension: async (dimId: number, data: any) => {
    const response = await apiClient.put(`/data-mart-ops/dimensions/${dimId}`, data);
    return response.data;
  },
  deleteDimension: async (dimId: number) => {
    const response = await apiClient.delete(`/data-mart-ops/dimensions/${dimId}`);
    return response.data;
  },
  getDimensionHierarchy: async (dimId: number) => {
    const response = await apiClient.get(`/data-mart-ops/dimensions/${dimId}/hierarchy`);
    return response.data;
  },
  // Metrics
  getMetrics: async (params?: { category?: string; catalog_only?: boolean }) => {
    const response = await apiClient.get('/data-mart-ops/metrics', { params });
    return response.data;
  },
  createMetric: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/metrics', data);
    return response.data;
  },
  updateMetric: async (metricId: number, data: any) => {
    const response = await apiClient.put(`/data-mart-ops/metrics/${metricId}`, data);
    return response.data;
  },
  deleteMetric: async (metricId: number) => {
    const response = await apiClient.delete(`/data-mart-ops/metrics/${metricId}`);
    return response.data;
  },
  searchCatalog: async (search?: string) => {
    const response = await apiClient.get('/data-mart-ops/metrics/catalog', { params: { search } });
    return response.data;
  },
  // Flow Stages
  getFlowStages: async () => {
    const response = await apiClient.get('/data-mart-ops/flow-stages');
    return response.data;
  },
  createFlowStage: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/flow-stages', data);
    return response.data;
  },
  getFlowPipeline: async () => {
    const response = await apiClient.get('/data-mart-ops/flow-stages/pipeline');
    return response.data;
  },
  // Optimizations
  getOptimizations: async (params?: { mart_id?: number; status?: string }) => {
    const response = await apiClient.get('/data-mart-ops/optimizations', { params });
    return response.data;
  },
  createOptimization: async (data: any) => {
    const response = await apiClient.post('/data-mart-ops/optimizations', data);
    return response.data;
  },
  applyOptimization: async (optId: number) => {
    const response = await apiClient.put(`/data-mart-ops/optimizations/${optId}/apply`);
    return response.data;
  },
  deleteOptimization: async (optId: number) => {
    const response = await apiClient.delete(`/data-mart-ops/optimizations/${optId}`);
    return response.data;
  },
  getSuggestions: async () => {
    const response = await apiClient.get('/data-mart-ops/optimizations/suggestions');
    return response.data;
  },
  // Overview
  getOverview: async () => {
    const response = await apiClient.get('/data-mart-ops/overview');
    return response.data;
  },
};
