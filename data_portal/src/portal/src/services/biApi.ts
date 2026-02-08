/**
 * DPR-004: BI API 클라이언트
 */
import { apiClient } from './apiUtils';

export const biApi = {
  // ── Query ──
  executeQuery: async (sql: string, limit = 1000) => {
    const response = await apiClient.post('/bi/query/execute', { sql, limit });
    return response.data;
  },
  executeQueryAsync: async (sql: string, limit = 1000) => {
    const response = await apiClient.post('/bi/query/execute-async', { sql, limit });
    return response.data;
  },
  getJobStatus: async (jobId: string) => {
    const response = await apiClient.get(`/bi/query/status/${jobId}`);
    return response.data;
  },
  getJobResult: async (jobId: string) => {
    const response = await apiClient.get(`/bi/query/result/${jobId}`);
    return response.data;
  },
  validateQuery: async (sql: string) => {
    const response = await apiClient.post('/bi/query/validate', { sql });
    return response.data;
  },
  getQueryHistory: async (limit = 50) => {
    const response = await apiClient.get('/bi/query/history', { params: { limit } });
    return response.data;
  },
  saveQuery: async (data: { name: string; description?: string; sql_text: string; tags?: string[]; shared?: boolean }) => {
    const response = await apiClient.post('/bi/query/save', data);
    return response.data;
  },
  getSavedQueries: async () => {
    const response = await apiClient.get('/bi/query/saved');
    return response.data;
  },
  deleteSavedQuery: async (queryId: number) => {
    const response = await apiClient.delete(`/bi/query/saved/${queryId}`);
    return response.data;
  },
  getTables: async () => {
    const response = await apiClient.get('/bi/query/tables');
    return response.data;
  },
  getColumns: async (table: string) => {
    const response = await apiClient.get(`/bi/query/columns/${table}`);
    return response.data;
  },

  // ── Charts ──
  createChart: async (data: { name: string; chart_type: string; sql_query: string; config?: any; description?: string }) => {
    const response = await apiClient.post('/bi/charts', data);
    return response.data;
  },
  getCharts: async () => {
    const response = await apiClient.get('/bi/charts');
    return response.data;
  },
  getChart: async (chartId: number) => {
    const response = await apiClient.get(`/bi/charts/${chartId}`);
    return response.data;
  },
  updateChart: async (chartId: number, data: any) => {
    const response = await apiClient.put(`/bi/charts/${chartId}`, data);
    return response.data;
  },
  deleteChart: async (chartId: number) => {
    const response = await apiClient.delete(`/bi/charts/${chartId}`);
    return response.data;
  },
  getChartRawData: async (chartId: number, limit = 500) => {
    const response = await apiClient.get(`/bi/charts/${chartId}/raw-data`, { params: { limit } });
    return response.data;
  },
  drillDown: async (chartId: number, data: { dimension: string; value: any; filters?: any }) => {
    const response = await apiClient.post(`/bi/charts/${chartId}/drill-down`, data);
    return response.data;
  },

  // ── Dashboards ──
  createDashboard: async (data: { name: string; description?: string; layout?: any; chart_ids?: number[]; shared?: boolean }) => {
    const response = await apiClient.post('/bi/dashboards', data);
    return response.data;
  },
  getDashboards: async () => {
    const response = await apiClient.get('/bi/dashboards');
    return response.data;
  },
  getDashboard: async (dashboardId: number) => {
    const response = await apiClient.get(`/bi/dashboards/${dashboardId}`);
    return response.data;
  },
  updateDashboard: async (dashboardId: number, data: any) => {
    const response = await apiClient.put(`/bi/dashboards/${dashboardId}`, data);
    return response.data;
  },
  deleteDashboard: async (dashboardId: number) => {
    const response = await apiClient.delete(`/bi/dashboards/${dashboardId}`);
    return response.data;
  },

  // ── Export ──
  exportReport: async (format: string, data: { chart_ids?: number[]; dashboard_id?: number; title?: string; include_data?: boolean }) => {
    const response = await apiClient.post(`/bi/export/${format}`, data, { responseType: 'blob' });
    return response.data;
  },

  // ── Overview ──
  getOverview: async () => {
    const response = await apiClient.get('/bi/overview');
    return response.data;
  },
};
