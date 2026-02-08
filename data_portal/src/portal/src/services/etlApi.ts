/**
 * ETL Jobs API (DIR-001) + Schema Monitor API (DIR-003)
 */

import { apiClient } from './apiUtils';

// ==================== ETL Jobs API (DIR-001) ====================

export const etlJobsApi = {
  // Job Groups
  getJobGroups: async () => {
    const response = await apiClient.get('/etl/job-groups');
    return response.data;
  },
  createJobGroup: async (data: { name: string; description?: string; priority?: number; enabled?: boolean }) => {
    const response = await apiClient.post('/etl/job-groups', data);
    return response.data;
  },
  updateJobGroup: async (groupId: number, data: { name: string; description?: string; priority?: number; enabled?: boolean }) => {
    const response = await apiClient.put(`/etl/job-groups/${groupId}`, data);
    return response.data;
  },
  deleteJobGroup: async (groupId: number) => {
    const response = await apiClient.delete(`/etl/job-groups/${groupId}`);
    return response.data;
  },

  // Jobs
  getJobs: async (params?: { group_id?: number; job_type?: string }) => {
    const response = await apiClient.get('/etl/jobs', { params });
    return response.data;
  },
  getJob: async (jobId: number) => {
    const response = await apiClient.get(`/etl/jobs/${jobId}`);
    return response.data;
  },
  createJob: async (data: { group_id: number; name: string; job_type: string; source_id?: string; target_table?: string; schedule?: string; dag_id?: string; config?: Record<string, unknown>; enabled?: boolean; sort_order?: number }) => {
    const response = await apiClient.post('/etl/jobs', data);
    return response.data;
  },
  updateJob: async (jobId: number, data: Record<string, unknown>) => {
    const response = await apiClient.put(`/etl/jobs/${jobId}`, data);
    return response.data;
  },
  deleteJob: async (jobId: number) => {
    const response = await apiClient.delete(`/etl/jobs/${jobId}`);
    return response.data;
  },
  reorderJob: async (jobId: number, sortOrder: number) => {
    const response = await apiClient.put(`/etl/jobs/${jobId}/reorder`, { sort_order: sortOrder });
    return response.data;
  },
  triggerJob: async (jobId: number) => {
    const response = await apiClient.post(`/etl/jobs/${jobId}/trigger`);
    return response.data;
  },

  // Dependencies
  getDependencies: async () => {
    const response = await apiClient.get('/etl/dependencies');
    return response.data;
  },
  getDependencyGraph: async () => {
    const response = await apiClient.get('/etl/dependencies/graph');
    return response.data;
  },
  autoDetectDependencies: async () => {
    const response = await apiClient.post('/etl/dependencies/auto-detect');
    return response.data;
  },
  createDependency: async (data: { source_table: string; target_table: string; relationship?: string; dep_type?: string }) => {
    const response = await apiClient.post('/etl/dependencies', data);
    return response.data;
  },
  deleteDependency: async (depId: number) => {
    const response = await apiClient.delete(`/etl/dependencies/${depId}`);
    return response.data;
  },
  getExecutionOrder: async () => {
    const response = await apiClient.get('/etl/dependencies/execution-order');
    return response.data;
  },

  // Logs
  getLogs: async (params?: { job_id?: number; status?: string; date_from?: string; date_to?: string; limit?: number }) => {
    const response = await apiClient.get('/etl/logs', { params });
    return response.data;
  },
  getLogDetail: async (logId: number) => {
    const response = await apiClient.get(`/etl/logs/${logId}`);
    return response.data;
  },

  // Alerts
  getAlertRules: async () => {
    const response = await apiClient.get('/etl/alert-rules');
    return response.data;
  },
  createAlertRule: async (data: { name: string; condition_type: string; threshold?: Record<string, unknown>; job_id?: number; channels?: string[]; webhook_url?: string; enabled?: boolean }) => {
    const response = await apiClient.post('/etl/alert-rules', data);
    return response.data;
  },
  updateAlertRule: async (ruleId: number, data: { name: string; condition_type: string; threshold?: Record<string, unknown>; job_id?: number; channels?: string[]; webhook_url?: string; enabled?: boolean }) => {
    const response = await apiClient.put(`/etl/alert-rules/${ruleId}`, data);
    return response.data;
  },
  deleteAlertRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/etl/alert-rules/${ruleId}`);
    return response.data;
  },
  getAlerts: async (params?: { severity?: string; acknowledged?: boolean; limit?: number }) => {
    const response = await apiClient.get('/etl/alerts', { params });
    return response.data;
  },
  acknowledgeAlert: async (alertId: number) => {
    const response = await apiClient.put(`/etl/alerts/${alertId}/acknowledge`);
    return response.data;
  },
  getUnreadAlertCount: async () => {
    const response = await apiClient.get('/etl/alerts/unread-count');
    return response.data;
  },
};

// ==================== Schema Monitor API (DIR-003) ====================

export const schemaMonitorApi = {
  // Section A: 스키마 변경
  captureSnapshot: async (sourceLabel?: string) => {
    const response = await apiClient.post('/schema-monitor/snapshot', null, {
      params: sourceLabel ? { source_label: sourceLabel } : undefined,
    });
    return response.data;
  },

  getDiff: async () => {
    const response = await apiClient.get('/schema-monitor/diff');
    return response.data;
  },

  getHistory: async (limit = 20) => {
    const response = await apiClient.get('/schema-monitor/history', { params: { limit } });
    return response.data;
  },

  getHistoryDetail: async (logId: number) => {
    const response = await apiClient.get(`/schema-monitor/history/${logId}`);
    return response.data;
  },

  // Section B: CDC
  getCDCStatus: async () => {
    const response = await apiClient.get('/schema-monitor/cdc/status');
    return response.data;
  },

  switchCDCMode: async (tableName: string, mode: string) => {
    const response = await apiClient.post('/schema-monitor/cdc/switch-mode', { table_name: tableName, mode });
    return response.data;
  },

  resetWatermark: async (tableName: string) => {
    const response = await apiClient.post('/schema-monitor/cdc/reset-watermark', { table_name: tableName });
    return response.data;
  },

  // Section C: 정책
  getPolicies: async () => {
    const response = await apiClient.get('/schema-monitor/policies');
    return response.data;
  },

  updatePolicy: async (key: string, policyValue: Record<string, unknown>, description?: string) => {
    const response = await apiClient.put(`/schema-monitor/policies/${encodeURIComponent(key)}`, {
      policy_value: policyValue,
      description,
    });
    return response.data;
  },

  getPolicyAudit: async () => {
    const response = await apiClient.get('/schema-monitor/policies/audit');
    return response.data;
  },
};
