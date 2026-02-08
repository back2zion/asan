/**
 * Metadata Management API
 */

import { apiClient } from './apiUtils';

export const metadataMgmtApi = {
  // Change Requests
  getChangeRequests: async (params?: { status?: string; request_type?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/change-requests', { params });
    return response.data;
  },
  createChangeRequest: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/change-requests', data);
    return response.data;
  },
  submitChangeRequest: async (id: number) => {
    const response = await apiClient.put(`/metadata-mgmt/change-requests/${id}/submit`);
    return response.data;
  },
  reviewChangeRequest: async (id: number, reviewer?: string, comment?: string) => {
    const response = await apiClient.put(`/metadata-mgmt/change-requests/${id}/review`, null, { params: { reviewer, comment } });
    return response.data;
  },
  approveChangeRequest: async (id: number, approver?: string, comment?: string) => {
    const response = await apiClient.put(`/metadata-mgmt/change-requests/${id}/approve`, null, { params: { approver, comment } });
    return response.data;
  },
  rejectChangeRequest: async (id: number, approver?: string, comment?: string) => {
    const response = await apiClient.put(`/metadata-mgmt/change-requests/${id}/reject`, null, { params: { approver, comment } });
    return response.data;
  },
  applyChangeRequest: async (id: number) => {
    const response = await apiClient.put(`/metadata-mgmt/change-requests/${id}/apply`);
    return response.data;
  },
  getChangeRequestStats: async () => {
    const response = await apiClient.get('/metadata-mgmt/change-requests/stats');
    return response.data;
  },
  // Quality Rules
  getQualityRules: async (params?: { table_name?: string; severity?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/quality-rules', { params });
    return response.data;
  },
  createQualityRule: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/quality-rules', data);
    return response.data;
  },
  updateQualityRule: async (ruleId: number, data: any) => {
    const response = await apiClient.put(`/metadata-mgmt/quality-rules/${ruleId}`, data);
    return response.data;
  },
  deleteQualityRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/metadata-mgmt/quality-rules/${ruleId}`);
    return response.data;
  },
  checkQualityRule: async (ruleId: number) => {
    const response = await apiClient.post(`/metadata-mgmt/quality-rules/${ruleId}/check`);
    return response.data;
  },
  checkAllQualityRules: async () => {
    const response = await apiClient.post('/metadata-mgmt/quality-rules/check-all');
    return response.data;
  },
  // Quality Dashboard & History & Alerts
  getQualityDashboard: async () => {
    const response = await apiClient.get('/metadata-mgmt/quality-dashboard');
    return response.data;
  },
  getQualityCheckRuns: async (params?: { date_from?: string; date_to?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/quality-check-history/runs', { params });
    return response.data;
  },
  getQualityCheckHistory: async (params?: { run_id?: string; date_from?: string; date_to?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/quality-check-history', { params });
    return response.data;
  },
  getQualityAlerts: async () => {
    const response = await apiClient.get('/metadata-mgmt/quality-alerts');
    return response.data;
  },
  createQualityAlert: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/quality-alerts', data);
    return response.data;
  },
  updateQualityAlert: async (alertId: number, data: any) => {
    const response = await apiClient.put(`/metadata-mgmt/quality-alerts/${alertId}`, data);
    return response.data;
  },
  deleteQualityAlert: async (alertId: number) => {
    const response = await apiClient.delete(`/metadata-mgmt/quality-alerts/${alertId}`);
    return response.data;
  },
  // Source Mappings
  getSourceMappings: async (params?: { source_system?: string; target_table?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/source-mappings', { params });
    return response.data;
  },
  createSourceMapping: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/source-mappings', data);
    return response.data;
  },
  deleteSourceMapping: async (id: number) => {
    const response = await apiClient.delete(`/metadata-mgmt/source-mappings/${id}`);
    return response.data;
  },
  getSourceMappingOverview: async () => {
    const response = await apiClient.get('/metadata-mgmt/source-mappings/overview');
    return response.data;
  },
  // Compliance
  getComplianceRules: async (params?: { category?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/compliance-rules', { params });
    return response.data;
  },
  createComplianceRule: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/compliance-rules', data);
    return response.data;
  },
  deleteComplianceRule: async (id: number) => {
    const response = await apiClient.delete(`/metadata-mgmt/compliance-rules/${id}`);
    return response.data;
  },
  getComplianceDashboard: async () => {
    const response = await apiClient.get('/metadata-mgmt/compliance-dashboard');
    return response.data;
  },
  // Pipeline Biz
  getPipelineBiz: async (params?: { job_type?: string; last_status?: string }) => {
    const response = await apiClient.get('/metadata-mgmt/pipeline-biz', { params });
    return response.data;
  },
  createPipelineBiz: async (data: any) => {
    const response = await apiClient.post('/metadata-mgmt/pipeline-biz', data);
    return response.data;
  },
  deletePipelineBiz: async (id: number) => {
    const response = await apiClient.delete(`/metadata-mgmt/pipeline-biz/${id}`);
    return response.data;
  },
  getPipelineBizDashboard: async () => {
    const response = await apiClient.get('/metadata-mgmt/pipeline-biz/dashboard');
    return response.data;
  },
  // Overview
  getOverview: async () => {
    const response = await apiClient.get('/metadata-mgmt/overview');
    return response.data;
  },
};
