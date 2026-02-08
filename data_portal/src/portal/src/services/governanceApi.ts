/**
 * Governance API
 */

import { apiClient, sanitizeText } from './apiUtils';

export const governanceApi = {
  getSensitivity: async () => {
    const response = await apiClient.get('/governance/sensitivity');
    return response.data;
  },

  updateSensitivity: async (data: { table_name: string; column_name: string; level: string; reason?: string }) => {
    const response = await apiClient.put('/governance/sensitivity', data);
    return response.data;
  },

  getRoles: async () => {
    const response = await apiClient.get('/governance/roles');
    return response.data;
  },

  createRole: async (data: { role_name: string; description?: string; access_scope: string; allowed_tables: string[]; security_level: string }) => {
    const response = await apiClient.post('/governance/roles', data);
    return response.data;
  },

  updateRole: async (roleId: number, data: { role_name: string; description?: string; access_scope: string; allowed_tables: string[]; security_level: string }) => {
    const response = await apiClient.put(`/governance/roles/${roleId}`, data);
    return response.data;
  },

  deleteRole: async (roleId: number) => {
    const response = await apiClient.delete(`/governance/roles/${roleId}`);
    return response.data;
  },

  getDeidentification: async () => {
    const response = await apiClient.get('/governance/deidentification');
    return response.data;
  },

  getDeidentRules: async () => {
    const response = await apiClient.get('/governance/deident-rules');
    return response.data;
  },

  createDeidentRule: async (data: { target_column: string; method: string; pattern?: string; enabled: boolean }) => {
    const response = await apiClient.post('/governance/deident-rules', data);
    return response.data;
  },

  updateDeidentRule: async (ruleId: number, data: { target_column: string; method: string; pattern?: string; enabled: boolean }) => {
    const response = await apiClient.put(`/governance/deident-rules/${ruleId}`, data);
    return response.data;
  },

  deleteDeidentRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/governance/deident-rules/${ruleId}`);
    return response.data;
  },

  getMetadataOverrides: async () => {
    const response = await apiClient.get('/governance/metadata');
    return response.data;
  },

  updateTableMetadata: async (data: { table_name: string; business_name?: string; description?: string; domain?: string; tags?: string[]; owner?: string }) => {
    const response = await apiClient.put('/governance/metadata/table', data);
    return response.data;
  },

  updateColumnMetadata: async (data: { table_name: string; column_name: string; business_name?: string; description?: string }) => {
    const response = await apiClient.put('/governance/metadata/column', data);
    return response.data;
  },

  getLineageDetail: async (nodeId: string) => {
    const response = await apiClient.get(`/governance/lineage-detail/${encodeURIComponent(nodeId)}`);
    return response.data;
  },

  // 표준 용어 사전 (DGR-001)
  getStandardTerms: async (params?: { domain?: string; search?: string }) => {
    const response = await apiClient.get('/governance/standard-terms', { params });
    return response.data;
  },
  createStandardTerm: async (data: { standard_name: string; definition: string; domain: string; synonyms?: string[]; abbreviation?: string; english_name?: string }) => {
    const response = await apiClient.post('/governance/standard-terms', data);
    return response.data;
  },
  updateStandardTerm: async (termId: number, data: { standard_name: string; definition: string; domain: string; synonyms?: string[]; abbreviation?: string; english_name?: string }) => {
    const response = await apiClient.put(`/governance/standard-terms/${termId}`, data);
    return response.data;
  },
  deleteStandardTerm: async (termId: number) => {
    const response = await apiClient.delete(`/governance/standard-terms/${termId}`);
    return response.data;
  },

  // 표준 지표 관리 (DGR-001)
  getStandardIndicators: async (params?: { category?: string }) => {
    const response = await apiClient.get('/governance/standard-indicators', { params });
    return response.data;
  },
  createStandardIndicator: async (data: { name: string; definition: string; formula: string; unit: string; frequency: string; owner_dept: string; data_source: string; category?: string }) => {
    const response = await apiClient.post('/governance/standard-indicators', data);
    return response.data;
  },
  updateStandardIndicator: async (indicatorId: number, data: { name: string; definition: string; formula: string; unit: string; frequency: string; owner_dept: string; data_source: string; category?: string }) => {
    const response = await apiClient.put(`/governance/standard-indicators/${indicatorId}`, data);
    return response.data;
  },
  deleteStandardIndicator: async (indicatorId: number) => {
    const response = await apiClient.delete(`/governance/standard-indicators/${indicatorId}`);
    return response.data;
  },

  // DGR-006: Pipeline 비식별 + 처리 로그 + 재식별
  getDeidentPipeline: async () => {
    const response = await apiClient.get('/governance/deident-pipeline');
    return response.data;
  },
  getDeidentProcessingLog: async (params?: { process_type?: string; limit?: number }) => {
    const response = await apiClient.get('/governance/deident-processing-log', { params });
    return response.data;
  },
  getDeidentMonitoring: async () => {
    const response = await apiClient.get('/governance/deident-monitoring');
    return response.data;
  },
  getReidentRequests: async () => {
    const response = await apiClient.get('/governance/reident-requests');
    return response.data;
  },
  createReidentRequest: async (data: { requester: string; purpose: string; target_tables: string[]; target_columns?: string[]; expires_at?: string }) => {
    const response = await apiClient.post('/governance/reident-requests', data);
    return response.data;
  },
  approveReidentRequest: async (requestId: number, approver?: string) => {
    const response = await apiClient.put(`/governance/reident-requests/${requestId}/approve`, null, { params: { approver: approver || '관리자' } });
    return response.data;
  },
  rejectReidentRequest: async (requestId: number, approver?: string, reason?: string) => {
    const response = await apiClient.put(`/governance/reident-requests/${requestId}/reject`, null, { params: { approver: approver || '관리자', reason: reason || '' } });
    return response.data;
  },

  // AI 자동 태그 추천 (AAR-001)
  suggestTags: async (tableName: string) => {
    const response = await apiClient.post('/governance/suggest-tags', { table_name: sanitizeText(tableName) });
    return response.data;
  },

  // 활용 기반 리니지 (AAR-001)
  getUsageLineage: async (tableName?: string) => {
    const url = tableName
      ? `/governance/usage-lineage/${encodeURIComponent(tableName)}`
      : '/governance/usage-lineage';
    const response = await apiClient.get(url);
    return response.data;
  },

  // DGR-009: 지능형 거버넌스
  getSmartOptimization: async () => {
    const response = await apiClient.get('/governance/smart-optimization');
    return response.data;
  },
  getSmartMetadata: async () => {
    const response = await apiClient.get('/governance/smart-metadata');
    return response.data;
  },
  getSmartSummary: async () => {
    const response = await apiClient.get('/governance/smart-summary');
    return response.data;
  },
};
