/**
 * Security Management API (DGR-005)
 */

import { apiClient } from './apiUtils';

export const securityMgmtApi = {
  // Security Policies
  getPolicies: async (params?: { target_type?: string; security_level?: string; enabled?: boolean }) => {
    const response = await apiClient.get('/security-mgmt/policies', { params });
    return response.data;
  },
  createPolicy: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/policies', data);
    return response.data;
  },
  updatePolicy: async (policyId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/policies/${policyId}`, data);
    return response.data;
  },
  deletePolicy: async (policyId: number) => {
    const response = await apiClient.delete(`/security-mgmt/policies/${policyId}`);
    return response.data;
  },
  getPolicyOverview: async () => {
    const response = await apiClient.get('/security-mgmt/policies/overview');
    return response.data;
  },

  // Term-based Security Rules
  getTermRules: async () => {
    const response = await apiClient.get('/security-mgmt/term-rules');
    return response.data;
  },
  createTermRule: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/term-rules', data);
    return response.data;
  },
  updateTermRule: async (ruleId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/term-rules/${ruleId}`, data);
    return response.data;
  },
  deleteTermRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/security-mgmt/term-rules/${ruleId}`);
    return response.data;
  },
  propagateTermRules: async () => {
    const response = await apiClient.post('/security-mgmt/term-rules/propagate');
    return response.data;
  },

  // Biz Security Meta
  getBizSecurity: async (params?: { table_name?: string; deident_target?: boolean }) => {
    const response = await apiClient.get('/security-mgmt/biz-security', { params });
    return response.data;
  },
  createBizSecurity: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/biz-security', data);
    return response.data;
  },
  updateBizSecurity: async (metaId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/biz-security/${metaId}`, data);
    return response.data;
  },
  deleteBizSecurity: async (metaId: number) => {
    const response = await apiClient.delete(`/security-mgmt/biz-security/${metaId}`);
    return response.data;
  },

  // Reid Requests
  getReidRequests: async (params?: { status?: string; department?: string }) => {
    const response = await apiClient.get('/security-mgmt/reid-requests', { params });
    return response.data;
  },
  getReidRequest: async (requestId: number) => {
    const response = await apiClient.get(`/security-mgmt/reid-requests/${requestId}`);
    return response.data;
  },
  createReidRequest: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/reid-requests', data);
    return response.data;
  },
  reviewReidRequest: async (requestId: number, reviewer?: string) => {
    const response = await apiClient.put(`/security-mgmt/reid-requests/${requestId}/review`, null, {
      params: reviewer ? { reviewer } : undefined,
    });
    return response.data;
  },
  approveReidRequest: async (requestId: number, scope: any, reviewerComment?: string) => {
    const response = await apiClient.put(`/security-mgmt/reid-requests/${requestId}/approve`, scope, {
      params: reviewerComment ? { reviewer_comment: reviewerComment } : undefined,
    });
    return response.data;
  },
  rejectReidRequest: async (requestId: number, reviewerComment?: string) => {
    const response = await apiClient.put(`/security-mgmt/reid-requests/${requestId}/reject`, null, {
      params: { reviewer_comment: reviewerComment || '반려' },
    });
    return response.data;
  },
  revokeReidRequest: async (requestId: number) => {
    const response = await apiClient.put(`/security-mgmt/reid-requests/${requestId}/revoke`);
    return response.data;
  },
  getReidRequestStats: async () => {
    const response = await apiClient.get('/security-mgmt/reid-requests/stats');
    return response.data;
  },

  // User Attributes
  getUserAttributes: async (params?: { department?: string; rank?: string; active?: boolean }) => {
    const response = await apiClient.get('/security-mgmt/user-attributes', { params });
    return response.data;
  },
  createUserAttribute: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/user-attributes', data);
    return response.data;
  },
  updateUserAttribute: async (attrId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/user-attributes/${attrId}`, data);
    return response.data;
  },
  deleteUserAttribute: async (attrId: number) => {
    const response = await apiClient.delete(`/security-mgmt/user-attributes/${attrId}`);
    return response.data;
  },

  // Dynamic Policies
  getDynamicPolicies: async (params?: { policy_type?: string; enabled?: boolean }) => {
    const response = await apiClient.get('/security-mgmt/dynamic-policies', { params });
    return response.data;
  },
  createDynamicPolicy: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/dynamic-policies', data);
    return response.data;
  },
  updateDynamicPolicy: async (dpId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/dynamic-policies/${dpId}`, data);
    return response.data;
  },
  deleteDynamicPolicy: async (dpId: number) => {
    const response = await apiClient.delete(`/security-mgmt/dynamic-policies/${dpId}`);
    return response.data;
  },
  evaluateDynamicPolicy: async (dpId: number, userId: string) => {
    const response = await apiClient.post(`/security-mgmt/dynamic-policies/${dpId}/evaluate?user_id=${userId}`);
    return response.data;
  },

  // Masking Rules
  getMaskingRules: async (params?: { masking_level?: string; target_table?: string }) => {
    const response = await apiClient.get('/security-mgmt/masking-rules', { params });
    return response.data;
  },
  createMaskingRule: async (data: any) => {
    const response = await apiClient.post('/security-mgmt/masking-rules', data);
    return response.data;
  },
  updateMaskingRule: async (ruleId: number, data: any) => {
    const response = await apiClient.put(`/security-mgmt/masking-rules/${ruleId}`, data);
    return response.data;
  },
  deleteMaskingRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/security-mgmt/masking-rules/${ruleId}`);
    return response.data;
  },

  // Access Logs
  getAccessLogs: async (params?: { user_id?: string; action?: string; result?: string; limit?: number }) => {
    const response = await apiClient.get('/security-mgmt/access-logs', { params });
    return response.data;
  },
  getAccessLogStats: async () => {
    const response = await apiClient.get('/security-mgmt/access-logs/stats');
    return response.data;
  },

  // Overview
  getOverview: async () => {
    const response = await apiClient.get('/security-mgmt/overview');
    return response.data;
  },
};
