/**
 * Permission Management API (DGR-007)
 */

import { apiClient } from './apiUtils';

export const permissionMgmtApi = {
  // Datasets
  getDatasets: async (params?: { dataset_type?: string; classification?: string; active?: boolean }) => {
    const response = await apiClient.get('/permission-mgmt/datasets', { params });
    return response.data;
  },
  getDataset: async (datasetId: number) => {
    const response = await apiClient.get(`/permission-mgmt/datasets/${datasetId}`);
    return response.data;
  },
  createDataset: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/datasets', data);
    return response.data;
  },
  updateDataset: async (datasetId: number, data: any) => {
    const response = await apiClient.put(`/permission-mgmt/datasets/${datasetId}`, data);
    return response.data;
  },
  deleteDataset: async (datasetId: number) => {
    const response = await apiClient.delete(`/permission-mgmt/datasets/${datasetId}`);
    return response.data;
  },

  // Grants
  getGrants: async (params?: { dataset_id?: number; grantee_id?: string; grant_type?: string; active?: boolean }) => {
    const response = await apiClient.get('/permission-mgmt/grants', { params });
    return response.data;
  },
  createGrant: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/grants', data);
    return response.data;
  },
  revokeGrant: async (grantId: number, reason?: string) => {
    const response = await apiClient.put(`/permission-mgmt/grants/${grantId}/revoke`, null, {
      params: reason ? { reason } : undefined,
    });
    return response.data;
  },
  deleteGrant: async (grantId: number) => {
    const response = await apiClient.delete(`/permission-mgmt/grants/${grantId}`);
    return response.data;
  },
  getEffectiveGrants: async (userId: string) => {
    const response = await apiClient.get(`/permission-mgmt/grants/effective/${userId}`);
    return response.data;
  },

  // Role Assignments
  getRoleAssignments: async (params?: { user_id?: string; role_id?: number; assignment_type?: string }) => {
    const response = await apiClient.get('/permission-mgmt/role-assignments', { params });
    return response.data;
  },
  createRoleAssignment: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/role-assignments', data);
    return response.data;
  },
  updateRoleAssignment: async (assignmentId: number, data: any) => {
    const response = await apiClient.put(`/permission-mgmt/role-assignments/${assignmentId}`, data);
    return response.data;
  },
  deleteRoleAssignment: async (assignmentId: number) => {
    const response = await apiClient.delete(`/permission-mgmt/role-assignments/${assignmentId}`);
    return response.data;
  },

  // Role Parameters
  getRoleParams: async (roleId?: number) => {
    const response = await apiClient.get('/permission-mgmt/role-params', { params: roleId ? { role_id: roleId } : undefined });
    return response.data;
  },
  createRoleParam: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/role-params', data);
    return response.data;
  },
  updateRoleParam: async (paramDefId: number, data: any) => {
    const response = await apiClient.put(`/permission-mgmt/role-params/${paramDefId}`, data);
    return response.data;
  },
  deleteRoleParam: async (paramDefId: number) => {
    const response = await apiClient.delete(`/permission-mgmt/role-params/${paramDefId}`);
    return response.data;
  },
  validateRoleParams: async (roleId: number, parameters: any) => {
    const response = await apiClient.post(`/permission-mgmt/role-params/validate?role_id=${roleId}`, parameters);
    return response.data;
  },

  // EAM Mappings
  getEamMappings: async (syncStatus?: string) => {
    const response = await apiClient.get('/permission-mgmt/eam-mappings', { params: syncStatus ? { sync_status: syncStatus } : undefined });
    return response.data;
  },
  createEamMapping: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/eam-mappings', data);
    return response.data;
  },
  updateEamMapping: async (mappingId: number, data: any) => {
    const response = await apiClient.put(`/permission-mgmt/eam-mappings/${mappingId}`, data);
    return response.data;
  },
  deleteEamMapping: async (mappingId: number) => {
    const response = await apiClient.delete(`/permission-mgmt/eam-mappings/${mappingId}`);
    return response.data;
  },
  syncAllEam: async () => {
    const response = await apiClient.post('/permission-mgmt/eam-mappings/sync-all');
    return response.data;
  },

  // EDW Migrations
  getEdwMigrations: async (params?: { status?: string; edw_source?: string }) => {
    const response = await apiClient.get('/permission-mgmt/edw-migrations', { params });
    return response.data;
  },
  createEdwMigration: async (data: any) => {
    const response = await apiClient.post('/permission-mgmt/edw-migrations', data);
    return response.data;
  },
  executeEdwMigration: async (migrationId: number) => {
    const response = await apiClient.put(`/permission-mgmt/edw-migrations/${migrationId}/migrate`);
    return response.data;
  },
  verifyEdwMigration: async (migrationId: number) => {
    const response = await apiClient.put(`/permission-mgmt/edw-migrations/${migrationId}/verify`);
    return response.data;
  },
  skipEdwMigration: async (migrationId: number, reason?: string) => {
    const response = await apiClient.put(`/permission-mgmt/edw-migrations/${migrationId}/skip`, null, {
      params: reason ? { reason } : undefined,
    });
    return response.data;
  },
  getEdwMigrationStats: async () => {
    const response = await apiClient.get('/permission-mgmt/edw-migrations/stats');
    return response.data;
  },

  // Audit
  getAudit: async (params?: { action?: string; actor?: string; limit?: number }) => {
    const response = await apiClient.get('/permission-mgmt/audit', { params });
    return response.data;
  },
  getAuditStats: async () => {
    const response = await apiClient.get('/permission-mgmt/audit/stats');
    return response.data;
  },

  // Overview
  getOverview: async () => {
    const response = await apiClient.get('/permission-mgmt/overview');
    return response.data;
  },
};
