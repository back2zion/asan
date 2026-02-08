/**
 * Data Catalog API (DGR-003)
 */

import { apiClient } from './apiUtils';

export const dataCatalogApi = {
  // Entries
  getEntries: async (params?: { entry_type?: string; domain?: string; search?: string }) => {
    const response = await apiClient.get('/data-catalog/entries', { params });
    return response.data;
  },
  getEntry: async (entryId: number) => {
    const response = await apiClient.get(`/data-catalog/entries/${entryId}`);
    return response.data;
  },
  createEntry: async (data: any) => {
    const response = await apiClient.post('/data-catalog/entries', data);
    return response.data;
  },
  updateEntry: async (entryId: number, data: any) => {
    const response = await apiClient.put(`/data-catalog/entries/${entryId}`, data);
    return response.data;
  },
  deleteEntry: async (entryId: number) => {
    const response = await apiClient.delete(`/data-catalog/entries/${entryId}`);
    return response.data;
  },
  getEntryDetail: async (entryId: number) => {
    const response = await apiClient.get(`/data-catalog/entries/${entryId}/detail`);
    return response.data;
  },
  // Ownership
  getOwnerships: async (params?: { entry_id?: number; owner_type?: string }) => {
    const response = await apiClient.get('/data-catalog/ownerships', { params });
    return response.data;
  },
  createOwnership: async (data: any) => {
    const response = await apiClient.post('/data-catalog/ownerships', data);
    return response.data;
  },
  deleteOwnership: async (ownershipId: number) => {
    const response = await apiClient.delete(`/data-catalog/ownerships/${ownershipId}`);
    return response.data;
  },
  getOwnershipMatrix: async () => {
    const response = await apiClient.get('/data-catalog/ownerships/matrix');
    return response.data;
  },
  // Relations
  getRelations: async (params?: { entry_id?: number; relation_type?: string }) => {
    const response = await apiClient.get('/data-catalog/relations', { params });
    return response.data;
  },
  createRelation: async (data: any) => {
    const response = await apiClient.post('/data-catalog/relations', data);
    return response.data;
  },
  deleteRelation: async (relationId: number) => {
    const response = await apiClient.delete(`/data-catalog/relations/${relationId}`);
    return response.data;
  },
  getRelationGraph: async () => {
    const response = await apiClient.get('/data-catalog/relations/graph');
    return response.data;
  },
  // Work History
  getWorkHistories: async (params?: { entry_id?: number; work_type?: string }) => {
    const response = await apiClient.get('/data-catalog/work-histories', { params });
    return response.data;
  },
  createWorkHistory: async (data: any) => {
    const response = await apiClient.post('/data-catalog/work-histories', data);
    return response.data;
  },
  deleteWorkHistory: async (historyId: number) => {
    const response = await apiClient.delete(`/data-catalog/work-histories/${historyId}`);
    return response.data;
  },
  // Resources
  getResources: async (params?: { entry_id?: number; resource_type?: string }) => {
    const response = await apiClient.get('/data-catalog/resources', { params });
    return response.data;
  },
  createResource: async (data: any) => {
    const response = await apiClient.post('/data-catalog/resources', data);
    return response.data;
  },
  deleteResource: async (resourceId: number) => {
    const response = await apiClient.delete(`/data-catalog/resources/${resourceId}`);
    return response.data;
  },
  getResourceDetail: async (resourceId: number) => {
    const response = await apiClient.get(`/data-catalog/resources/${resourceId}`);
    return response.data;
  },
  // Sync
  getSyncLogs: async (params?: { entry_id?: number; sync_type?: string }) => {
    const response = await apiClient.get('/data-catalog/sync-logs', { params });
    return response.data;
  },
  detectChanges: async (entryId?: number) => {
    const response = await apiClient.post('/data-catalog/sync/detect-changes', null, {
      params: entryId ? { entry_id: entryId } : undefined,
    });
    return response.data;
  },
  applySync: async (logId: number) => {
    const response = await apiClient.put(`/data-catalog/sync-logs/${logId}/apply`);
    return response.data;
  },
  // Overview
  getOverview: async () => {
    const response = await apiClient.get('/data-catalog/overview');
    return response.data;
  },
};
