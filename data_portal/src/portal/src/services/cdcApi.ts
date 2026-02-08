/**
 * CDC API (DIR-005) + Data Design API (DIT-001)
 */

import { apiClient } from './apiUtils';

// ==================== CDC API (DIR-005) ====================

export const cdcApi = {
  getConnectors: async () => {
    const response = await apiClient.get('/cdc/connectors');
    return response.data;
  },
  getConnector: async (connectorId: number) => {
    const response = await apiClient.get(`/cdc/connectors/${connectorId}`);
    return response.data;
  },
  createConnector: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/cdc/connectors', data);
    return response.data;
  },
  updateConnector: async (connectorId: number, data: Record<string, unknown>) => {
    const response = await apiClient.put(`/cdc/connectors/${connectorId}`, data);
    return response.data;
  },
  deleteConnector: async (connectorId: number) => {
    const response = await apiClient.delete(`/cdc/connectors/${connectorId}`);
    return response.data;
  },
  testConnector: async (connectorId: number) => {
    const response = await apiClient.post(`/cdc/connectors/${connectorId}/test`);
    return response.data;
  },
  controlService: async (connectorId: number, action: string) => {
    const response = await apiClient.post(`/cdc/connectors/${connectorId}/service`, { action });
    return response.data;
  },
  getServiceStatus: async () => {
    const response = await apiClient.get('/cdc/service-status');
    return response.data;
  },
  getTopics: async (connectorId?: number) => {
    const response = await apiClient.get('/cdc/topics', { params: connectorId ? { connector_id: connectorId } : undefined });
    return response.data;
  },
  createTopic: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/cdc/topics', data);
    return response.data;
  },
  getOffsets: async (connectorId?: number) => {
    const response = await apiClient.get('/cdc/offsets', { params: connectorId ? { connector_id: connectorId } : undefined });
    return response.data;
  },
  resetOffset: async (connectorId: number, topicName: string) => {
    const response = await apiClient.post(`/cdc/offsets/${connectorId}/reset`, null, { params: { topic_name: topicName } });
    return response.data;
  },
  getEvents: async (params?: { connector_id?: number; event_type?: string; limit?: number }) => {
    const response = await apiClient.get('/cdc/events', { params });
    return response.data;
  },
  getEventStats: async () => {
    const response = await apiClient.get('/cdc/events/stats');
    return response.data;
  },
  getIcebergSinks: async () => {
    const response = await apiClient.get('/cdc/iceberg-sinks');
    return response.data;
  },
  createIcebergSink: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/cdc/iceberg-sinks', data);
    return response.data;
  },
};

// ==================== Data Design API (DIT-001) ====================

export const dataDesignApi = {
  getZones: async () => {
    const response = await apiClient.get('/data-design/zones');
    return response.data;
  },
  createZone: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/data-design/zones', data);
    return response.data;
  },
  getEntities: async (params?: { zone_id?: number; domain?: string }) => {
    const response = await apiClient.get('/data-design/entities', { params });
    return response.data;
  },
  getEntity: async (entityId: number) => {
    const response = await apiClient.get(`/data-design/entities/${entityId}`);
    return response.data;
  },
  createEntity: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/data-design/entities', data);
    return response.data;
  },
  getRelations: async () => {
    const response = await apiClient.get('/data-design/relations');
    return response.data;
  },
  getERDGraph: async (zoneType?: string) => {
    const response = await apiClient.get('/data-design/erd-graph', { params: zoneType ? { zone_type: zoneType } : undefined });
    return response.data;
  },
  createRelation: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/data-design/relations', data);
    return response.data;
  },
  getNamingRules: async () => {
    const response = await apiClient.get('/data-design/naming-rules');
    return response.data;
  },
  createNamingRule: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/data-design/naming-rules', data);
    return response.data;
  },
  updateNamingRule: async (ruleId: number, data: Record<string, unknown>) => {
    const response = await apiClient.put(`/data-design/naming-rules/${ruleId}`, data);
    return response.data;
  },
  deleteNamingRule: async (ruleId: number) => {
    const response = await apiClient.delete(`/data-design/naming-rules/${ruleId}`);
    return response.data;
  },
  checkNaming: async (names: string[], target: string) => {
    const response = await apiClient.post('/data-design/naming-check', { names, target });
    return response.data;
  },
  getUnstructuredMappings: async () => {
    const response = await apiClient.get('/data-design/unstructured-mappings');
    return response.data;
  },
  createUnstructuredMapping: async (data: Record<string, unknown>) => {
    const response = await apiClient.post('/data-design/unstructured-mappings', data);
    return response.data;
  },
  getOverview: async () => {
    const response = await apiClient.get('/data-design/overview');
    return response.data;
  },
};
