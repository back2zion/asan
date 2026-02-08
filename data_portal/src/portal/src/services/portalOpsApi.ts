/**
 * DPR-005: 포털 운영 관리 API 클라이언트
 */
import { apiClient } from './apiUtils';

export const portalOpsApi = {
  // ── Monitoring ──
  createAccessLog: async (data: { user_id: string; action: string; resource?: string; ip_address?: string; duration_ms?: number }) => {
    const response = await apiClient.post('/portal-ops/logs/access', data);
    return response.data;
  },
  getAccessLogs: async (params?: { user_id?: string; action?: string; limit?: number }) => {
    const response = await apiClient.get('/portal-ops/logs/access', { params });
    return response.data;
  },
  getAccessLogStats: async () => {
    const response = await apiClient.get('/portal-ops/logs/access/stats');
    return response.data;
  },
  getSystemResources: async () => {
    const response = await apiClient.get('/portal-ops/system/resources');
    return response.data;
  },
  getServiceStatus: async () => {
    const response = await apiClient.get('/portal-ops/system/services');
    return response.data;
  },

  // ── Alerts ──
  createAlert: async (data: { severity: string; source: string; message: string }) => {
    const response = await apiClient.post('/portal-ops/alerts', data);
    return response.data;
  },
  getAlerts: async (params?: { status?: string; severity?: string }) => {
    const response = await apiClient.get('/portal-ops/alerts', { params });
    return response.data;
  },
  updateAlert: async (alertId: number, data: { status: string; resolved_by?: string }) => {
    const response = await apiClient.put(`/portal-ops/alerts/${alertId}`, data);
    return response.data;
  },

  // ── Data Quality ──
  getQualityRules: async () => {
    const response = await apiClient.get('/portal-ops/quality/rules');
    return response.data;
  },
  runQualityCheck: async () => {
    const response = await apiClient.post('/portal-ops/quality/check');
    return response.data;
  },
  getQualitySummary: async () => {
    const response = await apiClient.get('/portal-ops/quality/summary');
    return response.data;
  },

  // ── Announcements ──
  createAnnouncement: async (data: { title: string; content: string; ann_type?: string; priority?: string; is_pinned?: boolean }) => {
    const response = await apiClient.post('/portal-ops/announcements', data);
    return response.data;
  },
  getAnnouncements: async (params?: { status?: string; ann_type?: string; limit?: number }) => {
    const response = await apiClient.get('/portal-ops/announcements', { params });
    return response.data;
  },
  getActiveAnnouncements: async () => {
    const response = await apiClient.get('/portal-ops/announcements/active');
    return response.data;
  },
  getAnnouncement: async (annId: number) => {
    const response = await apiClient.get(`/portal-ops/announcements/${annId}`);
    return response.data;
  },
  updateAnnouncement: async (annId: number, data: any) => {
    const response = await apiClient.put(`/portal-ops/announcements/${annId}`, data);
    return response.data;
  },
  deleteAnnouncement: async (annId: number) => {
    const response = await apiClient.delete(`/portal-ops/announcements/${annId}`);
    return response.data;
  },

  // ── Menu Management ──
  getMenuItems: async (role?: string) => {
    const response = await apiClient.get('/portal-ops/menus', { params: role ? { role } : {} });
    return response.data;
  },
  createMenuItem: async (data: { menu_key: string; label: string; icon?: string; path?: string; parent_key?: string; sort_order?: number; roles?: string[] }) => {
    const response = await apiClient.post('/portal-ops/menus', data);
    return response.data;
  },
  updateMenuItem: async (menuKey: string, data: any) => {
    const response = await apiClient.put(`/portal-ops/menus/${menuKey}`, data);
    return response.data;
  },
  deleteMenuItem: async (menuKey: string) => {
    const response = await apiClient.delete(`/portal-ops/menus/${menuKey}`);
    return response.data;
  },

  // ── Settings ──
  getSettings: async () => {
    const response = await apiClient.get('/portal-ops/settings');
    return response.data;
  },
  updateSetting: async (key: string, value: any, description?: string) => {
    const response = await apiClient.put(`/portal-ops/settings/${key}`, { value, description });
    return response.data;
  },

  // ── Overview ──
  getOverview: async () => {
    const response = await apiClient.get('/portal-ops/overview');
    return response.data;
  },
};
