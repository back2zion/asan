/**
 * SER-002: 인증/인가 API 클라이언트
 */
import { apiClient } from './apiUtils';

const TOKEN_KEY = 'idp_access_token';
const REFRESH_KEY = 'idp_refresh_token';
const USER_KEY = 'idp_user';

export const authApi = {
  // ── 로그인/로그아웃 ──
  login: async (username: string, password: string) => {
    const response = await apiClient.post('/auth/login', { username, password });
    const data = response.data;
    localStorage.setItem(TOKEN_KEY, data.access_token);
    localStorage.setItem(REFRESH_KEY, data.refresh_token);
    localStorage.setItem(USER_KEY, JSON.stringify(data.user));
    return data;
  },

  logout: async () => {
    try {
      await apiClient.post('/auth/logout');
    } catch {
      // ignore
    }
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_KEY);
    localStorage.removeItem(USER_KEY);
  },

  refresh: async () => {
    const refreshToken = localStorage.getItem(REFRESH_KEY);
    if (!refreshToken) throw new Error('No refresh token');
    const response = await apiClient.post('/auth/refresh', { refresh_token: refreshToken });
    const data = response.data;
    localStorage.setItem(TOKEN_KEY, data.access_token);
    localStorage.setItem(REFRESH_KEY, data.refresh_token);
    localStorage.setItem(USER_KEY, JSON.stringify(data.user));
    return data;
  },

  // ── 현재 사용자 ──
  getMe: async () => {
    const response = await apiClient.get('/auth/me');
    return response.data;
  },

  changePassword: async (currentPassword: string, newPassword: string) => {
    const response = await apiClient.post('/auth/change-password', {
      current_password: currentPassword,
      new_password: newPassword,
    });
    return response.data;
  },

  // ── 토큰 관리 유틸 ──
  getToken: () => localStorage.getItem(TOKEN_KEY),
  getUser: () => {
    const raw = localStorage.getItem(USER_KEY);
    return raw ? JSON.parse(raw) : null;
  },
  isAuthenticated: () => !!localStorage.getItem(TOKEN_KEY),

  // ── 관리자 API ──
  listUsers: async (params?: { role?: string; is_active?: boolean }) => {
    const response = await apiClient.get('/auth/users', { params });
    return response.data;
  },

  createUser: async (data: {
    username: string;
    password: string;
    display_name: string;
    email?: string;
    role?: string;
    department?: string;
  }) => {
    const response = await apiClient.post('/auth/users', data);
    return response.data;
  },

  updateUser: async (userId: number, data: {
    display_name?: string;
    email?: string;
    role?: string;
    department?: string;
    is_active?: boolean;
  }) => {
    const response = await apiClient.put(`/auth/users/${userId}`, data);
    return response.data;
  },

  deactivateUser: async (userId: number) => {
    const response = await apiClient.delete(`/auth/users/${userId}`);
    return response.data;
  },

  unlockUser: async (userId: number) => {
    const response = await apiClient.post(`/auth/users/${userId}/unlock`);
    return response.data;
  },

  resetPassword: async (userId: number, newPassword: string) => {
    const response = await apiClient.post(`/auth/users/${userId}/reset-password`, {
      new_password: newPassword,
    });
    return response.data;
  },

  // ── 로그인 이력 ──
  listLoginAttempts: async (params?: { username?: string; success?: boolean; limit?: number }) => {
    const response = await apiClient.get('/auth/login-attempts', { params });
    return response.data;
  },

  // ── IP 화이트리스트 ──
  listIpWhitelist: async () => {
    const response = await apiClient.get('/auth/ip-whitelist');
    return response.data;
  },

  addIpWhitelist: async (data: { ip_pattern: string; description?: string; scope?: string }) => {
    const response = await apiClient.post('/auth/ip-whitelist', data);
    return response.data;
  },

  deleteIpWhitelist: async (whitelistId: number) => {
    const response = await apiClient.delete(`/auth/ip-whitelist/${whitelistId}`);
    return response.data;
  },

  // ── 보안 통계 ──
  getSecurityStats: async () => {
    const response = await apiClient.get('/auth/security-stats');
    return response.data;
  },
};
