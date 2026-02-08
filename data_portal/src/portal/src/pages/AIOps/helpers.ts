/**
 * AIOps 공통 API 헬퍼 & 상수
 */

import { fetchPost, fetchPut, fetchDelete } from '../../services/apiUtils';

export const API_BASE = '/api/v1/ai-ops';

export const COLORS_CHART = ['#52c41a', '#faad14', '#ff4d4f', '#1890ff', '#722ed1'];

export const STATUS_COLOR: Record<string, string> = {
  healthy: 'green', unhealthy: 'orange', offline: 'red', unknown: 'default',
};

export const HALL_COLOR: Record<string, string> = {
  pass: '#52c41a', warning: '#faad14', fail: '#ff4d4f',
};

export async function fetchJSON(url: string) {
  const res = await fetch(url, { credentials: 'include' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function postJSON(url: string, body: any) {
  const res = await fetchPost(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function putJSON(url: string, body: any) {
  const res = await fetchPut(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function deleteJSON(url: string) {
  const res = await fetchDelete(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
