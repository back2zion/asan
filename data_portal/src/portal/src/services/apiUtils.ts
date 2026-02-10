/**
 * API 공통 유틸리티 및 타입 정의
 * 시큐어 코딩 적용: XSS 방지, 입력 검증, 에러 핸들링
 */

import axios, { AxiosError, AxiosInstance, AxiosResponse } from 'axios';
import DOMPurify from 'dompurify';

// API 기본 설정
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';
const API_TIMEOUT = 30000;

// ==================== GET 응답 로컬 캐시 (오프라인 폴백) ====================
const CACHE_PREFIX = 'api_cache:';
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24시간

function cacheKey(url: string, params?: Record<string, unknown>): string {
  const p = params ? '?' + new URLSearchParams(
    Object.entries(params).filter(([, v]) => v != null).map(([k, v]) => [k, String(v)])
  ).toString() : '';
  return CACHE_PREFIX + url + p;
}

function cacheSet(key: string, data: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify({ ts: Date.now(), data }));
  } catch { /* quota exceeded — ignore */ }
}

function cacheGet(key: string): { data: unknown; stale: boolean } | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const { ts, data } = JSON.parse(raw);
    return { data, stale: Date.now() - ts > CACHE_TTL };
  } catch { return null; }
}

/** 캐시 데이터 여부 확인 플래그 — 컴포넌트에서 `response.headers['x-from-cache']`로 확인 가능 */
export let lastResponseFromCache = false;

// Axios 인스턴스 생성
export const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: API_TIMEOUT,
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true,
});

// SER-002: JWT 토큰 인터셉터 — 모든 요청에 Authorization 헤더 자동 추가
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('idp_access_token');
    if (token) {
      config.headers['Authorization'] = `Bearer ${token}`;
    }

    const csrfToken = document.cookie
      .split('; ')
      .find((row) => row.startsWith('csrf_token='))
      ?.split('=')[1];

    if (csrfToken) {
      config.headers['X-CSRF-Token'] = csrfToken;
    }

    return config;
  },
  (error) => Promise.reject(error)
);

// 응답 인터셉터: 성공 시 GET 캐시 저장 + 에러 시 캐시 폴백
apiClient.interceptors.response.use(
  (response: AxiosResponse) => {
    lastResponseFromCache = false;
    // GET 성공 → 캐시 저장
    if (response.config.method?.toUpperCase() === 'GET') {
      const key = cacheKey(
        (response.config.baseURL || '') + (response.config.url || ''),
        response.config.params,
      );
      cacheSet(key, response.data);
    }
    return response;
  },
  (error: AxiosError) => {
    const config = error.config;

    // GET 실패 → 캐시 폴백 시도 (네트워크 오류 또는 5xx)
    const isGet = config?.method?.toUpperCase() === 'GET';
    const isServerDown = !error.response || (error.response.status >= 500);
    if (isGet && isServerDown && config) {
      const key = cacheKey(
        (config.baseURL || '') + (config.url || ''),
        config.params as Record<string, unknown> | undefined,
      );
      const cached = cacheGet(key);
      if (cached) {
        lastResponseFromCache = true;
        // 합성 응답 반환
        return Promise.resolve({
          data: cached.data,
          status: 200,
          statusText: 'OK (cached)',
          headers: { 'x-from-cache': '1' },
          config: config,
        } as AxiosResponse);
      }
    }

    if (error.response?.status === 401) {
      window.location.href = '/login';
    }

    return Promise.reject(error);
  }
);

// XSS 방지를 위한 텍스트 sanitize
export const sanitizeText = (text: string): string => {
  return DOMPurify.sanitize(text, { ALLOWED_TAGS: [] });
};

// HTML 허용 sanitize (마크다운 렌더링용)
export const sanitizeHtml = (html: string): string => {
  return DOMPurify.sanitize(html, {
    ALLOWED_TAGS: ['p', 'br', 'strong', 'em', 'ul', 'ol', 'li', 'code', 'pre', 'a', 'h1', 'h2', 'h3', 'h4', 'table', 'thead', 'tbody', 'tr', 'th', 'td'],
    ALLOWED_ATTR: ['href', 'target', 'rel'],
  });
};

// ==================== CSRF-aware fetch helpers ====================
// 쿠키에서 CSRF 토큰 읽기
export const getCsrfToken = (): string =>
  document.cookie.split('; ').find(r => r.startsWith('csrf_token='))?.split('=')[1] || '';

const _csrfHeaders = (): Record<string, string> => {
  const t = getCsrfToken();
  return t ? { 'X-CSRF-Token': t } : {};
};

/** POST JSON with CSRF token */
export const fetchPost = async (url: string, body?: unknown): Promise<Response> =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ..._csrfHeaders() },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

/** PUT JSON with CSRF token */
export const fetchPut = async (url: string, body?: unknown): Promise<Response> =>
  fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ..._csrfHeaders() },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

/** DELETE with CSRF token */
export const fetchDelete = async (url: string): Promise<Response> =>
  fetch(url, { method: 'DELETE', headers: { ..._csrfHeaders() } });

// ==================== API 타입 정의 ====================

export interface ChatRequest {
  message: string;
  session_id?: string;
  user_id?: string;
  context?: Record<string, unknown>;
}

export interface ChatResponse {
  session_id: string;
  message_id: string;
  assistant_message: string;
  tool_results: Record<string, unknown>[];
  suggested_actions: Record<string, unknown>[];
  processing_time_ms: number;
  // Prompt Enhancement 결과 (PRD AAR-001 Step 2)
  original_query?: string;
  enhanced_query?: string;
  enhancement_applied: boolean;
  enhancement_confidence?: number;
  thinking_process?: {
    it_meta: Array<{
      table: string;
      row_count: number;
      columns: string[];
    }>;
    biz_meta: Array<{
      table: string;
      business_name: string;
      description: string;
      key_columns: Record<string, string>;
    }>;
    generated_at: number;
  };
}

export interface SearchResult {
  success: boolean;
  data: {
    tables: TableInfo[];
    columns: ColumnInfo[];
    total: number;
  };
}

export interface TableInfo {
  physical_name: string;
  business_name: string;
  description: string;
  domain: string;
  columns: ColumnInfo[];
  usage_count: number;
  tags: string[];
  last_modified?: string;
}

export interface ColumnInfo {
  physical_name: string;
  business_name: string;
  data_type: string;
  description: string;
  is_pk: boolean;
  is_nullable: boolean;
  sensitivity: string;
}

export interface FacetedSearchParams {
  q?: string;
  domains?: string[];
  tags?: string[];
  sensitivity?: string[];
  limit?: number;
  offset?: number;
}
