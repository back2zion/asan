/**
 * API 공통 유틸리티 및 타입 정의
 * 시큐어 코딩 적용: XSS 방지, 입력 검증, 에러 핸들링
 */

import axios, { AxiosError, AxiosInstance, AxiosResponse } from 'axios';
import DOMPurify from 'dompurify';

// API 기본 설정
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';
const API_TIMEOUT = 30000;

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
    // JWT 토큰 추가
    const token = localStorage.getItem('idp_access_token');
    if (token) {
      config.headers['Authorization'] = `Bearer ${token}`;
    }

    // CSRF 토큰 추가 (쿠키에서 가져오기)
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

// 응답 인터셉터: 에러 처리
apiClient.interceptors.response.use(
  (response: AxiosResponse) => response,
  (error: AxiosError) => {
    if (error.response) {
      const status = error.response.status;

      if (status === 401) {
        // 인증 만료 시 로그인 페이지로 리다이렉트
        window.location.href = '/login';
      } else if (status === 403) {
        console.error('접근 권한이 없습니다.');
      } else if (status >= 500) {
        console.error('서버 오류가 발생했습니다.');
      }
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
