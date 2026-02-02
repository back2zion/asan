/**
 * 서울아산병원 IDP API 클라이언트
 * 시큐어 코딩 적용: XSS 방지, 입력 검증, 에러 핸들링
 */

import axios, { AxiosError, AxiosInstance, AxiosResponse } from 'axios';
import DOMPurify from 'dompurify';

// API 기본 설정
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';
const API_TIMEOUT = 30000;

// Axios 인스턴스 생성
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: API_TIMEOUT,
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true,
});

// 요청 인터셉터: 입력 데이터 검증
apiClient.interceptors.request.use(
  (config) => {
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

// ==================== AI Assistant API ====================

export const chatApi = {
  // 대화 전송
  sendMessage: async (request: ChatRequest): Promise<ChatResponse> => {
    const sanitizedRequest = {
      ...request,
      message: sanitizeText(request.message),
    };
    const response = await apiClient.post<ChatResponse>('/chat', sanitizedRequest);
    return response.data;
  },

  // 세션 목록 조회
  getSessions: async (userId: string) => {
    const response = await apiClient.get('/chat/sessions', {
      params: { user_id: sanitizeText(userId) },
    });
    return response.data;
  },

  // 세션 상세 조회
  getSession: async (sessionId: string) => {
    const response = await apiClient.get(`/chat/sessions/${encodeURIComponent(sessionId)}`);
    return response.data;
  },

  // 타임라인 조회
  getTimeline: async (sessionId: string) => {
    const response = await apiClient.get(`/chat/sessions/${encodeURIComponent(sessionId)}/timeline`);
    return response.data;
  },

  // 상태 복원
  restoreState: async (sessionId: string, messageId: string) => {
    const response = await apiClient.post(
      `/chat/sessions/${encodeURIComponent(sessionId)}/restore/${encodeURIComponent(messageId)}`
    );
    return response.data;
  },
};

// ==================== Semantic Layer API ====================

export const semanticApi = {
  // 통합 검색
  search: async (query: string, domain?: string, limit = 20): Promise<SearchResult> => {
    const response = await apiClient.get<SearchResult>('/semantic/search', {
      params: {
        q: sanitizeText(query),
        domain: domain ? sanitizeText(domain) : undefined,
        limit,
      },
    });
    return response.data;
  },

  // Faceted Search
  facetedSearch: async (params: FacetedSearchParams) => {
    const response = await apiClient.get('/semantic/faceted-search', {
      params: {
        q: params.q ? sanitizeText(params.q) : undefined,
        domains: params.domains?.join(','),
        tags: params.tags?.join(','),
        sensitivity: params.sensitivity?.join(','),
        limit: params.limit || 50,
        offset: params.offset || 0,
      },
    });
    return response.data;
  },

  // 물리명 → 비즈니스명 변환
  translatePhysicalToBusiness: async (physicalNames: string[]) => {
    const response = await apiClient.post('/semantic/translate/physical-to-business', {
      physical_names: physicalNames.map(sanitizeText),
    });
    return response.data;
  },

  // 비즈니스명 → 물리명 변환
  translateBusinessToPhysical: async (query: string) => {
    const response = await apiClient.get('/semantic/translate/business-to-physical', {
      params: { q: sanitizeText(query) },
    });
    return response.data;
  },

  // 테이블 메타데이터 조회
  getTableMetadata: async (physicalName: string): Promise<{ data: TableInfo }> => {
    const response = await apiClient.get(`/semantic/table/${encodeURIComponent(physicalName)}`);
    return response.data;
  },

  // 도메인 목록
  getDomains: async () => {
    const response = await apiClient.get('/semantic/domains');
    return response.data;
  },

  // 도메인별 테이블 목록
  getTablesByDomain: async (domain: string) => {
    const response = await apiClient.get(`/semantic/domains/${encodeURIComponent(domain)}/tables`);
    return response.data;
  },

  // 데이터 계보
  getLineage: async (tableName: string) => {
    const response = await apiClient.get(`/semantic/lineage/${encodeURIComponent(tableName)}`);
    return response.data;
  },

  // SQL 컨텍스트
  getSqlContext: async (query: string) => {
    const response = await apiClient.get('/semantic/sql-context', {
      params: { q: sanitizeText(query) },
    });
    return response.data;
  },

  // 인기 데이터
  getPopularData: async (topN = 10) => {
    const response = await apiClient.get('/semantic/popular', {
      params: { top_n: topN },
    });
    return response.data;
  },

  // 연관 데이터
  getRelatedData: async (tableName: string, limit = 5) => {
    const response = await apiClient.get(`/semantic/related/${encodeURIComponent(tableName)}`, {
      params: { limit },
    });
    return response.data;
  },

  // 태그 목록
  getTags: async () => {
    const response = await apiClient.get('/semantic/tags');
    return response.data;
  },

  // 활용 기록
  recordUsage: async (
    userId: string,
    actionType: string,
    targetType: string,
    targetName: string,
    metadata?: Record<string, unknown>
  ) => {
    const response = await apiClient.post('/semantic/usage/record', {
      user_id: sanitizeText(userId),
      action_type: sanitizeText(actionType),
      target_type: sanitizeText(targetType),
      target_name: sanitizeText(targetName),
      metadata,
    });
    return response.data;
  },

  // 활용 통계
  getUsageStatistics: async (targetType?: string, targetName?: string, topN = 10) => {
    const response = await apiClient.get('/semantic/usage/statistics', {
      params: {
        target_type: targetType ? sanitizeText(targetType) : undefined,
        target_name: targetName ? sanitizeText(targetName) : undefined,
        top_n: topN,
      },
    });
    return response.data;
  },
};

// ==================== Vector DB API ====================

export const vectorApi = {
  // 벡터 검색
  search: async (query: string, collection = 'default', topK = 10) => {
    const response = await apiClient.post('/vector/search', {
      query: sanitizeText(query),
      collection: sanitizeText(collection),
      top_k: topK,
    });
    return response.data;
  },

  // 컬렉션 목록
  getCollections: async () => {
    const response = await apiClient.get('/vector/collections');
    return response.data;
  },
};

// ==================== MCP Server API ====================

export const mcpApi = {
  // 매니페스트 조회
  getManifest: async () => {
    const response = await apiClient.get('/mcp/manifest');
    return response.data;
  },

  // 도구 목록
  getTools: async (category?: string) => {
    const response = await apiClient.get('/mcp/tools', {
      params: category ? { category: sanitizeText(category) } : undefined,
    });
    return response.data;
  },

  // 도구 호출
  callTool: async (toolName: string, args: Record<string, unknown> = {}) => {
    const response = await apiClient.post(`/mcp/tools/${encodeURIComponent(toolName)}`, args);
    return response.data;
  },
};

// ==================== Health Check ====================

export const healthApi = {
  check: async () => {
    const response = await apiClient.get('/health');
    return response.data;
  },
};

export default {
  chat: chatApi,
  semantic: semanticApi,
  vector: vectorApi,
  mcp: mcpApi,
  health: healthApi,
  sanitizeText,
  sanitizeHtml,
};
