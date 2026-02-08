/**
 * Semantic Layer API
 */

import { apiClient, sanitizeText } from './apiUtils';
import type { SearchResult, FacetedSearchParams, TableInfo } from './apiUtils';

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

  // 물리명 -> 비즈니스명 변환
  translatePhysicalToBusiness: async (physicalNames: string[]) => {
    const response = await apiClient.post('/semantic/translate/physical-to-business', {
      physical_names: physicalNames.map(sanitizeText),
    });
    return response.data;
  },

  // 비즈니스명 -> 물리명 변환
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

  // 데이터 계보 (DPR-002)
  getLineage: async (tableName: string) => {
    const response = await apiClient.get(`/semantic/lineage/${encodeURIComponent(tableName)}`);
    return response.data;
  },

  // 데이터 품질 (DGR-004)
  getQuality: async (tableName: string) => {
    const response = await apiClient.get(`/semantic/quality/${encodeURIComponent(tableName)}`);
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
