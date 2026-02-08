/**
 * 서울아산병원 IDP API 클라이언트 - Re-export Hub
 *
 * 모든 API 모듈을 re-export하여 기존 import 경로 호환성 유지:
 *   import { chatApi, semanticApi, sanitizeText } from '../services/api';
 *
 * 모듈 구조:
 *   apiUtils.ts       - 공통 유틸 (apiClient, sanitize, 타입)
 *   chatApi.ts         - AI Assistant (Chat) API
 *   semanticApi.ts     - Semantic Layer API
 *   governanceApi.ts   - Governance API
 *   etlApi.ts          - ETL Jobs + Schema Monitor API
 *   cdcApi.ts          - CDC + Data Design API
 *   dataMartOpsApi.ts  - Data Mart Ops API
 *   metadataMgmtApi.ts - Metadata Management API
 *   miscApi.ts         - Vector, MCP, Health, Migration API
 */

// 공통 유틸 및 타입
export { API_BASE_URL, apiClient, sanitizeText, sanitizeHtml } from './apiUtils';
export type { ChatRequest, ChatResponse, SearchResult, TableInfo, ColumnInfo, FacetedSearchParams } from './apiUtils';

// API 모듈
export { chatApi } from './chatApi';
export { semanticApi } from './semanticApi';
export { governanceApi } from './governanceApi';
export { etlJobsApi, schemaMonitorApi } from './etlApi';
export { cdcApi, dataDesignApi } from './cdcApi';
export { dataMartOpsApi } from './dataMartOpsApi';
export { metadataMgmtApi } from './metadataMgmtApi';
export { dataCatalogApi } from './dataCatalogApi';
export { securityMgmtApi } from './securityMgmtApi';
export { permissionMgmtApi } from './permissionMgmtApi';
export { catalogExtApi } from './catalogExtApi';
export { catalogAnalyticsApi } from './catalogAnalyticsApi';
export { catalogRecommendApi } from './catalogRecommendApi';
export { catalogComposeApi } from './catalogComposeApi';
export { cohortApi } from './cohortApi';
export { biApi } from './biApi';
export { portalOpsApi } from './portalOpsApi';
export { aiArchitectureApi } from './aiArchitectureApi';
export { vectorApi, mcpApi, healthApi, migrationApi } from './miscApi';
export { authApi } from './authApi';

// 기존 default export 호환
import { sanitizeText, sanitizeHtml } from './apiUtils';
import { chatApi } from './chatApi';
import { semanticApi } from './semanticApi';
import { governanceApi } from './governanceApi';
import { etlJobsApi, schemaMonitorApi } from './etlApi';
import { cdcApi, dataDesignApi } from './cdcApi';
import { dataMartOpsApi } from './dataMartOpsApi';
import { metadataMgmtApi } from './metadataMgmtApi';
import { dataCatalogApi } from './dataCatalogApi';
import { securityMgmtApi } from './securityMgmtApi';
import { permissionMgmtApi } from './permissionMgmtApi';
import { catalogExtApi } from './catalogExtApi';
import { catalogAnalyticsApi } from './catalogAnalyticsApi';
import { catalogRecommendApi } from './catalogRecommendApi';
import { catalogComposeApi } from './catalogComposeApi';
import { cohortApi } from './cohortApi';
import { biApi } from './biApi';
import { portalOpsApi } from './portalOpsApi';
import { aiArchitectureApi } from './aiArchitectureApi';
import { vectorApi, mcpApi, healthApi, migrationApi } from './miscApi';
import { authApi } from './authApi';

export default {
  chat: chatApi,
  semantic: semanticApi,
  vector: vectorApi,
  mcp: mcpApi,
  health: healthApi,
  governance: governanceApi,
  migration: migrationApi,
  etlJobs: etlJobsApi,
  schemaMonitor: schemaMonitorApi,
  cdc: cdcApi,
  dataDesign: dataDesignApi,
  dataMartOps: dataMartOpsApi,
  metadataMgmt: metadataMgmtApi,
  dataCatalog: dataCatalogApi,
  securityMgmt: securityMgmtApi,
  permissionMgmt: permissionMgmtApi,
  catalogExt: catalogExtApi,
  catalogAnalytics: catalogAnalyticsApi,
  catalogRecommend: catalogRecommendApi,
  catalogCompose: catalogComposeApi,
  cohort: cohortApi,
  bi: biApi,
  portalOps: portalOpsApi,
  aiArchitecture: aiArchitectureApi,
  auth: authApi,
  sanitizeText,
  sanitizeHtml,
};
