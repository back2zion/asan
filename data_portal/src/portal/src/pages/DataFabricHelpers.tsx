/**
 * DataFabric 헬퍼 — 인터페이스, 상수, 유틸리티 함수
 */

/* ── Interfaces ── */

export interface SourceStats { [key: string]: string | number }

export interface DataSource {
  id: string;
  name: string;
  type: string;
  host: string;
  port: number;
  enabled: boolean;
  check_method: string;
  check_url: string;
  description: string;
  config: Record<string, any>;
  status: 'healthy' | 'degraded' | 'error' | 'disabled';
  latency_ms: number;
  stats: SourceStats;
}

export interface Flow {
  id: number;
  from: string;
  to: string;
  label: string;
  enabled: boolean;
  description: string;
}

export interface FabricData {
  sources: DataSource[];
  flows: Flow[];
  summary: { total: number; healthy: number; degraded: number; error: number };
  quality_data: { domain: string; score: number; issues: number }[];
  source_count: number;
}

/* ── Constants ── */

export const STATUS_CONFIG: Record<string, { color: string; text: string; badge: any }> = {
  healthy:  { color: '#52c41a', text: '정상', badge: 'success' },
  degraded: { color: '#faad14', text: '경고', badge: 'warning' },
  error:    { color: '#ff4d4f', text: '장애', badge: 'error' },
  disabled: { color: '#d9d9d9', text: '비활성', badge: 'default' },
};

export const API = '/api/v1/portal-ops';

/* ── Utility Functions ── */

export const getScoreColor = (score: number) => {
  if (score > 90) return '#52c41a';
  if (score > 80) return '#ff6600';
  return '#ff4d4f';
};
