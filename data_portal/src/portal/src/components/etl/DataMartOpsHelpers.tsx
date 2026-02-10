import React from 'react';
import { Tag, Space, Descriptions, Card, Timeline, Typography } from 'antd';
import {
  DatabaseOutlined, SafetyCertificateOutlined,
  SyncOutlined, ExperimentOutlined, RocketOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchDelete, fetchPost, fetchPut } from '../../services/apiUtils';

const { Text } = Typography;

export const API_BASE = '/api/v1/data-mart-ops';

export async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
export async function postJSON(url: string, body?: any) {
  const res = await fetchPost(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
export async function putJSON(url: string, body?: any) {
  const res = await fetchPut(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
export async function deleteJSON(url: string) {
  const res = await fetchDelete(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const ZONE_COLORS: Record<string, string> = {
  silver: '#805AD5', gold: '#D69E2E', mart: '#38A169',
};
export const CATEGORY_COLORS: Record<string, string> = {
  clinical: 'blue', operational: 'green', financial: 'gold', quality: 'purple', research: 'cyan',
};
export const OPT_COLORS: Record<string, string> = {
  materialized_view: 'blue', index: 'green', partition: 'orange', denormalize: 'purple', cache: 'cyan',
};
export const STAGE_COLORS: Record<string, string> = {
  ingest: '#E53E3E', cleanse: '#DD6B20', transform: '#805AD5', enrich: '#D69E2E', serve: '#38A169',
};

export function buildTreeData(node: any): any {
  if (!node) return { title: '-', key: '-' };
  return {
    title: node.name + (node.key ? ` (${node.key})` : ''),
    key: node.name + (node.key || ''),
    children: (node.children || []).map(buildTreeData),
  };
}

export function parseJsonField(v: any): any[] {
  if (!v) return [];
  return typeof v === 'string' ? JSON.parse(v) : v;
}

export const FLOW_STEPS = [
  { title: '원천 수집 (Source)', description: '원본 데이터 수집 — EHR/EMR, Lab, 병리, 영상, 수술기록. 원본 형태 그대로 보존.', status: 'process' as const },
  { title: '정제/클렌징 (Bronze→Silver)', description: '비표준 용어→표준 용어 매핑 (SNOMED CT, LOINC). 결측치 처리, 중복 제거, 타입 정규화.', status: 'process' as const },
  { title: '변환/표준화 (Silver→Gold)', description: '비정형→정형 구조화: 병리보고서 NLP 추출, 영상 DICOM 메타데이터 구조화, 수술기록 엔티티 추출. OMOP CDM 매핑.', status: 'process' as const },
  { title: '통합/강화 (Gold)', description: 'Dimension 결합, 표준 지표 산출, 집계 테이블 생성. 데이터 마트 구성.', status: 'process' as const },
  { title: '서빙/제공 (Mart)', description: '사용자 서비스: MV/인덱스 최적화, 카탈로그 등록, 접근 제어, BI/분석 도구 연동.', status: 'process' as const },
];

export const ZONE_OPTIONS = [
  { value: 'silver', label: 'Silver' },
  { value: 'gold', label: 'Gold' },
  { value: 'mart', label: 'Mart' },
];

export const CATEGORY_OPTIONS = ['clinical', 'operational', 'financial', 'quality', 'research'].map(c => ({ value: c, label: c }));

export const CATALOG_VISIBLE_OPTIONS = [
  { value: true, label: '공개' },
  { value: false, label: '비공개' },
];

export const CATALOG_SEARCH_COLUMNS = [
  { title: '지표명', dataIndex: 'logical_name', width: 120 },
  { title: '산식', dataIndex: 'formula', ellipsis: true },
  { title: '단위', dataIndex: 'unit', width: 60 },
  { title: '마트', dataIndex: 'mart_name', width: 120 },
];

export const SUGGESTION_COLUMNS = [
  { title: '마트', dataIndex: 'mart_name', width: 140 },
  { title: '유형', dataIndex: 'opt_type', width: 120, render: (v: string) => <Tag color={OPT_COLORS[v]}>{v}</Tag> },
  { title: '사유', dataIndex: 'reason', ellipsis: true },
  { title: '우선순위', dataIndex: 'priority', width: 80,
    render: (v: string) => <Tag color={v === 'high' ? 'red' : v === 'medium' ? 'orange' : 'default'}>{v}</Tag> },
];

export const SCHEMA_CHANGE_BASE_COLUMNS = [
  { title: 'ID', dataIndex: 'change_id', width: 50 },
  { title: '마트', dataIndex: 'mart_name', width: 140 },
  { title: '유형', dataIndex: 'change_type', width: 100,
    render: (v: string) => <Tag color={v === 'column_add' ? 'blue' : v === 'column_remove' ? 'red' : 'orange'}>{v}</Tag> },
  { title: '추가 컬럼', dataIndex: 'columns_added', width: 150,
    render: (v: any) => { const arr = parseJsonField(v); return arr.length ? <Space wrap>{arr.map((c: string) => <Tag key={c} color="green">{c}</Tag>)}</Space> : '-'; } },
  { title: '삭제 컬럼', dataIndex: 'columns_removed', width: 150,
    render: (v: any) => { const arr = parseJsonField(v); return arr.length ? <Space wrap>{arr.map((c: string) => <Tag key={c} color="red">{c}</Tag>)}</Space> : '-'; } },
  { title: '변경 컬럼', dataIndex: 'columns_modified', width: 150,
    render: (v: any) => { const arr = parseJsonField(v); return arr.length ? <Space wrap>{arr.map((c: any, i: number) => <Tag key={i} color="orange">{c.column}</Tag>)}</Space> : '-'; } },
  { title: '영향', dataIndex: 'impact_summary', ellipsis: true },
  { title: '상태', dataIndex: 'status', width: 90,
    render: (v: string) => <Tag color={v === 'applied' ? 'green' : v === 'pending' ? 'orange' : 'red'}>{v}</Tag> },
  { title: '일시', dataIndex: 'created_at', width: 100, render: (v: string) => dayjs(v).format('MM-DD HH:mm') },
];

export const DIMENSION_BASE_COLUMNS = [
  { title: 'ID', dataIndex: 'dim_id', width: 50 },
  { title: '물리명', dataIndex: 'dimension_name', width: 130 },
  { title: '논리명', dataIndex: 'logical_name', width: 130 },
  { title: '계층 수', dataIndex: 'hierarchy_levels', width: 80,
    render: (v: any) => parseJsonField(v).length },
  { title: '계층 레벨', dataIndex: 'hierarchy_levels', width: 250,
    render: (v: any) => <Space wrap size={[4, 4]}>{parseJsonField(v).map((l: any, i: number) => <Tag key={i} color="blue">{l.level}</Tag>)}</Space>,
  },
  { title: '속성 수', dataIndex: 'attributes', width: 80,
    render: (v: any) => parseJsonField(v).length },
  { title: '연결 마트', dataIndex: 'mart_ids', width: 100,
    render: (v: any) => <Tag>{parseJsonField(v).length}개</Tag> },
  { title: '설명', dataIndex: 'description', ellipsis: true },
];

export const METRICS_BASE_COLUMNS = [
  { title: 'ID', dataIndex: 'metric_id', width: 50 },
  { title: '물리명', dataIndex: 'metric_name', width: 140 },
  { title: '논리명', dataIndex: 'logical_name', width: 120 },
  { title: '산식', dataIndex: 'formula', ellipsis: true },
  { title: '단위', dataIndex: 'unit', width: 60 },
  { title: '분류', dataIndex: 'category', width: 90,
    render: (v: string) => <Tag color={CATEGORY_COLORS[v]}>{v}</Tag> },
  { title: '마트', dataIndex: 'mart_name', width: 120 },
  { title: '카탈로그', dataIndex: 'catalog_visible', width: 80,
    render: (v: boolean) => v ? <Tag color="blue">공개</Tag> : <Tag>비공개</Tag> },
  { title: '설명', dataIndex: 'description', ellipsis: true },
];

export const OPTIMIZATION_BASE_COLUMNS = [
  { title: 'ID', dataIndex: 'opt_id', width: 50 },
  { title: '마트', dataIndex: 'mart_name', width: 140 },
  { title: '유형', dataIndex: 'opt_type', width: 120, render: (v: string) => <Tag color={OPT_COLORS[v]}>{v}</Tag> },
  { title: '설명', dataIndex: 'description', ellipsis: true },
  { title: '상태', dataIndex: 'status', width: 80,
    render: (v: string) => <Tag color={v === 'applied' ? 'green' : v === 'proposed' ? 'purple' : 'default'}>{v}</Tag> },
  { title: '적용일', dataIndex: 'applied_at', width: 100, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
];

export const MART_CATALOG_BASE_COLUMNS = [
  { title: 'ID', dataIndex: 'mart_id', width: 50 },
  { title: '용도', dataIndex: 'purpose', width: 200, ellipsis: true },
  { title: '영역', dataIndex: 'zone', width: 80, render: (v: string) => <Tag color={ZONE_COLORS[v]}>{v}</Tag> },
  { title: '소스 테이블', dataIndex: 'source_tables', width: 200,
    render: (v: any) => <Space wrap size={[4, 4]}>{parseJsonField(v).map((t: string) => <Tag key={t} style={{ fontSize: 11 }}>{t}</Tag>)}</Space>,
  },
  { title: '행 수', dataIndex: 'row_count', width: 100, render: (v: number) => v ? v.toLocaleString() : '-' },
  { title: '담당', dataIndex: 'owner', width: 90 },
  { title: '갱신 주기', dataIndex: 'refresh_schedule', width: 110 },
  { title: '상태', dataIndex: 'status', width: 70,
    render: (v: string) => <Tag color={v === 'active' ? 'green' : 'default'}>{v}</Tag> },
  { title: '대기 변경', dataIndex: 'pending_changes', width: 80,
    render: (v: number) => v > 0 ? <Tag color="red">{v}</Tag> : <Tag color="green">없음</Tag> },
];

export const OVERVIEW_STATS = [
  { title: '활성 마트', key: 'marts', color: '#006241' },
  { title: '총 행 수', key: 'total_rows', suffix: '건' },
  { title: '대기 변경', key: 'pending_schema_changes', warnIfPositive: true },
  { title: 'Dimension', key: 'dimensions' },
  { title: '표준 지표', key: 'metrics_total' },
  { title: '카탈로그', key: 'metrics_in_catalog', color: '#1890ff' },
  { title: '적용 최적화', key: 'optimizations_applied', color: '#3f8600' },
  { title: '제안 최적화', key: 'optimizations_proposed', color: '#722ed1' },
];

export function renderMartDrawerContent(data: any) {
  return (<>
    <Descriptions column={2} size="small" bordered>
      <Descriptions.Item label="영역"><Tag color={ZONE_COLORS[data.zone]}>{data.zone}</Tag></Descriptions.Item>
      <Descriptions.Item label="상태"><Tag color={data.status === 'active' ? 'green' : 'default'}>{data.status}</Tag></Descriptions.Item>
      <Descriptions.Item label="용도" span={2}>{data.purpose}</Descriptions.Item>
      <Descriptions.Item label="담당">{data.owner}</Descriptions.Item>
      <Descriptions.Item label="행 수">{data.row_count?.toLocaleString()}</Descriptions.Item>
      <Descriptions.Item label="갱신 주기">{data.refresh_schedule}</Descriptions.Item>
      <Descriptions.Item label="보관">{data.retention_days}일</Descriptions.Item>
      <Descriptions.Item label="소스 테이블" span={2}>
        <Space wrap>{parseJsonField(data.source_tables).map((t: string) => <Tag key={t}>{t}</Tag>)}</Space>
      </Descriptions.Item>
    </Descriptions>
    {data.schema_changes?.length > 0 && (
      <Card size="small" title="스키마 변경 이력" style={{ marginTop: 16 }}>
        <Timeline items={data.schema_changes.map((c: any) => ({
          color: c.status === 'applied' ? 'green' : c.status === 'pending' ? 'orange' : 'red',
          children: <><Tag color={c.status === 'applied' ? 'green' : 'orange'}>{c.status}</Tag> {c.change_type} — {c.impact_summary} <Text type="secondary">({dayjs(c.created_at).format('YYYY-MM-DD')})</Text></>,
        }))} />
      </Card>
    )}
    {data.optimizations?.length > 0 && (
      <Card size="small" title="적용된 최적화" style={{ marginTop: 12 }}>
        {data.optimizations.map((o: any) => (
          <div key={o.opt_id} style={{ marginBottom: 8 }}>
            <Tag color={OPT_COLORS[o.opt_type]}>{o.opt_type}</Tag>
            <Tag color={o.status === 'applied' ? 'green' : 'default'}>{o.status}</Tag>
            <Text>{o.description}</Text>
          </div>
        ))}
      </Card>
    )}
    {data.metrics?.length > 0 && (
      <Card size="small" title="관련 표준 지표" style={{ marginTop: 12 }}>
        {data.metrics.map((m: any) => (
          <div key={m.metric_id} style={{ marginBottom: 6 }}>
            <Tag color={CATEGORY_COLORS[m.category]}>{m.category}</Tag>
            <Text strong>{m.logical_name}</Text> <Text type="secondary">({m.formula})</Text>
          </div>
        ))}
      </Card>
    )}
  </>);
}

const STAGE_ICONS: Record<string, React.ReactNode> = {
  ingest: <DatabaseOutlined />,
  cleanse: <SafetyCertificateOutlined />,
  transform: <SyncOutlined />,
  enrich: <ExperimentOutlined />,
  serve: <RocketOutlined />,
};

export function renderStageCard(s: any) {
  return (
    <div style={{
      minWidth: 200, padding: 16, borderRadius: 12, textAlign: 'center',
      background: `${STAGE_COLORS[s.stage_type]}15`,
      border: `2px solid ${STAGE_COLORS[s.stage_type]}`,
    }}>
      <div style={{ fontSize: 20, marginBottom: 4 }}>{STAGE_ICONS[s.stage_type]}</div>
      <div style={{ fontWeight: 700, fontSize: 14, color: STAGE_COLORS[s.stage_type] }}>{s.stage_name}</div>
      <Tag color={STAGE_COLORS[s.stage_type]} style={{ marginTop: 4 }}>{s.stage_type}</Tag>
      <div style={{ marginTop: 8, fontSize: 11, color: '#666' }}>
        <div>{s.storage_type} / {s.file_format}</div>
      </div>
    </div>
  );
}

