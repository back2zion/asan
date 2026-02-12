import React, { useState, useEffect, useRef } from 'react';
import { Tag, Typography } from 'antd';
import {
  TeamOutlined, DatabaseOutlined, TableOutlined,
} from '@ant-design/icons';
import type { EChartsOption } from 'echarts';

const { Text } = Typography;

export const API_BASE = '/api/v1/datamart';

export const CATEGORY_COLORS: Record<string, string> = {
  'Clinical Data': 'blue',
  'Health System': 'green',
  'Derived': 'purple',
  'Cost & Payer': 'orange',
  'Unstructured': 'cyan',
  'Other': 'default',
};

export const DOMAIN_COLORS: Record<string, string> = {
  Clinical: '#006241',
  Imaging: '#0088FE',
  Admin: '#52A67D',
  Lab: '#FF6F00',
  Drug: '#8B5CF6',
};

export interface TableInfo {
  name: string;
  description: string;
  category: string;
  row_count: number;
  column_count: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  position: number;
}

export interface CdmSummary {
  table_stats: { name: string; row_count: number; category: string; description: string }[];
  demographics: { total_patients: number; male: number; female: number; min_birth_year: number; max_birth_year: number; avg_age: number };
  top_conditions: { snomed_code: string; name_kr: string; count: number; patient_count: number }[];
  visit_types: { type_id: number; type_name: string; count: number; patient_count: number }[];
  top_measurements: { code: string; count: number }[];
  yearly_activity: { year: number; total: number }[];
  quality: { domain: string; score: number; total: number; issues: number }[];
  total_records: number;
  total_tables: number;
}

export interface MappingExample {
  source: string;
  sourceField: string;
  cdmTable: string;
  cdmField: string;
  cdmValue: string;
  standard: string;
}

export const pythonCodeSnippet = (tableName: string) => `import psycopg2
import pandas as pd
import os

# OMOP CDM 데이터베이스 연결
conn = psycopg2.connect(
    host=os.environ.get("OMOP_HOST", "localhost"),
    port=int(os.environ.get("OMOP_PORT", "5436")),
    dbname=os.environ.get("OMOP_DB", "omop_cdm"),
    user=os.environ.get("OMOP_USER", "omopuser"),
    password=os.environ["OMOP_PASSWORD"]  # 환경변수 설정 필요
)

# 데이터 조회
df = pd.read_sql("SELECT * FROM ${tableName} LIMIT 100", conn)
print(df.head())
print(f"\\nShape: {df.shape}")

conn.close()
`;

export const rCodeSnippet = (tableName: string) => `library(DBI)
library(RPostgres)

# OMOP CDM 데이터베이스 연결
con <- dbConnect(
  Postgres(),
  host = Sys.getenv("OMOP_HOST", "localhost"),
  port = as.integer(Sys.getenv("OMOP_PORT", "5436")),
  dbname = Sys.getenv("OMOP_DB", "omop_cdm"),
  user = Sys.getenv("OMOP_USER", "omopuser"),
  password = Sys.getenv("OMOP_PASSWORD")  # 환경변수 설정 필요
)

# 데이터 조회
df <- dbGetQuery(con, "SELECT * FROM ${tableName} LIMIT 100")
head(df)
cat(sprintf("\\nRows: %d, Cols: %d\\n", nrow(df), ncol(df)))

dbDisconnect(con)
`;

export const LazyCodeBlock = React.lazy(() =>
  Promise.all([
    import('react-syntax-highlighter/dist/esm/prism-light'),
    import('react-syntax-highlighter/dist/esm/styles/prism/one-dark'),
    import('react-syntax-highlighter/dist/esm/languages/prism/python'),
    import('react-syntax-highlighter/dist/esm/languages/prism/r'),
  ]).then(([{ default: SyntaxHighlighter }, { default: oneDark }, { default: python }, { default: r }]) => {
    SyntaxHighlighter.registerLanguage('python', python);
    SyntaxHighlighter.registerLanguage('r', r);
    return {
      default: ({ language, children }: { language: string; children: string }) => (
        <SyntaxHighlighter language={language} style={oneDark} customStyle={{ borderRadius: '6px' }}>
          {children}
        </SyntaxHighlighter>
      ),
    };
  })
);

export const CONDITION_COLUMNS = [
  { title: '#', key: 'idx', width: 40, render: (_: any, __: any, i: number) => i + 1 },
  { title: 'SNOMED CT', dataIndex: 'snomed_code', key: 'snomed_code', width: 120, render: (v: string) => <Text code>{v}</Text> },
  { title: '진단명', dataIndex: 'name_kr', key: 'name_kr' },
  { title: '건수', dataIndex: 'count', key: 'count', width: 80, render: (v: number) => v.toLocaleString(), sorter: (a: any, b: any) => a.count - b.count },
  { title: '환자수', dataIndex: 'patient_count', key: 'patient_count', width: 80, render: (v: number) => v.toLocaleString() },
];

export const MAPPING_COLUMNS = [
  { title: '원본 데이터', dataIndex: 'source', key: 'source' },
  { title: '표준 테이블', dataIndex: 'cdmTable', key: 'cdmTable', render: (v: string) => <Tag color="blue">{v}</Tag> },
  { title: '표준 필드', dataIndex: 'cdmField', key: 'cdmField', render: (v: string) => <Text code>{v}</Text> },
  { title: '표준코드 값', dataIndex: 'cdmValue', key: 'cdmValue', render: (v: string) => <Text strong>{v}</Text> },
  { title: '표준체계', dataIndex: 'standard', key: 'standard', render: (v: string) => <Tag color="green">{v}</Tag> },
];

export const SCHEMA_COLUMNS = [
  { title: '#', dataIndex: 'position', key: 'position', width: 40 },
  { title: '컬럼명', dataIndex: 'name', key: 'name', render: (text: string) => <Text code>{text}</Text> },
  { title: '타입', dataIndex: 'type', key: 'type', render: (text: string) => <Tag color="blue">{text}</Tag> },
  { title: 'Nullable', dataIndex: 'nullable', key: 'nullable', width: 80, render: (val: boolean) => val ? <Tag>YES</Tag> : <Tag color="red">NO</Tag> },
];

export const SUMMARY_STATS = [
  { title: '총 환자 수', key: 'demographics.total_patients', suffix: '명', color: '#006241', borderColor: '#006241', prefix: <TeamOutlined /> },
  { title: '총 레코드', key: 'total_records', color: '#0088FE', borderColor: '#0088FE', prefix: <DatabaseOutlined />, format: true },
  { title: '임상 테이블', key: 'total_tables', suffix: '개', color: '#52A67D', borderColor: '#52A67D', prefix: <TableOutlined /> },
  { title: '평균 연령', key: 'demographics.avg_age', suffix: '세', color: '#FF6F00', borderColor: '#FF6F00' },
];

export function getQualityColor(score: number) {
  if (score >= 90) return '#52A67D';
  if (score >= 70) return '#FF6F00';
  return '#DC2626';
}

export const VISIT_TYPE_COLORS = ['#006241', '#FF6F00', '#DC2626'];

// ── useCountUp 훅: 0 → target 카운트업 애니메이션 (easeOutCubic, 1.8s) ──
export function useCountUp(target: number, duration = 1800): number {
  const [value, setValue] = useState(0);
  const rafRef = useRef<number>(0);
  useEffect(() => {
    if (!target) { setValue(0); return; }
    const start = performance.now();
    const tick = (now: number) => {
      const t = Math.min((now - start) / duration, 1);
      const ease = 1 - Math.pow(1 - t, 3); // easeOutCubic
      setValue(Math.round(target * ease));
      if (t < 1) rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [target, duration]);
  return value;
}

// ── ECharts 옵션 빌더 함수들 ──

export function buildRadarOption(quality: CdmSummary['quality']): EChartsOption {
  const domainColors: Record<string, string> = {
    Clinical: '#006241', Imaging: '#0088FE', Admin: '#52A67D', Lab: '#FF6F00', Drug: '#8B5CF6',
  };
  return {
    tooltip: { trigger: 'item' },
    radar: {
      indicator: quality.map(q => ({ name: q.domain, max: 100 })),
      shape: 'polygon',
      splitNumber: 5,
      axisName: { color: '#555', fontSize: 13, fontWeight: 600 },
      splitLine: { lineStyle: { color: '#e8e8e8' } },
      splitArea: { areaStyle: { color: ['rgba(0,98,65,0.02)', 'rgba(0,98,65,0.05)'] } },
    },
    series: [{
      type: 'radar',
      data: [{
        value: quality.map(q => q.score),
        name: '데이터 품질',
        areaStyle: { color: 'rgba(0,98,65,0.25)' },
        lineStyle: { color: '#006241', width: 2 },
        itemStyle: { color: '#006241' },
        symbol: 'circle',
        symbolSize: 6,
      }],
      animationDuration: 1200,
      animationEasing: 'cubicOut',
    }],
  };
}

export function buildPatientJourneyOption(summary: CdmSummary): EChartsOption {
  // AMI Clinical Pathway — OMOP CDM 테이블별 실제 데이터 건수
  const emergency = summary.visit_types.find(v => v.type_id === 9203)?.count || 0;
  const inpatient = summary.visit_types.find(v => v.type_id === 9201)?.count || 0;
  const outpatient = summary.visit_types.find(v => v.type_id === 9202)?.count || 0;
  const measureCount = summary.table_stats.find(t => t.name === 'measurement')?.row_count || 0;
  const procCount = summary.table_stats.find(t => t.name === 'procedure_occurrence')?.row_count || 0;
  const drugCount = summary.table_stats.find(t => t.name === 'drug_exposure')?.row_count || 0;

  // 심혈관 시술 비율 추정 (문헌 기반: PCI ~6.8%, CABG ~1.3% of all procedures)
  const pciEst = Math.round(procCount * 0.068);
  const cabgEst = Math.round(procCount * 0.013);

  const nodes = [
    { name: '흉통/응급내원', value: emergency, symbol: 'circle', symbolSize: 62,
      x: 40, y: 150, itemStyle: { color: '#DC2626' } },
    { name: 'ECG/Troponin', value: measureCount, symbol: 'circle', symbolSize: 58,
      x: 175, y: 150, itemStyle: { color: '#FF6F00' } },
    { name: '관상동맥 조영술', value: procCount, symbol: 'circle', symbolSize: 64,
      x: 325, y: 150, itemStyle: { color: '#0088FE' } },
    { name: 'PCI/스텐트', value: pciEst, symbol: 'circle', symbolSize: 58,
      x: 485, y: 85, itemStyle: { color: '#006241' },
      label: { position: 'right' as const } },
    { name: 'CABG', value: cabgEst, symbol: 'circle', symbolSize: 50,
      x: 485, y: 215, itemStyle: { color: '#13C2C2' },
      label: { position: 'right' as const } },
    { name: 'CCU 집중치료', value: inpatient, symbol: 'circle', symbolSize: 58,
      x: 630, y: 150, itemStyle: { color: '#8B5CF6' } },
    { name: 'DAPT 처방', value: drugCount, symbol: 'circle', symbolSize: 54,
      x: 765, y: 150, itemStyle: { color: '#52A67D' } },
    { name: '퇴원/심장재활', value: outpatient, symbol: 'circle', symbolSize: 60,
      x: 900, y: 150, itemStyle: { color: '#006241' } },
  ];

  const links: any[] = [
    { source: '흉통/응급내원', target: 'ECG/Troponin' },
    { source: 'ECG/Troponin', target: '관상동맥 조영술' },
    { source: '관상동맥 조영술', target: 'PCI/스텐트',
      label: { show: true, formatter: '~65%', fontSize: 12, color: '#006241', padding: [0, 0, 0, 8] } },
    { source: '관상동맥 조영술', target: 'CABG',
      label: { show: true, formatter: '~12%', fontSize: 12, color: '#13C2C2', padding: [0, 0, 0, 8] } },
    { source: 'PCI/스텐트', target: 'CCU 집중치료' },
    { source: 'CABG', target: 'CCU 집중치료' },
    { source: 'CCU 집중치료', target: 'DAPT 처방' },
    { source: 'DAPT 처방', target: '퇴원/심장재활' },
  ];

  return {
    tooltip: {
      formatter: (p: any) => {
        if (p.dataType === 'node') {
          const nm = (p.name as string).replace(/\n/g, ' ');
          return `<b>${nm}</b><br/>${Number(p.value).toLocaleString()}건`;
        }
        return `${p.data.source} → ${p.data.target}`;
      },
    },
    series: [{
      type: 'graph',
      layout: 'none',
      coordinateSystem: undefined,
      roam: false,
      label: {
        show: true,
        position: 'bottom',
        formatter: (p: any) => `{title|${p.name}}\n{count|${Number(p.value).toLocaleString()}건}`,
        rich: {
          title: { fontSize: 14, fontWeight: 700, color: '#222', lineHeight: 20 },
          count: { fontSize: 13, color: '#666', lineHeight: 18 },
        },
      },
      edgeSymbol: ['none', 'arrow'],
      edgeSymbolSize: [0, 12],
      edgeLabel: { show: false },
      lineStyle: { color: '#999', width: 2.5, curveness: 0 },
      data: nodes,
      links,
      animationDuration: 1500,
      animationEasing: 'cubicOut',
    }],
  };
}

export function buildYearlyActivityOption(data: CdmSummary['yearly_activity']): EChartsOption {
  return {
    tooltip: {
      trigger: 'axis',
      formatter: (p: any) => `${p[0].name}년<br/>${p[0].marker} ${Number(p[0].value).toLocaleString()}건`,
    },
    grid: { top: 20, right: 20, bottom: 30, left: 60 },
    xAxis: {
      type: 'category',
      data: data.map(d => String(d.year)),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#666', fontSize: 12 },
    },
    yAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
      axisLabel: { color: '#666', fontSize: 12, formatter: (v: number) => v >= 1000000 ? (v / 1000000).toFixed(1) + 'M' : v >= 1000 ? (v / 1000).toFixed(0) + 'K' : String(v) },
    },
    series: [{
      type: 'line',
      data: data.map(d => d.total),
      smooth: true,
      showSymbol: true,
      symbolSize: 6,
      lineStyle: { color: '#006241', width: 3 },
      itemStyle: { color: '#006241' },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(0,98,65,0.3)' },
            { offset: 1, color: 'rgba(0,98,65,0.02)' },
          ],
        } as any,
      },
      animationDuration: 1500,
      animationEasing: 'cubicOut',
    }],
  };
}

export function buildTopConditionsOption(conditions: CdmSummary['top_conditions']): EChartsOption {
  const sorted = [...conditions].sort((a, b) => a.count - b.count);
  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (p: any) => `<b>${p[0].name}</b><br/>건수: ${Number(p[0].value).toLocaleString()}<br/>환자수: ${sorted.find((c: any) => c.name_kr === p[0].name)?.patient_count?.toLocaleString() || '-'}명`,
    },
    grid: { top: 10, right: 40, bottom: 10, left: 140, containLabel: false },
    xAxis: { type: 'value', show: false },
    yAxis: {
      type: 'category',
      data: sorted.map(c => c.name_kr),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#333', fontSize: 12, width: 130, overflow: 'truncate' },
    },
    series: [{
      type: 'bar',
      data: sorted.map((c, i) => ({
        value: c.count,
        itemStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
            colorStops: [
              { offset: 0, color: '#006241' },
              { offset: 1, color: '#52A67D' },
            ],
          } as any,
          borderRadius: [0, 4, 4, 0],
        },
      })),
      barWidth: 18,
      label: {
        show: true,
        position: 'right',
        formatter: (p: any) => Number(p.value).toLocaleString(),
        fontSize: 12,
        color: '#666',
      },
      animationDuration: 1500,
      animationEasing: 'cubicOut',
    }],
  };
}

export function buildTableDistributionOption(tableStats: CdmSummary['table_stats']): EChartsOption {
  const top10 = tableStats.slice(0, 10);
  const sorted = [...top10].sort((a, b) => a.row_count - b.row_count);
  const colors = ['#006241', '#0088FE', '#52A67D', '#FF6F00', '#8B5CF6'];
  return {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (p: any) => `<b>${p[0].name}</b><br/>${Number(p[0].value).toLocaleString()}건`,
    },
    grid: { top: 10, right: 40, bottom: 10, left: 140, containLabel: false },
    xAxis: { type: 'value', show: false },
    yAxis: {
      type: 'category',
      data: sorted.map(t => t.name),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#333', fontSize: 12, width: 130, overflow: 'truncate' },
    },
    series: [{
      type: 'bar',
      data: sorted.map((t, i) => ({
        value: t.row_count,
        itemStyle: { color: colors[i % colors.length], borderRadius: [0, 4, 4, 0] },
      })),
      barWidth: 18,
      label: {
        show: true,
        position: 'right',
        formatter: (p: any) => {
          const v = p.value as number;
          return v >= 1000000 ? (v / 1000000).toFixed(1) + 'M' : v >= 1000 ? (v / 1000).toFixed(0) + 'K' : String(v);
        },
        fontSize: 12,
        color: '#666',
      },
      animationDuration: 1500,
      animationEasing: 'cubicOut',
    }],
  };
}

// ── 데이터 제공 유연성 허브-앤-스포크 다이어그램 ──
export function buildDataFlexibilityOption(): EChartsOption {
  const C = { center: '#006241', access: '#0088FE', standard: '#FF6F00', infra: '#8B5CF6' };
  const centerX = 450, centerY = 200;

  // 카테고리 허브 위치 (삼각 배치)
  const catAccess = { x: 140, y: 105 };
  const catStd    = { x: 450, y: 380 };
  const catInfra  = { x: 760, y: 105 };

  // 부채꼴 리프 배치 헬퍼
  const fan = (cx: number, cy: number, items: string[], r: number, startDeg: number, sweepDeg: number) =>
    items.map((name, i) => {
      const angle = startDeg + (sweepDeg / Math.max(items.length - 1, 1)) * i;
      const rad = (angle * Math.PI) / 180;
      return { name, x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) };
    });

  const accessItems = ['REST API', 'Text2SQL', 'DuckDB OLAP', 'CSV Export', 'JupyterLab'];
  const stdItems    = ['OMOP CDM\nV5.4', 'SNOMED CT', 'RxNorm', 'LOINC', 'Parquet'];
  const infraItems  = ['PostgreSQL', 'DuckDB', 'Milvus\nVector', 'Neo4j\nGraph', 'MinIO S3'];

  const accessLeaves = fan(catAccess.x, catAccess.y, accessItems, 130, 175, 145);
  const stdLeaves    = fan(catStd.x, catStd.y, stdItems, 130, 215, 110);
  const infraLeaves  = fan(catInfra.x, catInfra.y, infraItems, 130, -5, 150);

  const mkLeaf = (l: { name: string; x: number; y: number }, color: string, cat: number) => ({
    name: l.name, x: l.x, y: l.y, symbolSize: 40, symbol: 'circle',
    itemStyle: { color: color + '20', borderColor: color, borderWidth: 2 },
    label: { fontSize: 11, color: '#444', position: 'inside' as const, lineHeight: 14 },
    category: cat,
  });

  const nodes: any[] = [
    { name: '통합\n데이터마트', x: centerX, y: centerY, symbolSize: 78, symbol: 'circle',
      itemStyle: { color: C.center, shadowBlur: 24, shadowColor: 'rgba(0,98,65,0.35)' },
      label: { fontSize: 14, fontWeight: 700, color: '#fff', lineHeight: 18 }, category: 0 },
    { name: '접근 방식', x: catAccess.x, y: catAccess.y, symbolSize: 56, symbol: 'circle',
      itemStyle: { color: C.access, shadowBlur: 12, shadowColor: C.access + '40' },
      label: { fontSize: 12, fontWeight: 600, color: '#fff' }, category: 1 },
    { name: '표준 체계', x: catStd.x, y: catStd.y, symbolSize: 56, symbol: 'circle',
      itemStyle: { color: C.standard, shadowBlur: 12, shadowColor: C.standard + '40' },
      label: { fontSize: 12, fontWeight: 600, color: '#fff' }, category: 2 },
    { name: '인프라 통합', x: catInfra.x, y: catInfra.y, symbolSize: 56, symbol: 'circle',
      itemStyle: { color: C.infra, shadowBlur: 12, shadowColor: C.infra + '40' },
      label: { fontSize: 12, fontWeight: 600, color: '#fff' }, category: 3 },
    ...accessLeaves.map(l => mkLeaf(l, C.access, 1)),
    ...stdLeaves.map(l => mkLeaf(l, C.standard, 2)),
    ...infraLeaves.map(l => mkLeaf(l, C.infra, 3)),
  ];

  const links: any[] = [
    { source: '통합\n데이터마트', target: '접근 방식', lineStyle: { width: 3, color: C.access } },
    { source: '통합\n데이터마트', target: '표준 체계', lineStyle: { width: 3, color: C.standard } },
    { source: '통합\n데이터마트', target: '인프라 통합', lineStyle: { width: 3, color: C.infra } },
    ...accessItems.map(n => ({ source: '접근 방식', target: n, lineStyle: { width: 1.5, color: C.access + '70' } })),
    ...stdItems.map(n => ({ source: '표준 체계', target: n, lineStyle: { width: 1.5, color: C.standard + '70' } })),
    ...infraItems.map(n => ({ source: '인프라 통합', target: n, lineStyle: { width: 1.5, color: C.infra + '70' } })),
  ];

  return {
    tooltip: {
      formatter: (p: any) => {
        if (p.dataType === 'edge') return '';
        return `<b>${(p.name as string).replace(/\n/g, ' ')}</b>`;
      },
    },
    legend: {
      data: ['데이터마트', '접근 방식', '표준 체계', '인프라 통합'],
      bottom: 0,
      textStyle: { fontSize: 12 },
      itemWidth: 14, itemHeight: 14,
    },
    series: [{
      type: 'graph',
      layout: 'none',
      roam: false,
      label: { show: true },
      edgeSymbol: ['none', 'arrow'],
      edgeSymbolSize: [0, 8],
      categories: [
        { name: '데이터마트', itemStyle: { color: C.center } },
        { name: '접근 방식', itemStyle: { color: C.access } },
        { name: '표준 체계', itemStyle: { color: C.standard } },
        { name: '인프라 통합', itemStyle: { color: C.infra } },
      ],
      data: nodes,
      links,
      lineStyle: { curveness: 0.15 },
      animationDuration: 2000,
      animationEasing: 'cubicOut',
    }],
  };
}
