/**
 * DataMart 페이지 테스트
 * 테이블 목록 조회, 스키마 표시, 샘플 데이터, CDM 요약
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { App as AntApp } from 'antd';

// Mock recharts to avoid SVG rendering issues in jsdom
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: React.ReactNode }) => <div data-testid="responsive-container">{children}</div>,
  BarChart: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  Bar: () => <div />,
  XAxis: () => <div />,
  YAxis: () => <div />,
  Tooltip: () => <div />,
  Cell: () => <div />,
  AreaChart: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  Area: () => <div />,
  CartesianGrid: () => <div />,
  PieChart: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  Pie: () => <div />,
}));

// Mock echarts-for-react
vi.mock('echarts-for-react/lib/core', () => ({
  default: () => <div data-testid="echarts-mock" />,
}));
vi.mock('echarts/core', () => ({
  use: () => {},
}));
vi.mock('echarts/charts', () => ({
  RadarChart: {},
  GraphChart: {},
  LineChart: {},
  BarChart: {},
  PieChart: {},
}));
vi.mock('echarts/components', () => ({
  GridComponent: {},
  TooltipComponent: {},
  RadarComponent: {},
  LegendComponent: {},
}));
vi.mock('echarts/renderers', () => ({
  SVGRenderer: {},
}));

// Mock react-syntax-highlighter lazy import
vi.mock('react-syntax-highlighter/dist/esm/prism-light', () => ({
  default: ({ children }: { children: string }) => <pre>{children}</pre>,
}));
vi.mock('react-syntax-highlighter/dist/esm/styles/prism/one-dark', () => ({
  default: {},
}));
vi.mock('react-syntax-highlighter/dist/esm/languages/prism/python', () => ({
  default: () => {},
}));
vi.mock('react-syntax-highlighter/dist/esm/languages/prism/r', () => ({
  default: () => {},
}));

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import DataMart from './DataMart';

const MOCK_CDM_SUMMARY = {
  table_stats: [
    { name: 'person', row_count: 76074, category: 'Clinical Data', description: 'Demographics' },
    { name: 'visit_occurrence', row_count: 4500000, category: 'Clinical Data', description: 'Visits' },
  ],
  demographics: { total_patients: 76074, male: 37796, female: 38278, min_birth_year: 1920, max_birth_year: 2010, avg_age: 45 },
  top_conditions: [{ snomed_code: '44054006', name_kr: '당뇨병', count: 5000, patient_count: 3000 }],
  visit_types: [
    { type_id: 9201, type_name: '입원', count: 100000, patient_count: 50000 },
    { type_id: 9202, type_name: '외래', count: 200000, patient_count: 60000 },
  ],
  top_measurements: [{ code: 'HbA1c', count: 10000 }],
  yearly_activity: [{ year: 2020, total: 5000 }],
  quality: [{ domain: 'Clinical', score: 95, total: 100000, issues: 500 }],
  total_records: 92260027,
  total_tables: 18,
};

const MOCK_TABLES = {
  tables: [
    { name: 'person', description: 'Demographics', category: 'Clinical Data', row_count: 76074, column_count: 18 },
    { name: 'visit_occurrence', description: 'Visits', category: 'Clinical Data', row_count: 4500000, column_count: 15 },
  ],
};

const MOCK_SCHEMA = {
  columns: [
    { name: 'person_id', type: 'bigint', nullable: false, default: null, position: 1 },
    { name: 'gender_source_value', type: 'varchar', nullable: true, default: null, position: 2 },
  ],
};

const MOCK_SAMPLE = {
  columns: ['person_id', 'gender_source_value'],
  rows: [{ person_id: 1, gender_source_value: 'M' }],
};

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false, staleTime: 5 * 60 * 1000, refetchOnWindowFocus: false },
    },
  });

const renderWithAntd = (ui: React.ReactElement) => {
  const queryClient = createTestQueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <AntApp>{ui}</AntApp>
    </QueryClientProvider>
  );
};

describe('DataMart Page', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation((url: string) => {
      if (url.includes('/cdm-summary')) {
        return Promise.resolve({ ok: true, json: async () => MOCK_CDM_SUMMARY });
      }
      if (url.includes('/cdm-mapping-examples')) {
        return Promise.resolve({ ok: true, json: async () => ({ examples: [] }) });
      }
      if (url.match(/\/tables\/\w+\/schema/)) {
        return Promise.resolve({ ok: true, json: async () => MOCK_SCHEMA });
      }
      if (url.match(/\/tables\/\w+\/sample/)) {
        return Promise.resolve({ ok: true, json: async () => MOCK_SAMPLE });
      }
      if (url.includes('/tables')) {
        return Promise.resolve({ ok: true, json: async () => MOCK_TABLES });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    }));

    Object.defineProperty(document, 'cookie', {
      get: () => '',
      configurable: true,
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders page title', () => {
    renderWithAntd(<DataMart />);
    expect(screen.getByText('데이터마트')).toBeInTheDocument();
  });

  it('renders subtitle with OMOP CDM', () => {
    renderWithAntd(<DataMart />);
    expect(screen.getByText(/OMOP CDM V5.4/)).toBeInTheDocument();
  });

  it('renders main tabs', () => {
    renderWithAntd(<DataMart />);
    expect(screen.getByText('임상 데이터 현황')).toBeInTheDocument();
    expect(screen.getByText('테이블 관리')).toBeInTheDocument();
  });

  it('renders cache clear button', () => {
    renderWithAntd(<DataMart />);
    expect(screen.getByText('캐시 초기화')).toBeInTheDocument();
  });

  it('shows hero stat labels after load', async () => {
    renderWithAntd(<DataMart />);
    await waitFor(() => {
      expect(screen.getByText('총 레코드')).toBeInTheDocument();
      expect(screen.getByText('등록 환자')).toBeInTheDocument();
      expect(screen.getByText('CDM 테이블')).toBeInTheDocument();
      expect(screen.getByText('데이터 품질')).toBeInTheDocument();
    });
  });
});
