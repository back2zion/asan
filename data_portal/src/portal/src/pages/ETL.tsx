/**
 * ETL 파이프라인 페이지 — 좌측 사이드바 그룹 메뉴 레이아웃
 * 16개 기능을 4개 카테고리로 그룹화하여 탐색성 향상
 */
import React, { useState, Suspense } from 'react';
import { Card, Typography, Menu, Spin, Grid } from 'antd';
import type { MenuProps } from 'antd';

const { useBreakpoint } = Grid;
import {
  SettingOutlined, AppstoreOutlined, ApartmentOutlined,
  FileTextOutlined, BellOutlined, CloudServerOutlined,
  BranchesOutlined, BookOutlined, ThunderboltOutlined,
  SwapOutlined, AuditOutlined, FileSearchOutlined,
  DatabaseOutlined, RocketOutlined, FundOutlined,
} from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const lazy = (fn: () => Promise<{ default: React.ComponentType }>) => React.lazy(fn);

const contentComponents: Record<string, React.LazyExoticComponent<React.ComponentType>> = {
  'job-groups': lazy(() => import('../components/etl/JobGroupManagement')),
  'dependencies': lazy(() => import('../components/etl/TableDependencyGraph')),
  'exec-logs': lazy(() => import('../components/etl/ExecutionLogs')),
  'alerts': lazy(() => import('../components/etl/AlertManagement')),
  'pipeline': lazy(() => import('../components/etl/PipelineDashboardTab')),
  'sources': lazy(() => import('../components/etl/HeterogeneousSourcesTab')),
  'templates': lazy(() => import('../components/etl/IngestionTemplatesTab')),
  'parallel': lazy(() => import('../components/etl/ParallelLoadingTab')),
  'mapping': lazy(() => import('../components/etl/MappingGeneratorTab')),
  'schema': lazy(() => import('../components/etl/SchemaVersioningTab')),
  'migration': lazy(() => import('../components/etl/MigrationVerificationTab')),
  'schema-monitor': lazy(() => import('../components/etl/SchemaChangeManagementTab')),
  'cdc': lazy(() => import('../components/etl/CDCManagement')),
  'data-design': lazy(() => import('../components/etl/DataDesignDashboard')),
  'data-pipeline': lazy(() => import('../components/etl/DataPipelineTab')),
  'mart-ops': lazy(() => import('../components/etl/DataMartOps')),
};

const menuItems: MenuProps['items'] = [
  {
    key: 'grp-pipeline',
    label: '파이프라인 운영',
    type: 'group' as const,
    children: [
      { key: 'job-groups', label: 'Job/Group 관리', icon: <AppstoreOutlined /> },
      { key: 'dependencies', label: '테이블 종속관계', icon: <ApartmentOutlined /> },
      { key: 'exec-logs', label: '실행 로그', icon: <FileTextOutlined /> },
      { key: 'alerts', label: '알림', icon: <BellOutlined /> },
      { key: 'pipeline', label: '파이프라인 대시보드', icon: <SettingOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-ingest',
    label: '데이터 수집',
    type: 'group' as const,
    children: [
      { key: 'sources', label: '이기종 소스 관리', icon: <CloudServerOutlined /> },
      { key: 'templates', label: '수집 템플릿', icon: <BookOutlined /> },
      { key: 'parallel', label: '병렬 적재 설정', icon: <ThunderboltOutlined /> },
      { key: 'mapping', label: '매핑 자동생성', icon: <SwapOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-schema',
    label: '스키마 & 이관',
    type: 'group' as const,
    children: [
      { key: 'schema', label: '스키마 버전 관리', icon: <BranchesOutlined /> },
      { key: 'migration', label: '이관 검증', icon: <AuditOutlined /> },
      { key: 'schema-monitor', label: '스키마 변경 관리', icon: <FileSearchOutlined /> },
      { key: 'cdc', label: 'CDC 관리', icon: <ThunderboltOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-ops',
    label: '데이터 운영',
    type: 'group' as const,
    children: [
      { key: 'data-design', label: '데이터 설계', icon: <DatabaseOutlined /> },
      { key: 'data-pipeline', label: '데이터 Pipeline', icon: <RocketOutlined /> },
      { key: 'mart-ops', label: '마트 운영', icon: <FundOutlined /> },
    ],
  },
];

const ETL: React.FC = () => {
  const screens = useBreakpoint();
  const isWide = screens.lg !== false;
  const [activeKey, setActiveKey] = useState('job-groups');

  const onMenuClick: MenuProps['onClick'] = ({ key }) => {
    setActiveKey(key);
  };

  return (
    <div>
      <Card>
        <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
          <SettingOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
          ETL 파이프라인
        </Title>
        <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 14, color: '#6c757d' }}>
          데이터 수집 · 변환 · 적재 파이프라인 관리 및 모니터링
        </Paragraph>
      </Card>

      <div style={{ display: 'flex', flexDirection: isWide ? 'row' : 'column', gap: 16, marginTop: 16 }}>
        <Card
          style={isWide ? { width: 240, flexShrink: 0 } : { width: '100%' }}
          styles={{ body: { padding: '8px 0' } }}
        >
          <Menu
            mode={isWide ? 'inline' : 'horizontal'}
            selectedKeys={[activeKey]}
            onClick={onMenuClick}
            items={menuItems}
            style={{ border: 'none', overflowX: 'auto' }}
          />
        </Card>

        <Card style={{ flex: 1, minWidth: 0, overflow: 'auto' }}>
          <Suspense fallback={<Spin style={{ display: 'block', textAlign: 'center', padding: 48 }} />}>
            {(() => { const C = contentComponents[activeKey]; return C ? <C /> : null; })()}
          </Suspense>
        </Card>
      </div>
    </div>
  );
};

export default ETL;
