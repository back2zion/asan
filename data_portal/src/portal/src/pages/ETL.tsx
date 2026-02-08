/**
 * ETL 파이프라인 페이지 — 좌측 사이드바 그룹 메뉴 레이아웃
 * 16개 기능을 4개 카테고리로 그룹화하여 탐색성 향상
 */
import React, { useState } from 'react';
import { Card, Typography, Row, Col, Menu } from 'antd';
import type { MenuProps } from 'antd';
import {
  SettingOutlined, AppstoreOutlined, ApartmentOutlined,
  FileTextOutlined, BellOutlined, CloudServerOutlined,
  BranchesOutlined, BookOutlined, ThunderboltOutlined,
  SwapOutlined, AuditOutlined, FileSearchOutlined,
  DatabaseOutlined, RocketOutlined, FundOutlined,
} from '@ant-design/icons';
import JobGroupManagement from '../components/etl/JobGroupManagement';
import TableDependencyGraph from '../components/etl/TableDependencyGraph';
import ExecutionLogs from '../components/etl/ExecutionLogs';
import AlertManagement from '../components/etl/AlertManagement';
import CDCManagement from '../components/etl/CDCManagement';
import DataDesignDashboard from '../components/etl/DataDesignDashboard';
import DataMartOps from '../components/etl/DataMartOps';
import PipelineDashboardTab from '../components/etl/PipelineDashboardTab';
import HeterogeneousSourcesTab from '../components/etl/HeterogeneousSourcesTab';
import SchemaVersioningTab from '../components/etl/SchemaVersioningTab';
import IngestionTemplatesTab from '../components/etl/IngestionTemplatesTab';
import ParallelLoadingTab from '../components/etl/ParallelLoadingTab';
import MappingGeneratorTab from '../components/etl/MappingGeneratorTab';
import MigrationVerificationTab from '../components/etl/MigrationVerificationTab';
import SchemaChangeManagementTab from '../components/etl/SchemaChangeManagementTab';
import DataPipelineTab from '../components/etl/DataPipelineTab';

const { Title } = Typography;

// 탭 전환 시 해당 컴포넌트만 렌더링 (lazy)
const contentComponents: Record<string, React.FC> = {
  'job-groups': JobGroupManagement,
  'dependencies': TableDependencyGraph,
  'exec-logs': ExecutionLogs,
  'alerts': AlertManagement,
  'pipeline': PipelineDashboardTab,
  'sources': HeterogeneousSourcesTab,
  'templates': IngestionTemplatesTab,
  'parallel': ParallelLoadingTab,
  'mapping': MappingGeneratorTab,
  'schema': SchemaVersioningTab,
  'migration': MigrationVerificationTab,
  'schema-monitor': SchemaChangeManagementTab,
  'cdc': CDCManagement,
  'data-design': DataDesignDashboard,
  'data-pipeline': DataPipelineTab,
  'mart-ops': DataMartOps,
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
  const [activeKey, setActiveKey] = useState('job-groups');

  const onMenuClick: MenuProps['onClick'] = ({ key }) => {
    setActiveKey(key);
  };

  return (
    <div>
      <Card>
        <Row align="middle">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              ETL 파이프라인
            </Title>
          </Col>
        </Row>
      </Card>

      <div style={{ display: 'flex', gap: 16, marginTop: 16 }}>
        <Card
          style={{ width: 240, flexShrink: 0 }}
          styles={{ body: { padding: '8px 0' } }}
        >
          <Menu
            mode="inline"
            selectedKeys={[activeKey]}
            onClick={onMenuClick}
            items={menuItems}
            style={{ border: 'none' }}
          />
        </Card>

        <Card style={{ flex: 1, minWidth: 0 }}>
          {(() => { const C = contentComponents[activeKey]; return C ? <C /> : null; })()}
        </Card>
      </div>
    </div>
  );
};

export default ETL;
