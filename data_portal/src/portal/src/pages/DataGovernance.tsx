/**
 * 데이터 거버넌스 페이지 — 좌측 사이드바 그룹 메뉴 레이아웃
 * PRD DGR 요구사항 기반 - 4그룹 12개 서브메뉴
 */

import React, { useState, Suspense } from 'react';
import { Card, Typography, Menu, Spin } from 'antd';
import type { MenuProps } from 'antd';
import {
  SafetyCertificateOutlined, CheckCircleOutlined,
  DatabaseOutlined, LockOutlined, BranchesOutlined, EyeInvisibleOutlined,
  BookOutlined, BarChartOutlined, AuditOutlined, AppstoreOutlined,
  SecurityScanOutlined, KeyOutlined, RobotOutlined,
} from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const lazy = (fn: () => Promise<{ default: React.ComponentType }>) => React.lazy(fn);

const contentComponents: Record<string, React.LazyExoticComponent<React.ComponentType>> = {
  'quality': lazy(() => import('../components/governance/DataQualityTab')),
  'terms': lazy(() => import('../components/governance/StandardTermsTab')),
  'indicators': lazy(() => import('../components/governance/StandardIndicatorsTab')),
  'metadata-edit': lazy(() => import('../components/governance/MetadataTab')),
  'metadata-mgmt': lazy(() => import('../components/governance/MetadataManagement')),
  'catalog-mgmt': lazy(() => import('../components/governance/CatalogManagement')),
  'lineage': lazy(() => import('../components/governance/LineageTab')),
  'standard-security': lazy(() => import('../components/governance/StandardSecurityTab')),
  'deidentification': lazy(() => import('../components/governance/DeidentificationTab')),
  'security-mgmt': lazy(() => import('../components/governance/SecurityManagement')),
  'permission-mgmt': lazy(() => import('../components/governance/PermissionManagement')),
  'smart': lazy(() => import('../components/governance/SmartGovernanceTab')),
};

const menuItems: MenuProps['items'] = [
  {
    key: 'grp-quality',
    label: '품질 & 표준',
    type: 'group' as const,
    children: [
      { key: 'quality', label: '데이터 품질', icon: <CheckCircleOutlined /> },
      { key: 'terms', label: '용어 사전', icon: <BookOutlined /> },
      { key: 'indicators', label: '표준 지표', icon: <BarChartOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-metadata',
    label: '메타데이터',
    type: 'group' as const,
    children: [
      { key: 'metadata-edit', label: '메타 편집', icon: <DatabaseOutlined /> },
      { key: 'metadata-mgmt', label: '메타데이터 관리', icon: <AuditOutlined /> },
      { key: 'catalog-mgmt', label: '카탈로그 관리', icon: <AppstoreOutlined /> },
      { key: 'lineage', label: '데이터 리니지', icon: <BranchesOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-security',
    label: '보안 & 접근제어',
    type: 'group' as const,
    children: [
      { key: 'standard-security', label: '표준/보안', icon: <LockOutlined /> },
      { key: 'deidentification', label: '비식별화', icon: <EyeInvisibleOutlined /> },
      { key: 'security-mgmt', label: '보안 관리', icon: <SecurityScanOutlined /> },
      { key: 'permission-mgmt', label: '권한 관리', icon: <KeyOutlined /> },
    ],
  },
  { type: 'divider' },
  {
    key: 'grp-smart',
    label: '지능형 거버넌스',
    type: 'group' as const,
    children: [
      { key: 'smart', label: '지능형 거버넌스', icon: <RobotOutlined /> },
    ],
  },
];

const DataGovernance: React.FC = () => {
  const [activeKey, setActiveKey] = useState('quality');

  const onMenuClick: MenuProps['onClick'] = ({ key }) => {
    setActiveKey(key);
  };

  return (
    <div>
      <Card>
        <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
          <SafetyCertificateOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
          데이터 거버넌스
        </Title>
        <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 14, color: '#6c757d' }}>
          품질 & 표준 · 메타데이터 · 보안 & 접근제어 · 지능형 거버넌스
        </Paragraph>
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
          <Suspense fallback={<Spin style={{ display: 'block', textAlign: 'center', padding: 48 }} />}>
            {(() => { const C = contentComponents[activeKey]; return C ? <C /> : null; })()}
          </Suspense>
        </Card>
      </div>
    </div>
  );
};

export default DataGovernance;
