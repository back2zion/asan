/**
 * 데이터 거버넌스 페이지
 * PRD DGR 요구사항 기반 - 4그룹 서브탭 구조
 */

import React from 'react';
import { Card, Tabs, Typography, Row, Col } from 'antd';
import {
  SafetyCertificateOutlined, CheckCircleOutlined,
  DatabaseOutlined, LockOutlined, BranchesOutlined, EyeInvisibleOutlined,
  BookOutlined, BarChartOutlined, AuditOutlined, AppstoreOutlined, SecurityScanOutlined, KeyOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import DataQualityTab from '../components/governance/DataQualityTab';
import MetadataTab from '../components/governance/MetadataTab';
import LineageTab from '../components/governance/LineageTab';
import StandardSecurityTab from '../components/governance/StandardSecurityTab';
import DeidentificationTab from '../components/governance/DeidentificationTab';
import StandardTermsTab from '../components/governance/StandardTermsTab';
import StandardIndicatorsTab from '../components/governance/StandardIndicatorsTab';
import MetadataManagement from '../components/governance/MetadataManagement';
import CatalogManagement from '../components/governance/CatalogManagement';
import SecurityManagement from '../components/governance/SecurityManagement';
import PermissionManagement from '../components/governance/PermissionManagement';
import SmartGovernanceTab from '../components/governance/SmartGovernanceTab';

const { Title, Paragraph } = Typography;

/* ── 그룹 1: 품질 & 표준 ── */
const QualityStandardsGroup: React.FC = () => (
  <Tabs
    defaultActiveKey="quality"
    items={[
      { key: 'quality', label: <span><CheckCircleOutlined /> 데이터 품질</span>, children: <DataQualityTab /> },
      { key: 'terms', label: <span><BookOutlined /> 용어 사전</span>, children: <StandardTermsTab /> },
      { key: 'indicators', label: <span><BarChartOutlined /> 표준 지표</span>, children: <StandardIndicatorsTab /> },
    ]}
  />
);

/* ── 그룹 2: 메타데이터 ── */
const MetadataGroup: React.FC = () => (
  <Tabs
    defaultActiveKey="metadata-edit"
    items={[
      { key: 'metadata-edit', label: <span><DatabaseOutlined /> 메타 편집</span>, children: <MetadataTab /> },
      { key: 'metadata-mgmt', label: <span><AuditOutlined /> 메타데이터 관리</span>, children: <MetadataManagement /> },
      { key: 'catalog-mgmt', label: <span><AppstoreOutlined /> 카탈로그 관리</span>, children: <CatalogManagement /> },
      { key: 'lineage', label: <span><BranchesOutlined /> 데이터 리니지</span>, children: <LineageTab /> },
    ]}
  />
);

/* ── 그룹 3: 보안 & 접근제어 ── */
const SecurityAccessGroup: React.FC = () => (
  <Tabs
    defaultActiveKey="standard-security"
    items={[
      { key: 'standard-security', label: <span><LockOutlined /> 표준/보안</span>, children: <StandardSecurityTab /> },
      { key: 'deidentification', label: <span><EyeInvisibleOutlined /> 비식별화</span>, children: <DeidentificationTab /> },
      { key: 'security-mgmt', label: <span><SecurityScanOutlined /> 보안 관리</span>, children: <SecurityManagement /> },
      { key: 'permission-mgmt', label: <span><KeyOutlined /> 권한 관리</span>, children: <PermissionManagement /> },
    ]}
  />
);

/* ── 최상위 탭 4개 ── */
const tabItems = [
  {
    key: 'quality-standards',
    label: <span><CheckCircleOutlined /> 품질 & 표준</span>,
    children: <QualityStandardsGroup />,
  },
  {
    key: 'metadata',
    label: <span><DatabaseOutlined /> 메타데이터</span>,
    children: <MetadataGroup />,
  },
  {
    key: 'security-access',
    label: <span><LockOutlined /> 보안 & 접근제어</span>,
    children: <SecurityAccessGroup />,
  },
  {
    key: 'smart',
    label: <span><RobotOutlined /> 지능형 거버넌스</span>,
    children: <SmartGovernanceTab />,
  },
];

const DataGovernance: React.FC = () => {
  return (
    <div>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SafetyCertificateOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              데이터 거버넌스
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              품질 & 표준 · 메타데이터 · 보안 & 접근제어 · 지능형 거버넌스
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Card>
        <Tabs items={tabItems} defaultActiveKey="quality-standards" />
      </Card>
    </div>
  );
};

export default DataGovernance;
