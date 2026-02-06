/**
 * 데이터 거버넌스 페이지
 * PRD DGR 요구사항 기반 - 품질, 메타데이터, 리니지, 표준/보안, 비식별화
 */

import React from 'react';
import { Card, Tabs, Typography, Row, Col } from 'antd';
import {
  SafetyCertificateOutlined, CheckCircleOutlined,
  DatabaseOutlined, LockOutlined, BranchesOutlined, EyeInvisibleOutlined,
} from '@ant-design/icons';
import DataQualityTab from '../components/governance/DataQualityTab';
import MetadataTab from '../components/governance/MetadataTab';
import LineageTab from '../components/governance/LineageTab';
import StandardSecurityTab from '../components/governance/StandardSecurityTab';
import DeidentificationTab from '../components/governance/DeidentificationTab';

const { Title, Paragraph } = Typography;

const tabItems = [
  {
    key: 'quality',
    label: <span><CheckCircleOutlined /> 데이터 품질</span>,
    children: <DataQualityTab />,
  },
  {
    key: 'metadata',
    label: <span><DatabaseOutlined /> 메타데이터</span>,
    children: <MetadataTab />,
  },
  {
    key: 'lineage',
    label: <span><BranchesOutlined /> 데이터 리니지</span>,
    children: <LineageTab />,
  },
  {
    key: 'security',
    label: <span><LockOutlined /> 표준/보안</span>,
    children: <StandardSecurityTab />,
  },
  {
    key: 'deidentification',
    label: <span><EyeInvisibleOutlined /> 비식별화</span>,
    children: <DeidentificationTab />,
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
              OMOP CDM 데이터 품질 · 메타데이터 · 리니지 · 표준/보안 · 비식별화
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Card>
        <Tabs items={tabItems} defaultActiveKey="quality" />
      </Card>
    </div>
  );
};

export default DataGovernance;
