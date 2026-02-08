/**
 * AI 운영관리 페이지 (AAR-003)
 *
 * 4개 탭: 모델 관리 | 리소스 모니터링 | AI 안전성 | 감사 로그
 */
import React from 'react';
import { Card, Tabs, Typography, Row, Col } from 'antd';
import {
  SettingOutlined, RobotOutlined, DashboardOutlined,
  SafetyCertificateOutlined, FileSearchOutlined,
} from '@ant-design/icons';

import ModelManagementTab from './ModelManagementTab';
import ResourceMonitoringTab from './ResourceMonitoringTab';
import SafetyTab from './SafetyTab';
import AuditTrailTab from './AuditTrailTab';

const { Title, Paragraph } = Typography;

const tabItems = [
  { key: 'models', label: <span><RobotOutlined /> AI 모델 관리</span>, children: <ModelManagementTab /> },
  { key: 'resources', label: <span><DashboardOutlined /> 리소스 모니터링</span>, children: <ResourceMonitoringTab /> },
  { key: 'safety', label: <span><SafetyCertificateOutlined /> AI 안전성</span>, children: <SafetyTab /> },
  { key: 'audit', label: <span><FileSearchOutlined /> 감사 로그</span>, children: <AuditTrailTab /> },
];

const AIOps: React.FC = () => {
  return (
    <div>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#005BAC', marginRight: '12px', fontSize: '28px' }} />
              AI 운영관리
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              모델 설정 관리 · 연결 테스트 · 테스트 쿼리 · PII 보호 · 감사 로그
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Card style={{ marginTop: 16 }}>
        <Tabs items={tabItems} defaultActiveKey="models" destroyOnHidden />
      </Card>
    </div>
  );
};

export default AIOps;
