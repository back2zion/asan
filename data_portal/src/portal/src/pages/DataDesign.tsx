/**
 * DIT-001: 통합적인 데이터 분석, 구성 체계 수립
 * 5개 탭: 개요, 데이터 영역(Zone), 논리/물리 ERD, 용어 표준/명명 규칙, 비정형 데이터 구조화
 *
 * SFR-001 보완:
 *  - ODS/DW/DM 매핑 테이블 + Zone 파이프라인에 RFP 용어 병기
 *  - 확장성 로드맵 카드 (Phase1 배치→Phase2 스트리밍→Phase3 IoT)
 *  - ERD 그래프 시각화 (Canvas 기반)
 */
import React, { useState } from 'react';
import { Card, Button, Typography, Tabs, Row, Col } from 'antd';
import {
  ApartmentOutlined, ReloadOutlined, CloudServerOutlined,
  FundOutlined, SafetyCertificateOutlined, ExperimentOutlined,
} from '@ant-design/icons';
import {
  OverviewTab,
  ZonesTab,
  ERDTab,
  NamingTab,
  UnstructuredTab,
} from '../components/datadesign';

const { Title, Paragraph } = Typography;

const DataDesign: React.FC = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const [refreshKey, setRefreshKey] = useState(0);

  const handleRefresh = () => {
    setRefreshKey(prev => prev + 1);
  };

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <ApartmentOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
              데이터 설계
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 14, color: '#6c757d' }}>
              통합적인 데이터 분석 · 구성 체계 수립
            </Paragraph>
          </Col>
          <Col>
            <Button icon={<ReloadOutlined />} onClick={handleRefresh}>
              새로고침
            </Button>
          </Col>
        </Row>
      </Card>

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'overview',
            label: <><FundOutlined /> 개요</>,
            children: <OverviewTab key={`overview-${refreshKey}`} />,
          },
          {
            key: 'zones',
            label: <><CloudServerOutlined /> 데이터 영역</>,
            children: <ZonesTab key={`zones-${refreshKey}`} />,
          },
          {
            key: 'erd',
            label: <><ApartmentOutlined /> 논리/물리 ERD</>,
            children: <ERDTab key={`erd-${refreshKey}`} />,
          },
          {
            key: 'naming',
            label: <><SafetyCertificateOutlined /> 명명 규칙</>,
            children: <NamingTab key={`naming-${refreshKey}`} />,
          },
          {
            key: 'unstructured',
            label: <><ExperimentOutlined /> 비정형 구조화</>,
            children: <UnstructuredTab key={`unstructured-${refreshKey}`} />,
          },
        ]}
      />
    </div>
  );
};

export default DataDesign;
