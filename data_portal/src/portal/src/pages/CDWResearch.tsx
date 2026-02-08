import React from 'react';
import { Card, Typography, Space, Row, Col, Badge, Tabs } from 'antd';
import { DatabaseOutlined, SearchOutlined, ExperimentOutlined, FileTextOutlined } from '@ant-design/icons';
import Text2SQLTab from '../components/cohort/Text2SQLTab';
import CohortBuilder from '../components/cohort/CohortBuilder';
import ChartReview from '../components/cohort/ChartReview';

const { Title, Paragraph } = Typography;

const CDWResearch: React.FC = () => {
  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* CDW Header */}
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <DatabaseOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              CDW 데이터 추출 및 연구 지원
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              Clinical Data Warehouse 통합 분석 플랫폼
            </Paragraph>
          </Col>
          <Col>
            <Space direction="vertical" size="small" style={{ textAlign: 'right' }}>
              <Badge status="processing" text={<span style={{ color: '#006241', fontWeight: '500' }}>시스템 정상</span>} />
              <Badge status="success" text={<span style={{ color: '#52A67D', fontWeight: '500' }}>데이터 품질 OK</span>} />
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 3-Tab Layout */}
      <Card>
        <Tabs
          defaultActiveKey="text2sql"
          size="large"
          items={[
            {
              key: 'text2sql',
              label: <span><SearchOutlined /> Text2SQL 질의</span>,
              children: <Text2SQLTab />,
            },
            {
              key: 'cohort',
              label: <span><ExperimentOutlined /> 코호트 빌더</span>,
              children: <CohortBuilder />,
            },
            {
              key: 'chart-review',
              label: <span><FileTextOutlined /> 차트 리뷰</span>,
              children: <ChartReview />,
            },
          ]}
        />
      </Card>
    </Space>
  );
};

export default CDWResearch;
