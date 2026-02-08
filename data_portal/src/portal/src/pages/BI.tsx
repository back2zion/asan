import React, { useState, useEffect } from 'react';
import { Card, Typography, Row, Col, Statistic, Tabs, Spin } from 'antd';
import {
  BarChartOutlined, PieChartOutlined, DashboardOutlined,
  CodeOutlined, FileTextOutlined, DatabaseOutlined,
} from '@ant-design/icons';
import { biApi } from '../services/biApi';
import SqlEditor from '../components/bi/SqlEditor';
import ChartBuilder from '../components/bi/ChartBuilder';
import DashboardBuilder from '../components/bi/DashboardBuilder';
import ReportExporter from '../components/bi/ReportExporter';

const { Title, Paragraph } = Typography;

interface BiStats {
  chart_count: number | null;
  dashboard_count: number | null;
  saved_query_count: number | null;
  query_history_count: number | null;
}

const BI: React.FC = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [stats, setStats] = useState<BiStats>({
    chart_count: null, dashboard_count: null, saved_query_count: null, query_history_count: null,
  });

  useEffect(() => {
    biApi.getOverview().then(data => {
      setStats({
        chart_count: data.chart_count ?? 0,
        dashboard_count: data.dashboard_count ?? 0,
        saved_query_count: data.saved_query_count ?? 0,
        query_history_count: data.query_history_count ?? 0,
      });
    }).catch(() => {});
  }, []);

  const statCards = [
    { title: '차트', value: stats.chart_count, icon: <PieChartOutlined />, color: '#006241' },
    { title: '대시보드', value: stats.dashboard_count, icon: <DashboardOutlined />, color: '#005BAC' },
    { title: '저장 쿼리', value: stats.saved_query_count, icon: <DatabaseOutlined />, color: '#52A67D' },
    { title: '실행 이력', value: stats.query_history_count, icon: <CodeOutlined />, color: '#FF6F00' },
  ];

  const tabItems = [
    {
      key: 'dashboard',
      label: <><DashboardOutlined /> 대시보드</>,
      children: <DashboardBuilder />,
    },
    {
      key: 'chart',
      label: <><BarChartOutlined /> 차트 빌더</>,
      children: <ChartBuilder />,
    },
    {
      key: 'sql',
      label: <><CodeOutlined /> SQL Editor</>,
      children: <SqlEditor />,
    },
    {
      key: 'report',
      label: <><FileTextOutlined /> 보고서</>,
      children: <ReportExporter />,
    },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 120px)' }}>
      {/* Header */}
      <Card style={{ marginBottom: 12 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <BarChartOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
              셀프서비스 BI 대시보드
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 15, color: '#6c757d' }}>
              No-code 차트 빌더, SQL Editor, 대시보드, 보고서 내보내기
            </Paragraph>
          </Col>
        </Row>
      </Card>

      {/* Stats */}
      <Row gutter={12} style={{ marginBottom: 12 }}>
        {statCards.map((s, i) => (
          <Col xs={12} md={6} key={i}>
            <Card styles={{ body: { padding: '12px 16px' } }}>
              <Statistic
                title={s.title}
                value={s.value ?? '-'}
                prefix={<span style={{ color: s.color }}>{s.icon}</span>}
                valueStyle={{ fontSize: 20 }}
                loading={s.value === null}
              />
            </Card>
          </Col>
        ))}
      </Row>

      {/* Tabs */}
      <Card style={{ flex: 1, overflow: 'hidden' }} styles={{ body: { height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' } }}>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={tabItems}
          style={{ flex: 1 }}
          tabBarStyle={{ marginBottom: 12 }}
        />
      </Card>
    </div>
  );
};

export default BI;
