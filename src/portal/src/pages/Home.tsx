import React from 'react';
import { Card, Row, Col, Statistic, Typography, Timeline, List } from 'antd';
import {
  UserOutlined,
  FileTextOutlined,
  MessageOutlined,
  BarChartOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';

const { Title } = Typography;

const Home: React.FC = () => {
  const recentActivities = [
    {
      time: '2024-11-02 09:30',
      activity: 'AI 진단 보조 시스템을 통한 환자 X-ray 분석 완료',
      type: 'success'
    },
    {
      time: '2024-11-02 08:45',
      activity: '의료 문서 10건 자동 분류 및 인덱싱',
      type: 'info'
    },
    {
      time: '2024-11-01 16:20',
      activity: 'GraphRAG를 통한 의료 지식베이스 업데이트',
      type: 'success'
    },
    {
      time: '2024-11-01 14:15',
      activity: '멀티모달 AI 모델 성능 최적화 완료',
      type: 'success'
    }
  ];

  const quickActions = [
    {
      title: 'AI 진단 상담',
      description: '실시간 AI 진단 보조 시스템',
      icon: <MessageOutlined style={{ fontSize: '24px', color: '#1890ff' }} />
    },
    {
      title: '의료 영상 분석',
      description: 'X-ray, CT, MRI 영상 AI 분석',
      icon: <BarChartOutlined style={{ fontSize: '24px', color: '#52c41a' }} />
    },
    {
      title: '문서 처리',
      description: '의료 문서 자동 분류 및 요약',
      icon: <FileTextOutlined style={{ fontSize: '24px', color: '#fa8c16' }} />
    }
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>대시보드</Title>
      
      {/* 통계 카드 */}
      <Row gutter={[16, 16]} style={{ marginBottom: '32px' }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="금일 처리된 질의"
              value={147}
              prefix={<MessageOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="분석된 의료 영상"
              value={23}
              prefix={<BarChartOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="처리된 문서"
              value={89}
              prefix={<FileTextOutlined />}
              valueStyle={{ color: '#fa8c16' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="활성 사용자"
              value={12}
              prefix={<UserOutlined />}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        {/* 빠른 작업 */}
        <Col xs={24} lg={12}>
          <Card title="빠른 작업" style={{ height: '400px' }}>
            <List
              dataSource={quickActions}
              renderItem={(item) => (
                <List.Item style={{ cursor: 'pointer', padding: '16px 0' }}>
                  <List.Item.Meta
                    avatar={item.icon}
                    title={item.title}
                    description={item.description}
                  />
                </List.Item>
              )}
            />
          </Card>
        </Col>

        {/* 최근 활동 */}
        <Col xs={24} lg={12}>
          <Card title="최근 활동" style={{ height: '400px' }}>
            <Timeline
              items={recentActivities.map((activity, index) => ({
                key: index,
                dot: <ClockCircleOutlined style={{ fontSize: '16px' }} />,
                color: activity.type === 'success' ? 'green' : 'blue',
                children: (
                  <>
                    <p style={{ margin: '0 0 4px 0', fontSize: '14px', fontWeight: 500 }}>
                      {activity.activity}
                    </p>
                    <p style={{ margin: 0, fontSize: '12px', color: '#666' }}>
                      {activity.time}
                    </p>
                  </>
                )
              }))}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Home;