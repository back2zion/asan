/**
 * 리소스 현황 카드
 */

import React from 'react';
import { Card, Row, Col, Typography, Progress, Statistic, Spin } from 'antd';

const { Text } = Typography;

interface ResourceOverviewProps {
  realSystemInfo: any;
}

const ResourceOverview: React.FC<ResourceOverviewProps> = ({ realSystemInfo }) => {
  if (!realSystemInfo) {
    return (
      <Row gutter={[16, 16]}>
        <Col span={24}>
          <Card style={{ textAlign: 'center', padding: '40px 0' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text>리소스 정보를 불러오고 있습니다...</Text>
            </div>
          </Card>
        </Col>
      </Row>
    );
  }

  const items = [
    { title: 'GPU 사용률', value: realSystemInfo.gpu.usage_percent, color: '#FF6F00', desc: realSystemInfo.gpu.description },
    { title: 'CPU 사용률', value: realSystemInfo.cpu.usage_percent, color: '#006241', desc: realSystemInfo.cpu.description },
    { title: '메모리 사용률', value: realSystemInfo.memory.usage_percent, color: '#52A67D', desc: realSystemInfo.memory.description },
  ];

  return (
    <Row gutter={[16, 16]}>
      {items.map((item) => (
        <Col xs={24} sm={8} key={item.title}>
          <Card style={{ textAlign: 'center' }}>
            <Statistic
              title={item.title}
              value={item.value}
              precision={1}
              suffix="%"
              valueStyle={{ color: item.color, fontSize: '24px', fontWeight: '700' }}
            />
            <Progress percent={item.value} strokeColor={item.color} size="small" />
            <Text type="secondary" style={{ fontSize: '12px' }}>{item.desc}</Text>
          </Card>
        </Col>
      ))}
    </Row>
  );
};

export default ResourceOverview;
