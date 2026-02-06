import React from 'react';
import { Card, Typography, Alert, Button, Row, Col, Statistic } from 'antd';
import { BarChartOutlined, PieChartOutlined, LineChartOutlined, DashboardOutlined, DatabaseOutlined } from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const SUPERSET_URL = 'http://localhost:18088';

const BI: React.FC = () => {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0 }}>
              <BarChartOutlined style={{ marginRight: 8 }} />
              셀프서비스 BI 대시보드
            </Title>
            <Paragraph type="secondary" style={{ marginTop: 8 }}>
              DPR-004: Apache Superset 기반의 No-Code 시각화 및 BI 분석 환경입니다.
            </Paragraph>
          </Col>
          <Col>
            <Button
              type="primary"
              size="large"
              icon={<DashboardOutlined />}
              href={SUPERSET_URL}
              target="_blank"
            >
              Superset 열기
            </Button>
          </Col>
        </Row>
      </Card>

      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic title="등록된 차트" value={12} prefix={<PieChartOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="대시보드" value={3} prefix={<DashboardOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="데이터셋" value={8} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="활성 사용자" value={5} prefix={<LineChartOutlined />} />
          </Card>
        </Col>
      </Row>

      <Card title="빠른 링크">
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Button block href={`${SUPERSET_URL}/dashboard/list/`} target="_blank">
              <DashboardOutlined /> 대시보드 목록
            </Button>
          </Col>
          <Col span={8}>
            <Button block href={`${SUPERSET_URL}/chart/list/`} target="_blank">
              <PieChartOutlined /> 차트 목록
            </Button>
          </Col>
          <Col span={8}>
            <Button block href={`${SUPERSET_URL}/tablemodelview/list/`} target="_blank">
              <DatabaseOutlined /> 데이터셋 관리
            </Button>
          </Col>
        </Row>
      </Card>

      <Alert
        message="Apache Superset BI 플랫폼"
        description={
          <div>
            <p>
              Superset에서 직접 데이터 소스를 연결하고, 차트를 생성하며, 인터랙티브 대시보드를 구축할 수 있습니다.
              드래그-앤-드롭 인터페이스를 통해 복잡한 분석을 시각적으로 수행하세요.
            </p>
            <p>
              <strong>로그인 정보:</strong> admin / admin
            </p>
          </div>
        }
        type="info"
        showIcon
      />
    </div>
  );
};

export default BI;
