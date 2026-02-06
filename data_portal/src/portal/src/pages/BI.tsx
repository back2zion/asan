import React, { useState, useEffect, useCallback } from 'react';
import { Card, Typography, Button, Row, Col, Statistic, Spin, Space, Alert } from 'antd';
import {
  BarChartOutlined,
  PieChartOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  ExpandOutlined,
} from '@ant-design/icons';

const { Title, Text, Paragraph } = Typography;

const SUPERSET_URL = 'http://localhost:18088';

interface SupersetStats {
  charts: number | null;
  dashboards: number | null;
  datasets: number | null;
}

const BI: React.FC = () => {
  const [currentPage, setCurrentPage] = useState<string>('dashboard/list');
  const [loading, setLoading] = useState(true);
  const [iframeError, setIframeError] = useState(false);
  const [stats, setStats] = useState<SupersetStats>({ charts: null, dashboards: null, datasets: null });

  const fetchStats = useCallback(async () => {
    try {
      const resp = await fetch('http://localhost:8000/api/v1/superset/stats');
      if (resp.ok) {
        const data = await resp.json();
        setStats({
          charts: data.charts ?? 0,
          dashboards: data.dashboards ?? 0,
          datasets: data.datasets ?? 0,
        });
      }
    } catch {
      // Backend not accessible
    }
  }, []);

  useEffect(() => {
    fetchStats();
  }, [fetchStats]);

  const quickLinks = [
    { key: 'dashboard/list', label: '대시보드', icon: <DashboardOutlined /> },
    { key: 'chart/list', label: '차트', icon: <PieChartOutlined /> },
    { key: 'tablemodelview/list', label: '데이터셋', icon: <DatabaseOutlined /> },
    { key: 'sqllab', label: 'SQL Lab', icon: <BarChartOutlined /> },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 120px)' }}>
      {/* 헤더 */}
      <Card style={{ marginBottom: 12 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <BarChartOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              셀프서비스 BI 대시보드
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              셀프서비스 분석 플랫폼
            </Paragraph>
          </Col>
          <Col>
            <Space>
              {quickLinks.map(link => (
                <Button
                  key={link.key}
                  type={currentPage === link.key ? 'primary' : 'default'}
                  size="small"
                  icon={link.icon}
                  onClick={() => { setCurrentPage(link.key); setLoading(true); setIframeError(false); }}
                >
                  {link.label}
                </Button>
              ))}
              <Button
                icon={<ExpandOutlined />}
                href={`${SUPERSET_URL}/${currentPage}/`}
                target="_blank"
              >
                새 창
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 통계 */}
      <Row gutter={12} style={{ marginBottom: 12 }}>
        <Col xs={12} md={8}>
          <Card styles={{ body: { padding: '12px 16px' } }}>
            <Statistic
              title="차트"
              value={stats.charts ?? '-'}
              prefix={<PieChartOutlined />}
              valueStyle={{ fontSize: 20 }}
              loading={stats.charts === null}
            />
          </Card>
        </Col>
        <Col xs={12} md={8}>
          <Card styles={{ body: { padding: '12px 16px' } }}>
            <Statistic
              title="대시보드"
              value={stats.dashboards ?? '-'}
              prefix={<DashboardOutlined />}
              valueStyle={{ fontSize: 20 }}
              loading={stats.dashboards === null}
            />
          </Card>
        </Col>
        <Col xs={12} md={8}>
          <Card styles={{ body: { padding: '12px 16px' } }}>
            <Statistic
              title="데이터셋"
              value={stats.datasets ?? '-'}
              prefix={<DatabaseOutlined />}
              valueStyle={{ fontSize: 20 }}
              loading={stats.datasets === null}
            />
          </Card>
        </Col>
      </Row>

      {/* Superset 임베드 */}
      <Card
        styles={{ body: { padding: 0, height: '100%', position: 'relative' } }}
        style={{ flex: 1, overflow: 'hidden' }}
      >
        {loading && !iframeError && (
          <div style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            zIndex: 10,
            textAlign: 'center'
          }}>
            <Spin size="large" />
            <div style={{ marginTop: 16, color: '#666' }}>Superset 로딩 중...</div>
            <div style={{ marginTop: 8, color: '#999', fontSize: 12 }}>
              로그인: admin / admin
            </div>
          </div>
        )}
        {iframeError ? (
          <div style={{ padding: 24, textAlign: 'center' }}>
            <Alert
              type="warning"
              message="Superset 임베딩이 차단되었습니다"
              description={
                <div>
                  <p>Superset의 X-Frame-Options 설정으로 인해 임베딩이 불가합니다.</p>
                  <Button
                    type="primary"
                    icon={<ExpandOutlined />}
                    href={`${SUPERSET_URL}/${currentPage}/`}
                    target="_blank"
                    style={{ marginTop: 16 }}
                  >
                    새 창에서 Superset 열기
                  </Button>
                  <div style={{ marginTop: 8 }}>
                    <Text type="secondary">로그인: admin / admin</Text>
                  </div>
                </div>
              }
              style={{ maxWidth: 500, margin: '0 auto' }}
            />
          </div>
        ) : (
          <div style={{
            width: '100%',
            height: '100%',
            overflow: 'hidden',
          }}>
            <iframe
              src={`${SUPERSET_URL}/${currentPage}/?standalone=3`}
              style={{
                width: '100%',
                height: 'calc(100% + 56px)',
                border: 'none',
                minHeight: 556,
                marginTop: -56,
              }}
              title="Apache Superset"
              onLoad={() => setLoading(false)}
              onError={() => setIframeError(true)}
            />
          </div>
        )}
      </Card>
    </div>
  );
};

export default BI;
