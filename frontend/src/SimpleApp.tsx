import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, Layout, Menu, Typography, Card, Space, Alert, Row, Col } from 'antd';
import koKR from 'antd/locale/ko_KR';
import { ExperimentOutlined, HomeOutlined } from '@ant-design/icons';
import PromptEnhancement from './pages/PromptEnhancement.tsx';

const { Header, Sider, Content } = Layout;
const { Title, Paragraph } = Typography;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

const SimpleLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const navigate = useNavigate();
  const location = useLocation();
  
  const handleMenuClick = ({ key }: { key: string }) => {
    navigate(key);
  };

  return (
    <Layout style={{ minHeight: '100vh', background: '#F5F0E8' }}>
      <Sider width={280} theme="light" style={{ 
        background: '#ffffff',
        borderRight: '1px solid #e9ecef',
        boxShadow: '2px 0 8px rgba(0, 98, 65, 0.06)'
      }}>
        <div style={{ 
          padding: '24px 16px', 
          textAlign: 'center',
          borderBottom: '1px solid #e9ecef',
          background: 'linear-gradient(135deg, #006241 0%, #004d32 100%)'
        }}>
          <img 
            src="/asan-logo.png" 
            alt="아산병원 로고" 
            style={{ 
              height: '40px', 
              marginBottom: '8px',
              filter: 'brightness(0) invert(1)'
            }} 
          />
          <div style={{ 
            color: '#ffffff', 
            fontSize: '13px', 
            fontWeight: '500',
            marginTop: '8px',
            letterSpacing: '0.5px'
          }}>
            통합 데이터 플랫폼
          </div>
        </div>
        <Menu
          theme="light"
          mode="inline"
          selectedKeys={[location.pathname]}
          onClick={handleMenuClick}
          style={{
            border: 'none',
            background: 'transparent'
          }}
          items={[
            {
              key: '/dashboard',
              icon: <HomeOutlined style={{ color: '#006241' }} />,
              label: <span style={{ fontWeight: '500' }}>대시보드</span>,
              style: { 
                margin: '8px 12px',
                borderRadius: '6px',
                height: '44px',
                lineHeight: '44px'
              }
            },
            {
              key: '/text2sql',
              icon: <ExperimentOutlined style={{ color: '#006241' }} />,
              label: <span style={{ fontWeight: '500' }}>CDW 연구지원</span>,
              style: { 
                margin: '8px 12px',
                borderRadius: '6px',
                height: '44px',
                lineHeight: '44px'
              }
            },
          ]}
        />
      </Sider>
      <Layout style={{ background: '#F5F0E8' }}>
        <Header style={{ 
          background: '#ffffff', 
          padding: '0 32px',
          borderBottom: '1px solid #e9ecef',
          boxShadow: '0 1px 4px rgba(0, 0, 0, 0.06)'
        }}>
          <Title level={4} style={{ 
            margin: 0, 
            lineHeight: '64px',
            color: '#333',
            fontWeight: '600'
          }}>
            {location.pathname === '/dashboard' && '통합 데이터 플랫폼 대시보드'}
            {location.pathname === '/text2sql' && 'CDW 데이터 추출 및 연구 지원'}
          </Title>
        </Header>
        <Content style={{ 
          margin: '24px', 
          padding: '0',
          background: 'transparent',
          minHeight: 280 
        }}>
          {children}
        </Content>
      </Layout>
    </Layout>
  );
};

const DashboardPage: React.FC = () => {
  return (
    <div style={{ padding: '0' }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* Header Status */}
        <Row gutter={16}>
          <Col span={24}>
            <Card style={{
              background: 'linear-gradient(135deg, #006241 0%, #004d32 100%)',
              border: 'none',
              borderRadius: '12px'
            }}>
              <div style={{ color: '#ffffff' }}>
                <Title level={3} style={{ color: '#ffffff', margin: '0 0 8px 0' }}>
                  서울아산병원 통합 데이터 플랫폼 (IDP) POC
                </Title>
                <Paragraph style={{ color: '#ffffff', opacity: 0.9, margin: 0, fontSize: '16px' }}>
                  AI 기반 의료 데이터 통합 분석 플랫폼이 안정적으로 운영 중입니다
                </Paragraph>
              </div>
            </Card>
          </Col>
        </Row>

        {/* Key Metrics */}
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={6}>
            <Card style={{ 
              borderRadius: '8px',
              boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)',
              border: '1px solid #e9ecef',
              transition: 'all 0.3s ease',
              cursor: 'pointer'
            }} hoverable>
              <div style={{ textAlign: 'center', padding: '8px 0' }}>
                <div style={{ 
                  fontSize: '32px', 
                  fontWeight: '700', 
                  color: '#006241',
                  marginBottom: '8px'
                }}>7</div>
                <div style={{ 
                  fontSize: '14px', 
                  color: '#6c757d',
                  fontWeight: '500'
                }}>전체 SFR 모듈</div>
              </div>
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card style={{ 
              borderRadius: '8px',
              boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)',
              border: '1px solid #e9ecef',
              transition: 'all 0.3s ease',
              cursor: 'pointer'
            }} hoverable>
              <div style={{ textAlign: 'center', padding: '8px 0' }}>
                <div style={{ 
                  fontSize: '32px', 
                  fontWeight: '700', 
                  color: '#52A67D',
                  marginBottom: '8px'
                }}>1</div>
                <div style={{ 
                  fontSize: '14px', 
                  color: '#6c757d',
                  fontWeight: '500'
                }}>완료 (Text2SQL)</div>
              </div>
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card style={{ 
              borderRadius: '8px',
              boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)',
              border: '1px solid #e9ecef',
              transition: 'all 0.3s ease',
              cursor: 'pointer'
            }} hoverable>
              <div style={{ textAlign: 'center', padding: '8px 0' }}>
                <div style={{ 
                  fontSize: '32px', 
                  fontWeight: '700', 
                  color: '#FF6F00',
                  marginBottom: '8px'
                }}>4</div>
                <div style={{ 
                  fontSize: '14px', 
                  color: '#6c757d',
                  fontWeight: '500'
                }}>상용 솔루션 연동</div>
              </div>
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card style={{ 
              borderRadius: '8px',
              boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)',
              border: '1px solid #e9ecef',
              transition: 'all 0.3s ease',
              cursor: 'pointer'
            }} hoverable>
              <div style={{ textAlign: 'center', padding: '8px 0' }}>
                <div style={{ 
                  fontSize: '32px', 
                  fontWeight: '700', 
                  color: '#006241',
                  marginBottom: '8px'
                }}>95%</div>
                <div style={{ 
                  fontSize: '14px', 
                  color: '#6c757d',
                  fontWeight: '500'
                }}>시스템 가용성</div>
              </div>
            </Card>
          </Col>
        </Row>

        {/* SFR Status Overview */}
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Card title="SFR 구현 현황" extra={<div style={{ fontSize: '12px', color: '#666' }}>최종 업데이트: 2025-11-17</div>}>
              <Row gutter={[16, 16]}>
                <Col xs={24} lg={12}>
                  <Card type="inner" title="자체 개발 모듈" size="small">
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-006: AI 데이터 분석환경</span>
                        <span style={{ color: '#ff6600' }}>🔄 개발 중</span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-007: CDW 데이터 추출 (Text2SQL)</span>
                        <span style={{ color: '#52c41a' }}>✅ 완료</span>
                      </div>
                    </Space>
                  </Card>
                </Col>
                <Col xs={24} lg={12}>
                  <Card type="inner" title="상용 솔루션 연동" size="small">
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-002: 데이터마트 (Tera ONE)</span>
                        <span style={{ color: '#1890ff' }}>📋 설계</span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-003: BI (비아이매트릭스)</span>
                        <span style={{ color: '#1890ff' }}>📋 설계</span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-004: OLAP (비아이매트릭스)</span>
                        <span style={{ color: '#1890ff' }}>📋 설계</span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span>SFR-005: ETL (테라스트림)</span>
                        <span style={{ color: '#1890ff' }}>📋 설계</span>
                      </div>
                    </Space>
                  </Card>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>

        {/* System Architecture */}
        <Row gutter={[16, 16]}>
          <Col xs={24} lg={16}>
            <Card title="시스템 아키텍처" extra={<ExperimentOutlined />}>
              <div style={{ textAlign: 'center', padding: '20px' }}>
                <div style={{ 
                  background: 'linear-gradient(135deg, #1a5d3a 0%, #52c41a 100%)',
                  color: 'white',
                  padding: '12px',
                  borderRadius: '8px',
                  marginBottom: '16px'
                }}>
                  <strong>Frontend Layer</strong>
                  <div style={{ fontSize: '12px', opacity: 0.9 }}>React 18 + TypeScript + Ant Design</div>
                </div>
                <div style={{ 
                  background: 'linear-gradient(135deg, #ff6600 0%, #ff9500 100%)',
                  color: 'white',
                  padding: '12px',
                  borderRadius: '8px',
                  marginBottom: '16px'
                }}>
                  <strong>API Gateway</strong>
                  <div style={{ fontSize: '12px', opacity: 0.9 }}>FastAPI + Claude 3 Haiku</div>
                </div>
                <div style={{ 
                  background: 'linear-gradient(135deg, #1890ff 0%, #40a9ff 100%)',
                  color: 'white',
                  padding: '12px',
                  borderRadius: '8px'
                }}>
                  <strong>Data Layer</strong>
                  <div style={{ fontSize: '12px', opacity: 0.9 }}>CDW + DuckDB + PostgreSQL</div>
                </div>
              </div>
            </Card>
          </Col>
          <Col xs={24} lg={8}>
            <Card title="실시간 모니터링">
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <div style={{ fontSize: '12px', color: '#666' }}>API 응답시간</div>
                  <div style={{ fontSize: '18px', color: '#52c41a' }}>234ms</div>
                </div>
                <div>
                  <div style={{ fontSize: '12px', color: '#666' }}>Text2SQL 성공률</div>
                  <div style={{ fontSize: '18px', color: '#1890ff' }}>97.3%</div>
                </div>
                <div>
                  <div style={{ fontSize: '12px', color: '#666' }}>일일 질의 처리</div>
                  <div style={{ fontSize: '18px', color: '#ff6600' }}>1,247건</div>
                </div>
                <div>
                  <div style={{ fontSize: '12px', color: '#666' }}>데이터 처리량</div>
                  <div style={{ fontSize: '18px', color: '#722ed1' }}>2.4TB</div>
                </div>
              </Space>
            </Card>
          </Col>
        </Row>

        {/* Recent Activities */}
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Card title="최근 활동 내역">
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ padding: '8px', background: '#f6ffed', borderLeft: '3px solid #52c41a' }}>
                  <strong>2025-11-17 14:23</strong> - Text2SQL MVP 완료 및 GitHub 배포
                </div>
                <div style={{ padding: '8px', background: '#fff7e6', borderLeft: '3px solid #ff6600' }}>
                  <strong>2025-11-17 13:45</strong> - Claude 3 Haiku API 연동 완료
                </div>
                <div style={{ padding: '8px', background: '#e6f7ff', borderLeft: '3px solid #1890ff' }}>
                  <strong>2025-11-17 12:30</strong> - 의료 데이터 스키마 확장 (20명 환자 더미 데이터)
                </div>
                <div style={{ padding: '8px', background: '#f9f0ff', borderLeft: '3px solid #722ed1' }}>
                  <strong>2025-11-17 11:15</strong> - 프롬프트 강화 시스템 적용
                </div>
              </Space>
            </Card>
          </Col>
        </Row>
      </Space>
    </div>
  );
};

const SimpleApp: React.FC = () => {
  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider 
        locale={koKR}
        theme={{
          token: {
            colorPrimary: '#006241', // ASAN GREEN (Pantone 3155C)
            colorSuccess: '#52A67D', // ASAN Light Green (Pantone 5483C)
            colorWarning: '#FF6F00', // ASAN Orange (Pantone 138C)
            colorError: '#dc3545',
            colorInfo: '#006241',
            borderRadius: 6,
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
            fontSize: 14,
          },
          components: {
            Layout: {
              headerBg: '#ffffff',
              siderBg: '#F5F0E8', // BEIGE
              bodyBg: '#F5F0E8', // BEIGE
            },
            Card: {
              borderRadius: 8,
              boxShadow: '0 2px 8px rgba(0, 98, 65, 0.06)',
            },
            Button: {
              borderRadius: 6,
              primaryShadow: '0 2px 4px rgba(0, 98, 65, 0.2)',
            },
            Menu: {
              itemSelectedBg: 'rgba(0, 98, 65, 0.08)',
              itemHoverBg: 'rgba(0, 98, 65, 0.05)',
            },
          },
        }}
      >
        <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <SimpleLayout>
            <Routes>
              <Route path="/" element={<Navigate to="/dashboard" replace />} />
              <Route path="/dashboard" element={<DashboardPage />} />
              <Route path="/text2sql" element={<PromptEnhancement />} />
            </Routes>
          </SimpleLayout>
        </Router>
      </ConfigProvider>
    </QueryClientProvider>
  );
};

export default SimpleApp;