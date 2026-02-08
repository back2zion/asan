/**
 * 서울아산병원 IDP 메인 대시보드
 * 모든 데이터를 /api/v1/portal-ops/home-dashboard에서 로딩
 */

import React, { useState, useEffect } from 'react';
import {
  Card,
  Row,
  Col,
  Typography,
  Input,
  Progress,
  List,
  Tag,
  Badge,
  Space,
  Button,
  Avatar,
  Tooltip,
  Spin,
} from 'antd';
import {
  SearchOutlined,
  BellOutlined,
  WarningOutlined,
  FileTextOutlined,
  DatabaseOutlined,
  RightOutlined,
  LeftOutlined,
  RobotOutlined,
  FireOutlined,
  FolderOutlined,
  ExperimentOutlined,
  MedicineBoxOutlined,
  HeartOutlined,
  DollarOutlined,
  TeamOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';

const { Title, Text } = Typography;

const BRAND = {
  PRIMARY: '#005BAC',
  SECONDARY: '#00A0B0',
  SUCCESS: '#52c41a',
  WARNING: '#faad14',
  ERROR: '#ff4d4f',
  GRAY: '#f5f5f5',
};

const DOMAIN_ICONS: Record<string, React.ReactNode> = {
  '임상데이터': <HeartOutlined />,
  '약물정보': <MedicineBoxOutlined />,
  '검사결과': <ExperimentOutlined />,
  '시술정보': <MedicineBoxOutlined />,
  '비용정보': <DollarOutlined />,
  '의료기관': <TeamOutlined />,
};

const DOMAIN_COLORS: Record<string, string> = {
  '임상데이터': '#eb2f96',
  '약물정보': '#52c41a',
  '검사결과': '#722ed1',
  '시술정보': '#fa8c16',
  '비용정보': '#13c2c2',
  '의료기관': '#1890ff',
};

const ANNOUNCE_ICONS: Record<string, React.ReactNode> = {
  info: <InfoCircleOutlined style={{ color: BRAND.PRIMARY }} />,
  warning: <WarningOutlined style={{ color: BRAND.WARNING }} />,
  notice: <BellOutlined style={{ color: BRAND.SECONDARY }} />,
  system: <DatabaseOutlined style={{ color: BRAND.PRIMARY }} />,
};

interface DashboardData {
  data_overview: {
    table_count: number;
    column_count: number;
    total_records: number;
    retention_pct: number;
  };
  quality: {
    overall_score: number;
    fill_rate: number;
    validity: number;
    accuracy: number;
    consistency: number;
  };
  monthly_data: number[];
  announcements: { type: string; title: string; date: string }[];
  popular_tables: { name: string; type: string; views: number }[];
  recent_chats: { role: string; content: string }[];
  interest_domains: { name: string; count: number; records: number }[];
}

const Home: React.FC = () => {
  const navigate = useNavigate();
  const [searchValue, setSearchValue] = useState('');
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/v1/portal-ops/home-dashboard')
      .then(res => { if (!res.ok) throw new Error(); return res.json(); })
      .then(d => setData(d))
      .catch(() => { /* API 실패 시 빈 대시보드 표시 */ })
      .finally(() => setLoading(false));
  }, []);

  const handleSearch = () => {
    if (searchValue.trim()) {
      navigate(`/catalog?q=${encodeURIComponent(searchValue)}`);
    }
  };

  if (loading) {
    return (
      <Spin size="large" tip="대시보드 로딩 중...">
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '60vh' }} />
      </Spin>
    );
  }

  const overview = data?.data_overview ?? { table_count: 0, column_count: 0, total_records: 0, retention_pct: 0 };
  const quality = data?.quality ?? { overall_score: 0, fill_rate: 0, validity: 0, accuracy: 0, consistency: 0 };
  const monthlyData = data?.monthly_data ?? [];
  const announcements = data?.announcements ?? [];
  const popularData = data?.popular_tables ?? [];
  const recentChats = data?.recent_chats ?? [];
  const interestData = data?.interest_domains ?? [];

  return (
    <div style={{ padding: '24px', backgroundColor: '#f0f2f5', minHeight: '100vh' }}>
      {/* 헤더 */}
      <div style={{ marginBottom: 24 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333' }}>
              Welcome to <span style={{ color: BRAND.PRIMARY }}>서울아산병원</span>
            </Title>
          </Col>
          <Col flex="auto" style={{ maxWidth: 400, marginLeft: 24 }}>
            <Input
              placeholder="통합검색 (진단 데이터)"
              prefix={<SearchOutlined style={{ color: '#bbb' }} />}
              value={searchValue}
              onChange={(e) => setSearchValue(e.target.value)}
              onPressEnter={handleSearch}
              style={{ borderRadius: 20 }}
              size="large"
            />
          </Col>
        </Row>
      </div>

      {/* 상단 3개 카드 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        {/* 데이터 보유현황 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><DatabaseOutlined style={{ color: BRAND.PRIMARY }} /><span>데이터 보유현황</span></Space>}
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <Row gutter={16}>
              <Col span={8}>
                <div style={{ textAlign: 'center' }}>
                  <Progress
                    type="dashboard"
                    percent={overview.retention_pct}
                    strokeColor={BRAND.PRIMARY}
                    size={100}
                    format={(percent) => (
                      <div>
                        <div style={{ fontSize: 24, fontWeight: 700, color: BRAND.PRIMARY }}>{percent}%</div>
                        <div style={{ fontSize: 10, color: '#999' }}>보유율</div>
                      </div>
                    )}
                  />
                  <div style={{ marginTop: 8 }}>
                    <Tag color="blue">테이블 {overview.table_count.toLocaleString()}</Tag>
                    <Tag color="green">컬럼 {overview.column_count.toLocaleString()}</Tag>
                  </div>
                </div>
              </Col>
              <Col span={16}>
                <div style={{ height: 150 }}>
                  <Text type="secondary" style={{ fontSize: 11 }}>월별 성공률 추이</Text>
                  <div style={{ display: 'flex', alignItems: 'flex-end', height: 120, gap: 4, marginTop: 8 }}>
                    {monthlyData.map((value, idx) => (
                      <Tooltip key={idx} title={`${idx + 1}월: ${value}%`}>
                        <div
                          style={{
                            flex: 1,
                            height: `${Math.max(value, 2)}%`,
                            backgroundColor: idx === monthlyData.length - 1 ? BRAND.PRIMARY : '#d9e8f5',
                            borderRadius: 2,
                            cursor: 'pointer',
                            transition: 'all 0.2s',
                          }}
                        />
                      </Tooltip>
                    ))}
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4 }}>
                    <Text type="secondary" style={{ fontSize: 10 }}>1월</Text>
                    <Text type="secondary" style={{ fontSize: 10 }}>12월</Text>
                  </div>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>

        {/* 데이터 품질현황 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><Badge status="processing" /><span>데이터 품질현황</span></Space>}
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <Row gutter={16}>
              <Col span={12}>
                <div style={{ textAlign: 'center' }}>
                  <Progress
                    type="circle"
                    percent={quality.overall_score}
                    strokeColor={{ '0%': BRAND.SECONDARY, '100%': BRAND.PRIMARY }}
                    size={120}
                    format={(percent) => (
                      <div>
                        <div style={{ fontSize: 28, fontWeight: 700 }}>{percent}%</div>
                        <div style={{ fontSize: 10, color: '#999' }}>품질점수</div>
                      </div>
                    )}
                  />
                </div>
              </Col>
              <Col span={12}>
                <div style={{ paddingTop: 16 }}>
                  {[
                    { label: '채움률', value: quality.fill_rate, color: '#52c41a' },
                    { label: '유효성', value: quality.validity, color: '#faad14' },
                    { label: '정확성', value: quality.accuracy, color: '#1890ff' },
                    { label: '일관성', value: quality.consistency, color: '#ff4d4f' },
                  ].map((item) => (
                    <div key={item.label} style={{ marginBottom: 12 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                        <Text style={{ fontSize: 12 }}>{item.label}</Text>
                        <Text strong style={{ fontSize: 12 }}>{item.value}%</Text>
                      </div>
                      <Progress percent={item.value} size="small" strokeColor={item.color} showInfo={false} />
                    </div>
                  ))}
                </div>
              </Col>
            </Row>
          </Card>
        </Col>

        {/* 데이터맵 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><FolderOutlined style={{ color: BRAND.SECONDARY }} /><span>데이터맵</span></Space>}
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <div style={{ height: 200 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-around', height: '100%' }}>
                {[
                  { label: 'Source', sub: `${overview.table_count} Tables`, color: '#1890ff', bg: '#e6f7ff' },
                  { label: 'Bronze', sub: 'Raw Data', color: '#CD7F32', bg: '#fff7e6' },
                  { label: 'Silver', sub: 'Cleaned', color: '#C0C0C0', bg: '#f5f5f5' },
                  { label: 'Gold', sub: 'Analytics', color: '#FFD700', bg: '#fffbe6' },
                ].map((zone, idx, arr) => (
                  <React.Fragment key={zone.label}>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{
                        width: 60, height: 60, borderRadius: 8,
                        backgroundColor: zone.bg, border: `2px solid ${zone.color}`,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        flexDirection: 'column', fontSize: 10,
                      }}>
                        <FolderOutlined style={{ fontSize: 20, color: zone.color }} />
                        <span>{zone.label}</span>
                      </div>
                      <Text type="secondary" style={{ fontSize: 10 }}>{zone.sub}</Text>
                    </div>
                    {idx < arr.length - 1 && <RightOutlined style={{ color: '#bbb' }} />}
                  </React.Fragment>
                ))}
              </div>
            </div>
          </Card>
        </Col>
      </Row>

      {/* 중간 3개 섹션 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        {/* 공지사항 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><BellOutlined style={{ color: BRAND.WARNING }} /><span>공지사항</span></Space>}
            style={{ height: 280 }}
            styles={{ body: { padding: '8px 16px' } }}
          >
            {announcements.length > 0 ? (
              <List
                dataSource={announcements}
                renderItem={(item) => (
                  <List.Item style={{ padding: '8px 0', borderBottom: '1px solid #f0f0f0' }}>
                    <Space>
                      {ANNOUNCE_ICONS[item.type] ?? <InfoCircleOutlined style={{ color: BRAND.PRIMARY }} />}
                      <div>
                        <Text style={{ fontSize: 13 }}>{item.title}</Text>
                        {item.date && (
                          <Text type="secondary" style={{ fontSize: 11, display: 'block' }}>{item.date}</Text>
                        )}
                      </div>
                    </Space>
                  </List.Item>
                )}
              />
            ) : (
              <div style={{ padding: 24, textAlign: 'center' }}>
                <Text type="secondary">등록된 공지사항이 없습니다</Text>
              </div>
            )}
          </Card>
        </Col>

        {/* 인기 테이블 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><FireOutlined style={{ color: '#ff4d4f' }} /><span>인기 테이블</span></Space>}
            style={{ height: 280 }}
            styles={{ body: { padding: '8px 16px' } }}
          >
            <List
              dataSource={popularData}
              renderItem={(item) => (
                <List.Item
                  style={{ padding: '8px 0', borderBottom: '1px solid #f0f0f0', cursor: 'pointer' }}
                  onClick={() => navigate(`/catalog?q=${encodeURIComponent(item.name)}`)}
                >
                  <Space>
                    <FileTextOutlined style={{ color: BRAND.PRIMARY }} />
                    <div>
                      <Text style={{ fontSize: 13 }}>{item.name}</Text>
                      <Text type="secondary" style={{ fontSize: 11, display: 'block' }}>
                        조회 {item.views.toLocaleString()}회
                      </Text>
                    </div>
                  </Space>
                </List.Item>
              )}
            />
          </Card>
        </Col>

        {/* 최근 대화 */}
        <Col xs={24} lg={8}>
          <Card
            title={<Space><RobotOutlined style={{ color: BRAND.SECONDARY }} /><span>최근 대화</span></Space>}
            extra={<Button type="link" size="small" onClick={() => navigate('/chat')}>전체보기</Button>}
            style={{ height: 280, backgroundColor: '#f8fffe' }}
            styles={{ body: { padding: '8px 16px', backgroundColor: '#f0faf9' } }}
          >
            <div style={{ height: 180, display: 'flex', flexDirection: 'column', gap: 8 }}>
              {recentChats.length > 0 ? recentChats.map((chat, idx) => (
                <div
                  key={idx}
                  style={{ display: 'flex', justifyContent: chat.role === 'user' ? 'flex-end' : 'flex-start' }}
                >
                  <div style={{
                    maxWidth: '80%', padding: '8px 12px',
                    borderRadius: chat.role === 'user' ? '12px 12px 4px 12px' : '12px 12px 12px 4px',
                    backgroundColor: chat.role === 'user' ? BRAND.PRIMARY : 'white',
                    color: chat.role === 'user' ? 'white' : '#333',
                    fontSize: 13, boxShadow: '0 1px 2px rgba(0,0,0,0.1)',
                  }}>
                    {chat.content}
                  </div>
                </div>
              )) : (
                <div style={{ textAlign: 'center', padding: 24 }}>
                  <Text type="secondary">아직 대화 이력이 없습니다</Text>
                </div>
              )}
            </div>
            <div style={{ marginTop: 8 }}>
              <Input
                placeholder="질문을 입력하세요..."
                suffix={<RobotOutlined style={{ color: BRAND.SECONDARY }} />}
                style={{ borderRadius: 20 }}
                onPressEnter={(e) => {
                  const value = (e.target as HTMLInputElement).value;
                  if (value.trim()) navigate(`/chat?q=${encodeURIComponent(value)}`);
                }}
              />
            </div>
          </Card>
        </Col>
      </Row>

      {/* 하단 - 관심데이터 */}
      <Card
        title={<Space><HeartOutlined style={{ color: '#eb2f96' }} /><span>관심데이터</span></Space>}
        extra={<Space><Button type="text" icon={<LeftOutlined />} size="small" /><Button type="text" icon={<RightOutlined />} size="small" /></Space>}
      >
        <div style={{ display: 'flex', gap: 16, overflowX: 'auto', paddingBottom: 8 }}>
          {interestData.map((item, idx) => {
            const color = DOMAIN_COLORS[item.name] || '#1890ff';
            return (
              <Card
                key={idx}
                hoverable
                style={{ minWidth: 160, borderRadius: 12, border: `1px solid ${color}20` }}
                styles={{ body: { padding: 16, textAlign: 'center' } }}
                onClick={() => navigate(`/catalog?domain=${encodeURIComponent(item.name)}`)}
              >
                <Avatar
                  size={48}
                  style={{ backgroundColor: `${color}20`, color, marginBottom: 12 }}
                  icon={DOMAIN_ICONS[item.name] ?? <FolderOutlined />}
                />
                <div>
                  <Text strong style={{ display: 'block', fontSize: 14 }}>{item.name}</Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>{item.count} 테이블</Text>
                </div>
              </Card>
            );
          })}
        </div>
      </Card>
    </div>
  );
};

export default Home;
