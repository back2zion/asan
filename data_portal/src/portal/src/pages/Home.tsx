/**
 * 서울아산병원 IDP 메인 대시보드
 * PRD 기반 UI 구현
 */

import React, { useState } from 'react';
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
  ClockCircleOutlined,
  FolderOutlined,
  ExperimentOutlined,
  MedicineBoxOutlined,
  HeartOutlined,
  DollarOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';

const { Title, Text, Paragraph } = Typography;

// 아산병원 브랜드 컬러
const BRAND = {
  PRIMARY: '#005BAC',
  SECONDARY: '#00A0B0',
  SUCCESS: '#52c41a',
  WARNING: '#faad14',
  ERROR: '#ff4d4f',
  GRAY: '#f5f5f5',
};

const Home: React.FC = () => {
  const navigate = useNavigate();
  const [searchValue, setSearchValue] = useState('');

  // 공지사항 데이터
  const announcements = [
    {
      type: 'info',
      icon: <DatabaseOutlined style={{ color: BRAND.PRIMARY }} />,
      title: '도메인이 등록 시 사이버릴',
      date: '2026-01-15',
    },
    {
      type: 'warning',
      icon: <WarningOutlined style={{ color: BRAND.WARNING }} />,
      title: '승인자 사망인지팀',
      date: '2026-01-10 ~ 12/17/25',
    },
    {
      type: 'info',
      icon: <FileTextOutlined style={{ color: BRAND.PRIMARY }} />,
      title: '[DataIntro/meeting] SAMSUMG 승원',
      date: '',
    },
    {
      type: 'info',
      icon: <BellOutlined style={{ color: BRAND.SECONDARY }} />,
      title: '[데모저장] workflow 수정 요청',
      date: '',
    },
  ];

  // 인기/최신 자료
  const popularData = [
    { name: '입원 진료정보 요약', type: 'table', views: 1234 },
    { name: '6개 시스템 index- 파일 정의서', type: 'doc', views: 987 },
    { name: 'Gw DataSource', type: 'dataset', views: 756 },
    { name: '입원 진료정보 요약', type: 'table', views: 543 },
  ];

  // 최근 대화
  const recentChats = [
    { role: 'user', content: '당뇨환자 입원환자는 몇 명이야?' },
    { role: 'assistant', content: '당뇨병 진단을 받은 입원 환자는 총 1,234명입니다.' },
  ];

  // 관심 데이터 카테고리
  const interestData = [
    { name: 'AI Dataset', icon: <ExperimentOutlined />, color: '#722ed1', count: 156 },
    { name: '사망정보', icon: <HeartOutlined />, color: '#eb2f96', count: 89 },
    { name: '진료비 정보', icon: <DollarOutlined />, color: '#52c41a', count: 234 },
    { name: '임종돌봄', icon: <MedicineBoxOutlined />, color: '#fa8c16', count: 67 },
    { name: 'Radiology', icon: <FolderOutlined />, color: '#13c2c2', count: 445 },
    { name: '환자기본', icon: <TeamOutlined />, color: '#1890ff', count: 1203 },
  ];

  // 월별 데이터 (막대 차트용)
  const monthlyData = [95, 82, 90, 88, 92, 85, 78, 92, 88, 94, 90, 92];

  // 검색 핸들러
  const handleSearch = () => {
    if (searchValue.trim()) {
      navigate(`/catalog?q=${encodeURIComponent(searchValue)}`);
    }
  };

  return (
    <div style={{ padding: '24px', backgroundColor: '#f0f2f5', minHeight: '100vh' }}>
      {/* 헤더 - Welcome 메시지 & 검색 */}
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
            title={
              <Space>
                <DatabaseOutlined style={{ color: BRAND.PRIMARY }} />
                <span>데이터 보유현황</span>
              </Space>
            }
            extra={<Text type="secondary" style={{ fontSize: 12 }}>2026.01.29</Text>}
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <Row gutter={16}>
              <Col span={8}>
                <div style={{ textAlign: 'center' }}>
                  <Progress
                    type="dashboard"
                    percent={92}
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
                    <Tag color="blue">테이블 234</Tag>
                    <Tag color="green">컬럼 1,234</Tag>
                  </div>
                </div>
              </Col>
              <Col span={16}>
                <div style={{ height: 150 }}>
                  <Text type="secondary" style={{ fontSize: 11 }}>월별 데이터 증가 추이</Text>
                  <div style={{ display: 'flex', alignItems: 'flex-end', height: 120, gap: 4, marginTop: 8 }}>
                    {monthlyData.map((value, idx) => (
                      <Tooltip key={idx} title={`${idx + 1}월: ${value}%`}>
                        <div
                          style={{
                            flex: 1,
                            height: `${value}%`,
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
            title={
              <Space>
                <Badge status="processing" />
                <span>데이터 품질현황</span>
              </Space>
            }
            extra={<Text type="secondary" style={{ fontSize: 12 }}>2026.01.29</Text>}
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <Row gutter={16}>
              <Col span={12}>
                <div style={{ textAlign: 'center', position: 'relative' }}>
                  <Progress
                    type="circle"
                    percent={53}
                    strokeColor={{
                      '0%': BRAND.SECONDARY,
                      '100%': BRAND.PRIMARY,
                    }}
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
                  <div style={{ marginBottom: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text style={{ fontSize: 12 }}>채움률</Text>
                      <Text strong style={{ fontSize: 12 }}>78%</Text>
                    </div>
                    <Progress percent={78} size="small" strokeColor="#52c41a" showInfo={false} />
                  </div>
                  <div style={{ marginBottom: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text style={{ fontSize: 12 }}>유효성</Text>
                      <Text strong style={{ fontSize: 12 }}>45%</Text>
                    </div>
                    <Progress percent={45} size="small" strokeColor="#faad14" showInfo={false} />
                  </div>
                  <div style={{ marginBottom: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text style={{ fontSize: 12 }}>정확성</Text>
                      <Text strong style={{ fontSize: 12 }}>62%</Text>
                    </div>
                    <Progress percent={62} size="small" strokeColor="#1890ff" showInfo={false} />
                  </div>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text style={{ fontSize: 12 }}>일관성</Text>
                      <Text strong style={{ fontSize: 12 }}>34%</Text>
                    </div>
                    <Progress percent={34} size="small" strokeColor="#ff4d4f" showInfo={false} />
                  </div>
                </div>
              </Col>
            </Row>
          </Card>
        </Col>

        {/* 데이터맵 */}
        <Col xs={24} lg={8}>
          <Card
            title={
              <Space>
                <FolderOutlined style={{ color: BRAND.SECONDARY }} />
                <span>데이터맵</span>
              </Space>
            }
            style={{ height: 280 }}
            styles={{ body: { padding: '12px 16px' } }}
          >
            <div style={{ height: 200 }}>
              {/* 간단한 데이터 플로우 다이어그램 */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-around', height: '100%' }}>
                {/* Data Source */}
                <div style={{ textAlign: 'center' }}>
                  <div style={{
                    width: 60, height: 60, borderRadius: 8,
                    backgroundColor: '#e6f7ff', border: '2px solid #1890ff',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexDirection: 'column', fontSize: 10,
                  }}>
                    <DatabaseOutlined style={{ fontSize: 20, color: '#1890ff' }} />
                    <span>Source</span>
                  </div>
                  <Text type="secondary" style={{ fontSize: 10 }}>12 Sources</Text>
                </div>

                <RightOutlined style={{ color: '#bbb' }} />

                {/* Bronze */}
                <div style={{ textAlign: 'center' }}>
                  <div style={{
                    width: 60, height: 60, borderRadius: 8,
                    backgroundColor: '#fff7e6', border: '2px solid #CD7F32',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexDirection: 'column', fontSize: 10,
                  }}>
                    <FolderOutlined style={{ fontSize: 20, color: '#CD7F32' }} />
                    <span>Bronze</span>
                  </div>
                  <Text type="secondary" style={{ fontSize: 10 }}>Raw Data</Text>
                </div>

                <RightOutlined style={{ color: '#bbb' }} />

                {/* Silver */}
                <div style={{ textAlign: 'center' }}>
                  <div style={{
                    width: 60, height: 60, borderRadius: 8,
                    backgroundColor: '#f5f5f5', border: '2px solid #C0C0C0',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexDirection: 'column', fontSize: 10,
                  }}>
                    <FolderOutlined style={{ fontSize: 20, color: '#C0C0C0' }} />
                    <span>Silver</span>
                  </div>
                  <Text type="secondary" style={{ fontSize: 10 }}>Cleaned</Text>
                </div>

                <RightOutlined style={{ color: '#bbb' }} />

                {/* Gold */}
                <div style={{ textAlign: 'center' }}>
                  <div style={{
                    width: 60, height: 60, borderRadius: 8,
                    backgroundColor: '#fffbe6', border: '2px solid #FFD700',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexDirection: 'column', fontSize: 10,
                  }}>
                    <FolderOutlined style={{ fontSize: 20, color: '#FFD700' }} />
                    <span>Gold</span>
                  </div>
                  <Text type="secondary" style={{ fontSize: 10 }}>Analytics</Text>
                </div>
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
            title={
              <Space>
                <BellOutlined style={{ color: BRAND.WARNING }} />
                <span>공지사항</span>
              </Space>
            }
            style={{ height: 280 }}
            styles={{ body: { padding: '8px 16px' } }}
          >
            <List
              dataSource={announcements}
              renderItem={(item) => (
                <List.Item style={{ padding: '8px 0', borderBottom: '1px solid #f0f0f0' }}>
                  <Space>
                    {item.icon}
                    <div>
                      <Text style={{ fontSize: 13 }}>{item.title}</Text>
                      {item.date && (
                        <Text type="secondary" style={{ fontSize: 11, display: 'block' }}>
                          {item.date}
                        </Text>
                      )}
                    </div>
                  </Space>
                </List.Item>
              )}
            />
          </Card>
        </Col>

        {/* 인기/최신자료 */}
        <Col xs={24} lg={8}>
          <Card
            title={
              <Space>
                <FireOutlined style={{ color: '#ff4d4f' }} />
                <span>인기/최신자료</span>
              </Space>
            }
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

        {/* 최근 대화 (AI Assistant 미니) */}
        <Col xs={24} lg={8}>
          <Card
            title={
              <Space>
                <RobotOutlined style={{ color: BRAND.SECONDARY }} />
                <span>최근 대화</span>
              </Space>
            }
            extra={
              <Button type="link" size="small" onClick={() => navigate('/chat')}>
                전체보기
              </Button>
            }
            style={{ height: 280, backgroundColor: '#f8fffe' }}
            styles={{ body: { padding: '8px 16px', backgroundColor: '#f0faf9' } }}
          >
            <div style={{ height: 180, display: 'flex', flexDirection: 'column', gap: 8 }}>
              {recentChats.map((chat, idx) => (
                <div
                  key={idx}
                  style={{
                    display: 'flex',
                    justifyContent: chat.role === 'user' ? 'flex-end' : 'flex-start',
                  }}
                >
                  <div
                    style={{
                      maxWidth: '80%',
                      padding: '8px 12px',
                      borderRadius: chat.role === 'user' ? '12px 12px 4px 12px' : '12px 12px 12px 4px',
                      backgroundColor: chat.role === 'user' ? BRAND.PRIMARY : 'white',
                      color: chat.role === 'user' ? 'white' : '#333',
                      fontSize: 13,
                      boxShadow: '0 1px 2px rgba(0,0,0,0.1)',
                    }}
                  >
                    {chat.content}
                  </div>
                </div>
              ))}
            </div>
            <div style={{ marginTop: 8 }}>
              <Input
                placeholder="질문을 입력하세요..."
                suffix={<RobotOutlined style={{ color: BRAND.SECONDARY }} />}
                style={{ borderRadius: 20 }}
                onPressEnter={(e) => {
                  const value = (e.target as HTMLInputElement).value;
                  if (value.trim()) {
                    navigate(`/chat?q=${encodeURIComponent(value)}`);
                  }
                }}
              />
            </div>
          </Card>
        </Col>
      </Row>

      {/* 하단 - 관심데이터 */}
      <Card
        title={
          <Space>
            <HeartOutlined style={{ color: '#eb2f96' }} />
            <span>관심데이터</span>
          </Space>
        }
        extra={
          <Space>
            <Button type="text" icon={<LeftOutlined />} size="small" />
            <Button type="text" icon={<RightOutlined />} size="small" />
          </Space>
        }
      >
        <div style={{ display: 'flex', gap: 16, overflowX: 'auto', paddingBottom: 8 }}>
          {interestData.map((item, idx) => (
            <Card
              key={idx}
              hoverable
              style={{
                minWidth: 160,
                borderRadius: 12,
                border: `1px solid ${item.color}20`,
              }}
              styles={{ body: { padding: 16, textAlign: 'center' } }}
              onClick={() => navigate(`/catalog?domain=${encodeURIComponent(item.name)}`)}
            >
              <Avatar
                size={48}
                style={{ backgroundColor: `${item.color}20`, color: item.color, marginBottom: 12 }}
                icon={item.icon}
              />
              <div>
                <Text strong style={{ display: 'block', fontSize: 14 }}>{item.name}</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>{item.count} 테이블</Text>
              </div>
            </Card>
          ))}
        </div>
      </Card>
    </div>
  );
};

export default Home;
