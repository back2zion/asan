/**
 * 메인 레이아웃 컴포넌트
 * PRD 기반 UI - 상단 회색 헤더 + 청록색 사이드바
 */

import React, { useState, useEffect } from 'react';
import { Layout, Menu, Typography, Button, Tooltip, Space, Badge, Avatar, Dropdown, Drawer, Modal, List, Tag, Switch, Divider, App } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  HomeOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  SafetyCertificateOutlined,
  ApiOutlined,
  RobotOutlined,
  ExperimentOutlined,
  BellOutlined,
  UserOutlined,
  SettingOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  AppstoreOutlined,
  FileTextOutlined,
  FilePptOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  MailOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import AIAssistantPanel from '../ai/AIAssistantPanel';
import { useSettings } from '../../contexts/SettingsContext';

const { Header, Sider, Content } = Layout;
const { Text } = Typography;

// PRD 기반 컬러
const COLORS = {
  HEADER_BG: '#4a5568',       // 상단 헤더 짙은 회색
  SIDEBAR_BG: '#1a4d3e',      // 사이드바 짙은 녹색 (아산병원 녹색)
  PRIMARY: '#005BAC',
  SECONDARY: '#00A0B0',
  TEXT_MUTED: '#8fbfaa',
};

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { settings, updateSetting } = useSettings();
  const { message, modal } = App.useApp();
  const [collapsed, setCollapsed] = useState(false);
  const [aiPanelVisible, setAiPanelVisible] = useState(false);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);

  // AI 도우미 자동 실행
  useEffect(() => {
    if (settings.aiAutoOpen) setAiPanelVisible(true);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const notifications = [
    { id: 1, type: 'success', title: 'ETL 파이프라인 완료', desc: 'OMOP CDM 일일 적재 완료 (133만 건)', time: '10분 전' },
    { id: 2, type: 'warning', title: '데이터 품질 경고', desc: 'measurement.value_as_number NULL 비율 100%', time: '1시간 전' },
    { id: 3, type: 'info', title: '시스템 업데이트', desc: 'CDM 변환 요약 대시보드가 추가되었습니다', time: '2시간 전' },
  ];

  const handleUserMenuClick = ({ key }: { key: string }) => {
    if (key === 'profile') setProfileOpen(true);
    else if (key === 'settings') setSettingsOpen(true);
    else if (key === 'logout') {
      modal.confirm({
        title: '로그아웃',
        content: '정말 로그아웃 하시겠습니까?',
        okText: '로그아웃',
        cancelText: '취소',
        okButtonProps: { danger: true },
        onOk: () => message.success('로그아웃 되었습니다 (데모)'),
      });
    }
  };

  const menuItems: MenuProps['items'] = [
    {
      key: '/dashboard',
      icon: <HomeOutlined />,
      label: '홈',
    },
    {
      key: 'data-management',
      icon: <DatabaseOutlined />,
      label: '데이터 관리',
      children: [
        {
          key: '/etl',
          icon: <ApiOutlined />,
          label: 'ETL 파이프라인',
        },
        {
          key: '/governance',
          icon: <SafetyCertificateOutlined />,
          label: '데이터 거버넌스',
        },
        {
          key: '/catalog',
          icon: <AppstoreOutlined />,
          label: '데이터 카탈로그',
        },
        {
          key: '/datamart',
          icon: <DatabaseOutlined />,
          label: '데이터마트',
        },
      ],
    },
    {
      key: '/bi',
      icon: <BarChartOutlined />,
      label: 'BI 대시보드',
    },
    {
      key: 'ai-research',
      icon: <ExperimentOutlined />,
      label: 'AI & 연구',
      children: [
        {
          key: '/ai-environment',
          icon: <RobotOutlined />,
          label: 'AI 분석환경',
        },
        {
          key: '/cdw',
          icon: <FileTextOutlined />,
          label: 'CDW 연구지원',
        },
        // {
        //   key: '/presentation',
        //   icon: <FilePptOutlined />,
        //   label: '프레젠테이션',
        // },
        {
          key: '/ner',
          icon: <ExperimentOutlined />,
          label: '비정형 구조화',
        },
      ],
    },
  ];

  const handleMenuClick = ({ key }: { key: string }) => {
    if (key.startsWith('/')) {
      navigate(key);
    }
  };

  const userMenuItems: MenuProps['items'] = [
    { key: 'profile', icon: <UserOutlined />, label: '프로필' },
    { key: 'settings', icon: <SettingOutlined />, label: '설정' },
    { type: 'divider' },
    { key: 'logout', icon: <LogoutOutlined />, label: '로그아웃', danger: true },
  ];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {/* ===== 상단 전체 회색 헤더 ===== */}
      <Header
        style={{
          background: COLORS.HEADER_BG,
          padding: '0 24px',
          height: 56,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          zIndex: 1000,
        }}
      >
        {/* 좌측: 로고 */}
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <img
            src="/src/assets/asan_logo_full.png"
            alt="서울아산병원"
            style={{
              height: 32,
              objectFit: 'contain',
              filter: 'brightness(0) invert(1)',  // 흰색으로 변환
            }}
          />
        </div>

        {/* 우측: 버튼들 */}
        <Space size="middle">
          <Tooltip title="AI Assistant">
            <Button
              type={aiPanelVisible ? 'primary' : 'default'}
              icon={<RobotOutlined />}
              onClick={() => setAiPanelVisible(!aiPanelVisible)}
              style={{
                background: aiPanelVisible ? COLORS.SECONDARY : 'transparent',
                borderColor: aiPanelVisible ? COLORS.SECONDARY : 'rgba(255,255,255,0.3)',
                color: 'white',
              }}
            >
              AI 도우미
            </Button>
          </Tooltip>

          <Tooltip title={settings.notificationsEnabled ? '알림' : '알림 꺼짐'}>
            <Badge count={settings.notificationsEnabled ? notifications.length : 0} size="small">
              <Button
                type="text"
                icon={<BellOutlined style={{ fontSize: 18, color: settings.notificationsEnabled ? 'white' : 'rgba(255,255,255,0.3)' }} />}
                onClick={() => {
                  if (settings.notificationsEnabled) setNotificationOpen(true);
                  else message.info('알림 수신이 비활성화 상태입니다. 설정에서 활성화하세요.');
                }}
              />
            </Badge>
          </Tooltip>

          <Dropdown menu={{ items: userMenuItems, onClick: handleUserMenuClick }} placement="bottomRight">
            <Space style={{ cursor: 'pointer' }}>
              <Avatar size={32} style={{ background: COLORS.SECONDARY }}>
                <UserOutlined />
              </Avatar>
              <Text style={{ color: 'white', fontSize: 13 }}>관리자</Text>
            </Space>
          </Dropdown>
        </Space>
      </Header>

      {/* ===== 하단 영역 (사이드바 + 컨텐츠) ===== */}
      <Layout style={{ marginTop: 56 }}>
        {/* 사이드바 - 청록색 */}
        <Sider
          width={220}
          collapsedWidth={64}
          collapsed={collapsed}
          style={{
            background: COLORS.SIDEBAR_BG,
            position: 'fixed',
            left: 0,
            top: 56,
            bottom: 0,
            zIndex: 100,
            overflow: 'auto',
          }}
        >
          {/* 메뉴 - 트리 구조 */}
          <style>
            {`
              .sidebar-menu .ant-menu-sub {
                background: #153d32 !important;
              }
              .sidebar-menu .ant-menu-item:hover,
              .sidebar-menu .ant-menu-submenu-title:hover {
                background: #2a5d4d !important;
              }
              .sidebar-menu .ant-menu-item-selected {
                background: #2a5d4d !important;
              }
            `}
          </style>
          <Menu
            className="sidebar-menu"
            mode="inline"
            selectedKeys={[location.pathname]}
            defaultOpenKeys={['data-management', 'ai-research']}
            items={menuItems}
            onClick={handleMenuClick}
            style={{
              background: 'transparent',
              borderRight: 'none',
              marginTop: 8,
            }}
            theme="dark"
          />

          {/* 하단 - 접기 버튼 */}
          <div style={{
            position: 'absolute',
            bottom: 0,
            left: 0,
            right: 0,
            padding: '12px 16px',
            borderTop: '1px solid rgba(255,255,255,0.15)',
          }}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
              style={{ color: COLORS.TEXT_MUTED, width: '100%', textAlign: 'left' }}
            >
              {!collapsed && '메뉴 접기'}
            </Button>
          </div>
        </Sider>

        {/* 메인 컨텐츠 */}
        <Content
          style={{
            marginLeft: collapsed ? 64 : 220,
            padding: 24,
            background: '#f0f2f5',
            minHeight: 'calc(100vh - 56px)',
            transition: 'margin-left 0.2s',
          }}
        >
          <Outlet />
        </Content>
      </Layout>

      {/* AI Assistant 패널 */}
      <AIAssistantPanel
        visible={aiPanelVisible}
        onClose={() => setAiPanelVisible(false)}
        currentContext={{
          currentTable: undefined,
          currentColumns: undefined,
          userRole: 'admin',
        }}
      />

      {/* 알림 Drawer */}
      <Drawer
        title={<><BellOutlined /> 알림 센터</>}
        placement="right"
        width={380}
        open={notificationOpen}
        onClose={() => setNotificationOpen(false)}
      >
        <List
          dataSource={notifications}
          renderItem={(item) => (
            <List.Item style={{ padding: '12px 0' }}>
              <List.Item.Meta
                avatar={
                  item.type === 'success' ? <CheckCircleOutlined style={{ fontSize: 20, color: '#52c41a' }} /> :
                  item.type === 'warning' ? <WarningOutlined style={{ fontSize: 20, color: '#faad14' }} /> :
                  <InfoCircleOutlined style={{ fontSize: 20, color: '#1890ff' }} />
                }
                title={<span style={{ fontSize: 13 }}>{item.title}</span>}
                description={
                  <>
                    <div style={{ fontSize: 12, color: '#666' }}>{item.desc}</div>
                    <div style={{ fontSize: 11, color: '#aaa', marginTop: 4 }}><ClockCircleOutlined /> {item.time}</div>
                  </>
                }
              />
            </List.Item>
          )}
        />
      </Drawer>

      {/* 프로필 Modal */}
      <Modal
        title="관리자 프로필"
        open={profileOpen}
        onCancel={() => setProfileOpen(false)}
        footer={<Button onClick={() => setProfileOpen(false)}>닫기</Button>}
        width={420}
      >
        <div style={{ textAlign: 'center', padding: '16px 0' }}>
          <Avatar size={72} style={{ background: COLORS.SECONDARY, fontSize: 28 }}>
            <UserOutlined />
          </Avatar>
          <h3 style={{ marginTop: 12, marginBottom: 4 }}>시스템 관리자</h3>
          <Tag color="green">Admin</Tag>
        </div>
        <Divider />
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <Text type="secondary"><MailOutlined /> 이메일</Text>
            <Text>admin@amc.seoul.kr</Text>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <Text type="secondary"><UserOutlined /> 부서</Text>
            <Text>의료정보실</Text>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <Text type="secondary"><SafetyCertificateOutlined /> 권한</Text>
            <Tag color="blue">전체 관리자</Tag>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <Text type="secondary"><ClockCircleOutlined /> 마지막 접속</Text>
            <Text>{new Date().toLocaleString('ko-KR')}</Text>
          </div>
        </div>
      </Modal>

      {/* 설정 Modal */}
      <Modal
        title={<><SettingOutlined /> 시스템 설정</>}
        open={settingsOpen}
        onCancel={() => setSettingsOpen(false)}
        footer={<Button onClick={() => setSettingsOpen(false)}>닫기</Button>}
        width={480}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20, padding: '8px 0' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontWeight: 600 }}>다크 모드</div>
              <div style={{ fontSize: 12, color: '#999' }}>어두운 테마 사용</div>
            </div>
            <Switch size="small" checked={settings.darkMode} onChange={(v) => updateSetting('darkMode', v)} />
          </div>
          <Divider style={{ margin: 0 }} />
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontWeight: 600 }}>알림 수신</div>
              <div style={{ fontSize: 12, color: '#999' }}>시스템 알림 및 경고 수신</div>
            </div>
            <Switch size="small" checked={settings.notificationsEnabled} onChange={(v) => { updateSetting('notificationsEnabled', v); message.info(v ? '알림 활성화' : '알림 비활성화'); }} />
          </div>
          <Divider style={{ margin: 0 }} />
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontWeight: 600 }}>자동 새로고침</div>
              <div style={{ fontSize: 12, color: '#999' }}>대시보드 10초 주기 갱신</div>
            </div>
            <Switch size="small" checked={settings.autoRefresh} onChange={(v) => { updateSetting('autoRefresh', v); message.info(v ? '자동 새로고침 켜짐' : '자동 새로고침 꺼짐'); }} />
          </div>
          <Divider style={{ margin: 0 }} />
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontWeight: 600 }}>AI 도우미 자동 실행</div>
              <div style={{ fontSize: 12, color: '#999' }}>페이지 진입 시 AI 패널 자동 열기</div>
            </div>
            <Switch size="small" checked={settings.aiAutoOpen} onChange={(v) => { updateSetting('aiAutoOpen', v); if (v) setAiPanelVisible(true); }} />
          </div>
        </div>
      </Modal>
    </Layout>
  );
};

export default MainLayout;
