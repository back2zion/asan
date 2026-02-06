/**
 * 메인 레이아웃 컴포넌트
 * PRD 기반 UI - 상단 회색 헤더 + 청록색 사이드바
 */

import React, { useState } from 'react';
import { Layout, Menu, Typography, Button, Tooltip, Space, Badge, Avatar, Dropdown } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  HomeOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  FundOutlined,
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
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import AIAssistantPanel from '../ai/AIAssistantPanel';

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
  const [collapsed, setCollapsed] = useState(false);
  const [aiPanelVisible, setAiPanelVisible] = useState(false);

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
          key: '/catalog',
          icon: <AppstoreOutlined />,
          label: '데이터 카탈로그',
        },
        {
          key: '/datamart',
          icon: <DatabaseOutlined />,
          label: '데이터마트',
        },
        {
          key: '/etl',
          icon: <ApiOutlined />,
          label: 'ETL 파이프라인',
        },
      ],
    },
    {
      key: 'analysis',
      icon: <BarChartOutlined />,
      label: '분석 & 시각화',
      children: [
        {
          key: '/bi',
          icon: <BarChartOutlined />,
          label: 'BI 대시보드',
        },
        {
          key: '/olap',
          icon: <FundOutlined />,
          label: 'OLAP 분석',
        },
      ],
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

          <Tooltip title="알림">
            <Badge count={3} size="small">
              <Button
                type="text"
                icon={<BellOutlined style={{ fontSize: 18, color: 'white' }} />}
              />
            </Badge>
          </Tooltip>

          <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
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
            defaultOpenKeys={['data-management', 'analysis', 'ai-research']}
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
    </Layout>
  );
};

export default MainLayout;
