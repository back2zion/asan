/**
 * 메인 레이아웃 컴포넌트
 * DPR-001: AI Assistant 패널 통합
 */

import React, { useState } from 'react';
import { Layout, Menu, Typography, Button, Tooltip, Space, Input, Badge, Avatar, Dropdown } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  HomeOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  FundOutlined,
  ApiOutlined,
  RobotOutlined,
  ExperimentOutlined,
  SearchOutlined,
  BellOutlined,
  UserOutlined,
  SettingOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  AppstoreOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import AIAssistantPanel from '../ai/AIAssistantPanel';

const { Header, Sider, Content } = Layout;
const { Title, Text } = Typography;

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [aiPanelVisible, setAiPanelVisible] = useState(false);
  const [searchValue, setSearchValue] = useState('');

  const menuItems: MenuProps['items'] = [
    {
      key: '/dashboard',
      icon: <HomeOutlined />,
      label: '대시보드',
    },
    {
      key: '/catalog',
      icon: <AppstoreOutlined />,
      label: '데이터 카탈로그',
    },
    {
      type: 'divider',
    },
    {
      key: 'group-data',
      label: '데이터 관리',
      type: 'group',
      children: [
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
      key: 'group-analysis',
      label: '분석 & 시각화',
      type: 'group',
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
      key: 'group-ai',
      label: 'AI & 연구',
      type: 'group',
      children: [
        {
          key: '/ai-environment',
          icon: <RobotOutlined />,
          label: 'AI 분석환경',
        },
        {
          key: '/cdw',
          icon: <ExperimentOutlined />,
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

  const handleSearch = (value: string) => {
    if (value.trim()) {
      navigate(`/catalog?q=${encodeURIComponent(value.trim())}`);
    }
  };

  const userMenuItems: MenuProps['items'] = [
    {
      key: 'profile',
      icon: <UserOutlined />,
      label: '프로필',
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: '설정',
    },
    {
      type: 'divider',
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: '로그아웃',
      danger: true,
    },
  ];

  const getPageTitle = () => {
    const pathMap: Record<string, string> = {
      '/dashboard': '대시보드',
      '/catalog': '데이터 카탈로그',
      '/datamart': '데이터마트',
      '/etl': 'ETL 파이프라인',
      '/bi': 'BI 대시보드',
      '/olap': 'OLAP 분석',
      '/ai-environment': 'AI 분석환경',
      '/cdw': 'CDW 연구지원',
    };
    return pathMap[location.pathname] || '서울아산병원 IDP';
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {/* 사이드바 */}
      <Sider
        width={240}
        collapsedWidth={80}
        collapsed={collapsed}
        theme="light"
        style={{
          borderRight: '1px solid #e9ecef',
          position: 'fixed',
          left: 0,
          top: 0,
          bottom: 0,
          zIndex: 100,
          overflow: 'auto',
        }}
      >
        {/* 로고 */}
        <div
          style={{
            padding: collapsed ? '16px 8px' : '16px',
            textAlign: 'center',
            borderBottom: '1px solid #e9ecef',
            height: 64,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          {collapsed ? (
            <div
              style={{
                width: 40,
                height: 40,
                borderRadius: 8,
                backgroundColor: '#005BAC',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'white',
                fontWeight: 'bold',
                fontSize: 16,
              }}
            >
              IDP
            </div>
          ) : (
            <Space>
              <div
                style={{
                  width: 40,
                  height: 40,
                  borderRadius: 8,
                  backgroundColor: '#005BAC',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'white',
                  fontWeight: 'bold',
                  fontSize: 12,
                }}
              >
                IDP
              </div>
              <div style={{ textAlign: 'left' }}>
                <Text strong style={{ display: 'block', fontSize: 14 }}>
                  서울아산병원
                </Text>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  통합 데이터 플랫폼
                </Text>
              </div>
            </Space>
          )}
        </div>

        {/* 메뉴 */}
        <Menu
          theme="light"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={handleMenuClick}
          style={{ borderRight: 0, marginTop: 8 }}
        />
      </Sider>

      {/* 메인 컨텐츠 영역 */}
      <Layout style={{ marginLeft: collapsed ? 80 : 240, transition: 'margin-left 0.2s' }}>
        {/* 헤더 */}
        <Header
          style={{
            padding: '0 24px',
            background: '#fff',
            borderBottom: '1px solid #e9ecef',
            position: 'sticky',
            top: 0,
            zIndex: 99,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Space>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
            />
            <Title level={4} style={{ margin: 0 }}>
              {getPageTitle()}
            </Title>
          </Space>

          <Space size="middle">
            {/* 검색 */}
            <Input.Search
              placeholder="테이블, 컬럼 검색..."
              value={searchValue}
              onChange={(e) => setSearchValue(e.target.value)}
              onSearch={handleSearch}
              style={{ width: 280 }}
              allowClear
            />

            {/* AI Assistant 버튼 */}
            <Tooltip title="AI Assistant">
              <Badge dot={aiPanelVisible}>
                <Button
                  type={aiPanelVisible ? 'primary' : 'default'}
                  icon={<RobotOutlined />}
                  onClick={() => setAiPanelVisible(!aiPanelVisible)}
                  style={
                    aiPanelVisible
                      ? { backgroundColor: '#005BAC', borderColor: '#005BAC' }
                      : undefined
                  }
                >
                  AI 도우미
                </Button>
              </Badge>
            </Tooltip>

            {/* 알림 */}
            <Tooltip title="알림">
              <Badge count={3} size="small">
                <Button type="text" icon={<BellOutlined />} />
              </Badge>
            </Tooltip>

            {/* 사용자 메뉴 */}
            <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
              <Space style={{ cursor: 'pointer' }}>
                <Avatar size="small" icon={<UserOutlined />} />
                <Text>관리자</Text>
              </Space>
            </Dropdown>
          </Space>
        </Header>

        {/* 컨텐츠 */}
        <Content
          style={{
            margin: 24,
            padding: 24,
            minHeight: 'calc(100vh - 64px - 48px)',
            background: '#fff',
            borderRadius: 8,
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
