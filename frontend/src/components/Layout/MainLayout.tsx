import React from 'react';
import { Layout, Menu, Space, Typography } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  DashboardOutlined,
  DatabaseOutlined,
  ExperimentOutlined,
  CloudServerOutlined,
} from '@ant-design/icons';

const { Header, Sider, Content } = Layout;
const { Title } = Typography;

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    {
      key: '/dashboard',
      icon: <DashboardOutlined />,
      label: '대시보드',
    },
    {
      type: 'divider' as const,
    },
    {
      key: 'mvp-group',
      label: 'MVP 기능',
      type: 'group' as const,
    },
    {
      key: '/cdw-research',
      icon: <ExperimentOutlined />,
      label: 'SFR-007 CDW 연구 (Text2SQL)',
    },
    {
      type: 'divider' as const,
    },
    {
      key: 'commercial-group',
      label: '상용 솔루션 (개발 예정)',
      type: 'group' as const,
    },
    {
      key: '/data-fabric',
      icon: <CloudServerOutlined />,
      label: '데이터 페브릭',
    },
    {
      key: '/datamart',
      icon: <DatabaseOutlined />,
      label: 'SFR-002 데이터마트 (Tera ONE)',
      disabled: true,
    },
  ];

  const handleMenuClick = ({ key }: { key: string }) => {
    navigate(key);
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={280} theme="dark">
        <div style={{ padding: '16px', textAlign: 'center' }}>
          <Title level={4} style={{ color: 'white', margin: 0 }}>
            🏥 아산병원 IDP POC
          </Title>
          <div style={{ color: '#999', fontSize: '12px', marginTop: '4px' }}>
            통합 데이터 플랫폼
          </div>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={handleMenuClick}
        />
      </Sider>
      <Layout>
        <Header style={{ background: '#fff', padding: '0 24px' }}>
          <Title level={4} style={{ margin: 0, lineHeight: '64px' }}>
            {menuItems.find((item) => item?.key === location.pathname)?.label || '아산병원 IDP'}
          </Title>
        </Header>
        <Content style={{ margin: '24px', padding: '24px', background: '#fff', minHeight: 280 }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
};

export default MainLayout;