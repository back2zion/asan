/**
 * 서울아산병원 IDP 메인 앱
 * 시큐어 코딩 적용
 */

import React, { Suspense } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, App as AntApp, theme, Spin } from 'antd';
import koKR from 'antd/locale/ko_KR';
import MainLayout from './components/Layout/MainLayout';
import { SettingsProvider, useSettings } from './contexts/SettingsContext';

import './App.css';

// Route-level code splitting — 페이지별 lazy loading
const Dashboard = React.lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const DataCatalog = React.lazy(() => import('./pages/DataCatalog'));
const DataMart = React.lazy(() => import('./pages/DataMart'));
const BI = React.lazy(() => import('./pages/BI'));
const DataGovernance = React.lazy(() => import('./pages/DataGovernance'));
const ETL = React.lazy(() => import('./pages/ETL'));
const AIEnvironment = React.lazy(() => import('./pages/AIEnvironment'));
const CDWResearch = React.lazy(() => import('./pages/CDWResearch'));
const Presentation = React.lazy(() => import('./pages/Presentation'));
const MedicalNER = React.lazy(() => import('./pages/MedicalNER'));
const AIOps = React.lazy(() => import('./pages/AIOps'));
const DataDesign = React.lazy(() => import('./pages/DataDesign'));
const Ontology = React.lazy(() => import('./pages/Ontology'));
const PortalOps = React.lazy(() => import('./pages/PortalOps'));
const AIArchitecture = React.lazy(() => import('./pages/AIArchitecture'));
const DataFabric = React.lazy(() => import('./pages/DataFabric'));

// React Query 클라이언트 설정
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000, // 5분
    },
  },
});

// 아산병원 테마 색상 (파란색 기반)
const asanTheme = {
  token: {
    // Primary brand color
    colorPrimary: '#005BAC', // ASAN BLUE

    // Neutral colors for text, borders, backgrounds
    colorTextBase: '#333333', // Darker text for better readability
    colorTextSecondary: '#666666',
    colorBorder: '#d9d9d9',
    colorBgContainer: '#ffffff', // Card and component backgrounds
    colorBgLayout: '#f0f2f5', // Main layout background

    // Status colors - keeping existing for now, can be refined later
    colorSuccess: '#52c41a',
    colorWarning: '#faad14',
    colorError: '#ff4d4f',
    colorInfo: '#005BAC',

    // Border radius for a softer look
    borderRadius: 8, // Slightly more rounded corners

    // Typography
    fontFamily:
      '"Noto Sans KR", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif', // Prioritize Noto Sans KR
    fontSize: 14,
    lineHeight: 1.5714285714285714, // Default Ant Design line height for 14px font

    // Box shadow for depth
    boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.03), 0 1px 6px -1px rgba(0, 0, 0, 0.02), 0 2px 4px 0 rgba(0, 0, 0, 0.02)',
    boxShadowSecondary: '0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 9px 18px 8px rgba(0, 0, 0, 0.05)',
  },
  components: {
    Layout: {
      headerBg: '#ffffff',
      siderBg: '#ffffff',
      bodyBg: '#f5f7fa',
      // Further refine layout padding and margins if needed in MainLayout component
    },
    Card: {
      borderRadius: 12, // Slightly more rounded for cards
      boxShadow: '0 2px 8px rgba(0, 0, 0, 0.09)', // Subtle shadow for cards
    },
    Button: {
      borderRadius: 6,
      // Consider hover/active states for buttons if not handled by default
    },
    Menu: {
      itemSelectedBg: 'rgba(0, 91, 172, 0.1)', // Slightly more opaque
      itemHoverBg: 'rgba(0, 91, 172, 0.06)', // Slightly more opaque
      itemSelectedColor: '#005BAC',
      // Further fine-tune menu item padding/margin if necessary
    },
    Table: {
      headerBg: '#f5f7fa', // Lighter header background
      // Consider adding subtle borders to table cells
    },
    // Input fields
    Input: {
      borderRadius: 6,
    },
    Select: {
      borderRadius: 6,
    },
    DatePicker: {
      borderRadius: 6,
    },
  },
};

const ThemedApp: React.FC = () => {
  const { settings } = useSettings();

  const currentTheme = {
    ...asanTheme,
    algorithm: settings.darkMode ? theme.darkAlgorithm : theme.defaultAlgorithm,
  };

  return (
    <ConfigProvider locale={koKR} theme={currentTheme}>
      <AntApp>
        <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <Routes>
            <Route path="/" element={<MainLayout />}>
              <Route index element={<Navigate to="/dashboard" replace />} />
              <Route path="dashboard" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><Dashboard /></Suspense>} />
              <Route path="catalog" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><DataCatalog /></Suspense>} />
              <Route path="datamart" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><DataMart /></Suspense>} />
              <Route path="bi" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><BI /></Suspense>} />
              <Route path="governance" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><DataGovernance /></Suspense>} />
              <Route path="etl" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><ETL /></Suspense>} />
              <Route path="ai-environment" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><AIEnvironment /></Suspense>} />
              <Route path="cdw" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><CDWResearch /></Suspense>} />
              <Route path="presentation" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><Presentation /></Suspense>} />
              <Route path="ner" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><MedicalNER /></Suspense>} />
              <Route path="ai-ops" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><AIOps /></Suspense>} />
              <Route path="data-design" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><DataDesign /></Suspense>} />
              <Route path="ontology" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><Ontology /></Suspense>} />
              <Route path="portal-ops" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><PortalOps /></Suspense>} />
              <Route path="ai-architecture" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><AIArchitecture /></Suspense>} />
              <Route path="data-fabric" element={<Suspense fallback={<Spin size="large" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }} />}><DataFabric /></Suspense>} />
              <Route path="*" element={<Navigate to="/dashboard" replace />} />
            </Route>
          </Routes>
        </Router>
      </AntApp>
    </ConfigProvider>
  );
};

const App: React.FC = () => {
  return (
    <QueryClientProvider client={queryClient}>
      <SettingsProvider>
        <ThemedApp />
      </SettingsProvider>
    </QueryClientProvider>
  );
};

export default App;
