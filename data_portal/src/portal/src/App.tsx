/**
 * 서울아산병원 IDP 메인 앱
 * 시큐어 코딩 적용
 */

import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, App as AntApp, theme } from 'antd';
import koKR from 'antd/locale/ko_KR';
import MainLayout from './components/Layout/MainLayout';
import { Dashboard } from './pages/Dashboard';
import DataCatalog from './pages/DataCatalog';
import DataMart from './pages/DataMart';
import BI from './pages/BI';
import DataGovernance from './pages/DataGovernance';
import ETL from './pages/ETL';
import AIEnvironment from './pages/AIEnvironment';
import CDWResearch from './pages/CDWResearch';
import Presentation from './pages/Presentation';
import MedicalNER from './pages/MedicalNER';
import AIOps from './pages/AIOps';
import DataDesign from './pages/DataDesign';
import Ontology from './pages/Ontology';
import PortalOps from './pages/PortalOps';
import AIArchitecture from './pages/AIArchitecture';
import DataFabric from './pages/DataFabric';
import { SettingsProvider, useSettings } from './contexts/SettingsContext';

import './App.css';

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
              <Route path="dashboard" element={<Dashboard />} />
              <Route path="catalog" element={<DataCatalog />} />
              <Route path="datamart" element={<DataMart />} />
              <Route path="bi" element={<BI />} />
              <Route path="governance" element={<DataGovernance />} />
              <Route path="etl" element={<ETL />} />
              <Route path="ai-environment" element={<AIEnvironment />} />
              <Route path="cdw" element={<CDWResearch />} />
              <Route path="presentation" element={<Presentation />} />
              <Route path="ner" element={<MedicalNER />} />
              <Route path="ai-ops" element={<AIOps />} />
              <Route path="data-design" element={<DataDesign />} />
              <Route path="ontology" element={<Ontology />} />
              <Route path="portal-ops" element={<PortalOps />} />
              <Route path="ai-architecture" element={<AIArchitecture />} />
              <Route path="data-fabric" element={<DataFabric />} />
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
