/**
 * 서울아산병원 IDP 메인 앱
 * 시큐어 코딩 적용
 */

import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, App as AntApp } from 'antd';
import koKR from 'antd/locale/ko_KR';
import MainLayout from './components/Layout/MainLayout';
import { Dashboard } from './pages/Dashboard';
import DataCatalog from './pages/DataCatalog';
import DataMart from './pages/DataMart';
import BI from './pages/BI';
import OLAP from './pages/OLAP';
import ETL from './pages/ETL';
import AIEnvironment from './pages/AIEnvironment';
import CDWResearch from './pages/CDWResearch';

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
    colorPrimary: '#005BAC', // ASAN BLUE
    colorSuccess: '#52c41a',
    colorWarning: '#faad14',
    colorError: '#ff4d4f',
    colorInfo: '#005BAC',
    borderRadius: 6,
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", "Helvetica Neue", Arial, sans-serif',
    fontSize: 14,
  },
  components: {
    Layout: {
      headerBg: '#ffffff',
      siderBg: '#ffffff',
      bodyBg: '#f5f7fa',
    },
    Card: {
      borderRadius: 8,
    },
    Button: {
      borderRadius: 6,
    },
    Menu: {
      itemSelectedBg: 'rgba(0, 91, 172, 0.08)',
      itemHoverBg: 'rgba(0, 91, 172, 0.04)',
      itemSelectedColor: '#005BAC',
    },
    Table: {
      headerBg: '#fafafa',
    },
  },
};

const App: React.FC = () => {
  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider locale={koKR} theme={asanTheme}>
        <AntApp>
          <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
            <Routes>
              <Route path="/" element={<MainLayout />}>
                <Route index element={<Navigate to="/dashboard" replace />} />
                <Route path="dashboard" element={<Dashboard />} />
                <Route path="catalog" element={<DataCatalog />} />
                <Route path="datamart" element={<DataMart />} />
                <Route path="bi" element={<BI />} />
                <Route path="olap" element={<OLAP />} />
                <Route path="etl" element={<ETL />} />
                <Route path="ai-environment" element={<AIEnvironment />} />
                <Route path="cdw" element={<CDWResearch />} />
                <Route path="*" element={<Navigate to="/dashboard" replace />} />
              </Route>
            </Routes>
          </Router>
        </AntApp>
      </ConfigProvider>
    </QueryClientProvider>
  );
};

export default App;
