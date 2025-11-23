import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider } from 'antd';
import koKR from 'antd/locale/ko_KR';
import MainLayout from './components/Layout/MainLayout';
import Dashboard from './pages/Dashboard';
import CDWResearch from './pages/CDWResearch';
import DataFabric from './pages/DataFabric';
import './App.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

const App: React.FC = () => {
  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider 
        locale={koKR}
        theme={{
          token: {
            colorPrimary: '#1a5d3a', // Asan Hospital Green
            colorSuccess: '#52c41a',
            colorWarning: '#ff6600', // Asan Hospital Orange
            colorError: '#ff4d4f',
            colorInfo: '#1890ff',
            borderRadius: 8,
          },
        }}
      >
        <Router>
          <div className="App">
            <Routes>
              <Route path="/" element={<MainLayout />}>
                <Route index element={<Navigate to="/dashboard" replace />} />
                <Route path="dashboard" element={<Dashboard />} />
                <Route path="cdw-research" element={<CDWResearch />} />
                <Route path="data-fabric" element={<DataFabric />} />
                {/* TODO: 상용 솔루션 페이지들 추가 예정 */}
                {/* <Route path="datamart" element={<DataMart />} /> 데이터스트림즈 Tera ONE */}
                {/* <Route path="olap" element={<OLAP />} /> 비아이매트릭스 OLAP 솔루션 */}
                {/* <Route path="etl" element={<ETL />} /> 테라스트림 ETL 솔루션 */}
              </Route>
            </Routes>
          </div>
        </Router>
      </ConfigProvider>
    </QueryClientProvider>
  );
};

export default App;