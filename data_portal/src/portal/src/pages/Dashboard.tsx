import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, Typography, Row, Col, App } from 'antd';
import { HomeOutlined } from '@ant-design/icons';
import { Activity, Layers, Cpu, Network, Layout, Download, Database } from 'lucide-react';
import { useSettings } from '../contexts/SettingsContext';
import {
  API_BASE,
  FALLBACK_QUALITY,
  OperationalView,
  ArchitectureView,
  LakehouseView,
  DrillDownModal,
  LayoutModal,
  ReportModal,
  exportReport,
} from '../components/dashboard';
import type { SystemInfo, PipelineInfo, GpuInfo } from '../components/dashboard';

const { Title, Paragraph } = Typography;

export const Dashboard: React.FC = () => {
  const { settings } = useSettings();
  const { message } = App.useApp();
  const [viewMode, setViewMode] = useState<'LAKEHOUSE' | 'OPERATIONAL' | 'ARCHITECTURE'>(() => {
    const saved = localStorage.getItem('dashboard_viewMode');
    return (saved === 'OPERATIONAL' || saved === 'ARCHITECTURE') ? saved : 'LAKEHOUSE';
  });
  useEffect(() => { localStorage.setItem('dashboard_viewMode', viewMode); }, [viewMode]);
  const [drillDownData, setDrillDownData] = useState<{ title: string; data: any } | null>(null);
  const [layoutModalOpen, setLayoutModalOpen] = useState(false);
  const [layoutChoice, setLayoutChoice] = useState(() =>
    localStorage.getItem('dashboard_layoutChoice') || 'default'
  );
  const [reportModalOpen, setReportModalOpen] = useState(false);
  const [reportFormat, setReportFormat] = useState<'csv' | 'txt'>('csv');

  // --- Data Queries (React Query with 5-min default cache) ---
  const { data: dashStats } = useQuery({
    queryKey: ['dashboard-stats'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/datamart/dashboard-stats`);
      if (!res.ok) return null;
      return res.json();
    },
  });

  const qualityData = dashStats?.quality?.length ? dashStats.quality : FALLBACK_QUALITY;
  const activityData: { month: string; count: number }[] = dashStats?.activity_timeline?.length ? dashStats.activity_timeline : [];
  const totalRecords: number = dashStats?.total_records || 0;
  const visitTypeData: { type: string; count: number }[] = dashStats?.visit_type_distribution?.length ? dashStats.visit_type_distribution : [];
  const pipelineInfo: PipelineInfo | null = dashStats?.pipeline || null;
  const queryLatency: number | null = dashStats?.query_latency_ms ?? null;
  const securityScore: number | null = dashStats?.security_score ?? null;

  const { data: systemInfo = { cpuPercent: 0, memPercent: 0, memUsedGb: 0, memTotalGb: 0 } } = useQuery<SystemInfo>({
    queryKey: ['system-resources'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/ai-environment/resources/system`);
      if (!res.ok) return { cpuPercent: 0, memPercent: 0, memUsedGb: 0, memTotalGb: 0 };
      const data = await res.json();
      return {
        cpuPercent: data.cpu?.percent ?? 0,
        memPercent: data.memory?.percent ?? 0,
        memUsedGb: data.memory?.used_gb ?? 0,
        memTotalGb: data.memory?.total_gb ?? 0,
      };
    },
    refetchInterval: settings.autoRefresh ? 10_000 : false,
  });

  const { data: containers = [] } = useQuery<any[]>({
    queryKey: ['containers'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/ai-environment/containers`);
      if (!res.ok) return [];
      const data = await res.json();
      return data.containers || [];
    },
    refetchInterval: settings.autoRefresh ? 10_000 : false,
  });

  const { data: gpuInfo = { available: false, gpus: [] } } = useQuery<GpuInfo>({
    queryKey: ['gpu-resources'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/ai-environment/resources/gpu`);
      if (!res.ok) return { available: false, gpus: [] };
      const data = await res.json();
      return { available: data.available || false, gpus: data.gpus || [] };
    },
    refetchInterval: settings.autoRefresh ? 10_000 : false,
  });

  // --- Event Handlers ---
  const handleChartClick = (data: any, title: string) => {
    if (data && (data.activePayload || data.payload)) {
      const payload = data.activePayload ? data.activePayload[0].payload : data.payload;
      setDrillDownData({ title, data: payload });
    }
  };

  const handleExportReport = () => {
    setReportModalOpen(false);
    message.loading({ content: '리포트 생성 중...', key: 'report', duration: 1.5 });
    setTimeout(() => {
      exportReport(
        { totalRecords, systemInfo, qualityData, activityData, pipelineInfo, securityScore, containers },
        reportFormat,
        () => message.success({ content: '리포트가 다운로드 되었습니다', key: 'report' }),
      );
    }, 1500);
  };

  return (
    <div className="space-y-6 animate-fade-in text-[#53565A] pb-20">
      {/* Header */}
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <HomeOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              {'플랫폼 현황'} (Dashboard)
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              CDW/EDW 통합 데이터 레이크하우스 · 실시간 모니터링 · 아키텍처
            </Paragraph>
          </Col>
          <Col></Col>
        </Row>
      </Card>

      {/* Toolbar */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div></div>
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center bg-white rounded-lg p-1 border border-gray-200 shadow-sm">
             <button
                onClick={() => setViewMode('LAKEHOUSE')}
                className={`px-3 py-1.5 text-xs font-bold rounded flex items-center gap-2 transition-all ${viewMode === 'LAKEHOUSE' ? 'bg-[#006241] text-white shadow' : 'text-[#A8A8A8] hover:bg-gray-100'}`}
             >
                <Database size={14} /> 레이크하우스
             </button>
             <button
                onClick={() => setViewMode('OPERATIONAL')}
                className={`px-3 py-1.5 text-xs font-bold rounded flex items-center gap-2 transition-all ${viewMode === 'OPERATIONAL' ? 'bg-[#006241] text-white shadow' : 'text-[#A8A8A8] hover:bg-gray-100'}`}
             >
                <Activity size={14} /> 운영 뷰
             </button>
             <button
                onClick={() => setViewMode('ARCHITECTURE')}
                className={`px-3 py-1.5 text-xs font-bold rounded flex items-center gap-2 transition-all ${viewMode === 'ARCHITECTURE' ? 'bg-[#006241] text-white shadow' : 'text-[#A8A8A8] hover:bg-gray-100'}`}
             >
                <Layers size={14} /> 아키텍처 뷰
             </button>
          </div>
          <div className="h-6 w-px bg-gray-300 mx-1"></div>
          <button
            onClick={() => setLayoutModalOpen(true)}
            className="flex items-center gap-2 px-3 py-2 bg-white border border-gray-300 rounded-lg text-sm text-[#53565A] hover:bg-[#F5F0E8] transition-colors shadow-sm font-medium"
          >
            <Layout size={16} /> {'레이아웃 편집'}
          </button>
          <button
            onClick={() => setReportModalOpen(true)}
            className="flex items-center gap-2 px-3 py-2 bg-[#006241] text-white rounded-lg text-sm hover:bg-[#004e32] transition-colors shadow-sm font-medium"
          >
            <Download size={16} /> {'리포트 내보내기'}
          </button>
        </div>
      </div>

      {/* System Status Banner */}
      <div className="flex items-center gap-4 bg-white px-5 py-3 rounded-lg border border-gray-200 shadow-sm w-fit animate-slide-in-up">
          <div className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full bg-[#52A67D] animate-pulse shadow-[0_0_8px_#52A67D]"></div>
            <span className="text-sm text-[#53565A] font-bold">System Online</span>
          </div>
          <div className="h-4 w-px bg-gray-300"></div>
          <div className="flex items-center gap-2 text-xs text-[#A8A8A8]">
            <Cpu size={14} />
            <span>CPU: {(systemInfo?.cpuPercent ?? 0).toFixed(1)}%</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-[#A8A8A8]">
            <Network size={14} />
            <span>RAM: {systemInfo?.memUsedGb ?? 0}/{systemInfo?.memTotalGb ?? 0} GB ({(systemInfo?.memPercent ?? 0).toFixed(0)}%)</span>
          </div>
      </div>

      {/* View Content */}
      {viewMode === 'LAKEHOUSE' ? (
        <LakehouseView />
      ) : viewMode === 'OPERATIONAL' ? (
        <OperationalView
          totalRecords={totalRecords}
          pipelineInfo={pipelineInfo}
          queryLatency={queryLatency}
          securityScore={securityScore}
          visitTypeData={visitTypeData}
          qualityData={qualityData}
          activityData={activityData}
          containers={containers}
          gpuInfo={gpuInfo}
          onChartClick={handleChartClick}
          layout={layoutChoice}
        />
      ) : (
        <ArchitectureView />
      )}

      {/* Modals */}
      <DrillDownModal drillDownData={drillDownData} onClose={() => setDrillDownData(null)} />
      <LayoutModal
        open={layoutModalOpen}
        layoutChoice={layoutChoice}
        onLayoutChange={setLayoutChoice}
        onCancel={() => setLayoutModalOpen(false)}
        onOk={() => { localStorage.setItem('dashboard_layoutChoice', layoutChoice); message.success('레이아웃이 적용되었습니다'); setLayoutModalOpen(false); }}
      />
      <ReportModal
        open={reportModalOpen}
        reportFormat={reportFormat}
        onFormatChange={setReportFormat}
        onCancel={() => setReportModalOpen(false)}
        onOk={handleExportReport}
      />
    </div>
  );
};
