import React from 'react';
import { AlertTriangle } from 'lucide-react';

export const API_BASE = '/api/v1';

// Visit type color mapping
export const VISIT_TYPE_COLORS: Record<string, string> = {
  '\uC678\uB798': '#006241',
  '\uC751\uAE09': '#FF6F00',
  '\uC785\uC6D0': '#52A67D',
};

export const FALLBACK_QUALITY = [
  { domain: '\uC784\uC0C1(Clinical)', score: 98, issues: 12 },
  { domain: '\uC601\uC0C1(Imaging)', score: 88, issues: 78 },
  { domain: '\uC6D0\uBB34(Admin)', score: 99, issues: 3 },
  { domain: '\uAC80\uC0AC(Lab)', score: 85, issues: 156 },
  { domain: '\uC57D\uBB3C(Drug)', score: 92, issues: 45 },
];

// Container type mapping
export const getContainerType = (name: string): { label: string; color: string } => {
  const lowerName = name.toLowerCase();
  if (lowerName.includes('jupyter')) return { label: 'Notebook', color: '#FF6F00' };
  if (lowerName.includes('mlflow')) return { label: 'MLOps', color: '#52A67D' };
  if (lowerName.includes('qdrant')) return { label: 'Vector DB', color: '#006241' };
  if (lowerName.includes('redis')) return { label: 'Cache', color: '#FFA500' };
  if (lowerName.includes('xiyan') || lowerName.includes('sql')) return { label: 'AI/SQL', color: '#1890ff' };
  if (lowerName.includes('omop')) return { label: 'OMOP CDM', color: '#DC2626' };
  if (lowerName.includes('superset')) return { label: 'BI', color: '#FAAD14' };
  if (lowerName.includes('airflow')) return { label: 'Pipeline', color: '#13C2C2' };
  if (lowerName.includes('api')) return { label: 'API', color: '#2F54EB' };
  if (lowerName.includes('postgres') || lowerName.includes('db')) return { label: 'Database', color: '#722ED1' };
  return { label: 'Service', color: '#94a3b8' };
};

export const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white/95 p-3 border border-[#52A67D] shadow-xl rounded-lg backdrop-blur-sm z-50 min-w-[150px]">
        <p className="text-[#006241] font-bold text-sm mb-2 border-b border-gray-100 pb-1">{label}</p>
        <div className="space-y-1.5">
          {payload.map((entry: any, index: number) => (
            <div key={index} className="flex items-center justify-between gap-4 text-xs">
               <span className="text-[#53565A] flex items-center gap-1">
                 <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: entry.color }}></div>
                 {entry.name}:
               </span>
               <span className="font-mono font-bold text-[#006241]">
                 {entry.value.toLocaleString()}
                 {entry.name.includes('\uC810\uC218') || entry.name.includes('Rate') ? '' : ''}
               </span>
            </div>
          ))}
        </div>
        <p className="text-[10px] text-[#A8A8A8] mt-2 pt-1 text-center font-medium bg-[#F5F0E8]/50 rounded py-1">
          \uD83D\uDC46 \uD074\uB9AD\uD558\uC5EC \uC0C1\uC138 \uBD84\uC11D
        </p>
      </div>
    );
  }
  return null;
};

export const StatCard: React.FC<{
  title: string;
  value: string;
  sub: string;
  icon: React.ReactNode;
  status?: 'normal' | 'warning' | 'critical';
}> = ({ title, value, sub, icon, status = 'normal' }) => (
  <div className={`bg-white p-5 rounded-lg border shadow-sm hover:shadow-md transition-all group ${
    status === 'critical' ? 'border-[#FF6F00] border-l-4' :
    status === 'warning' ? 'border-[#FF6F00] border-l-4' :
    'border-gray-200 border-l-4 border-l-[#006241]'
  }`}>
    <div className="flex justify-between items-start mb-2">
      <div className={`p-2 rounded transition-colors group-hover:bg-[#006241] group-hover:text-white ${status === 'warning' || status === 'critical' ? 'bg-[#FF6F00]/10 text-[#FF6F00]' : 'bg-[#006241]/10 text-[#006241]'}`}>
        {icon}
      </div>
      {status === 'critical' && <AlertTriangle size={16} className="text-[#FF6F00] animate-pulse" />}
    </div>
    <div className="mt-2">
      <p className="text-[#A8A8A8] text-xs uppercase tracking-wider font-semibold">{title}</p>
      <h3 className="text-2xl font-bold text-[#53565A] mt-1 group-hover:text-[#006241] transition-colors">{value}</h3>
      <p className="text-xs text-[#A8A8A8] mt-1">{sub}</p>
    </div>
  </div>
);

// Shared type definitions
export interface SystemInfo {
  cpuPercent: number;
  memPercent: number;
  memUsedGb: number;
  memTotalGb: number;
}

export interface PipelineInfo {
  total_dags: number;
  active: number;
  recent_success: number;
  recent_failed: number;
  recent_running: number;
}

export interface GpuInfo {
  available: boolean;
  gpus: any[];
}
