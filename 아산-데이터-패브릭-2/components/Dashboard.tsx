
import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, AreaChart, Area, CartesianGrid, LineChart, Line } from 'recharts';
import { Server, Database, Activity, ShieldCheck, AlertTriangle, Cpu, Network, Layout, Download, ServerCog, Wifi, ToggleLeft, ToggleRight, Layers, Search, X } from 'lucide-react';

// Mock Data for Charts
const ingestionData = [
  { time: '00:00', volume: 450, errorRate: 0.01 },
  { time: '04:00', volume: 320, errorRate: 0.02 },
  { time: '08:00', volume: 1200, errorRate: 0.05 },
  { time: '12:00', volume: 2400, errorRate: 0.08 },
  { time: '16:00', volume: 1800, errorRate: 0.04 },
  { time: '20:00', volume: 950, errorRate: 0.03 },
  { time: '23:59', volume: 500, errorRate: 0.01 },
];

const qualityData = [
  { domain: '임상(Clinical)', score: 98, issues: 12 },
  { domain: '유전체(Genomics)', score: 92, issues: 45 },
  { domain: '영상(Imaging)', score: 88, issues: 78 },
  { domain: '원무(Admin)', score: 99, issues: 3 },
  { domain: 'IoT/활력징후', score: 85, issues: 156 },
];

// Mock Data for IoT Stream
const generateIoTData = () => Array.from({ length: 20 }, (_, i) => ({
  time: i,
  heartRate: 60 + Math.random() * 40,
  spo2: 95 + Math.random() * 5
}));

const CustomTooltip = ({ active, payload, label }: any) => {
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
                 {entry.name.includes('점수') || entry.name.includes('Rate') ? '' : ''}
               </span>
            </div>
          ))}
        </div>
        <p className="text-[10px] text-[#A8A8A8] mt-2 pt-1 text-center font-medium bg-[#F5F0E8]/50 rounded py-1">
          👆 클릭하여 상세 분석
        </p>
      </div>
    );
  }
  return null;
};

const StatCard: React.FC<{ title: string; value: string; sub: string; icon: React.ReactNode; status?: 'normal' | 'warning' | 'critical' }> = ({ title, value, sub, icon, status = 'normal' }) => (
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

export const Dashboard: React.FC = () => {
  const [viewMode, setViewMode] = useState<'OPERATIONAL' | 'ARCHITECTURE'>('OPERATIONAL');
  const [iotData, setIotData] = useState(generateIoTData());
  const [drillDownData, setDrillDownData] = useState<{ title: string; data: any } | null>(null);

  useEffect(() => {
    const interval = setInterval(() => {
      setIotData(prev => [
        ...prev.slice(1),
        { time: prev[prev.length - 1].time + 1, heartRate: 60 + Math.random() * 40, spo2: 95 + Math.random() * 5 }
      ]);
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const handleChartClick = (data: any, title: string) => {
    if (data && (data.activePayload || data.payload)) {
      const payload = data.activePayload ? data.activePayload[0].payload : data.payload;
      setDrillDownData({ title, data: payload });
    }
  };

  return (
    <div className="p-6 space-y-6 animate-fade-in text-[#53565A] pb-20">
      {/* Header */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h2 className="text-2xl font-bold text-[#53565A]">플랫폼 현황 (Dashboard)</h2>
          <p className="text-[#A8A8A8] text-sm">아산 의료 데이터 패브릭 실시간 모니터링</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center bg-white rounded-lg p-1 border border-gray-200 shadow-sm">
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
          <button className="flex items-center gap-2 px-3 py-2 bg-white border border-gray-300 rounded-lg text-sm text-[#53565A] hover:bg-[#F5F0E8] transition-colors shadow-sm font-medium">
            <Layout size={16} /> 레이아웃 편집
          </button>
          <button className="flex items-center gap-2 px-3 py-2 bg-[#006241] text-white rounded-lg text-sm hover:bg-[#004e32] transition-colors shadow-sm font-medium">
            <Download size={16} /> 리포트 내보내기
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
            <span>GPU Load: 42%</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-[#A8A8A8]">
            <Network size={14} />
            <span>Traffic: 1.2 GB/s</span>
          </div>
      </div>

      {viewMode === 'OPERATIONAL' ? (
        <>
          {/* KPI Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <StatCard 
              title="데이터 레이크" 
              value="4.2 PB" 
              sub="S3 / Apache Iceberg" 
              icon={<Database size={20} />} 
            />
            <StatCard 
              title="활성 파이프라인" 
              value="128" 
              sub="125 Running, 3 Failed" 
              icon={<Activity size={20} />} 
              status="warning"
            />
            <StatCard 
              title="쿼리 응답시간" 
              value="142 ms" 
              sub="Trino / Starburst" 
              icon={<Server size={20} />} 
            />
            <StatCard 
              title="보안 준수율" 
              value="99.9%" 
              sub="HIPAA / GDPR" 
              icon={<ShieldCheck size={20} />} 
            />
          </div>

          {/* Main Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-2 bg-white p-6 rounded-xl border border-gray-200 shadow-sm relative group">
              <div className="flex justify-between items-center mb-6">
                 <h3 className="text-lg font-bold text-[#53565A] flex items-center gap-2">
                    <Network size={18} className="text-[#006241]" />
                    시간대별 데이터 수집량 (Data Ingestion)
                 </h3>
                 <span className="text-xs text-[#A8A8A8] bg-[#F5F0E8] px-2 py-1 rounded">단위: GB/hr</span>
              </div>
              <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart 
                    data={ingestionData} 
                    onClick={(data) => handleChartClick(data, '데이터 수집량 상세')}
                    className="cursor-pointer"
                  >
                    <defs>
                      <linearGradient id="colorVol" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#006241" stopOpacity={0.2}/>
                        <stop offset="95%" stopColor="#006241" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                    <XAxis dataKey="time" stroke="#94a3b8" tick={{fill: '#64748b', fontSize: 11}} axisLine={false} tickLine={false} />
                    <YAxis stroke="#94a3b8" tick={{fill: '#64748b', fontSize: 11}} axisLine={false} tickLine={false} />
                    <Tooltip content={<CustomTooltip />} />
                    <Area 
                        type="monotone" 
                        dataKey="volume" 
                        name="수집량"
                        stroke="#006241" 
                        strokeWidth={2}
                        fillOpacity={1} 
                        fill="url(#colorVol)" 
                        activeDot={{ r: 6, strokeWidth: 2, stroke: '#fff' }}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <div className="absolute top-6 right-6 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
                 <div className="flex items-center gap-1 text-[10px] text-[#006241] bg-[#006241]/10 px-2 py-1 rounded">
                    <Search size={10} /> 클릭하여 상세 분석
                 </div>
              </div>
            </div>

            <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm group">
              <div className="flex justify-between items-center mb-6">
                <h3 className="text-lg font-bold text-[#53565A] flex items-center gap-2">
                    <ShieldCheck size={18} className="text-[#52A67D]" />
                    데이터 품질 지수
                </h3>
              </div>
              <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart 
                    data={qualityData} 
                    layout="vertical"
                    onClick={(data) => handleChartClick(data, '품질 지수 상세')}
                    className="cursor-pointer"
                  >
                    <XAxis type="number" domain={[0, 100]} hide />
                    <YAxis dataKey="domain" type="category" stroke="#53565A" width={90} tick={{fontSize: 11, fontWeight: 500}} axisLine={false} tickLine={false} />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar dataKey="score" name="품질 점수" radius={[0, 4, 4, 0]} barSize={24} background={{ fill: '#f8fafc' }}>
                       {qualityData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.score > 90 ? '#52A67D' : entry.score > 80 ? '#FF6F00' : '#DC2626'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Infrastructure & IoT Panel */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
             {/* Infrastructure Resource Status (FIX: Explicit H100 4 Units) */}
             <div className="lg:col-span-2 bg-white rounded-xl border border-gray-200 shadow-sm p-6">
                <h3 className="text-lg font-bold text-[#53565A] mb-4 flex items-center gap-2">
                    <ServerCog size={20} className="text-[#FF6F00]" />
                    인프라 자원 상태 (Infrastructure Status)
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* KubeVirt */}
                    <div className="p-4 bg-[#F5F0E8] rounded-xl border border-gray-200">
                      <h4 className="font-bold text-[#53565A] text-sm mb-3 flex items-center justify-between">
                          <span className="flex items-center gap-2"><Layout size={14} /> KubeVirt Nodes</span>
                          <span className="text-[10px] bg-[#52A67D] text-white px-2 py-0.5 rounded font-bold">Healthy</span>
                      </h4>
                      <div className="space-y-4">
                          <div className="flex justify-between text-xs">
                            <span className="text-[#A8A8A8]">vCPU Usage</span>
                            <span className="font-mono font-bold text-[#53565A]">824 / 1,024</span>
                          </div>
                          <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                            <div className="bg-[#006241] h-2 rounded-full transition-all duration-500" style={{width: '80%'}}></div>
                          </div>
                          <div className="grid grid-cols-4 gap-1 mt-2">
                             {[...Array(8)].map((_, i) => (
                                <div key={i} className={`h-1.5 rounded-sm ${i < 6 ? 'bg-[#52A67D]' : 'bg-gray-300'}`}></div>
                             ))}
                          </div>
                      </div>
                    </div>

                    {/* NVIDIA H100 MIG (Visualized as 4 Cards) */}
                    <div className="p-4 bg-[#F5F0E8] rounded-xl border border-gray-200">
                      <h4 className="font-bold text-[#53565A] text-sm mb-3 flex items-center justify-between">
                          <span className="flex items-center gap-2"><Cpu size={14} /> NVIDIA H100 (4 Units)</span>
                          <span className="text-[10px] bg-[#FF6F00] text-white px-2 py-0.5 rounded font-bold">High Load</span>
                      </h4>
                      <div className="space-y-2">
                          <div className="flex justify-between text-xs">
                            <span className="text-[#A8A8A8]">MIG Instances Active</span>
                            <span className="font-mono font-bold text-[#53565A]">26 / 28</span>
                          </div>
                          {/* 4 Explicit Units Visualizer */}
                          <div className="flex gap-2 mt-2">
                             {[1, 2, 3, 4].map((gpu) => (
                                <div key={gpu} className="flex-1 bg-white p-1 rounded border border-gray-200 flex flex-col gap-0.5 shadow-sm">
                                   <div className="text-[8px] text-center text-[#A8A8A8] font-mono uppercase">H100 #{gpu}</div>
                                   <div className="h-8 w-full bg-gray-100 rounded flex flex-col-reverse overflow-hidden relative">
                                      {/* Simulated MIG Slices */}
                                      <div className={`absolute bottom-0 w-full transition-all duration-500 ${gpu === 2 ? 'bg-[#52A67D]' : 'bg-[#FF6F00]'}`} style={{height: `${gpu === 2 ? 40 : 85}%`}}></div>
                                      <div className="absolute inset-0 grid grid-rows-7 w-full h-full pointer-events-none">
                                         {[...Array(7)].map((_, i) => <div key={i} className="border-b border-white/20 w-full h-full"></div>)}
                                      </div>
                                   </div>
                                </div>
                             ))}
                          </div>
                          <div className="text-[10px] text-[#A8A8A8] text-right mt-1">Total Memory: 320GB HBM3</div>
                      </div>
                    </div>
                </div>
             </div>

             {/* Real-time IoT Widget */}
             <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 flex flex-col">
                <h3 className="text-lg font-bold text-[#53565A] mb-4 flex items-center gap-2">
                    <Wifi size={20} className="text-[#52A67D] animate-pulse" />
                    IoT 활력징후 (Real-time)
                </h3>
                <div className="flex-grow min-h-[150px]">
                   <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={iotData}>
                         <Line type="monotone" dataKey="heartRate" stroke="#FF6F00" strokeWidth={2} dot={false} isAnimationActive={false} />
                         <Line type="monotone" dataKey="spo2" stroke="#52A67D" strokeWidth={2} dot={false} isAnimationActive={false} />
                         <YAxis hide domain={['auto', 'auto']} />
                      </LineChart>
                   </ResponsiveContainer>
                </div>
                <div className="flex justify-between text-xs mt-2 font-mono">
                   <div className="text-[#FF6F00]">HR: {Math.round(iotData[iotData.length-1].heartRate)} bpm</div>
                   <div className="text-[#52A67D]">SpO2: {Math.round(iotData[iotData.length-1].spo2)}%</div>
                </div>
             </div>
          </div>
        </>
      ) : (
        /* Architecture View */
        <div className="bg-white p-10 rounded-xl border border-gray-200 shadow-sm min-h-[500px] flex items-center justify-center relative overflow-hidden">
           <div className="absolute inset-0 bg-[radial-gradient(#e5e7eb_1px,transparent_1px)] [background-size:16px_16px] opacity-30"></div>
           <div className="relative z-10 w-full max-w-4xl">
              <h3 className="text-center font-bold text-xl text-[#006241] mb-10">Asan Intelligent Data Fabric Architecture</h3>
              <div className="flex justify-between items-center gap-4">
                 {/* Source */}
                 <div className="flex flex-col items-center gap-2 p-4 border-2 border-dashed border-gray-300 rounded-xl bg-gray-50">
                    <Database size={32} className="text-gray-400" />
                    <span className="font-bold text-sm text-gray-500">HIS / EMR / PACS</span>
                 </div>
                 <div className="h-0.5 flex-1 bg-gray-300 relative">
                    <div className="absolute -top-3 left-1/2 -translate-x-1/2 text-[10px] bg-white px-2 text-gray-400">CDC / HL7</div>
                    <div className="absolute right-0 -top-1.5 w-0 h-0 border-t-[6px] border-t-transparent border-l-[8px] border-l-gray-300 border-b-[6px] border-b-transparent"></div>
                 </div>
                 
                 {/* Ingestion */}
                 <div className="flex flex-col items-center gap-2 p-4 border-2 border-[#52A67D] rounded-xl bg-[#52A67D]/5 shadow-lg shadow-[#52A67D]/10">
                    <Activity size={32} className="text-[#52A67D]" />
                    <span className="font-bold text-sm text-[#006241]">Apache Flink</span>
                    <span className="text-[10px] text-[#52A67D]">Stream Processing</span>
                 </div>

                 <div className="h-0.5 flex-1 bg-gray-300 relative">
                    <div className="absolute right-0 -top-1.5 w-0 h-0 border-t-[6px] border-t-transparent border-l-[8px] border-l-gray-300 border-b-[6px] border-b-transparent"></div>
                 </div>

                 {/* Storage */}
                 <div className="flex flex-col items-center gap-2 p-6 border-2 border-[#006241] rounded-xl bg-[#006241]/5 shadow-xl shadow-[#006241]/20">
                    <Layers size={40} className="text-[#006241]" />
                    <span className="font-bold text-base text-[#006241]">Apache Iceberg</span>
                    <span className="text-xs text-[#53565A]">Data Lakehouse (S3)</span>
                 </div>

                 <div className="h-0.5 flex-1 bg-gray-300 relative">
                     <div className="absolute -top-3 left-1/2 -translate-x-1/2 text-[10px] bg-white px-2 text-gray-400">Trino Federation</div>
                     <div className="absolute right-0 -top-1.5 w-0 h-0 border-t-[6px] border-t-transparent border-l-[8px] border-l-gray-300 border-b-[6px] border-b-transparent"></div>
                 </div>

                 {/* Compute & AI */}
                 <div className="flex flex-col gap-2">
                    <div className="flex flex-col items-center gap-2 p-4 border-2 border-[#FF6F00] rounded-xl bg-[#FF6F00]/5 shadow-lg shadow-[#FF6F00]/10">
                       <Cpu size={32} className="text-[#FF6F00]" />
                       <span className="font-bold text-sm text-[#FF6F00]">AI Computing</span>
                       <span className="text-[10px] text-[#53565A]">H100 GPU Farm</span>
                    </div>
                 </div>
              </div>
           </div>
        </div>
      )}

      {/* Drill Down Modal */}
      {drillDownData && (
        <div className="fixed inset-0 bg-black/40 z-[60] flex items-center justify-center p-4 backdrop-blur-sm animate-in fade-in duration-200">
           <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden border border-gray-100">
              <div className="bg-[#006241] p-4 flex justify-between items-center text-white">
                 <h3 className="font-bold flex items-center gap-2">
                    <Search size={18} /> 상세 분석: {drillDownData.title}
                 </h3>
                 <button onClick={() => setDrillDownData(null)} className="hover:bg-white/20 p-1 rounded transition-colors">
                    <X size={20} />
                 </button>
              </div>
              <div className="p-6 bg-[#F5F0E8]/30">
                 <div className="mb-4">
                    <div className="text-sm text-[#A8A8A8] font-bold uppercase mb-1">선택된 항목</div>
                    <div className="text-2xl font-bold text-[#53565A] font-mono">
                       {drillDownData.data.activeLabel || drillDownData.data.domain || drillDownData.data.time || 'N/A'}
                    </div>
                 </div>

                 <div className="space-y-3 bg-white p-4 rounded-lg border border-gray-200">
                    <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                       <span className="text-gray-500">주요 지표 (Value)</span>
                       <span className="font-mono font-bold text-[#006241]">
                          {drillDownData.data.volume || drillDownData.data.score || 'N/A'}
                       </span>
                    </div>
                    {drillDownData.data.errorRate !== undefined && (
                        <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                           <span className="text-gray-500">오류율 (Error Rate)</span>
                           <span className={`font-mono font-bold ${drillDownData.data.errorRate > 0.05 ? 'text-red-500' : 'text-[#52A67D]'}`}>
                              {(drillDownData.data.errorRate * 100).toFixed(1)}%
                           </span>
                        </div>
                    )}
                     {drillDownData.data.issues !== undefined && (
                        <div className="flex justify-between items-center text-sm">
                           <span className="text-gray-500">감지된 이슈 (Issues)</span>
                           <span className="font-mono font-bold text-[#FF6F00]">
                              {drillDownData.data.issues} 건
                           </span>
                        </div>
                    )}
                 </div>

                 <div className="mt-6 flex justify-end gap-2">
                    <button onClick={() => setDrillDownData(null)} className="px-4 py-2 text-sm font-medium text-gray-500 hover:bg-gray-100 rounded-lg transition-colors">닫기</button>
                    <button className="px-4 py-2 text-sm font-bold text-white bg-[#006241] hover:bg-[#004e32] rounded-lg shadow-sm transition-colors">심층 리포트 생성</button>
                 </div>
              </div>
           </div>
        </div>
      )}
    </div>
  );
};
