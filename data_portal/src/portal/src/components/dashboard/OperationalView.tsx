import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, AreaChart, Area, CartesianGrid } from 'recharts';
import { Database, Activity, Server, ShieldCheck, Network, ServerCog, Cpu } from 'lucide-react';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import {
  VISIT_TYPE_COLORS,
  CustomTooltip,
  StatCard,
  getContainerType,
  type SystemInfo,
  type PipelineInfo,
  type GpuInfo,
} from './dashboardConstants';

interface OperationalViewProps {
  totalRecords: number;
  pipelineInfo: PipelineInfo | null;
  queryLatency: number | null;
  securityScore: number | null;
  visitTypeData: { type: string; count: number }[];
  qualityData: { domain: string; score: number; issues: number }[];
  activityData: { month: string; count: number }[];
  containers: any[];
  gpuInfo: GpuInfo;
  onChartClick: (data: any, title: string) => void;
}

const OperationalView: React.FC<OperationalViewProps> = ({
  totalRecords,
  pipelineInfo,
  queryLatency,
  securityScore,
  visitTypeData,
  qualityData,
  activityData,
  containers,
  gpuInfo,
  onChartClick,
}) => {
  const protectedContainers = containers.filter(c => c.is_protected);
  const runningProtected = protectedContainers.filter(c => c.status === 'running');
  const stoppedProtected = protectedContainers.filter(c => c.status !== 'running');

  return (
    <>
      {/* KPI Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="\uB370\uC774\uD130 \uB808\uC774\uD06C"
          value={totalRecords ? `${(totalRecords / 1000000).toFixed(2)}M` : '4.2 PB'}
          sub={totalRecords ? 'OMOP CDM \uB808\uCF54\uB4DC' : 'S3 / Apache Iceberg'}
          icon={<Database size={20} />}
        />
        <StatCard
          title="\uD65C\uC131 \uD30C\uC774\uD504\uB77C\uC778"
          value={pipelineInfo ? `${pipelineInfo.total_dags} DAGs` : '\u2014'}
          sub={pipelineInfo ? `${pipelineInfo.active} Active, ${pipelineInfo.recent_running} Running${pipelineInfo.recent_failed > 0 ? `, ${pipelineInfo.recent_failed} Failed` : ''}` : 'Airflow \uC5F0\uACB0 \uC911...'}
          icon={<Activity size={20} />}
          status={pipelineInfo && pipelineInfo.recent_failed > 0 ? 'warning' : 'normal'}
        />
        <StatCard
          title="\uCFFC\uB9AC \uC751\uB2F5\uC2DC\uAC04"
          value={queryLatency != null ? `${queryLatency.toFixed(0)} ms` : '\u2014 ms'}
          sub="OMOP CDM (PostgreSQL)"
          icon={<Server size={20} />}
        />
        <StatCard
          title="\uBCF4\uC548 \uC900\uC218\uC728"
          value={securityScore != null ? `${securityScore.toFixed(1)}%` : '\u2014%'}
          sub="PII \uBE44\uC2DD\uBCC4\uD654 \uBD84\uC11D"
          icon={<ShieldCheck size={20} />}
        />
      </div>

      {/* Main Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Visit Type Distribution */}
        <div className="lg:col-span-2 bg-white p-6 rounded-xl border border-gray-200 shadow-sm relative group">
          <div className="flex justify-between items-center mb-6">
             <h3 className="text-lg font-bold text-[#53565A] flex items-center gap-2">
                <Network size={18} className="text-[#006241]" />
                \uC9C4\uB8CC\uC720\uD615\uBCC4 \uBD84\uD3EC (Visit Type Distribution)
             </h3>
             <span className="text-xs text-[#A8A8A8] bg-[#F5F0E8] px-2 py-1 rounded">OMOP CDM \uC2E4\uB370\uC774\uD130</span>
          </div>
          <div className="h-64 w-full">
            {visitTypeData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={visitTypeData}
                onClick={(data) => onChartClick(data, '\uC9C4\uB8CC\uC720\uD615 \uC0C1\uC138')}
                className="cursor-pointer"
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="type" stroke="#94a3b8" tick={{fill: '#64748b', fontSize: 13, fontWeight: 600}} axisLine={false} tickLine={false} />
                <YAxis stroke="#94a3b8" tick={{fill: '#64748b', fontSize: 11}} axisLine={false} tickLine={false} tickFormatter={(v) => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : v >= 1000 ? `${(v/1000).toFixed(0)}K` : v} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="count" name="\uAC74\uC218" radius={[6, 6, 0, 0]} barSize={80}>
                  {visitTypeData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={VISIT_TYPE_COLORS[entry.type] || '#94a3b8'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-sm text-[#A8A8A8]">\uB370\uC774\uD130 \uB85C\uB529 \uC911...</div>
            )}
          </div>
          {visitTypeData.length > 0 && (
          <div className="flex justify-between text-xs mt-2 font-mono">
            <div className="text-[#006241]">\uCD1D {visitTypeData.reduce((s, d) => s + d.count, 0).toLocaleString()}\uAC74</div>
            <div className="text-[#A8A8A8]">{visitTypeData.length}\uAC1C \uC720\uD615</div>
          </div>
          )}
        </div>

        {/* Data Quality Index */}
        <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm group">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-lg font-bold text-[#53565A] flex items-center gap-2">
                <ShieldCheck size={18} className="text-[#52A67D]" />
                \uB370\uC774\uD130 \uD488\uC9C8 \uC9C0\uC218
            </h3>
          </div>
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={qualityData}
                layout="vertical"
                onClick={(data) => onChartClick(data, '\uD488\uC9C8 \uC9C0\uC218 \uC0C1\uC138')}
                className="cursor-pointer"
              >
                <XAxis type="number" domain={[0, 100]} hide />
                <YAxis dataKey="domain" type="category" stroke="#53565A" width={90} tick={{fontSize: 11, fontWeight: 500}} axisLine={false} tickLine={false} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="score" name="\uD488\uC9C8 \uC810\uC218" radius={[0, 4, 4, 0]} barSize={24} background={{ fill: '#f8fafc' }}>
                   {qualityData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.score > 90 ? '#52A67D' : entry.score > 80 ? '#FF6F00' : '#DC2626'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Infrastructure & Activity Panel */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
         {/* Infrastructure Resource Status */}
         <div className="lg:col-span-2 bg-white rounded-xl border border-gray-200 shadow-sm p-6">
            <h3 className="text-lg font-bold text-[#53565A] mb-4 flex items-center gap-2">
                <ServerCog size={20} className="text-[#FF6F00]" />
                \uC778\uD504\uB77C \uC790\uC6D0 \uC0C1\uD0DC (Infrastructure Status)
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Docker Containers */}
                <div className="p-4 bg-[#F5F0E8] rounded-xl border border-gray-200">
                  <h4 className="font-bold text-[#53565A] text-sm mb-3 flex items-center justify-between">
                      <span className="flex items-center gap-2"><Server size={14} /> Infrastructure Containers</span>
                      <span className="text-[10px] bg-[#52A67D] text-white px-2 py-0.5 rounded font-bold">
                        {runningProtected.length}/{protectedContainers.length}
                      </span>
                  </h4>
                  <div className="space-y-3">
                      {/* Progress bar */}
                      {protectedContainers.length > 0 && (
                        <div className="space-y-1">
                          <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                            <div
                              className="bg-[#52A67D] h-2 rounded-full transition-all duration-500"
                              style={{
                                width: `${(runningProtected.length / protectedContainers.length * 100)}%`
                              }}
                            ></div>
                          </div>
                          <div className="flex justify-between text-[10px] text-[#A8A8A8]">
                            <span>{runningProtected.length} Running</span>
                            <span>{Math.round(runningProtected.length / protectedContainers.length * 100)}%</span>
                          </div>
                        </div>
                      )}

                      {/* Running containers with type badges */}
                      {runningProtected.length > 0 && (
                        <div className="space-y-1.5">
                          {runningProtected.map((c, i) => {
                            const containerType = getContainerType(c.name);
                            return (
                              <div key={i} className="flex items-center gap-2 text-xs">
                                <CheckCircleOutlined className="text-[#52A67D] text-sm flex-shrink-0" />
                                <span className="text-[#53565A] font-mono flex-grow">{c.name.replace('asan-', '')}</span>
                                <span
                                  className="text-[9px] px-1.5 py-0.5 rounded font-bold text-white flex-shrink-0"
                                  style={{ backgroundColor: containerType.color }}
                                >
                                  {containerType.label}
                                </span>
                              </div>
                            );
                          })}
                        </div>
                      )}

                      {/* Stopped containers */}
                      {stoppedProtected.length > 0 && (
                        <div className="space-y-1.5 pt-2 border-t border-gray-200">
                          {stoppedProtected.map((c, i) => {
                            const containerType = getContainerType(c.name);
                            return (
                              <div key={i} className="flex items-center gap-2 text-xs opacity-60">
                                <CloseCircleOutlined className="text-[#DC2626] text-sm flex-shrink-0" />
                                <span className="text-[#A8A8A8] font-mono line-through flex-grow">{c.name.replace('asan-', '')}</span>
                                <span
                                  className="text-[9px] px-1.5 py-0.5 rounded font-bold text-white flex-shrink-0 opacity-50"
                                  style={{ backgroundColor: containerType.color }}
                                >
                                  {containerType.label}
                                </span>
                              </div>
                            );
                          })}
                        </div>
                      )}

                      {/* No infrastructure containers */}
                      {protectedContainers.length === 0 && (
                        <div className="text-xs text-[#A8A8A8] text-center py-2">
                          No infrastructure containers
                        </div>
                      )}

                      <div className="text-[10px] text-[#A8A8A8] text-right mt-3 pt-2 border-t border-gray-200">
                        Docker Compose Stack
                      </div>
                  </div>
                </div>

                {/* GPU Resources */}
                <div className="p-4 bg-[#F5F0E8] rounded-xl border border-gray-200">
                  <h4 className="font-bold text-[#53565A] text-sm mb-3 flex items-center justify-between">
                      <span className="flex items-center gap-2"><Cpu size={14} /> GPU Resources</span>
                      <span className={`text-[10px] text-white px-2 py-0.5 rounded font-bold ${gpuInfo.available ? 'bg-[#FF6F00]' : 'bg-gray-400'}`}>
                        {gpuInfo.available ? `${gpuInfo.gpus.length} GPU${gpuInfo.gpus.length > 1 ? 's' : ''}` : 'N/A'}
                      </span>
                  </h4>
                  {gpuInfo.available ? (
                    <div className="space-y-3">
                      {/* Progress bar */}
                      {gpuInfo.gpus.length > 0 && (
                        <div className="space-y-1">
                          <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                            <div
                              className={`h-2 rounded-full transition-all duration-500 ${
                                Math.round(gpuInfo.gpus.reduce((sum: number, g: any) => sum + g.utilization_percent, 0) / gpuInfo.gpus.length) > 70
                                  ? 'bg-[#FF6F00]'
                                  : Math.round(gpuInfo.gpus.reduce((sum: number, g: any) => sum + g.utilization_percent, 0) / gpuInfo.gpus.length) > 40
                                  ? 'bg-[#FFA500]'
                                  : 'bg-[#52A67D]'
                              }`}
                              style={{
                                width: `${gpuInfo.gpus.length > 0 ? Math.round(gpuInfo.gpus.reduce((sum: number, g: any) => sum + g.utilization_percent, 0) / gpuInfo.gpus.length) : 0}%`
                              }}
                            ></div>
                          </div>
                          <div className="flex justify-between text-[10px] text-[#A8A8A8]">
                            <span>Avg Utilization</span>
                            <span>{gpuInfo.gpus.length > 0 ? Math.round(gpuInfo.gpus.reduce((sum: number, g: any) => sum + g.utilization_percent, 0) / gpuInfo.gpus.length) : 0}%</span>
                          </div>
                        </div>
                      )}

                      {/* GPU List with badges */}
                      {gpuInfo.gpus.length > 0 && (
                        <div className="space-y-1.5">
                          {gpuInfo.gpus.map((gpu: any, idx: number) => {
                            const utilizationColor = gpu.utilization_percent > 70 ? '#FF6F00' : gpu.utilization_percent > 40 ? '#FFA500' : '#52A67D';
                            return (
                              <div key={idx} className="flex items-center gap-2 text-xs">
                                <span className="text-[#53565A] font-mono flex-shrink-0">GPU #{gpu.index}</span>
                                <div className="flex-grow flex items-center gap-1">
                                  <div className="w-full bg-gray-200 rounded-full h-1.5 overflow-hidden">
                                    <div
                                      className="h-1.5 rounded-full transition-all duration-500"
                                      style={{ width: `${gpu.utilization_percent}%`, backgroundColor: utilizationColor }}
                                    ></div>
                                  </div>
                                </div>
                                <span
                                  className="text-[9px] px-1.5 py-0.5 rounded font-bold text-white flex-shrink-0"
                                  style={{ backgroundColor: utilizationColor }}
                                >
                                  {gpu.utilization_percent}%
                                </span>
                                <span className="text-[9px] text-[#A8A8A8] font-mono flex-shrink-0">
                                  {(gpu.memory_used_mb / 1024).toFixed(1)}/{(gpu.memory_total_mb / 1024).toFixed(1)}GB
                                </span>
                              </div>
                            );
                          })}
                        </div>
                      )}

                      <div className="text-[10px] text-[#A8A8A8] text-right mt-3 pt-2 border-t border-gray-200">
                        {gpuInfo.gpus.length > 0 ? `GPU Server (SSH Tunnel)` : 'No GPUs detected'}
                      </div>
                    </div>
                  ) : (
                    <div className="flex flex-col items-center justify-center h-24 text-center">
                      <CloseCircleOutlined className="text-2xl text-gray-300 mb-2" />
                      <span className="text-xs text-[#A8A8A8]">GPU \uC11C\uBC84 \uC5F0\uACB0 \uC5C6\uC74C</span>
                      <span className="text-[10px] text-[#A8A8A8] mt-1">SSH \uD130\uB110 \uD655\uC778 \uD544\uC694</span>
                    </div>
                  )}
                </div>
            </div>
         </div>

         {/* Measurement Activity Timeline */}
         <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 flex flex-col">
            <h3 className="text-lg font-bold text-[#53565A] mb-4 flex items-center gap-2">
                <Activity size={20} className="text-[#52A67D]" />
                \uC5F0\uB3C4\uBCC4 \uAC80\uC0AC \uD65C\uB3D9 (Measurement Activity)
            </h3>
            <div className="flex-grow min-h-[150px]">
               {activityData.length > 0 ? (
               <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={activityData}>
                     <defs>
                       <linearGradient id="colorActivity" x1="0" y1="0" x2="0" y2="1">
                         <stop offset="5%" stopColor="#52A67D" stopOpacity={0.3}/>
                         <stop offset="95%" stopColor="#52A67D" stopOpacity={0}/>
                       </linearGradient>
                     </defs>
                     <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                     <XAxis dataKey="month" tick={{fontSize: 10, fill: '#94a3b8'}} axisLine={false} tickLine={false} />
                     <YAxis tick={{fontSize: 10, fill: '#94a3b8'}} axisLine={false} tickLine={false} />
                     <Tooltip content={<CustomTooltip />} />
                     <Area type="monotone" dataKey="count" name="\uAC80\uC0AC \uAC74\uC218" stroke="#52A67D" strokeWidth={2} fillOpacity={1} fill="url(#colorActivity)" />
                  </AreaChart>
               </ResponsiveContainer>
               ) : (
               <div className="flex items-center justify-center h-full text-sm text-[#A8A8A8]">\uB370\uC774\uD130 \uB85C\uB529 \uC911...</div>
               )}
            </div>
            {activityData.length > 0 && (
            <div className="flex justify-between text-xs mt-2 font-mono">
               <div className="text-[#52A67D]">\uCD1D {activityData.reduce((s, d) => s + d.count, 0).toLocaleString()}\uAC74</div>
               <div className="text-[#A8A8A8]">{activityData.length}\uAC1C\uC6D4</div>
            </div>
            )}
         </div>
      </div>
    </>
  );
};

export default OperationalView;
