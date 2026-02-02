
import React from 'react';
import { GitCommit, RefreshCw, CheckCircle2, XCircle, Clock, Database, Shield, Zap, Layers } from 'lucide-react';
import { PipelineJob } from '../../types/types';

const mockJobs: PipelineJob[] = [
  { id: '1', name: 'EMR_Daily_Ingest', source: 'Oracle (병원 HIS)', target: 'S3 / Bronze (원시)', status: 'Completed', startTime: '02:00 AM', duration: '45분', recordsProcessed: 1250000, type: 'Batch', deid: true },
  { id: '2', name: 'PACS_Image_Sync', source: 'PACS 서버', target: 'S3 Object Store', status: 'Running', startTime: '03:30 AM', duration: '실행 중 (2시간)', recordsProcessed: 450, type: 'Batch', deid: true },
  { id: '3', name: 'Lab_Results_Standardization', source: 'S3 / Bronze', target: 'Iceberg / Silver', status: 'Failed', startTime: '04:15 AM', duration: '12분', recordsProcessed: 0, type: 'Batch', deid: false },
  { id: '4', name: 'IoT_Vital_Stream', source: 'IoT Gateway', target: 'Flink / Kafka', status: 'Running', startTime: '-', duration: '실시간', recordsProcessed: 5400, type: 'Stream', deid: true },
];

export const ETLPipeline: React.FC = () => {
  return (
    <div className="p-6 text-[#53565A]">
      <div className="flex justify-between items-center mb-8">
        <div>
          <h2 className="text-2xl font-bold text-[#53565A] mb-2">데이터 파이프라인 (ETL)</h2>
          <p className="text-[#A8A8A8] text-sm">데이터 수집, 변환 및 적재 워크플로우를 모니터링합니다.</p>
        </div>
        <button className="flex items-center gap-2 bg-[#006241] hover:bg-[#004e32] text-white px-4 py-2 rounded-lg font-medium transition-colors shadow-sm">
          <RefreshCw size={18} />
          <span>상태 새로고침</span>
        </button>
      </div>

      <div className="grid grid-cols-1 gap-4">
        {mockJobs.map((job) => (
          <div key={job.id} className="bg-white rounded-xl border border-gray-200 p-6 flex items-center justify-between hover:shadow-md transition-all shadow-sm">
            <div className="flex items-center gap-4">
              <div className={`p-3 rounded-full border-2 ${
                job.status === 'Completed' ? 'border-[#52A67D]/50 bg-[#52A67D]/10 text-[#52A67D]' :
                job.status === 'Running' ? 'border-blue-500/50 bg-blue-500/10 text-blue-500' :
                job.status === 'Failed' ? 'border-red-500/50 bg-red-500/10 text-red-500' :
                'border-gray-500/50 bg-gray-500/10 text-gray-500'
              }`}>
                {job.status === 'Running' ? <RefreshCw className="animate-spin" size={20} /> : 
                 job.status === 'Completed' ? <CheckCircle2 size={20} /> :
                 job.status === 'Failed' ? <XCircle size={20} /> :
                 <Clock size={20} />}
              </div>
              
              <div>
                <div className="flex items-center gap-2 mb-1">
                    <h3 className="text-lg font-bold text-[#53565A]">{job.name}</h3>
                    {/* SFR-005: Batch/Stream Badge */}
                    <span className={`text-[10px] px-2 py-0.5 rounded font-bold border ${
                        job.type === 'Stream' 
                        ? 'bg-purple-100 text-purple-600 border-purple-200' 
                        : 'bg-gray-100 text-gray-600 border-gray-200'
                    } flex items-center gap-1`}>
                        {job.type === 'Stream' ? <Zap size={10} fill="currentColor" /> : <Layers size={10} />}
                        {job.type}
                    </span>
                    {/* SFR-005, DGR-006: De-id Indicator */}
                    {job.deid && (
                        <span className="text-[10px] bg-blue-50 text-blue-600 border border-blue-200 px-2 py-0.5 rounded flex items-center gap-1 font-medium" title="비식별화(De-identification) 적용됨">
                            <Shield size={10} /> De-ID
                        </span>
                    )}
                </div>
                <div className="flex items-center gap-2 text-sm text-[#A8A8A8] mt-1">
                  <span className="flex items-center gap-1"><Database size={12} /> {job.source}</span>
                  <GitCommit size={14} className="text-gray-400" />
                  <span className="flex items-center gap-1"><Database size={12} /> {job.target}</span>
                </div>
              </div>
            </div>

            <div className="flex items-center gap-8 text-right">
               <div>
                  <p className="text-xs text-[#A8A8A8] uppercase">처리 레코드</p>
                  <p className="font-mono text-[#53565A]">{job.recordsProcessed.toLocaleString()}</p>
               </div>
               <div>
                  <p className="text-xs text-[#A8A8A8] uppercase">소요 시간</p>
                  <p className="font-mono text-[#53565A]">{job.duration}</p>
               </div>
               <div>
                  <p className="text-xs text-[#A8A8A8] uppercase">상태</p>
                  <span className={`font-bold text-sm px-2 py-1 rounded ${
                     job.status === 'Completed' ? 'text-[#52A67D] bg-[#52A67D]/10' :
                     job.status === 'Failed' ? 'text-red-500 bg-red-500/10' :
                     job.status === 'Running' ? 'text-blue-500 bg-blue-500/10' :
                     'text-gray-400'
                  }`}>{job.status}</span>
               </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};
