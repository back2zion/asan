import React from 'react';
import { Database, Activity, Server, ShieldCheck, Cpu, Network, ServerCog, Search } from 'lucide-react';

const ArchitectureView: React.FC = () => {
  return (
    <div className="space-y-6">
      {/* Data Flow Diagram */}
      <div className="bg-white p-8 rounded-xl border border-gray-200 shadow-sm relative overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(#e5e7eb_1px,transparent_1px)] [background-size:16px_16px] opacity-20"></div>
        <div className="relative z-10">
          <h3 className="text-center font-bold text-xl text-[#006241] mb-2">{'서울아산병원 통합 데이터 플랫폼 구성 체계도'}</h3>
          <p className="text-center text-xs text-[#A8A8A8] mb-8">SFR-001: {'목표시스템 구성 및 데이터 흐름'}</p>

          {/* Row 1: Data Sources */}
          <div className="mb-4">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Data Sources ({'원천 시스템'})</div>
            <div className="grid grid-cols-5 gap-3">
              {[
                { name: 'EMR (전자의무기록)', icon: <Database size={18} />, color: '#E53E3E' },
                { name: 'OCS (처방)', icon: <Database size={18} />, color: '#DD6B20' },
                { name: 'LIS (검사)', icon: <Database size={18} />, color: '#805AD5' },
                { name: 'PACS (영상)', icon: <Database size={18} />, color: '#D69E2E' },
                { name: 'ERP/원무', icon: <Database size={18} />, color: '#38A169' },
              ].map((s, i) => (
                <div key={i} className="flex flex-col items-center gap-1 p-3 border-2 border-dashed rounded-lg bg-gray-50" style={{ borderColor: s.color + '80' }}>
                  <div style={{ color: s.color }}>{s.icon}</div>
                  <span className="text-[11px] font-bold text-center" style={{ color: s.color }}>{s.name}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Arrow down */}
          <div className="flex justify-center my-3">
            <div className="flex flex-col items-center">
              <div className="w-0.5 h-4 bg-gray-300"></div>
              <div className="text-[10px] bg-[#E53E3E] text-white px-3 py-1 rounded-full font-bold">CDC / Debezium / HL7 FHIR</div>
              <div className="w-0.5 h-4 bg-gray-300"></div>
              <div className="w-0 h-0 border-l-[6px] border-l-transparent border-r-[6px] border-r-transparent border-t-[8px] border-t-gray-300"></div>
            </div>
          </div>

          {/* Row 2: Data Lake Zones (ODS -> DW -> DM) */}
          <div className="mb-4">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Data Lake Zones (ODS {'→'} DW {'→'} DM)</div>
            <div className="grid grid-cols-5 gap-2">
              {[
                { zone: 'Source (ODS)', desc: '원본 수집', color: '#E53E3E', tech: 'JSON/CSV', layer: 'ODS' },
                { zone: 'Bronze (ODS)', desc: '스키마 보존 적재', color: '#DD6B20', tech: 'Parquet/S3', layer: 'ODS' },
                { zone: 'Silver (DW)', desc: 'OMOP CDM 표준 변환', color: '#805AD5', tech: 'Apache Iceberg', layer: 'DW' },
                { zone: 'Gold (DW)', desc: '큐레이션/비식별화', color: '#D69E2E', tech: 'Apache Iceberg', layer: 'DW' },
                { zone: 'Mart (DM)', desc: '서비스별 집계 마트', color: '#38A169', tech: 'PostgreSQL/Delta', layer: 'DM' },
              ].map((z, i) => (
                <div key={i} className="p-3 rounded-lg border-2 text-center" style={{ borderColor: z.color, background: z.color + '08' }}>
                  <div className="text-[9px] font-bold text-white px-2 py-0.5 rounded inline-block mb-1" style={{ background: z.color }}>{z.layer}</div>
                  <div className="text-xs font-bold" style={{ color: z.color }}>{z.zone}</div>
                  <div className="text-[10px] text-gray-500 mt-1">{z.desc}</div>
                  <div className="text-[9px] text-gray-400 font-mono mt-1">{z.tech}</div>
                </div>
              ))}
            </div>
            {/* Arrow labels between zones */}
            <div className="grid grid-cols-5 gap-2 mt-1">
              {['', '→ ETL 수집', '→ CDM 변환', '→ 품질검증', '→ 집계/요약'].map((label, i) => (
                <div key={i} className="text-center text-[9px] text-gray-400 font-mono">{label}</div>
              ))}
            </div>
          </div>

          {/* Arrow down */}
          <div className="flex justify-center my-3">
            <div className="flex flex-col items-center">
              <div className="w-0.5 h-4 bg-gray-300"></div>
              <div className="text-[10px] bg-[#006241] text-white px-3 py-1 rounded-full font-bold">Trino Query Federation / FastAPI</div>
              <div className="w-0.5 h-4 bg-gray-300"></div>
              <div className="w-0 h-0 border-l-[6px] border-l-transparent border-r-[6px] border-r-transparent border-t-[8px] border-t-gray-300"></div>
            </div>
          </div>

          {/* Row 3: Service Layer */}
          <div className="mb-4">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Service & AI Layer ({'서비스 계층'})</div>
            <div className="grid grid-cols-4 gap-3">
              {[
                { name: 'BI 대시보드', desc: 'Apache Superset', icon: <Activity size={18} />, color: '#006241' },
                { name: 'AI 분석환경', desc: 'JupyterLab + MLflow', icon: <Cpu size={18} />, color: '#FF6F00' },
                { name: 'NLP 구조화', desc: 'BioClinicalBERT + Qwen3', icon: <ServerCog size={18} />, color: '#805AD5' },
                { name: 'CDW 연구지원', desc: 'Text2SQL + RAG', icon: <Search size={18} />, color: '#D69E2E' },
              ].map((s, i) => (
                <div key={i} className="flex flex-col items-center gap-1 p-4 rounded-lg border-2 shadow-sm" style={{ borderColor: s.color, background: s.color + '08' }}>
                  <div style={{ color: s.color }}>{s.icon}</div>
                  <span className="text-xs font-bold" style={{ color: s.color }}>{s.name}</span>
                  <span className="text-[10px] text-gray-500">{s.desc}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Row 4: Governance Bar */}
          <div className="p-3 rounded-lg border border-gray-300 bg-gradient-to-r from-[#006241]/5 to-[#52A67D]/5">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Governance & Security Layer ({'거버넌스'})</div>
            <div className="flex gap-4 justify-center flex-wrap">
              {['메타데이터 관리', '데이터 품질', '표준 용어', '비식별화 (K-익명성)', '접근 제어 (RBAC)', '데이터 리니지'].map((g, i) => (
                <span key={i} className="text-[11px] text-[#006241] font-medium flex items-center gap-1">
                  <ShieldCheck size={12} /> {g}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* HW/SW Deployment Layout */}
      <div className="bg-white p-8 rounded-xl border border-gray-200 shadow-sm">
        <h3 className="font-bold text-lg text-[#53565A] mb-1 flex items-center gap-2">
          <ServerCog size={20} className="text-[#006241]" />
          HW/SW {'배치 구성안'}
        </h3>
        <p className="text-xs text-[#A8A8A8] mb-4">SFR-001: {'구성 영역별 하드웨어 및 소프트웨어 배치'}</p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* App Server */}
          <div className="border rounded-xl p-4 bg-blue-50/50 border-blue-200">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center"><Server size={16} className="text-white" /></div>
              <div>
                <div className="text-sm font-bold text-blue-700">{'애플리케이션 서버'}</div>
                <div className="text-[10px] text-blue-500">Ubuntu 22.04 LTS</div>
              </div>
            </div>
            <div className="space-y-1.5 text-[11px]">
              <div className="flex justify-between"><span className="text-gray-500">CPU / RAM</span><span className="font-mono text-gray-700">16 Core / 64 GB</span></div>
              <div className="flex justify-between"><span className="text-gray-500">Disk</span><span className="font-mono text-gray-700">SSD 1 TB</span></div>
              <div className="border-t border-blue-200 pt-1.5 mt-1.5 space-y-1">
                {['FastAPI (Backend :8000)', 'Vite (Frontend :5173)', 'Nginx (Reverse Proxy :80)', 'Airflow (Scheduler :18080)', 'Redis (Cache :16379)', 'Milvus (Vector DB :19530)', 'MinIO (S3 :19000)'].map((sw, i) => (
                  <div key={i} className="flex items-center gap-1 text-gray-600"><div className="w-1.5 h-1.5 rounded-full bg-blue-400"></div>{sw}</div>
                ))}
              </div>
            </div>
          </div>

          {/* DB Server */}
          <div className="border rounded-xl p-4 bg-purple-50/50 border-purple-200">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 rounded-lg bg-purple-500 flex items-center justify-center"><Database size={16} className="text-white" /></div>
              <div>
                <div className="text-sm font-bold text-purple-700">{'데이터베이스 서버'}</div>
                <div className="text-[10px] text-purple-500">Ubuntu 22.04 LTS</div>
              </div>
            </div>
            <div className="space-y-1.5 text-[11px]">
              <div className="flex justify-between"><span className="text-gray-500">CPU / RAM</span><span className="font-mono text-gray-700">32 Core / 128 GB</span></div>
              <div className="flex justify-between"><span className="text-gray-500">Disk</span><span className="font-mono text-gray-700">NVMe 4 TB (RAID 10)</span></div>
              <div className="border-t border-purple-200 pt-1.5 mt-1.5 space-y-1">
                {['PostgreSQL 13 (OMOP CDM :5436)', 'PostgreSQL 15 (Superset :15432)', 'PostgreSQL 14 (Airflow Meta :15432)', 'Apache Superset (BI :18088)', 'MLflow (Model Registry :5000)'].map((sw, i) => (
                  <div key={i} className="flex items-center gap-1 text-gray-600"><div className="w-1.5 h-1.5 rounded-full bg-purple-400"></div>{sw}</div>
                ))}
              </div>
            </div>
          </div>

          {/* GPU Server */}
          <div className="border rounded-xl p-4 bg-orange-50/50 border-orange-200">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 rounded-lg bg-orange-500 flex items-center justify-center"><Cpu size={16} className="text-white" /></div>
              <div>
                <div className="text-sm font-bold text-orange-700">GPU {'서버'} (AI)</div>
                <div className="text-[10px] text-orange-500">SSH Tunnel {'접근'}</div>
              </div>
            </div>
            <div className="space-y-1.5 text-[11px]">
              <div className="flex justify-between"><span className="text-gray-500">GPU</span><span className="font-mono text-gray-700">NVIDIA A100 80GB</span></div>
              <div className="flex justify-between"><span className="text-gray-500">CPU / RAM</span><span className="font-mono text-gray-700">64 Core / 256 GB</span></div>
              <div className="border-t border-orange-200 pt-1.5 mt-1.5 space-y-1">
                {['Qwen3 LLM (Text2SQL :28888)', 'Paper2Slides (발표자료 :29001)', 'Medical NER (BioClinicalBERT :28100)', 'JupyterLab (분석환경 :18888)'].map((sw, i) => (
                  <div key={i} className="flex items-center gap-1 text-gray-600"><div className="w-1.5 h-1.5 rounded-full bg-orange-400"></div>{sw}</div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Network Topology */}
        <div className="mt-4 p-3 rounded-lg border border-gray-200 bg-gray-50">
          <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Network Topology ({'포트 매핑'})</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[10px]">
            {[
              { port: ':80', desc: 'Nginx → 프론트/API', color: '#006241' },
              { port: ':8000', desc: 'FastAPI Backend', color: '#2F54EB' },
              { port: ':5436', desc: 'OMOP CDM (PG13)', color: '#722ED1' },
              { port: ':19530', desc: 'Milvus Vector DB', color: '#13C2C2' },
              { port: ':19000', desc: 'MinIO S3 API', color: '#C41D7F' },
              { port: ':18080', desc: 'Airflow Webserver', color: '#13C2C2' },
              { port: ':18088', desc: 'Apache Superset', color: '#FAAD14' },
              { port: ':28888', desc: 'Qwen3 LLM (SSH)', color: '#FF6F00' },
              { port: ':28100', desc: 'Medical NER (SSH)', color: '#FF6F00' },
            ].map((p, i) => (
              <div key={i} className="flex items-center gap-2 bg-white rounded px-2 py-1 border border-gray-200">
                <span className="font-mono font-bold" style={{ color: p.color }}>{p.port}</span>
                <span className="text-gray-500">{p.desc}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ArchitectureView;
