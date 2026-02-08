import React from 'react';
import { Database, Activity, Server, ShieldCheck, Cpu, Network, ServerCog, Search } from 'lucide-react';

const ArchitectureView: React.FC = () => {
  return (
    <div className="space-y-6">
      {/* Data Flow Diagram */}
      <div className="bg-white p-8 rounded-xl border border-gray-200 shadow-sm relative overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(#e5e7eb_1px,transparent_1px)] [background-size:16px_16px] opacity-20"></div>
        <div className="relative z-10">
          <h3 className="text-center font-bold text-xl text-[#006241] mb-2">{'\uC11C\uC6B8\uC544\uC0B0\uBCD1\uC6D0 \uD1B5\uD569 \uB370\uC774\uD130 \uD50C\uB7AB\uD3FC \uAD6C\uC131 \uCCB4\uACC4\uB3C4'}</h3>
          <p className="text-center text-xs text-[#A8A8A8] mb-8">SFR-001: {'\uBAA9\uD45C\uC2DC\uC2A4\uD15C \uAD6C\uC131 \uBC0F \uB370\uC774\uD130 \uD750\uB984'}</p>

          {/* Row 1: Data Sources */}
          <div className="mb-4">
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Data Sources ({'\uC6D0\uCC9C \uC2DC\uC2A4\uD15C'})</div>
            <div className="grid grid-cols-5 gap-3">
              {[
                { name: 'EMR (\uC804\uC790\uC758\uBB34\uAE30\uB85D)', icon: <Database size={18} />, color: '#E53E3E' },
                { name: 'OCS (\uCC98\uBC29)', icon: <Database size={18} />, color: '#DD6B20' },
                { name: 'LIS (\uAC80\uC0AC)', icon: <Database size={18} />, color: '#805AD5' },
                { name: 'PACS (\uC601\uC0C1)', icon: <Database size={18} />, color: '#D69E2E' },
                { name: 'ERP/\uC6D0\uBB34', icon: <Database size={18} />, color: '#38A169' },
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
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Data Lake Zones (ODS {'\u2192'} DW {'\u2192'} DM)</div>
            <div className="grid grid-cols-5 gap-2">
              {[
                { zone: 'Source (ODS)', desc: '\uC6D0\uBCF8 \uC218\uC9D1', color: '#E53E3E', tech: 'JSON/CSV', layer: 'ODS' },
                { zone: 'Bronze (ODS)', desc: '\uC2A4\uD0A4\uB9C8 \uBCF4\uC874 \uC801\uC7AC', color: '#DD6B20', tech: 'Parquet/S3', layer: 'ODS' },
                { zone: 'Silver (DW)', desc: 'OMOP CDM \uD45C\uC900 \uBCC0\uD658', color: '#805AD5', tech: 'Apache Iceberg', layer: 'DW' },
                { zone: 'Gold (DW)', desc: '\uD050\uB808\uC774\uC158/\uBE44\uC2DD\uBCC4\uD654', color: '#D69E2E', tech: 'Apache Iceberg', layer: 'DW' },
                { zone: 'Mart (DM)', desc: '\uC11C\uBE44\uC2A4\uBCC4 \uC9D1\uACC4 \uB9C8\uD2B8', color: '#38A169', tech: 'PostgreSQL/Delta', layer: 'DM' },
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
              {['', '\u2192 ETL \uC218\uC9D1', '\u2192 CDM \uBCC0\uD658', '\u2192 \uD488\uC9C8\uAC80\uC99D', '\u2192 \uC9D1\uACC4/\uC694\uC57D'].map((label, i) => (
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
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2 pl-1">Service & AI Layer ({'\uC11C\uBE44\uC2A4 \uACC4\uCE35'})</div>
            <div className="grid grid-cols-4 gap-3">
              {[
                { name: 'BI \uB300\uC2DC\uBCF4\uB4DC', desc: 'Apache Superset', icon: <Activity size={18} />, color: '#006241' },
                { name: 'AI \uBD84\uC11D\uD658\uACBD', desc: 'JupyterLab + MLflow', icon: <Cpu size={18} />, color: '#FF6F00' },
                { name: 'NLP \uAD6C\uC870\uD654', desc: 'BioClinicalBERT + Qwen3', icon: <ServerCog size={18} />, color: '#805AD5' },
                { name: 'CDW \uC5F0\uAD6C\uC9C0\uC6D0', desc: 'Text2SQL + RAG', icon: <Search size={18} />, color: '#D69E2E' },
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
            <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Governance & Security Layer ({'\uAC70\uBC84\uB10C\uC2A4'})</div>
            <div className="flex gap-4 justify-center flex-wrap">
              {['\uBA54\uD0C0\uB370\uC774\uD130 \uAD00\uB9AC', '\uB370\uC774\uD130 \uD488\uC9C8', '\uD45C\uC900 \uC6A9\uC5B4', '\uBE44\uC2DD\uBCC4\uD654 (K-\uC775\uBA85\uC131)', '\uC811\uADFC \uC81C\uC5B4 (RBAC)', '\uB370\uC774\uD130 \uB9AC\uB2C8\uC9C0'].map((g, i) => (
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
          HW/SW {'\uBC30\uCE58 \uAD6C\uC131\uC548'}
        </h3>
        <p className="text-xs text-[#A8A8A8] mb-4">SFR-001: {'\uAD6C\uC131 \uC601\uC5ED\uBCC4 \uD558\uB4DC\uC6E8\uC5B4 \uBC0F \uC18C\uD504\uD2B8\uC6E8\uC5B4 \uBC30\uCE58'}</p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* App Server */}
          <div className="border rounded-xl p-4 bg-blue-50/50 border-blue-200">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center"><Server size={16} className="text-white" /></div>
              <div>
                <div className="text-sm font-bold text-blue-700">{'\uC560\uD50C\uB9AC\uCF00\uC774\uC158 \uC11C\uBC84'}</div>
                <div className="text-[10px] text-blue-500">Ubuntu 22.04 LTS</div>
              </div>
            </div>
            <div className="space-y-1.5 text-[11px]">
              <div className="flex justify-between"><span className="text-gray-500">CPU / RAM</span><span className="font-mono text-gray-700">16 Core / 64 GB</span></div>
              <div className="flex justify-between"><span className="text-gray-500">Disk</span><span className="font-mono text-gray-700">SSD 1 TB</span></div>
              <div className="border-t border-blue-200 pt-1.5 mt-1.5 space-y-1">
                {['FastAPI (Backend :8000)', 'Vite (Frontend :5173)', 'Nginx (Reverse Proxy :80)', 'Airflow (Scheduler :18080)', 'Redis (Cache :16379)', 'Qdrant (Vector DB :16333)'].map((sw, i) => (
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
                <div className="text-sm font-bold text-purple-700">{'\uB370\uC774\uD130\uBCA0\uC774\uC2A4 \uC11C\uBC84'}</div>
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
                <div className="text-sm font-bold text-orange-700">GPU {'\uC11C\uBC84'} (AI)</div>
                <div className="text-[10px] text-orange-500">SSH Tunnel {'\uC811\uADFC'}</div>
              </div>
            </div>
            <div className="space-y-1.5 text-[11px]">
              <div className="flex justify-between"><span className="text-gray-500">GPU</span><span className="font-mono text-gray-700">NVIDIA A100 80GB</span></div>
              <div className="flex justify-between"><span className="text-gray-500">CPU / RAM</span><span className="font-mono text-gray-700">64 Core / 256 GB</span></div>
              <div className="border-t border-orange-200 pt-1.5 mt-1.5 space-y-1">
                {['Qwen3 LLM (Text2SQL :28888)', 'Paper2Slides (\uBC1C\uD45C\uC790\uB8CC :29001)', 'Medical NER (BioClinicalBERT :28100)', 'JupyterLab (\uBD84\uC11D\uD658\uACBD :18888)'].map((sw, i) => (
                  <div key={i} className="flex items-center gap-1 text-gray-600"><div className="w-1.5 h-1.5 rounded-full bg-orange-400"></div>{sw}</div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Network Topology */}
        <div className="mt-4 p-3 rounded-lg border border-gray-200 bg-gray-50">
          <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Network Topology ({'\uD3EC\uD2B8 \uB9E4\uD551'})</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[10px]">
            {[
              { port: ':80', desc: 'Nginx \u2192 \uD504\uB860\uD2B8/API', color: '#006241' },
              { port: ':8000', desc: 'FastAPI Backend', color: '#2F54EB' },
              { port: ':5436', desc: 'OMOP CDM (PG13)', color: '#722ED1' },
              { port: ':16333', desc: 'Qdrant Vector DB', color: '#13C2C2' },
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
