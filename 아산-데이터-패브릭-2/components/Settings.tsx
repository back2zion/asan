
import React from 'react';
import { Server, Users, Key, Database, Save, Network, Cpu, Box } from 'lucide-react';

export const Settings: React.FC = () => {
  return (
    <div className="p-6 text-[#53565A] max-w-4xl mx-auto">
      <div className="mb-8 border-b border-gray-200 pb-6">
        <h2 className="text-2xl font-bold text-[#53565A] mb-2">플랫폼 설정 (Configuration)</h2>
        <p className="text-[#A8A8A8] text-sm">연결 문자열, API 키 및 컴퓨팅 리소스를 관리합니다.</p>
      </div>

      <div className="space-y-6">
        {/* 1. Data Lake & Query */}
        <section className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <Server className="text-[#006241]" size={24} />
            <h3 className="font-bold text-lg">데이터 레이크 및 쿼리 엔진 (Data Lake & Query)</h3>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">S3 오브젝트 스토리지 엔드포인트</label>
                <input type="text" defaultValue="https://s3.asan-data.internal:9000" className="w-full bg-[#F5F0E8] border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
             </div>
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">Trino 코디네이터 URL</label>
                <input type="text" defaultValue="https://trino-master.asan.internal:8080" className="w-full bg-[#F5F0E8] border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
             </div>
          </div>
        </section>

        {/* 2. Knowledge Graph */}
        <section className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <Network className="text-[#52A67D]" size={24} />
            <h3 className="font-bold text-lg">지식 그래프 (Neo4j Knowledge Graph)</h3>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">Neo4j Bolt URL</label>
                <input type="text" defaultValue="bolt://neo4j-core.asan.internal:7687" className="w-full bg-[#F5F0E8] border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
             </div>
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">인증 (Username)</label>
                <input type="text" defaultValue="neo4j" className="w-full bg-[#F5F0E8] border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
             </div>
          </div>
        </section>

        {/* 3. Container Resources (ECR-003 NEW ADDITION) */}
        <section className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <Box className="text-[#FF6F00]" size={24} />
            <h3 className="font-bold text-lg">분석 환경 자원 관리 (Container Resources)</h3>
          </div>
          <div className="bg-[#F5F0E8]/50 p-4 rounded-lg mb-4 border border-[#A8A8A8]/20 text-sm text-[#53565A]">
             KubeVirt 기반 사용자 컨테이너(JupyterLab)의 기본 리소스 할당량을 설정합니다.
             <br/>GPU는 NVIDIA MIG(Multi-Instance GPU) 슬라이스 단위로 할당됩니다.
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">사용자당 최대 vCPU</label>
                <div className="flex items-center gap-2">
                   <Cpu size={16} className="text-[#A8A8A8]" />
                   <input type="number" defaultValue="4" className="w-full bg-white border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
                   <span className="text-xs text-[#A8A8A8]">Cores</span>
                </div>
             </div>
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">기본 메모리 (Memory)</label>
                <div className="flex items-center gap-2">
                   <Database size={16} className="text-[#A8A8A8]" />
                   <input type="number" defaultValue="16" className="w-full bg-white border border-gray-300 rounded px-3 py-2 text-[#53565A]" />
                   <span className="text-xs text-[#A8A8A8]">GiB</span>
                </div>
             </div>
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">GPU 할당 (MIG Slice)</label>
                <div className="flex items-center gap-2">
                   <Box size={16} className="text-[#A8A8A8]" />
                   <select className="w-full bg-white border border-gray-300 rounded px-3 py-2 text-[#53565A]">
                      <option>None (CPU Only)</option>
                      <option>1g.5gb (1 Slice)</option>
                      <option selected>2g.10gb (2 Slices)</option>
                      <option>3g.20gb (3 Slices)</option>
                      <option>Full GPU (7 Slices)</option>
                   </select>
                </div>
             </div>
          </div>
        </section>

        {/* 4. AI & Security */}
        <section className="bg-white rounded-xl p-6 border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <Key className="text-[#C9B037]" size={24} />
            <h3 className="font-bold text-lg">AI 및 보안 (AI & Security)</h3>
          </div>
          <div className="space-y-4">
             <div>
                <label className="block text-sm text-[#A8A8A8] mb-2">Gemini API 키 (환경 변수)</label>
                <input type="password" value="************************" disabled className="w-full bg-gray-100 border border-gray-300 rounded px-3 py-2 text-gray-400 cursor-not-allowed" />
             </div>
             <div className="flex items-center justify-between p-4 bg-[#F5F0E8] rounded border border-gray-300">
                <div>
                   <h4 className="font-medium">데이터 비식별화 프록시 (De-identification)</h4>
                   <p className="text-xs text-[#A8A8A8]">LLM 전송 전 개인정보(PII) 마스킹 처리.</p>
                </div>
                <div className="w-10 h-6 bg-[#006241] rounded-full relative cursor-pointer">
                   <div className="w-4 h-4 bg-white rounded-full absolute right-1 top-1"></div>
                </div>
             </div>
          </div>
        </section>

        <div className="flex justify-end">
           <button className="bg-[#006241] hover:bg-[#004e32] text-white px-6 py-2 rounded-lg font-bold flex items-center gap-2 shadow-md">
              <Save size={18} /> 설정 저장
           </button>
        </div>
      </div>
    </div>
  );
};
