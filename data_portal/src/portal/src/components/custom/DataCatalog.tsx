
import React, { useState } from 'react';
import { Search, Database, FileText, Table, Lock, Eye, Tag, Share2, Calendar, Link, Globe, Shield, AlertTriangle, LayoutTemplate, TerminalSquare, FileSearch } from 'lucide-react';
import { DataAsset } from '../../types/types';

const mockAssets: DataAsset[] = [
  { 
    id: '1', 
    name: 'EMR_PATIENT_DIM', 
    type: 'Table', 
    domain: 'Clinical', 
    owner: '헬스이노베이션빅데이터센터', 
    description: '연령, 성별, 지역 정보를 포함한 핵심 환자 인구통계 데이터.', 
    sensitivity: 'Confidential', 
    qualityScore: 99, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-01-10',
    tags: ['환자', '인구통계', '마스터'],
    relatedAssets: ['2', '4', '6']
  },
  { 
    id: '2', 
    name: 'LAB_RESULTS_FACT', 
    type: 'Table', 
    domain: 'Clinical', 
    owner: '임상시험센터', 
    description: '환자 방문과 연계된 통합 검사 결과 데이터.', 
    sensitivity: 'Restricted', 
    qualityScore: 98, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-02-15',
    tags: ['검사', '진단', '수치'],
    relatedAssets: ['1']
  },
  { 
    id: '3', 
    name: 'MRI_Brain_Scan_V4', 
    type: 'Unstructured', 
    domain: 'Imaging', 
    owner: '의공학연구소', 
    description: '뇌졸중 예측 모델을 위한 비식별화된 뇌 MRI 데이터셋.', 
    sensitivity: 'Restricted', 
    qualityScore: 95, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-03-20',
    tags: ['MRI', '뇌영상', 'DICOM', 'AI학습'],
    relatedAssets: []
  },
  { 
    id: '4', 
    name: 'Genomic_Variant_Call', 
    type: 'View', 
    domain: 'Genomics', 
    owner: '조직세포자원센터', 
    description: '임상 결과와 매핑된 VCF 데이터 뷰.', 
    sensitivity: 'Internal', 
    qualityScore: 92, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-04-05',
    tags: ['유전체', 'VCF', '변이'],
    relatedAssets: ['1']
  },
  { 
    id: '5', 
    name: 'HL7_FHIR_API', 
    type: 'API', 
    domain: 'Admin', 
    owner: 'R&D사업단', 
    description: '병원 간 진료 정보 교류를 위한 표준 FHIR API 엔드포인트.', 
    sensitivity: 'Public', 
    qualityScore: 100, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-01-01',
    tags: ['FHIR', 'API', '표준', '상호운용성'],
    relatedAssets: ['1', '2']
  },
  { 
    id: '6', 
    name: 'Stem_Cell_Therapy_Log', 
    type: 'Table', 
    domain: 'Clinical', 
    owner: '세포치료센터', 
    description: '줄기세포 치료 임상 시험 참여자의 투여 기록 및 경과.', 
    sensitivity: 'Restricted', 
    qualityScore: 94, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-06-15',
    tags: ['줄기세포', '임상시험', '재생의학'],
    relatedAssets: ['1']
  },
  { 
    id: '7', 
    name: 'Research_Ethics_Guidelines', 
    type: 'Unstructured', 
    domain: 'Admin', 
    owner: '임상연구보호센터', 
    description: '임상 연구 수행 시 준수해야 할 윤리 규정 및 IRB 가이드라인.', 
    sensitivity: 'Public', 
    qualityScore: 100, 
    lastUpdated: '2025-11-23',
    creationDate: '2024-01-20',
    tags: ['IRB', '윤리', '가이드라인'],
    relatedAssets: []
  }
];

export const DataCatalog: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');

  const filteredAssets = mockAssets.filter(asset => 
    asset.name.toLowerCase().includes(searchTerm.toLowerCase()) || 
    asset.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
    asset.tags.some(tag => tag.toLowerCase().includes(searchTerm.toLowerCase()))
  );

  const getTypeConfig = (type: string) => {
    switch (type) {
      case 'Table': return { icon: <Table size={20} />, color: 'bg-[#006241]/10 text-[#006241]', label: '테이블' };
      case 'View': return { icon: <LayoutTemplate size={20} />, color: 'bg-blue-100 text-blue-600', label: '뷰 (View)' };
      case 'Unstructured': return { icon: <FileText size={20} />, color: 'bg-purple-100 text-purple-600', label: '비정형' };
      case 'API': return { icon: <Globe size={20} />, color: 'bg-[#FF6F00]/10 text-[#FF6F00]', label: 'API' };
      default: return { icon: <Database size={20} />, color: 'bg-gray-100 text-gray-600', label: '기타' };
    }
  };

  const getSensitivityConfig = (level: string) => {
    switch (level) {
      case 'Public': return { icon: <Globe size={12} />, color: 'border-[#52A67D]/30 text-[#52A67D] bg-[#52A67D]/5', label: '공개 (Public)' };
      case 'Internal': return { icon: <Shield size={12} />, color: 'border-blue-500/30 text-blue-500 bg-blue-500/5', label: '대외비 (Internal)' };
      case 'Confidential': return { icon: <Lock size={12} />, color: 'border-[#FF6F00]/30 text-[#FF6F00] bg-[#FF6F00]/5', label: '비밀 (Confidential)' };
      case 'Restricted': return { icon: <AlertTriangle size={12} />, color: 'border-red-500/30 text-red-500 bg-red-500/5', label: '극비 (Restricted)' };
      default: return { icon: <Lock size={12} />, color: 'border-gray-500/30 text-gray-500', label: level };
    }
  };

  return (
    <div className="p-6 h-full flex flex-col">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-[#53565A] mb-2">데이터 카탈로그</h2>
        <p className="text-[#A8A8A8] text-sm">의료 데이터 자산을 검색하고 탐색하며 접근을 요청합니다.</p>
      </div>

      <div className="bg-white p-4 rounded-xl border border-gray-200 shadow-sm mb-6 sticky top-0 z-10">
        <div className="relative">
          <Search className="absolute left-3 top-3.5 text-gray-400" size={20} />
          <input 
            type="text" 
            placeholder="데이터셋, 태그 또는 비즈니스 용어 검색 (예: '환자', 'MRI')..." 
            className="w-full bg-[#F5F0E8] border border-gray-300 rounded-lg pl-10 pr-4 py-3 text-[#53565A] focus:outline-none focus:ring-2 focus:ring-[#006241] transition-all placeholder-gray-400"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <div className="flex gap-2 mt-3 overflow-x-auto pb-1">
          {['전체', '임상(Clinical)', '유전체(Genomics)', '영상(Imaging)', '원무(Admin)'].map((filter) => (
            <button key={filter} className="px-3 py-1 bg-white border border-gray-300 hover:bg-[#F5F0E8] rounded-full text-xs text-[#53565A] transition-colors whitespace-nowrap">
              {filter}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 overflow-y-auto pb-6">
        {filteredAssets.map((asset) => {
          const typeConfig = getTypeConfig(asset.type);
          const sensitivityConfig = getSensitivityConfig(asset.sensitivity);

          return (
            <div key={asset.id} className="bg-white rounded-xl border border-gray-200 p-5 hover:shadow-md hover:border-[#006241]/30 transition-all group shadow-sm">
              <div className="flex justify-between items-start">
                <div className="flex items-start gap-4 w-full">
                  {/* Type Icon */}
                  <div className={`p-3 rounded-lg shrink-0 ${typeConfig.color}`} title={typeConfig.label}>
                    {typeConfig.icon}
                  </div>
                  
                  <div className="w-full">
                    <div className="flex items-center gap-2 flex-wrap">
                      <h3 className="text-lg font-bold text-[#53565A] group-hover:text-[#006241] transition-colors">{asset.name}</h3>
                      {/* Sensitivity Badge with Icon */}
                      <span className={`text-[10px] px-2 py-0.5 rounded border flex items-center gap-1 font-medium ${sensitivityConfig.color}`}>
                        {sensitivityConfig.icon}
                        {sensitivityConfig.label}
                      </span>
                    </div>
                    <p className="text-[#A8A8A8] text-sm mt-1">{asset.description}</p>
                    
                    {/* Detailed Metadata Section */}
                    <div className="mt-4 pt-3 border-t border-gray-100 grid grid-cols-1 gap-2">
                      {/* Creation Date */}
                      <div className="flex items-center gap-2 text-xs">
                        <Calendar size={14} className="text-[#A8A8A8]" />
                        <span className="text-[#A8A8A8] min-w-[60px]">생성일:</span>
                        <span className="text-[#53565A] font-medium">{asset.creationDate}</span>
                      </div>

                      {/* Tags */}
                      <div className="flex items-center gap-2 text-xs">
                        <Tag size={14} className="text-[#A8A8A8]" />
                        <span className="text-[#A8A8A8] min-w-[60px]">태그:</span>
                        <div className="flex flex-wrap gap-1">
                          {asset.tags.map((tag, idx) => (
                            <span key={idx} className="bg-[#F5F0E8] text-[#006241] px-2 py-0.5 rounded text-[10px] font-medium">
                              #{tag}
                            </span>
                          ))}
                        </div>
                      </div>

                      {/* Related Assets */}
                      <div className="flex items-start gap-2 text-xs">
                        <Link size={14} className="text-[#A8A8A8] mt-0.5" />
                        <span className="text-[#A8A8A8] min-w-[60px]">연관 자산:</span>
                        <div className="flex flex-wrap gap-2">
                          {asset.relatedAssets.length > 0 ? (
                             asset.relatedAssets.map((relId) => {
                               const relAsset = mockAssets.find(a => a.id === relId);
                               if (!relAsset) return null;
                               const relTypeConfig = getTypeConfig(relAsset.type);
                               return (
                                 <span key={relId} className="flex items-center gap-1 text-[#006241] hover:underline cursor-pointer bg-gray-50 px-1.5 py-0.5 rounded border border-gray-100">
                                    {React.cloneElement(relTypeConfig.icon as React.ReactElement<any>, { size: 10 })}
                                    {relAsset.name}
                                 </span>
                               );
                             })
                          ) : (
                            <span className="text-gray-400">-</span>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-4 mt-3 text-xs text-gray-500 mb-3">
                      <span className="flex items-center gap-1 bg-gray-100 px-2 py-0.5 rounded">{asset.domain}</span>
                      <span className="flex items-center gap-1"><Lock size={12} /> 소유자: {asset.owner}</span>
                      <span className="flex items-center gap-1">수정: {asset.lastUpdated}</span>
                    </div>

                    {/* Action Buttons */}
                    <div className="flex gap-2 mt-4 pt-3 border-t border-gray-100">
                      <button className="flex-1 bg-[#006241] text-white py-2 rounded text-sm font-medium hover:bg-[#004e32] transition-colors flex items-center justify-center gap-2 shadow-sm">
                        <FileSearch size={16} />
                        IRB 데이터 추출 신청
                      </button>
                      <button className="flex-1 bg-white border border-[#006241] text-[#006241] py-2 rounded text-sm font-medium hover:bg-[#006241]/5 transition-colors flex items-center justify-center gap-2 shadow-sm">
                        <TerminalSquare size={16} />
                        JupyterLab 분석
                      </button>
                    </div>

                  </div>
                </div>
                
                <div className="flex flex-col items-end gap-2 shrink-0 ml-4">
                   <div className="text-right">
                      <span className="block text-xs text-gray-400">품질 점수</span>
                      <span className={`font-bold ${asset.qualityScore >= 90 ? 'text-[#52A67D]' : 'text-[#FF6F00]'}`}>{asset.qualityScore}%</span>
                   </div>
                   <div className="flex gap-2 mt-2">
                      <button className="p-2 hover:bg-[#F5F0E8] rounded text-gray-400 hover:text-[#006241]" title="데이터 미리보기">
                          <Eye size={18} />
                      </button>
                      <button className="p-2 hover:bg-[#F5F0E8] rounded text-gray-400 hover:text-[#006241]" title="데이터 리니지">
                          <Share2 size={18} />
                      </button>
                   </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
