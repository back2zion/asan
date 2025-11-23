
import React from 'react';
import { Shield, FileCheck, Users, Lock, AlertOctagon } from 'lucide-react';
import { GovernancePolicy } from '../types';

const policies: GovernancePolicy[] = [
  { id: '1', name: '개인정보(PII) 비식별화 표준', category: 'Security', status: 'Active', complianceRate: 100 },
  { id: '2', name: '임상 데이터 완전성(Completeness)', category: 'Quality', status: 'Active', complianceRate: 98.5 },
  { id: '3', name: '연구용 데이터 접근 제어 (IRB)', category: 'Compliance', status: 'Active', complianceRate: 100 },
  { id: '4', name: '스키마 변경(Drift) 모니터링', category: 'Quality', status: 'Monitoring', complianceRate: 92 },
];

export const Governance: React.FC = () => {
  return (
    <div className="p-6 text-[#53565A]">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-[#53565A] mb-2">거버넌스 & 보안 (Governance)</h2>
        <p className="text-[#A8A8A8] text-sm">데이터 정책, 품질 표준 및 접근 제어를 관리합니다.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-4 text-[#006241]">
            <Shield size={24} />
            <h3 className="font-bold text-[#53565A]">보안 상태 (Security)</h3>
          </div>
          <div className="space-y-3">
             <div className="flex justify-between text-sm">
                <span className="text-[#A8A8A8]">저장 데이터 암호화</span>
                <span className="text-[#52A67D] font-bold">적용됨</span>
             </div>
             <div className="flex justify-between text-sm">
                <span className="text-[#A8A8A8]">감사 로그(Audit)</span>
                <span className="text-[#52A67D] font-bold">활성</span>
             </div>
             <div className="flex justify-between text-sm">
                <span className="text-[#A8A8A8]">취약점 스캔</span>
                <span className="text-[#53565A]">0 Critical</span>
             </div>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-4 text-[#52A67D]">
            <FileCheck size={24} />
            <h3 className="font-bold text-[#53565A]">데이터 품질 (Quality)</h3>
          </div>
          <div className="flex items-end gap-2 mb-2">
            <span className="text-4xl font-bold text-[#53565A]">98.2</span>
            <span className="text-sm text-[#A8A8A8] mb-1">/ 100</span>
          </div>
          <p className="text-xs text-gray-500">전체 도메인 통합 신뢰도 점수.</p>
        </div>

        <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm">
          <div className="flex items-center gap-3 mb-4 text-[#FF6F00]">
            <Users size={24} />
            <h3 className="font-bold text-[#53565A]">접근 요청 (Access)</h3>
          </div>
          <div className="space-y-3">
             <div className="flex justify-between items-center bg-[#F5F0E8] p-2 rounded">
                <span className="text-sm">김 박사 (순환기내과)</span>
                <span className="text-[10px] bg-[#FF6F00]/20 text-[#FF6F00] px-2 py-1 rounded">승인 대기</span>
             </div>
             <div className="flex justify-between items-center bg-[#F5F0E8] p-2 rounded">
                <span className="text-sm">AI 연구팀</span>
                <span className="text-[10px] bg-[#52A67D]/20 text-[#52A67D] px-2 py-1 rounded">승인됨</span>
             </div>
          </div>
        </div>
      </div>

      <h3 className="text-xl font-bold text-[#53565A] mb-4">활성 정책 (Active Policies)</h3>
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden shadow-sm">
        <table className="w-full text-left">
          <thead className="bg-gray-50 text-[#A8A8A8] text-xs uppercase">
            <tr>
              <th className="p-4">정책명</th>
              <th className="p-4">카테고리</th>
              <th className="p-4">상태</th>
              <th className="p-4 text-right">준수율</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {policies.map(policy => (
              <tr key={policy.id} className="hover:bg-[#F5F0E8] transition-colors">
                <td className="p-4 font-medium text-[#53565A]">{policy.name}</td>
                <td className="p-4">
                  <span className={`text-xs px-2 py-1 rounded border ${
                    policy.category === 'Security' ? 'border-red-500/30 text-red-500' :
                    policy.category === 'Quality' ? 'border-[#52A67D]/30 text-[#52A67D]' :
                    'border-purple-500/30 text-purple-500'
                  }`}>{policy.category}</span>
                </td>
                <td className="p-4 flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-[#52A67D]"></span>
                  <span className="text-sm text-gray-600">{policy.status}</span>
                </td>
                <td className="p-4 text-right">
                  <span className={`font-mono font-bold ${policy.complianceRate === 100 ? 'text-[#52A67D]' : 'text-[#FF6F00]'}`}>
                    {policy.complianceRate}%
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
