
import React, { useState } from 'react';
import { Sparkles, Terminal, Database, Play, Table as TableIcon, Network, MessageSquare, Share2, Download, FileSpreadsheet } from 'lucide-react';
import { generateSqlFromNaturalLanguage } from '../../services/gemini';
import { AISqlResult } from '../../types/types';

const SAMPLE_QUERIES = [
  { id: 1, label: "2형 당뇨병 합병증 추적", query: "2형 당뇨병(E11) 진단 후 3년 이내에 망막병증(Retinopathy) 또는 신장병증(Nephropathy) 진단을 받은 환자 목록을 보여줘." },
  { id: 2, label: "급성 심근경색(AMI) CP 경로", query: "응급실로 내원한 STEMI 환자 중 90분 이내(Door-to-Balloon) PCI 시술을 받은 환자와 시술 의사, 입원 병동 경로를 분석해줘." },
  { id: 3, label: "고위험군(40대 이상) 대사증후군 선별", query: "서울 거주 40세 이상 환자 중 고혈압(I10)과 고지혈증 약물을 동시에 처방받고 있는 대사증후군 고위험군을 선별해줘." },
  { id: 4, label: "항암제 부작용 재입원 분석", query: "폐암 항암치료(Cisplatin) 후 30일 이내에 호중구 감소성 발열(Febrile Neutropenia)로 응급실에 재방문한 환자를 찾아줘." },
  { id: 5, label: "다제약물(Polypharmacy) 노인 환자 관리", query: "65세 이상 노인 환자 중 5개 이상의 만성질환 약물을 복용하면서 낙상 위험 약물(Benzodiazepine)을 중복 처방받은 케이스를 조회해줘." },
  { id: 6, label: "원내 감염(VRE/MRSA) 전파 경로 추적", query: "중환자실(ICU) 내 VRE 양성 환자들의 병상 이동 경로와 접촉 의료진을 네트워크로 분석하여 감염 전파 경로를 시각화해줘." },
];

export const AIAnalytics: React.FC = () => {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState<AISqlResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [viewMode, setViewMode] = useState<'TABLE' | 'GRAPH'>('TABLE');

  const handleAnalyze = async (inputQuery: string = query) => {
    if (!inputQuery.trim()) return;
    setLoading(true);
    setResult(null);
    setQuery(inputQuery);
    
    // Auto switch to graph view for network/relationship queries
    if (inputQuery.includes('관계') || inputQuery.includes('네트워크') || inputQuery.includes('시각화') || inputQuery.includes('망') || inputQuery.includes('경로')) {
        setViewMode('GRAPH');
    } else {
        setViewMode('TABLE');
    }
    
    try {
      const data = await generateSqlFromNaturalLanguage(inputQuery);
      setResult(data);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 h-full flex flex-col text-[#53565A] overflow-hidden">
      <div className="mb-6 flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-[#53565A] mb-2 flex items-center gap-2">
            <Sparkles className="text-[#006241]" />
            AI 데이터 분석가
          </h2>
          <p className="text-[#A8A8A8] text-sm">자연어로 질문하면 SQL 조회 및 Neo4j 지식 그래프(GraphRAG) 분석을 수행합니다.</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 flex-grow overflow-hidden">
        
        {/* Left Panel: Inputs & Samples */}
        <div className="lg:col-span-4 flex flex-col gap-4 overflow-y-auto pr-2">
          
          {/* Query Input */}
          <div className="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
            <label className="text-sm font-bold text-[#53565A] mb-3 block flex items-center gap-2">
              <MessageSquare size={16} className="text-[#006241]"/> 자연어 질의 (Natural Language)
            </label>
            <textarea
              className="w-full bg-[#F5F0E8] border border-gray-300 rounded-lg p-3 text-[#53565A] resize-none focus:ring-2 focus:ring-[#006241] focus:outline-none min-h-[100px] text-sm leading-relaxed placeholder-gray-400"
              placeholder="예: 당뇨병 환자와 담당 의사의 관계를 보여줘..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleAnalyze(query)}
            />
            <div className="mt-3 flex justify-end">
              <button 
                onClick={() => handleAnalyze(query)}
                disabled={loading || !query}
                className="bg-[#006241] hover:bg-[#004e32] text-white px-5 py-2 rounded-lg font-bold text-sm flex items-center gap-2 transition-all disabled:opacity-50 shadow-md shadow-[#006241]/20"
              >
                {loading ? <Sparkles className="animate-spin" size={16} /> : <Play size={16} />}
                분석 실행
              </button>
            </div>
          </div>

          {/* Sample Queries */}
          <div className="bg-white p-5 rounded-xl border border-gray-200 shadow-sm flex-grow">
            <h3 className="text-sm font-bold text-[#53565A] mb-4 flex items-center gap-2">
              <Share2 size={16} className="text-[#52A67D]"/> 추천 시나리오 (GraphRAG)
            </h3>
            <div className="grid grid-cols-1 gap-2">
              {SAMPLE_QUERIES.map((sample) => (
                <button
                  key={sample.id}
                  onClick={() => handleAnalyze(sample.query)}
                  className="text-left px-3 py-3 rounded-lg bg-[#F5F0E8] hover:bg-gray-200 border border-transparent hover:border-[#006241]/30 transition-all group flex items-start gap-3"
                >
                  <div className="mt-1 min-w-[20px] h-5 rounded-full bg-[#A8A8A8] text-[10px] flex items-center justify-center text-white group-hover:bg-[#006241] transition-colors">
                    {sample.id}
                  </div>
                  <div>
                    <span className="block text-sm font-medium text-[#53565A] group-hover:text-[#006241]">{sample.label}</span>
                    <span className="block text-xs text-[#A8A8A8] line-clamp-1 group-hover:text-[#53565A]">{sample.query}</span>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Right Panel: Output & Results */}
        <div className="lg:col-span-8 flex flex-col gap-4 overflow-hidden h-full">
          
          {/* SQL Console / Plan Execution */}
          <div className="bg-[#2D2D2D] rounded-xl border border-gray-300 overflow-hidden flex flex-col max-h-[150px] shrink-0 shadow-sm">
            <div className="bg-[#1E1E1E] px-4 py-2 border-b border-gray-700 flex justify-between items-center">
              {/* SFR-006, AAR-002: Updated Header */}
              <span className="text-xs font-mono text-gray-400 flex items-center gap-2">
                <Terminal size={14} className="text-[#FF6F00]" /> LangGraph 에이전트 실행 계획 (Agent Execution Plan)
              </span>
            </div>
            <div className="p-3 overflow-auto font-mono text-xs text-[#52A67D] leading-relaxed whitespace-pre-wrap">
              {loading ? (
                <span className="text-gray-400 animate-pulse">Gemini 3.0 Pro Thinking Mode 작동 중... (LangGraph Plan-and-Execute)</span>
              ) : result ? (
                <>
                  <div className="text-gray-300 mb-2 italic">"{result.explanation}"</div>
                  <div>{result.sql_query}</div>
                </>
              ) : (
                <span className="text-gray-500">// 분석 대기 중...</span>
              )}
            </div>
          </div>

          {/* View Mode Tabs & Export Actions */}
          <div className="flex items-center justify-between border-b border-gray-200">
             <div className="flex items-center gap-2">
                <button 
                    onClick={() => setViewMode('TABLE')}
                    className={`flex items-center gap-2 px-4 py-2 rounded-t-lg font-bold text-sm transition-colors ${viewMode === 'TABLE' ? 'bg-white text-[#006241] border-t border-x border-gray-200 shadow-sm relative top-[1px]' : 'text-[#A8A8A8] hover:text-[#53565A]'}`}
                >
                    <TableIcon size={16} /> 테이블 보기
                </button>
                <button 
                    onClick={() => setViewMode('GRAPH')}
                    className={`flex items-center gap-2 px-4 py-2 rounded-t-lg font-bold text-sm transition-colors ${viewMode === 'GRAPH' ? 'bg-white text-[#FF6F00] border-t border-x border-gray-200 shadow-sm relative top-[1px]' : 'text-[#A8A8A8] hover:text-[#53565A]'}`}
                >
                    <Network size={16} /> 지식 그래프 (Neo4j)
                </button>
             </div>
             {/* SFR-006: Data Export Buttons */}
             <div className="flex gap-2 pb-1">
                <button className="flex items-center gap-1 px-3 py-1.5 text-xs font-medium text-[#006241] border border-[#006241]/30 rounded hover:bg-[#006241]/5 transition-colors" disabled={!result} title="CSV 다운로드">
                    <Download size={14} /> CSV
                </button>
                <button className="flex items-center gap-1 px-3 py-1.5 text-xs font-medium text-[#006241] border border-[#006241]/30 rounded hover:bg-[#006241]/5 transition-colors" disabled={!result} title="Excel 다운로드">
                    <FileSpreadsheet size={14} /> Excel
                </button>
             </div>
          </div>

          {/* Result Area (Table or Graph) */}
          <div className="bg-white rounded-b-xl rounded-tr-xl border border-gray-200 flex-grow flex flex-col overflow-hidden shadow-sm relative -mt-[1px]">
            
            {loading ? (
               <div className="h-full flex flex-col items-center justify-center text-[#A8A8A8] gap-3">
                  <div className="w-8 h-8 border-2 border-[#006241] border-t-transparent rounded-full animate-spin"></div>
                  <p className="text-sm">데이터 관계 분석 및 시각화 생성 중...</p>
               </div>
            ) : !result ? (
               <div className="h-full flex flex-col items-center justify-center text-[#A8A8A8] gap-2">
                  <Network size={48} className="opacity-20" />
                  <p>질문을 입력하여 의료 데이터 인사이트를 발견하세요.</p>
               </div>
            ) : (
               <>
                  {viewMode === 'TABLE' && (
                     <div className="flex-grow overflow-auto p-0 bg-white">
                        {result.query_results && result.query_results.length > 0 ? (
                            <table className="w-full text-left text-sm border-collapse">
                                <thead className="bg-[#F5F0E8] sticky top-0 z-10">
                                <tr>
                                    {result.columns?.map((col, idx) => (
                                    <th key={idx} className="px-4 py-3 font-semibold text-[#53565A] border-b border-gray-200 whitespace-nowrap">
                                        {col}
                                    </th>
                                    ))}
                                </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                {result.query_results.map((row, rIdx) => (
                                    <tr key={rIdx} className="hover:bg-[#F5F0E8] transition-colors">
                                    {result.columns?.map((col, cIdx) => (
                                        <td key={cIdx} className="px-4 py-3 text-[#53565A] whitespace-nowrap border-r border-gray-100 last:border-0">
                                        {row[col] !== undefined ? String(row[col]) : '-'}
                                        </td>
                                    ))}
                                    </tr>
                                ))}
                                </tbody>
                            </table>
                        ) : (
                            <div className="p-8 text-center text-[#A8A8A8]">결과 데이터가 없습니다.</div>
                        )}
                     </div>
                  )}

                  {viewMode === 'GRAPH' && (
                     <div className="flex-grow bg-[#F5F0E8]/50 relative overflow-hidden flex items-center justify-center">
                        {result.graph_data && result.graph_data.nodes.length > 0 ? (
                           <svg width="100%" height="100%" viewBox="0 0 800 500" className="w-full h-full cursor-move">
                              <defs>
                                 <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="28" refY="3.5" orient="auto">
                                    <polygon points="0 0, 10 3.5, 0 7" fill="#A8A8A8" />
                                 </marker>
                              </defs>
                              
                              {/* Links */}
                              {result.graph_data.links.map((link, idx) => {
                                 const source = result.graph_data?.nodes.find(n => n.id === link.source);
                                 const target = result.graph_data?.nodes.find(n => n.id === link.target);
                                 if (!source || !target) return null;
                                 return (
                                    <g key={idx}>
                                       <line 
                                          x1={source.x} y1={source.y} 
                                          x2={target.x} y2={target.y} 
                                          stroke="#A8A8A8" strokeWidth="1.5"
                                          markerEnd="url(#arrowhead)"
                                       />
                                       <rect 
                                          x={(source.x! + target.x!) / 2 - 20} 
                                          y={(source.y! + target.y!) / 2 - 10} 
                                          width="40" height="12" fill="#F5F0E8" rx="2"
                                       />
                                       <text x={(source.x! + target.x!) / 2} y={(source.y! + target.y!) / 2} textAnchor="middle" fill="#53565A" fontSize="9" dominantBaseline="middle">
                                          {link.label}
                                       </text>
                                    </g>
                                 );
                              })}

                              {/* Nodes */}
                              {result.graph_data.nodes.map((node, idx) => (
                                 <g key={idx} className="hover:opacity-90 transition-opacity cursor-pointer">
                                    <circle 
                                       cx={node.x} cy={node.y} r="24" 
                                       fill={
                                          node.type === 'Patient' ? '#FFFFFF' : // White bg for patient
                                          node.type === 'Doctor' ? '#ffffff' :
                                          node.type === 'Diagnosis' ? '#ffffff' : '#ffffff'
                                       }
                                       stroke={
                                          node.type === 'Patient' ? '#006241' : // Asan Green
                                          node.type === 'Doctor' ? '#52A67D' :  // Light Green
                                          node.type === 'Diagnosis' ? '#FF6F00' : // Asan Orange
                                          node.type === 'Medication' ? '#C9B037' : // Gold
                                          '#53565A' // Gray
                                       }
                                       strokeWidth="3"
                                    />
                                    {/* Icon placeholder or Initials */}
                                    <text x={node.x} y={node.y! + 5} textAnchor="middle" 
                                          fill={
                                            node.type === 'Patient' ? '#006241' : 
                                            node.type === 'Doctor' ? '#52A67D' : 
                                            node.type === 'Diagnosis' ? '#FF6F00' : '#53565A'
                                          } 
                                          fontSize="12" fontWeight="bold" pointerEvents="none">
                                       {node.type === 'Patient' ? 'Pt' : node.type === 'Doctor' ? 'Dr' : node.type === 'Diagnosis' ? 'Dx' : node.type === 'Medication' ? 'Rx' : 'Pr'}
                                    </text>

                                    <text x={node.x} y={node.y! + 40} textAnchor="middle" fill="#53565A" fontSize="11" fontWeight="600" className="drop-shadow-sm">
                                       {node.label}
                                    </text>
                                    <text x={node.x} y={node.y! + 52} textAnchor="middle" fill="#A8A8A8" fontSize="9">
                                       {node.type}
                                    </text>
                                 </g>
                              ))}
                           </svg>
                        ) : (
                           <div className="text-center text-gray-500">
                              <Network size={48} className="mx-auto mb-2 opacity-30" />
                              <p>표시할 그래프 데이터가 없습니다.</p>
                           </div>
                        )}
                        <div className="absolute bottom-4 right-4 bg-white/90 p-3 rounded-lg border border-gray-200 text-[10px] text-[#53565A] shadow-sm">
                           <div className="font-bold mb-2 text-[#006241]">범례 (Legend)</div>
                           <div className="flex items-center gap-2 mb-1"><span className="w-3 h-3 rounded-full border-2 border-[#006241] bg-white"></span> 환자 (Patient)</div>
                           <div className="flex items-center gap-2 mb-1"><span className="w-3 h-3 rounded-full border-2 border-[#52A67D] bg-white"></span> 의료진 (Doctor)</div>
                           <div className="flex items-center gap-2 mb-1"><span className="w-3 h-3 rounded-full border-2 border-[#FF6F00] bg-white"></span> 진단 (Diagnosis)</div>
                           <div className="flex items-center gap-2 mb-1"><span className="w-3 h-3 rounded-full border-2 border-[#C9B037] bg-white"></span> 약물 (Medication)</div>
                           <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full border-2 border-[#53565A] bg-white"></span> 시술/검사 (Procedure)</div>
                        </div>
                     </div>
                  )}
               </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
