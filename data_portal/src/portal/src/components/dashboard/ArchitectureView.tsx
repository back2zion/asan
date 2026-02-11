import React from 'react';
import { ShieldCheck, Stethoscope, Users, Cpu } from 'lucide-react';

/* ── Animations ── */
const STYLE_ID = 'arch-anims';
if (typeof document !== 'undefined' && !document.getElementById(STYLE_ID)) {
  const s = document.createElement('style');
  s.id = STYLE_ID;
  s.textContent = `
    @keyframes archFade{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
    @keyframes archDash{to{stroke-dashoffset:-14}}
    @keyframes archPulse{0%,100%{box-shadow:0 0 0 0 rgba(59,130,246,.25)}50%{box-shadow:0 0 0 6px rgba(59,130,246,0)}}
    .af{opacity:0;animation:archFade .45s ease-out forwards}
  `;
  document.head.appendChild(s);
}

/* ── Small pipeline arrow ── */
const PipeArrow = () => (
  <div className="flex justify-center py-1">
    <svg width="12" height="16">
      <line x1="6" y1="0" x2="6" y2="10" stroke="#006241" strokeWidth="2"
        strokeDasharray="3 2" style={{ animation: 'archDash .55s linear infinite' }} />
      <polygon points="2,10 6,16 10,10" fill="#006241" opacity=".55" />
    </svg>
  </div>
);

/* ── Section-to-section arrow ── */
const FlowArrow: React.FC<{ label?: string }> = ({ label }) => (
  <div className="flex flex-col items-center py-2">
    <svg width="18" height="24">
      <line x1="9" y1="0" x2="9" y2="17" stroke="#006241" strokeWidth="3"
        strokeDasharray="5 3" style={{ animation: 'archDash .6s linear infinite' }} />
      <polygon points="3,16 9,24 15,16" fill="#006241" opacity=".7" />
    </svg>
    {label && (
      <span className="text-sm font-bold text-[#006241] bg-emerald-50 border border-emerald-200 px-3 py-0.5 rounded-full mt-1">
        {label}
      </span>
    )}
  </div>
);

/* ── Horizontal arrow → ── */
const HArrow: React.FC<{ delay?: number }> = ({ delay = 0 }) => (
  <svg width="30" height="16" className="flex-shrink-0">
    <line x1="0" y1="8" x2="20" y2="8" stroke="#38A169" strokeWidth="2.5"
      strokeDasharray="5 3" style={{ animation: `archDash .55s linear infinite ${delay}ms` }} />
    <polygon points="20,3 30,8 20,13" fill="#38A169" opacity=".7" />
  </svg>
);

/* ── Converging arrows: 정형(CDC) + 비정형 → Ingestion ── */
const SourceArrows: React.FC = () => (
  <div className="py-1">
    <svg viewBox="0 0 800 55" width="100%" height="55" preserveAspectRatio="xMidYMid meet">
      {/* 정형 → center (left) */}
      <line x1="200" y1="0" x2="400" y2="48" stroke="#E11D48" strokeWidth="2.5" strokeDasharray="6 4"
        style={{ animation: 'archDash .7s linear infinite' }} />
      {/* CDC label on left arrow */}
      <rect x="195" y="14" width="110" height="20" rx="10" fill="#E11D48" />
      <text x="250" y="28" fill="white" fontSize="11" fontWeight="bold" textAnchor="middle">CDC 실시간</text>

      {/* 비정형 → center (right) */}
      <line x1="600" y1="0" x2="400" y2="48" stroke="#7C3AED" strokeWidth="2.5" strokeDasharray="6 4"
        style={{ animation: 'archDash .7s linear infinite 100ms' }} />

      {/* Arrow head */}
      <polygon points="394,46 400,55 406,46" fill="#006241" opacity=".7" />
    </svg>
  </div>
);

/* ── Split arrows: Landing → 정형/공통/비정형 Processing ── */
const SplitArrows: React.FC = () => (
  <div className="py-0.5">
    <svg viewBox="0 0 900 35" width="100%" height="35" preserveAspectRatio="xMidYMid meet">
      {/* center → left (정형) */}
      <line x1="450" y1="0" x2="150" y2="28" stroke="#EA580C" strokeWidth="2" strokeDasharray="5 3"
        style={{ animation: 'archDash .6s linear infinite' }} />
      <polygon points="146,25 150,34 154,25" fill="#EA580C" opacity=".7" />
      <text x="260" y="16" fill="#EA580C" fontSize="11" fontWeight="bold">정형</text>

      {/* center → center (공통) */}
      <line x1="450" y1="0" x2="450" y2="28" stroke="#EA580C" strokeWidth="2" strokeDasharray="5 3"
        style={{ animation: 'archDash .6s linear infinite 60ms' }} />
      <polygon points="446,26 450,34 454,26" fill="#EA580C" opacity=".7" />

      {/* center → right (비정형) */}
      <line x1="450" y1="0" x2="750" y2="28" stroke="#EA580C" strokeWidth="2" strokeDasharray="5 3"
        style={{ animation: 'archDash .6s linear infinite 120ms' }} />
      <polygon points="746,25 750,34 754,25" fill="#EA580C" opacity=".7" />
      <text x="630" y="16" fill="#EA580C" fontSize="11" fontWeight="bold">비정형</text>
    </svg>
  </div>
);

/* ══════════════════════════════════════════════════════ */
const ArchitectureView: React.FC = () => (
  <div className="af bg-white px-6 py-5 rounded-2xl border border-gray-200 shadow-sm relative overflow-hidden">
    <div className="absolute inset-0 bg-[radial-gradient(#d1d5db_1px,transparent_1px)] [background-size:20px_20px] opacity-[0.05]" />
    <div className="relative z-10">

      {/* Title */}
      <h3 className="text-center font-extrabold text-2xl text-[#006241] mb-1">
        서울아산병원 통합 데이터 플랫폼 구성 체계도
      </h3>
      <p className="text-center text-base text-gray-400 mb-5">
        수집 — 정제 — 표준화 — 적재 전 과정을 자동화하여 데이터 품질과 활용 속도를 동시에 확보
      </p>

      {/* ── 1. 데이터 소스 ── */}
      <section className="af" style={{ animationDelay: '80ms' }}>
        <div className="bg-rose-500 text-white font-bold text-base px-4 py-1.5 rounded-t-lg">데이터 소스</div>
        <div className="grid grid-cols-2 gap-4 p-4 border border-t-0 border-rose-200 rounded-b-lg bg-rose-50/20">
          {/* 정형데이터 box */}
          <div className="rounded-xl border-2 border-rose-400 p-4 bg-white shadow-sm">
            <p className="font-bold text-rose-600 text-lg mb-2">정형데이터 — HIS (AMIS 3.0)</p>
            <div className="flex flex-wrap gap-1.5">
              {['처방','EMR','간호','검사','투약','약제','중환자실','수술/마취',
                '심전도','내시경','원무/수납','보험청구','인사/총무','종합검진','재무회계','구매재고'].map(t => (
                <span key={t} className="bg-rose-50 text-gray-700 text-base px-2 py-0.5 rounded border border-rose-200">{t}</span>
              ))}
            </div>
          </div>
          {/* 비정형데이터 box */}
          <div className="rounded-xl border-2 border-purple-400 p-4 bg-white shadow-sm">
            <p className="font-bold text-purple-600 text-lg mb-2">비정형데이터</p>
            <div className="flex flex-wrap gap-1.5">
              {['nGLIS','PACS','Bio-signal','Digital Pathology','검사결과','Free-Text'].map(t => (
                <span key={t} className="bg-purple-50 text-gray-700 text-base px-2 py-0.5 rounded border border-purple-200">{t}</span>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── Converging arrows: 정형(CDC) + 비정형 → Ingestion ── */}
      <SourceArrows />

      {/* ── 2. IDP Pipeline ── */}
      <section className="af rounded-lg border-2 border-[#006241]/30 overflow-hidden" style={{ animationDelay: '250ms' }}>
        <div className="bg-[#006241] text-white font-bold text-base px-4 py-1.5 flex items-center gap-3">
          통합 데이터 플랫폼 (IDP)
          <span className="text-sm font-normal opacity-70">→ Metadata-Driven Pipeline</span>
        </div>

        <div className="p-4 bg-[#006241]/[0.015] space-y-1">
          {/* Ingestion Layer */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-2.5 flex items-center justify-between">
            <span className="font-bold text-blue-700 text-lg">Ingestion Layer (데이터 수집)</span>
            <span className="bg-blue-500 text-white text-sm font-bold px-4 py-1 rounded-full"
              style={{ animation: 'archPulse 2.8s ease-in-out infinite' }}>
              CDC 실시간 동기화
            </span>
          </div>

          <PipeArrow />

          {/* Data Lakehouse — 전체 감싸기: Landing → Processing → Clean → Curated */}
          <div className="bg-green-50 border-2 border-green-300 rounded-lg p-3 space-y-1">
            <p className="font-bold text-green-700 text-lg mb-1">
              Data Lakehouse
              <span className="text-sm font-normal text-gray-400 ml-2">(EDW + CDW + Object Storage)</span>
            </p>

            {/* Landing Zone */}
            <div className="bg-white border border-green-200 rounded-lg px-4 py-2.5 text-center">
              <span className="font-bold text-green-700 text-base">Landing Zone</span>
              <span className="text-sm text-gray-400 ml-2">(Raw 원본 데이터 적재)</span>
            </div>

            {/* Split: 정형 / 공통 / 비정형 */}
            <SplitArrows />

            {/* Processing Layer + 가상화/분석 */}
            <div className="flex gap-3 items-stretch">
              <div className="flex-1 bg-orange-50 border border-orange-200 rounded-lg p-3">
                <p className="font-bold text-orange-700 text-lg mb-2">Processing Layer</p>
                <div className="grid grid-cols-3 gap-3">
                  {[
                    { t: '정형 Processing', items: ['Validate', 'Clean', 'Standardize'] },
                    { t: '공통 Processing', items: ['Transform', 'ETL / ELT', '표준 매핑'] },
                    { t: '비정형 Processing', items: ['NLP/NER 구조화', 'DICOM 영상 파싱', '신호/유전체 파싱'] },
                  ].map((p, i) => (
                    <div key={i} className="bg-white border border-orange-100 rounded-lg p-3">
                      <p className="font-bold text-orange-600 text-base mb-1">{p.t}</p>
                      {p.items.map((x, j) => (
                        <p key={j} className="text-sm text-gray-500 leading-relaxed">• {x}</p>
                      ))}
                    </div>
                  ))}
                </div>
              </div>

              {/* ←→ 양방향 화살표 */}
              <div className="flex flex-col items-center justify-center gap-3 py-4">
                <svg width="28" height="12">
                  <line x1="0" y1="6" x2="18" y2="6" stroke="#0D9488" strokeWidth="2.5" strokeDasharray="4 3"
                    style={{ animation: 'archDash .55s linear infinite' }} />
                  <polygon points="18,1.5 28,6 18,10.5" fill="#0D9488" opacity=".7" />
                </svg>
                <svg width="28" height="12">
                  <line x1="28" y1="6" x2="10" y2="6" stroke="#0D9488" strokeWidth="2.5" strokeDasharray="4 3"
                    style={{ animation: 'archDash .55s linear infinite 200ms' }} />
                  <polygon points="10,1.5 0,6 10,10.5" fill="#0D9488" opacity=".7" />
                </svg>
              </div>

              {/* 가상화 + 분석 */}
              <div className="flex flex-col gap-3 w-36">
                <div className="flex-1 bg-teal-50 border-2 border-teal-300 rounded-lg flex items-center justify-center">
                  <span className="font-bold text-teal-700 text-base">데이터 가상화</span>
                </div>
                <div className="flex-1 bg-teal-50 border-2 border-teal-300 rounded-lg flex items-center justify-center">
                  <span className="font-bold text-teal-700 text-base">데이터 분석</span>
                </div>
              </div>
            </div>

            <PipeArrow />

            {/* Clean → Curated */}
            <div className="flex items-center gap-2">
              {([['Clean Zone', '파싱 / 검증'], ['Curated Zone', 'OMOP CDM 표준']] as const).map(([name, desc], i, a) => (
                <React.Fragment key={i}>
                  <div className="flex-1 bg-white border border-green-200 rounded-lg py-2.5 text-center">
                    <p className="font-bold text-green-600 text-base">{name}</p>
                    <p className="text-sm text-gray-400">({desc})</p>
                  </div>
                  {i < a.length - 1 && <HArrow delay={0} />}
                </React.Fragment>
              ))}
            </div>
          </div>

          <PipeArrow />

          {/* 거버넌스 */}
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-3">
            <p className="font-bold text-gray-600 text-lg mb-2">데이터 거버넌스 (데이터 카탈로그)</p>
            <div className="flex gap-2 flex-wrap">
              {['메타(IT메타)', '비즈메타', '데이터 품질', '비식별/재식별', '비정형 메타'].map((g, i) => (
                <span key={i} className="bg-white px-3 py-1.5 rounded-lg border border-gray-200 text-base text-gray-600 font-medium flex items-center gap-1.5">
                  <ShieldCheck size={15} className="text-[#006241]" />
                  {g}
                </span>
              ))}
            </div>
          </div>
        </div>
      </section>

      <FlowArrow />

      {/* ── 3. 포털 서비스 ── */}
      <section className="af rounded-lg border border-blue-200 overflow-hidden" style={{ animationDelay: '450ms' }}>
        <div className="bg-blue-600 text-white font-bold text-base px-4 py-1.5">포털 서비스</div>
        <div className="grid grid-cols-5 gap-3 p-4 bg-blue-50/30">
          {[
            { n: 'BI / 시각화', d: '치료·통계·시스템', c: '#006241' },
            { n: '검색/조회/추출', d: '코호트·RAG·RWD', c: '#2F54EB' },
            { n: '데이터분석가', d: 'Python/R 환경', c: '#FF6F00' },
            { n: '데이터관리자', d: '품질·메타 관리', c: '#722ED1' },
            { n: '대외 연구환경', d: '(2단계)', c: '#8C8C8C' },
          ].map((s, i) => (
            <div key={i} className="text-center py-2.5 rounded-xl border-2 bg-white" style={{ borderColor: s.c + '40' }}>
              <p className="font-bold text-base" style={{ color: s.c }}>{s.n}</p>
              <p className="text-sm text-gray-400">{s.d}</p>
            </div>
          ))}
        </div>
      </section>

      <FlowArrow />

      {/* ── 4. 사용자 ── */}
      <section className="af rounded-lg border border-violet-200 overflow-hidden" style={{ animationDelay: '600ms' }}>
        <div className="bg-violet-600 text-white font-bold text-base px-4 py-1.5">사용자</div>
        <div className="grid grid-cols-3 gap-4 p-4 bg-violet-50/30">
          {[
            { n: '원내 의료진 / 연구자', c: '#006241', icon: <Stethoscope size={22} /> },
            { n: '외부 연구자 / 기업', c: '#FF6F00', icon: <Users size={22} /> },
            { n: 'DA / DBA / 개발자', c: '#722ED1', icon: <Cpu size={22} /> },
          ].map((u, i) => (
            <div key={i} className="flex items-center justify-center gap-3 py-3 rounded-xl border-2 bg-white"
              style={{ borderColor: u.c + '40' }}>
              <span style={{ color: u.c }}>{u.icon}</span>
              <span className="font-bold text-base" style={{ color: u.c }}>{u.n}</span>
            </div>
          ))}
        </div>
      </section>

    </div>
  </div>
);

export default ArchitectureView;
