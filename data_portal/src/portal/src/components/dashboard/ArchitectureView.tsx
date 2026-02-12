import React from 'react';
import { BarChart3, Brain, FileText, Search, Shield, Database, ImageIcon } from 'lucide-react';

const STYLE_ID = 'arch-anims';
if (typeof document !== 'undefined' && !document.getElementById(STYLE_ID)) {
  const s = document.createElement('style');
  s.id = STYLE_ID;
  s.textContent = `
    @keyframes archFade{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
    @keyframes archDash{to{stroke-dashoffset:-14}}
    .af{opacity:0;animation:archFade .4s ease-out forwards}
    .arch-node{transition:transform .2s ease,box-shadow .2s ease}
    .arch-node:hover{transform:translateY(-4px);box-shadow:0 8px 24px rgba(0,0,0,.12)}
  `;
  document.head.appendChild(s);
}

/* ── 점선 오른쪽 화살표 — 노드 중앙 높이 ── */
const StepArrow = ({ color = '#006241' }: { color?: string }) => (
  <div className="flex-shrink-0 flex items-center mx-1.5">
    <svg width="52" height="16">
      <line x1="0" y1="8" x2="40" y2="8" stroke={color} strokeWidth="2.5"
        strokeDasharray="5 3" style={{ animation: 'archDash .5s linear infinite' }} />
      <polygon points="40,3 52,8 40,13" fill={color} opacity=".7" />
    </svg>
  </div>
);

/* ── 수렴 화살표: 정형 + 비정형 → 원본수집 (DOM ref 기반 정확한 위치) ── */
const ConvergeArrows = () => {
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const [lines, setLines] = React.useState<{ x1: number; x2: number; color: string }[]>([]);

  React.useEffect(() => {
    const measure = () => {
      const wrap = wrapRef.current;
      if (!wrap) return;
      const root = wrap.closest('.relative.z-10');
      if (!root) return;
      const wrapRect = wrap.getBoundingClientRect();
      // 원천 시스템: grid-cols-2 박스 2개
      const sourceBoxes = root.querySelectorAll('.grid.grid-cols-2 > div');
      // 파이프라인: flex 안 첫 번째 .flex-1
      const firstPipeBox = root.querySelector('.flex.items-center .flex-1');
      if (sourceBoxes.length < 2 || !firstPipeBox) return;
      const s0 = sourceBoxes[0].getBoundingClientRect();
      const s1 = sourceBoxes[1].getBoundingClientRect();
      const tgt = (firstPipeBox as HTMLElement).getBoundingClientRect();
      setLines([
        { x1: s0.left + s0.width / 2 - wrapRect.left, x2: tgt.left + tgt.width / 2 - wrapRect.left, color: '#E11D48' },
        { x1: s1.left + s1.width / 2 - wrapRect.left, x2: tgt.left + tgt.width / 2 - wrapRect.left, color: '#7C3AED' },
      ]);
    };
    measure();
    const t = setTimeout(measure, 300);
    window.addEventListener('resize', measure);
    return () => { clearTimeout(t); window.removeEventListener('resize', measure); };
  }, []);

  return (
    <div ref={wrapRef} style={{ height: 48, position: 'relative' }}>
      <svg width="100%" height="48" style={{ position: 'absolute', left: 0, top: 0 }}>
        {lines.map((l, i) => (
          <React.Fragment key={i}>
            <path d={`M${l.x1},0 C${l.x1},24 ${l.x2},20 ${l.x2},40`}
              fill="none" stroke={l.color} strokeWidth="2.5" strokeDasharray="6 4"
              style={{ animation: 'archDash .7s linear infinite' }} />
          </React.Fragment>
        ))}
        {lines.length > 0 && (
          <polygon points={`${lines[0].x2 - 6},36 ${lines[0].x2},48 ${lines[0].x2 + 6},36`} fill="#E11D48" opacity=".8" />
        )}
      </svg>
    </div>
  );
};

/* ── 연구마트 → 통합 데이터 질의 엔진 배지 → 서비스 4개 (DOM ref 기반) ── */
const DivergeArrows = ({ badge }: { badge: string }) => {
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const [pos, setPos] = React.useState<{ srcX: number; targets: number[]; w: number } | null>(null);

  React.useEffect(() => {
    const measure = () => {
      const wrap = wrapRef.current;
      if (!wrap) return;
      const root = wrap.closest('.relative.z-10');
      if (!root) return;
      const wrapRect = wrap.getBoundingClientRect();
      // 연구 마트: 파이프라인 flex의 마지막 .flex-1
      const pipeBoxes = root.querySelectorAll('.flex.items-center .flex-1');
      const lastBox = pipeBoxes[pipeBoxes.length - 1];
      // 서비스 계층: grid-cols-4의 각 셀
      const serviceBoxes = root.querySelectorAll('.grid.grid-cols-4 > div');
      if (!lastBox || serviceBoxes.length < 1) return;
      const lb = (lastBox as HTMLElement).getBoundingClientRect();
      const srcX = lb.left + lb.width / 2 - wrapRect.left;
      const targets = Array.from(serviceBoxes).map(el => {
        const r = el.getBoundingClientRect();
        return r.left + r.width / 2 - wrapRect.left;
      });
      setPos({ srcX, targets, w: wrapRect.width });
    };
    measure();
    const t = setTimeout(measure, 300);
    window.addEventListener('resize', measure);
    return () => { clearTimeout(t); window.removeEventListener('resize', measure); };
  }, []);

  const midX = pos ? pos.w / 2 : 500;
  const srcX = pos ? pos.srcX : 900;
  const targets = pos ? pos.targets : [125, 375, 625, 875];
  const bw = Math.max(badge.length * 18, 220);

  return (
    <div ref={wrapRef} style={{ height: 100, position: 'relative' }}>
      <svg width="100%" height="100" style={{ position: 'absolute', left: 0, top: 0 }}>
        {/* 연구마트 → 배지 곡선 */}
        <path d={`M${srcX},0 C${srcX},18 ${midX},5 ${midX},26`} fill="none" stroke="#0891B2" strokeWidth="2.5" strokeDasharray="6 4"
          style={{ animation: 'archDash .7s linear infinite' }} />
        <polygon points={`${midX - 6},22 ${midX},32 ${midX + 6},22`} fill="#006241" opacity=".8" />
        {/* 배지 */}
        <rect x={midX - bw / 2} y="30" width={bw} height="32" rx="16" fill="#006241" />
        <text x={midX} y="52" fill="white" fontSize="16" fontWeight="bold" textAnchor="middle">{badge}</text>
        {/* 배지 → 각 서비스 노드 */}
        {targets.map((tx, i) => (
          <React.Fragment key={i}>
            <path d={`M${midX},62 C${midX},80 ${tx},76 ${tx},90`} fill="none" stroke="#006241" strokeWidth="2" strokeDasharray="6 4"
              style={{ animation: 'archDash .6s linear infinite', animationDelay: `${i * 80}ms` }} />
            <polygon points={`${tx - 5},87 ${tx},98 ${tx + 5},87`} fill="#006241" opacity=".7" />
          </React.Fragment>
        ))}
      </svg>
    </div>
  );
};

/* ══════════════════════════════════════════ */
const ArchitectureView: React.FC = () => (
  <div className="af bg-white px-6 py-5 rounded-2xl border border-gray-200 shadow-sm relative overflow-hidden">
    <div className="absolute inset-0 bg-[radial-gradient(#d1d5db_1px,transparent_1px)] [background-size:20px_20px] opacity-[0.05]" />
    <div className="relative z-10">

      <h3 className="text-center font-extrabold text-3xl text-[#006241] mb-0.5">
        서울아산병원 통합 데이터 플랫폼 구성 체계도
      </h3>
      <p className="text-center text-base text-gray-400 mb-5">
        목표시스템 구성 및 데이터 흐름
      </p>

      {/* ── 1. 원천 시스템 ── */}
      <p className="text-base font-semibold text-gray-500 mb-2">원천 시스템</p>
      <div className="af grid grid-cols-2 gap-4" style={{ animationDelay: '80ms' }}>
        <div className="arch-node rounded-xl border-2 border-rose-400 p-4 bg-rose-50/50 text-center">
          <span className="inline-block mb-1 text-rose-600"><Database size={28} /></span>
          <p className="font-bold text-rose-600 text-xl mb-1">정형데이터</p>
          <p className="text-base text-rose-500 font-semibold mb-2">HIS (AMIS 3.0)</p>
          <div className="flex flex-wrap gap-1.5 justify-center">
            {['처방','EMR','간호','검사','투약','약제','중환자실','수술/마취',
              '심전도','내시경','원무/수납','보험청구','인사/총무','종합검진','재무회계','구매재고'].map(t => (
              <span key={t} className="bg-white text-gray-700 text-sm px-2 py-0.5 rounded border border-rose-200">{t}</span>
            ))}
          </div>
        </div>
        <div className="arch-node rounded-xl border-2 border-purple-400 p-4 bg-purple-50/50 text-center">
          <span className="inline-block mb-1 text-purple-600"><ImageIcon size={28} /></span>
          <p className="font-bold text-purple-600 text-xl mb-1">비정형데이터</p>
          <div className="flex flex-wrap gap-1.5 justify-center mt-2">
            {['nGLIS','PACS','Bio-signal','Digital Pathology','검사결과','Free-Text'].map(t => (
              <span key={t} className="bg-white text-gray-700 text-sm px-2 py-0.5 rounded border border-purple-200">{t}</span>
            ))}
          </div>
        </div>
      </div>

      {/* ── 수렴 화살표 → 원본 수집 ── */}
      <ConvergeArrows />

      {/* ── 2. 데이터 정제 파이프라인 ── */}
      <p className="text-base font-semibold text-gray-500 mb-2">데이터 정제 파이프라인</p>
      <div className="af flex items-center gap-1 px-6" style={{ animationDelay: '200ms' }}>
        {([
          { badge: '수집', title: '원본 수집', desc: '원천 데이터 보존\n원본 그대로', bc: '#E11D48', bg: '#FFF1F2' },
          { badge: '정제', title: '정제 · 저장', desc: '구조화 저장\n포맷 통일', bc: '#EA580C', bg: '#FFF7ED' },
          { badge: '변환', title: '표준 변환', desc: '국제표준 매핑\n용어 표준화', bc: '#7C3AED', bg: '#F5F3FF' },
          { badge: '검증', title: '품질 검증', desc: '비식별화 · 큐레이션\n품질 보증', bc: '#059669', bg: '#ECFDF5' },
          { badge: '제공', title: '연구 마트', desc: '목적별 집계 · 제공\n즉시 조회', bc: '#0891B2', bg: '#ECFEFF' },
        ] as const).map((step, i, a) => (
          <React.Fragment key={i}>
            <div className="arch-node flex-1 rounded-xl border-2 p-3 text-center"
              style={{ borderColor: step.bc, backgroundColor: step.bg }}>
              <span className="inline-block text-white text-sm font-bold px-3 py-0.5 rounded-full mb-1.5"
                style={{ backgroundColor: step.bc }}>{step.badge}</span>
              <p className="font-bold text-lg" style={{ color: step.bc }}>{step.title}</p>
              {step.desc.split('\n').map((line, li) => (
                <p key={li} className="text-sm text-gray-500 leading-snug">{line}</p>
              ))}
            </div>
            {i < a.length - 1 && <StepArrow color={a[i + 1].bc} />}
          </React.Fragment>
        ))}
      </div>

      {/* ── 연구마트 → 통합 데이터 질의 엔진 → 서비스 ── */}
      <DivergeArrows badge="통합 데이터 질의 엔진" />

      {/* ── 3. 서비스 계층 ── */}
      <p className="text-base font-semibold text-gray-500 mb-2">서비스 계층</p>
      <div className="af grid grid-cols-4 gap-3" style={{ animationDelay: '400ms' }}>
        {[
          { icon: <Search size={28} />, n: 'CDW 연구지원', sub: '자연어 데이터 조회', c: '#7C3AED', bg: '#F5F3FF' },
          { icon: <BarChart3 size={28} />, n: 'BI 대시보드', sub: '시각화 · 반복 조회', c: '#006241', bg: '#ECFDF5' },
          { icon: <Brain size={28} />, n: 'AI 분석환경', sub: 'Python/R 코딩 환경', c: '#EA580C', bg: '#FFF7ED' },
          { icon: <FileText size={28} />, n: '의무기록 분석', sub: '진단 · 약물 · 검사 자동추출', c: '#0D9488', bg: '#F0FDFA' },
        ].map((s, i) => (
          <div key={i} className="arch-node rounded-xl border-2 p-4 text-center"
            style={{ borderColor: s.c, backgroundColor: s.bg }}>
            <span className="inline-block mb-1" style={{ color: s.c }}>{s.icon}</span>
            <p className="font-bold text-lg" style={{ color: s.c }}>{s.n}</p>
            <p className="text-sm text-gray-500">{s.sub}</p>
          </div>
        ))}
      </div>

      {/* ── 4. 거버넌스 · 보안 계층 ── */}
      <div className="af arch-node mt-4 rounded-xl border-2 border-gray-300 bg-gray-50 px-4 py-3" style={{ animationDelay: '550ms' }}>
        <p className="text-base font-semibold text-gray-500 mb-2 flex items-center gap-1">
          <Shield size={16} /> 거버넌스 · 보안 계층
        </p>
        <div className="flex flex-wrap gap-2 justify-center">
          {['메타데이터 관리','데이터 품질','표준 용어','비식별화 (K-익명성)','접근 제어 (역할 기반)','데이터 이력 추적'].map(t => (
            <span key={t} className="bg-white text-gray-600 text-sm px-3 py-1 rounded-full border border-gray-300 flex items-center gap-1">
              <span className="w-1.5 h-1.5 rounded-full bg-[#006241]" /> {t}
            </span>
          ))}
        </div>
      </div>

    </div>
  </div>
);

export default ArchitectureView;
