/**
 * AI Assistant 패널 — 공유 상수 및 타입 정의
 * AIAssistantPanel.tsx 에서 분리
 */

/** 페이지 경로 → 한글 라벨 매핑 */
export const PAGE_LABELS: Record<string, string> = {
  '/dashboard': '홈 대시보드',
  '/etl': 'ETL 파이프라인',
  '/governance': '데이터 거버넌스',
  '/catalog': '데이터 카탈로그',
  '/datamart': '데이터마트',
  '/bi': 'BI 대시보드',
  '/ai-environment': 'AI 분석환경',
  '/cdw': 'CDW 연구지원',
  '/ner': '비정형 구조화',
  '/ai-ops': 'AI 운영관리',
  '/data-design': '데이터 설계',
  '/ontology': '의료 온톨로지',
};

/** 민트색 테마 컬러 */
export const MINT = {
  PRIMARY: '#00A0B0',
  LIGHT: '#e0f7f7',
  BG: '#f0faf9',
  DARK: '#008080',
  SEND_BTN: '#00A0B0',
} as const;

/** ChatInput 외부 제어용 핸들 */
export interface ChatInputHandle {
  setValue: (v: string) => void;
  focus: () => void;
}

/** 현재 페이지 상태 (대화 복원용) */
export interface PageState {
  path: string;
  search: string;
  label: string; // 화면 이름 (예: "데이터 카탈로그")
}

/** 사고 과정 단계 */
export interface ThinkingStep {
  label: string;
  detail: string;
  icon: 'search' | 'code' | 'database';
}

/** 채팅 메시지 */
export interface Message {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  toolResults?: Record<string, unknown>[];
  suggestedActions?: Record<string, unknown>[];
  enhancedQuery?: string;
  enhancementApplied?: boolean;
  enhancementConfidence?: number;
  pageState?: PageState;
  thinkingSteps?: ThinkingStep[];
}

/** AIAssistantPanel 컴포넌트 Props */
export interface AIAssistantPanelProps {
  visible: boolean;
  onClose: () => void;
  currentContext?: {
    currentPage?: string;
    currentTable?: string;
    currentColumns?: string[];
    userRole?: string;
  };
}
