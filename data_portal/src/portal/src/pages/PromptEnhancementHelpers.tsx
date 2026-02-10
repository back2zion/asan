// PromptEnhancementHelpers.tsx
// Interfaces, constants, and utility functions for PromptEnhancement

// --- Interfaces ---

export interface EnhancementResult {
  original_question: string;
  enhanced_question: string;
  enhancements_applied: string[];
  enhancement_confidence: number;
  sql: string;
  sql_explanation: string;
  sql_confidence: number;
  execution_result?: {
    results: any[];
    row_count: number;
    columns: string[];
    execution_time_ms: number;
    natural_language_explanation: string;
    error?: string;
  };
}

export interface TableInfo {
  name: string;
  description: string;
  category: string;
  row_count: number;
  column_count: number;
}

export interface SnomedTerm {
  term: string;
  code: string;
  name: string;
  codeSystem: string;
}

// --- Constants ---

export const API_BASE = '/api/v1';

export const exampleQuestions = [
  "고혈압 환자",
  "당뇨 입원",
  "50대 남성 고혈압",
  "심방세동 약물",
  "당뇨 검사결과",
  "뇌졸중 환자 수"
];

// --- Utility functions ---

export const getConfidenceColor = (confidence: number): string => {
  if (confidence >= 0.8) return 'success';
  if (confidence >= 0.6) return 'warning';
  return 'error';
};
