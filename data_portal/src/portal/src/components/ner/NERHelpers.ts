import React from 'react';
import { fetchPost } from '../../services/apiUtils';

// ── 엔티티 타입 정의 ──
export interface NEREntity {
  text: string;
  type: 'condition' | 'drug' | 'measurement' | 'procedure' | 'person';
  start: number;
  end: number;
  omopConcept: string;
  standardCode: string;
  codeSystem: string;
  confidence: number;
}

export interface TextProcessResult {
  job_id: number;
  sections: Record<string, string>;
  entities: NEREntity[];
  omop: { note_id: number; note_nlp_count: number };
  s3_key: string;
  processing_time_ms: number;
}

export interface DicomResult {
  job_id: number;
  filename: string;
  dicom_meta: Record<string, string | number | null>;
  omop: { imaging_study_id: number };
  s3_key: string;
  processing_time_ms: number;
}

export interface JobRecord {
  job_id: number;
  job_type: string;
  source_type: string | null;
  status: string;
  input_summary: string | null;
  result_count: number;
  omop_records_created: number;
  processing_time_ms: number | null;
  created_at: string | null;
}

export interface PipelineStats {
  total_jobs: number;
  by_type: Record<string, number>;
  by_status: Record<string, number>;
  omop_records: Record<string, number>;
  recent_jobs: JobRecord[];
}

// ── 엔티티 색상 맵 ──
export const ENTITY_COLORS: Record<string, { bg: string; border: string; text: string; label: string }> = {
  condition: { bg: '#fff1f0', border: '#ffa39e', text: '#cf1322', label: '진단' },
  drug: { bg: '#e6f7ff', border: '#91d5ff', text: '#096dd9', label: '약물' },
  measurement: { bg: '#f6ffed', border: '#b7eb8f', text: '#389e0d', label: '검사' },
  procedure: { bg: '#f9f0ff', border: '#d3adf7', text: '#722ed1', label: '시술' },
  person: { bg: '#fff7e6', border: '#ffd591', text: '#d46b08', label: '인물' },
};

// ── 예시 텍스트 ──
export const SAMPLE_TEXTS = [
  {
    label: '심장내과 경과기록',
    text: '62세 남성 김철수, 급성 심근경색 진단. Troponin-I 2.8 ng/mL 상승. 관상동맥 조영술 시행, 좌전하행지 90% 협착 확인. 스텐트 삽입술 시행. Clopidogrel 75mg qd, Atorvastatin 40mg qd 처방. Creatinine 1.4 mg/dL.',
  },
  {
    label: '영상의학 소견',
    text: '흉부 X-ray 소견: Cardiomegaly 의심. 심초음파 추가 검사 필요. BNP 450 pg/mL 상승 소견. 심부전 가능성 높음. Losartan 50mg qd, Aspirin 100mg qd 처방.',
  },
  {
    label: '당뇨 진료기록',
    text: '55세 남성 환자 홍길동, 2형 당뇨병 진단. HbA1c 7.8%, LDL 145mg/dL, eGFR 68. Metformin 500mg bid 처방, Glimepiride 2mg qd 추가. 고혈압 동반되어 Amlodipine 5mg qd 병용.',
  },
  {
    label: '혈액검사 결과',
    text: '박영희 환자, 만성 신장질환 경과 관찰. Creatinine 2.1 mg/dL, eGFR 32, CRP 3.5 mg/L, WBC 8.2. HbA1c 8.1%, LDL 162mg/dL. 관상동맥질환 및 협심증 병력. Nitroglycerin SL 처방.',
  },
];

export const SECTION_LABELS: Record<string, string> = {
  chief_complaint: '주소/주호소',
  present_illness: '현병력',
  findings: '소견/검사결과',
  assessment: '평가/진단',
  plan: '계획/처방',
  raw: '원본 (섹션분리 실패)',
};

export const SOURCE_TYPES = ['경과기록', '병리보고서', '영상소견', '퇴원요약', '수술기록'];

export const DICOM_META_LABELS: Record<string, string> = {
  PatientID: '환자 ID',
  PatientName: '환자명',
  StudyDate: '촬영일',
  Modality: '촬영장비',
  BodyPartExamined: '촬영부위',
  StudyDescription: '검사 설명',
  SeriesDescription: '시리즈 설명',
  Rows: '이미지 높이 (px)',
  Columns: '이미지 너비 (px)',
  BitsAllocated: '비트 할당',
  Manufacturer: '제조사',
  InstitutionName: '기관명',
};

export const STATUS_COLORS: Record<string, string> = {
  completed: 'green',
  processing: 'blue',
  pending: 'default',
  failed: 'red',
};

export const TYPE_LABELS: Record<string, string> = {
  text: '텍스트',
  dicom: 'DICOM',
  signal: '신호',
};

// ── API 호출 ──
export const NER_API_BASE = '/api/v1/ner';
export const UNSTRUCT_API_BASE = '/api/v1/unstructured';

export async function callNERApi(text: string): Promise<{ entities: NEREntity[]; processingTimeMs: number; model: string } | null> {
  try {
    const response = await fetchPost(`${NER_API_BASE}/analyze`, { text });
    if (!response.ok) return null;
    const data = await response.json();
    return {
      entities: data.entities.map((e: NEREntity & { source?: string }) => ({
        text: e.text,
        type: e.type as NEREntity['type'],
        start: e.start,
        end: e.end,
        omopConcept: e.omopConcept,
        standardCode: e.standardCode,
        codeSystem: e.codeSystem,
        confidence: e.confidence,
      })),
      processingTimeMs: data.processingTimeMs,
      model: data.model,
    };
  } catch {
    return null;
  }
}

export async function checkNERHealth(): Promise<{ healthy: boolean; device?: string }> {
  try {
    const response = await fetch(`${NER_API_BASE}/health`);
    if (!response.ok) return { healthy: false };
    const data = await response.json();
    return { healthy: data.status === 'healthy', device: data.device };
  } catch {
    return { healthy: false };
  }
}

// ── 하이라이트 렌더러 ──
export function renderHighlightedText(text: string, entities: NEREntity[]): React.ReactNode {
  if (entities.length === 0) return React.createElement('span', null, text);

  const parts: React.ReactNode[] = [];
  let lastIndex = 0;

  entities.forEach((entity, i) => {
    if (entity.start > lastIndex) {
      parts.push(React.createElement('span', { key: `t-${i}` }, text.slice(lastIndex, entity.start)));
    }
    const color = ENTITY_COLORS[entity.type];
    parts.push(
      React.createElement('span', {
        key: `e-${i}`,
        style: {
          background: color.bg,
          border: `1px solid ${color.border}`,
          color: color.text,
          fontWeight: 600,
          padding: '1px 4px',
          borderRadius: 3,
          cursor: 'pointer',
        },
        title: `${color.label}: ${entity.omopConcept} (${entity.codeSystem}: ${entity.standardCode})`,
      },
        text.slice(entity.start, entity.end),
        React.createElement('sup', { style: { fontSize: 9, marginLeft: 2, opacity: 0.7 } }, color.label),
      )
    );
    lastIndex = entity.end;
  });

  if (lastIndex < text.length) {
    parts.push(React.createElement('span', { key: 'end' }, text.slice(lastIndex)));
  }

  return parts;
}
