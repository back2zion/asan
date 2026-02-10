/**
 * 채팅 응답에서 사고 과정(ThinkingStep) 생성
 * - 백엔드 thinking_process가 있으면 실제 IT/비즈메타 사용
 * - 없으면 하드코딩 폴백
 */
import type { ThinkingStep } from './chatConstants';
import type { ChatResponse } from '../../services/api';

const TABLE_MAP: Record<string, string> = {
  person: '환자 기본정보 (76,074명)',
  condition_occurrence: '진단 이력',
  visit_occurrence: '방문 이력 (입원/외래/응급)',
  drug_exposure: '약물 처방',
  measurement: '검사 결과 (36.6M건)',
  observation: '관찰 기록',
  imaging_study: '흉부 X-ray 영상 (112,120건)',
};

const BIZ_META_MAP: Record<string, string> = {
  person: 'gender_source_value → 성별(M/F), year_of_birth → 출생년도(나이 산출)',
  condition_occurrence: 'condition_source_value → SNOMED CT 진단코드',
  imaging_study: 'finding_labels → 판독 소견, view_position → 촬영 방향(AP/PA)',
  measurement: 'measurement_source_value → 검사항목 코드, value_as_number → 수치',
  drug_exposure: 'drug_source_value → 약물 코드, days_supply → 처방 일수',
  visit_occurrence: 'visit_concept_id → 방문유형(9201=입원, 9202=외래, 9203=응급)',
  observation: 'observation_source_value → 관찰항목 코드',
};

const COL_TO_TABLE: Record<string, string> = {
  imaging_study_id: 'imaging_study', finding_labels: 'imaging_study',
  condition_occurrence_id: 'condition_occurrence',
  visit_occurrence_id: 'visit_occurrence',
  drug_exposure_id: 'drug_exposure', measurement_id: 'measurement',
  observation_id: 'observation', person_id: 'person', avg_age: 'person',
};

export function buildThinkingSteps(response: ChatResponse): ThinkingStep[] {
  const msg = response.assistant_message || '';
  const sqlMatch = msg.match(/```sql\s*([\s\S]*?)```/);
  const sql = sqlMatch ? sqlMatch[1].trim() : '';

  const tp = response.thinking_process;

  let itMetaLines = '';
  let bizMetaLines = '';

  if (tp && tp.it_meta && tp.it_meta.length > 0) {
    // ── 실제 백엔드 데이터 사용 ──
    itMetaLines = tp.it_meta
      .map((t) => `• ${t.table}: ${tp.biz_meta?.find((b) => b.table === t.table)?.business_name || t.table} (${t.row_count.toLocaleString()}건)`)
      .join('\n');
    bizMetaLines = (tp.biz_meta || [])
      .map((b) => {
        const cols = Object.entries(b.key_columns || {})
          .map(([col, desc]) => `${col} → ${desc}`)
          .join(', ');
        return `• ${b.table}: ${cols}`;
      })
      .filter(Boolean)
      .join('\n');
  } else {
    // ── 폴백: 기존 하드코딩 ──
    const trCols = (response.tool_results || [])
      .map((t: any) => (t.columns || []).join(' '))
      .join(' ')
      .toLowerCase();
    const searchText = `${sql} ${msg} ${trCols}`.toLowerCase();
    const detectedFromCols = new Set<string>();
    for (const [col, table] of Object.entries(COL_TO_TABLE)) {
      if (trCols.includes(col)) detectedFromCols.add(table);
    }
    const usedTables = Object.keys(TABLE_MAP).filter(
      (t) => searchText.includes(t) || detectedFromCols.has(t)
    );
    itMetaLines = usedTables.map((t) => `• ${t}: ${TABLE_MAP[t]}`).join('\n') || '• 전체 스키마 분석 완료';
    bizMetaLines = usedTables
      .map((t) => BIZ_META_MAP[t] ? `• ${t}: ${BIZ_META_MAP[t]}` : '')
      .filter(Boolean).join('\n') || '• 컨텍스트 기반 업무 의미 추론 완료';
  }

  const steps: ThinkingStep[] = [
    {
      label: 'IT메타 기반 비즈메타 생성',
      detail: `[IT메타 — 스키마 분석]\n${itMetaLines || '• 전체 스키마 분석 완료'}\n\n[비즈메타 — 업무 의미 생성]\n${bizMetaLines || '• 컨텍스트 기반 업무 의미 추론 완료'}`,
      icon: 'search',
    },
    {
      label: 'SQL 생성',
      detail: sql || '(내부 최적화 쿼리로 직접 실행)',
      icon: 'code',
    },
  ];

  const tr = response.tool_results || [];
  if (tr.length > 0) {
    const rowCount = (tr[0] as any)?.results?.length || 0;
    steps.push({
      label: '데이터 조회',
      detail: `${rowCount}건 조회 완료`,
      icon: 'database',
    });
  }

  return steps;
}
