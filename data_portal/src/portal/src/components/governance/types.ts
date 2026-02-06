/**
 * 데이터 거버넌스 공용 타입 및 상수
 */

// ── OMOP CDM v5.4 주요 테이블 ──
export const OMOP_TABLES = [
  'person', 'observation_period', 'visit_occurrence', 'visit_detail',
  'condition_occurrence', 'drug_exposure', 'procedure_occurrence', 'device_exposure',
  'measurement', 'observation', 'death', 'note', 'note_nlp', 'specimen',
  'condition_era', 'drug_era', 'dose_era', 'cost',
  'care_site', 'provider', 'location', 'concept',
];

export interface TableQuality {
  table_name: string;
  row_count: number;
  null_rate: number;
  completeness: number;
  status: 'good' | 'warning' | 'error';
}

export interface SecurityClassification {
  level: string;
  color: string;
  count: number;
  columns: string[];
}
