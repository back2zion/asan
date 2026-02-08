/**
 * Cohort Builder API (DPR-003: 임상/연구 특화 코호트 빌더)
 */

import { apiClient } from './apiUtils';

// ── Criterion Types ──────────────────────────────────────

export interface AgeRangeCriterion {
  type: 'age_range';
  label: string;
  min_age: number;
  max_age: number;
}

export interface GenderCriterion {
  type: 'gender';
  label: string;
  gender: 'M' | 'F';
}

export interface ConditionCriterion {
  type: 'condition';
  label: string;
  concept_code: string;
  date_from?: string;
  date_to?: string;
}

export interface DrugCriterion {
  type: 'drug';
  label: string;
  concept_code: string;
  date_from?: string;
  date_to?: string;
}

export interface ProcedureCriterion {
  type: 'procedure';
  label: string;
  concept_code: string;
  date_from?: string;
  date_to?: string;
}

export interface MeasurementCriterion {
  type: 'measurement';
  label: string;
  concept_code: string;
  value_min?: number;
  value_max?: number;
  date_from?: string;
  date_to?: string;
}

export interface VisitTypeCriterion {
  type: 'visit_type';
  label: string;
  visit_concept_id: 9201 | 9202 | 9203;
  date_from?: string;
  date_to?: string;
}

export type Criterion =
  | AgeRangeCriterion
  | GenderCriterion
  | ConditionCriterion
  | DrugCriterion
  | ProcedureCriterion
  | MeasurementCriterion
  | VisitTypeCriterion;

// ── Response Types ───────────────────────────────────────

export interface FlowStep {
  step_type: 'inclusion' | 'exclusion';
  criterion: Criterion;
  label?: string;
}

export interface FlowStepResult {
  step_type: string;
  label: string;
  remaining_count: number;
  excluded_count: number;
}

export interface ExecuteFlowResponse {
  total_population: number;
  steps: FlowStepResult[];
  final_count: number;
}

export interface SetOperationResponse {
  count_a: number;
  count_b: number;
  count_overlap: number;
  count_a_only: number;
  count_b_only: number;
  count_result: number;
  operation: string;
}

export interface PatientRow {
  person_id: number;
  gender: string;
  birth_year: number;
  age: number;
}

export interface TimelineEvent {
  domain: string;
  event_date: string;
  code: string;
  value: string;
}

export interface PatientTimeline {
  patient: PatientRow;
  events: TimelineEvent[];
}

export interface SummaryStatsResponse {
  gender_dist: { gender: string; count: number }[];
  age_stats: { min: number; max: number; mean: number; median: number };
  age_dist: { age_group: string; count: number }[];
  condition_top10: { code: string; count: number }[];
  visit_type_dist: { visit_type: string; count: number }[];
}

// ── API Methods ──────────────────────────────────────────

export const cohortApi = {
  count: async (criterion: Criterion) => {
    const response = await apiClient.post('/cohort/count', { criterion });
    return response.data as { count: number; sql_used: string; execution_time_ms: number };
  },

  executeFlow: async (steps: FlowStep[]) => {
    const response = await apiClient.post('/cohort/execute-flow', { steps });
    return response.data as ExecuteFlowResponse;
  },

  setOperation: async (
    group_a: Criterion[],
    group_b: Criterion[],
    operation: 'intersection' | 'union' | 'difference',
  ) => {
    const response = await apiClient.post('/cohort/set-operation', {
      group_a, group_b, operation,
    });
    return response.data as SetOperationResponse;
  },

  drillDown: async (criteria: Criterion[], limit = 50, offset = 0) => {
    const response = await apiClient.post('/cohort/drill-down', {
      criteria, limit, offset,
    });
    return response.data as { patients: PatientRow[]; count: number };
  },

  patientTimeline: async (personId: number) => {
    const response = await apiClient.get(`/cohort/patient/${personId}/timeline`);
    return response.data as PatientTimeline;
  },

  summaryStats: async (criteria: Criterion[]) => {
    const response = await apiClient.post('/cohort/summary-stats', { criteria });
    return response.data as SummaryStatsResponse;
  },
};
