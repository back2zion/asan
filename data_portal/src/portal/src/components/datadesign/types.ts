/**
 * Shared types and constants for DataDesign components
 */

// ─── Types ───

export interface Zone {
  zone_id: number;
  zone_name: string;
  zone_type: string;
  storage_type: string;
  storage_path: string;
  file_format: string;
  description: string;
  retention_days: number;
  partition_strategy: string;
  entity_count: number;
  total_rows: number;
  created_at: string;
}

export interface Entity {
  entity_id: number;
  entity_name: string;
  logical_name: string;
  zone_id: number;
  zone_name: string;
  zone_type: string;
  domain: string;
  entity_type: string;
  description: string;
  column_count: number;
  row_count: number;
}

export interface EntityDetail {
  entity_id: number;
  entity_name: string;
  logical_name: string;
  zone_name: string;
  zone_type: string;
  domain: string;
  entity_type: string;
  description: string;
  columns: { name: string; type: string; nullable: boolean }[];
  row_count: number;
}

export interface Relation {
  relation_id: number;
  source_entity: string;
  target_entity: string;
  relation_type: string;
  fk_columns: string;
  description: string;
}

export interface NamingRule {
  rule_id: number;
  rule_name: string;
  target: string;
  pattern: string;
  example: string;
  description: string;
}

export interface NamingCheckResult {
  name: string;
  valid: boolean;
  violations: { rule: string; pattern: string; example: string }[];
}

export interface UnstructuredMapping {
  mapping_id: number;
  source_type: string;
  source_description: string;
  target_table: string;
  extraction_method: string;
  nlp_model: string;
  output_columns: string[];
  description: string;
  status: string;
}

export interface Overview {
  zones: Record<string, number>;
  entities: { total: number; domains: number };
  relations: number;
  naming_rules: number;
  naming_compliance: number;
  unstructured: { total: number; active: number };
}

export interface ERDNode {
  id: string;
  position: { x: number; y: number };
  data: {
    label: string;
    zone: string;
    domain: string;
    color: string;
    rowCount: number;
    columnCount: number;
    entityType: string;
  };
}

export interface ERDEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  animated: boolean;
  style: { stroke: string; strokeDasharray: string };
}

// ─── Constants ───

export const ZONE_COLORS: Record<string, string> = {
  source: '#E53E3E',
  bronze: '#DD6B20',
  silver: '#805AD5',
  gold: '#D69E2E',
  mart: '#38A169',
};

export const ZONE_LABELS: Record<string, string> = {
  source: 'Source',
  bronze: 'Bronze',
  silver: 'Silver',
  gold: 'Gold',
  mart: 'Mart',
};

export const STATUS_COLORS: Record<string, string> = {
  active: 'green',
  testing: 'orange',
  planned: 'blue',
  deprecated: 'default',
};
