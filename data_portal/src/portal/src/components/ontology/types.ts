/**
 * Shared types, constants, and metadata for the Ontology Knowledge Graph page.
 */

import React from 'react';
import {
  DeploymentUnitOutlined, NodeIndexOutlined, ApartmentOutlined,
  ExpandOutlined, DatabaseOutlined, MedicineBoxOutlined, FundOutlined,
  BranchesOutlined, ExperimentOutlined, ClusterOutlined, EyeOutlined,
  FileTextOutlined, ShareAltOutlined, InfoCircleOutlined,
} from '@ant-design/icons';

// ═══════════════════════════════════════════════════
//  Type Definitions
// ═══════════════════════════════════════════════════

export interface OntologyNode {
  id: string;
  label: string;
  type: string;
  color: string;
  size: number;
  row_count?: number;
  record_count?: number;
  patient_count?: number;
  concept_id?: number;
  description?: string;
  full_label?: string;
  domain?: string;
  icon?: string;
  table_name?: string;
  [key: string]: any;
}

export interface OntologyLink {
  source: string;
  target: string;
  label: string;
  type: string;
  confidence?: number;
  description?: string;
  co_patients?: number;
  strength?: number;
}

export interface Triple {
  subject: string | { id: string; label: string; type: string };
  predicate: string;
  object: string | { id: string; label: string; type: string };
  type?: string;
  triple_type?: string;
  description?: string;
}

export interface GraphData {
  nodes: OntologyNode[];
  links: OntologyLink[];
  triples: Triple[];
  stats: any;
  causal_chains: any[];
  built_at: string;
}

// ═══════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════

export const NODE_TYPE_META: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  domain: { label: 'OMOP CDM 테이블', color: '#1A365D', icon: React.createElement(DatabaseOutlined) },
  condition: { label: '진단/질환', color: '#E53E3E', icon: React.createElement(MedicineBoxOutlined) },
  drug: { label: '약물', color: '#805AD5', icon: React.createElement(ExperimentOutlined) },
  measurement: { label: '검사/측정', color: '#319795', icon: React.createElement(FundOutlined) },
  procedure: { label: '시술', color: '#DD6B20', icon: React.createElement(BranchesOutlined) },
  vocabulary: { label: '표준 용어', color: '#2D3748', icon: React.createElement(FileTextOutlined) },
  body_system: { label: '신체계통', color: '#4A5568', icon: React.createElement(ClusterOutlined) },
  drug_class: { label: '약물 분류', color: '#6B46C1', icon: React.createElement(NodeIndexOutlined) },
  visit: { label: '방문 유형', color: '#38A169', icon: React.createElement(ApartmentOutlined) },
  causal: { label: '인과 관계', color: '#B83280', icon: React.createElement(ShareAltOutlined) },
  observation: { label: '관찰', color: '#D69E2E', icon: React.createElement(EyeOutlined) },
  person: { label: '환자', color: '#005BAC', icon: React.createElement(DeploymentUnitOutlined) },
  cost: { label: '비용', color: '#00B5D8', icon: React.createElement(FundOutlined) },
  device: { label: '의료기기', color: '#D53F8C', icon: React.createElement(DeploymentUnitOutlined) },
  death: { label: '사망', color: '#718096', icon: React.createElement(InfoCircleOutlined) },
  comorbidity_cluster: { label: '동반질환군', color: '#C05621', icon: React.createElement(ClusterOutlined) },
};

// CDM 5.4 공식 도메인별 색상 (OHDSI 표준 ERD 기준)
export const CDM_DOMAIN_META: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  'Clinical Data': { label: '임상 데이터 (Clinical Data)', color: '#E76F51', icon: React.createElement(MedicineBoxOutlined) },
  'Health System Data': { label: '의료기관 (Health System)', color: '#2A9D8F', icon: React.createElement(ApartmentOutlined) },
  'Health Economics': { label: '의료경제 (Health Economics)', color: '#6C5CE7', icon: React.createElement(FundOutlined) },
  'Derived Elements': { label: '파생 요소 (Derived Elements)', color: '#0077B6', icon: React.createElement(BranchesOutlined) },
  'Extension': { label: '확장 테이블 (Extension)', color: '#E9C46A', icon: React.createElement(ExperimentOutlined) },
};

export const LINK_TYPE_COLORS: Record<string, string> = {
  schema_fk: '#718096',
  treatment: '#805AD5',
  diagnostic: '#319795',
  comorbidity: '#E53E3E',
  vocabulary_mapping: '#2D3748',
  data_instance: '#A0AEC0',
  taxonomy: '#4A5568',
  drug_classification: '#6B46C1',
  pharmacology: '#9F7AEA',
  causality: '#B83280',
  causal_chain: '#D53F8C',
  visit_classification: '#38A169',
  includes_step: '#D53F8C',
};

export const VIEW_OPTIONS = [
  { label: '전체', value: 'full', icon: React.createElement(ExpandOutlined) },
  { label: 'CDM 스키마', value: 'schema', icon: React.createElement(DatabaseOutlined) },
  { label: '의료 개념', value: 'medical', icon: React.createElement(MedicineBoxOutlined) },
  { label: '인과 관계', value: 'causality', icon: React.createElement(ShareAltOutlined) },
  { label: '표준 용어', value: 'vocabulary', icon: React.createElement(FileTextOutlined) },
];

// ═══════════════════════════════════════════════════
//  Helper: Custom node canvas rendering
// ═══════════════════════════════════════════════════

export const drawNode = (node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
  const r = node.size || 6;
  const label = node.label || '';
  const fontSize = Math.max(10 / globalScale, 2);

  // Subtle shadow
  ctx.shadowColor = 'rgba(0,0,0,0.15)';
  ctx.shadowBlur = 4;
  ctx.shadowOffsetX = 1;
  ctx.shadowOffsetY = 1;

  // Draw node circle
  ctx.beginPath();
  ctx.arc(node.x!, node.y!, r, 0, 2 * Math.PI);
  ctx.fillStyle = node.color || '#718096';
  ctx.fill();

  // Border
  ctx.shadowBlur = 0;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = 0;
  ctx.strokeStyle = '#ffffff';
  ctx.lineWidth = 1.5 / globalScale;
  ctx.stroke();

  // Inner highlight
  ctx.beginPath();
  ctx.arc(node.x! - r * 0.2, node.y! - r * 0.2, r * 0.35, 0, 2 * Math.PI);
  ctx.fillStyle = 'rgba(255,255,255,0.3)';
  ctx.fill();

  // Label (show only when zoomed in enough or for important nodes)
  if (globalScale > 0.6 || r > 12) {
    const maxLen = globalScale > 1.5 ? 30 : globalScale > 0.8 ? 18 : 10;
    const displayLabel = label.length > maxLen ? label.substring(0, maxLen) + '...' : label;

    ctx.font = `${fontSize}px 'Noto Sans KR', sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';

    // Text background
    const textWidth = ctx.measureText(displayLabel).width;
    const padding = 2 / globalScale;
    ctx.fillStyle = 'rgba(255, 255, 255, 0.92)';
    ctx.fillRect(
      node.x! - textWidth / 2 - padding,
      node.y! + r + 2 / globalScale,
      textWidth + padding * 2,
      fontSize + padding * 2
    );

    // Text
    ctx.fillStyle = '#1a202c';
    ctx.fillText(displayLabel, node.x!, node.y! + r + 2 / globalScale + padding);
  }
};
