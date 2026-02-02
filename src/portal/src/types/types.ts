
export interface DataAsset {
  id: string;
  name: string;
  type: 'Table' | 'View' | 'Unstructured' | 'API';
  domain: 'Clinical' | 'Genomics' | 'Imaging' | 'Admin';
  owner: string;
  description: string;
  sensitivity: 'Public' | 'Internal' | 'Confidential' | 'Restricted';
  qualityScore: number;
  lastUpdated: string;
  creationDate: string;
  tags: string[];
  relatedAssets: string[];
}

export interface PipelineJob {
  id: string;
  name: string;
  source: string;
  target: string;
  status: 'Running' | 'Completed' | 'Failed' | 'Pending';
  startTime: string;
  duration: string;
  recordsProcessed: number;
  type?: 'Batch' | 'Stream'; // Added for SFR-005
  deid?: boolean;            // Added for SFR-005 (PII Masking)
}

export interface GovernancePolicy {
  id: string;
  name: string;
  category: 'Security' | 'Quality' | 'Compliance';
  status: 'Active' | 'Monitoring';
  complianceRate: number;
}

export interface GraphNode {
  id: string;
  label: string;
  type: 'Patient' | 'Doctor' | 'Diagnosis' | 'Department' | 'Medication' | 'Procedure';
  x?: number;
  y?: number;
}

export interface GraphLink {
  source: string;
  target: string;
  label: string;
}

export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
}

export interface AISqlResult {
  sql_query: string;
  explanation: string;
  tables_used: string[];
  columns?: string[];
  query_results?: any[];
  graph_data?: GraphData;
}

export enum AppView {
  DASHBOARD = 'DASHBOARD',
  CATALOG = 'CATALOG',
  PIPELINE = 'PIPELINE',
  GOVERNANCE = 'GOVERNANCE',
  ANALYTICS = 'ANALYTICS',
  SETTINGS = 'SETTINGS'
}
