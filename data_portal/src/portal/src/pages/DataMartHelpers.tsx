import React from 'react';
import { Tag, Typography } from 'antd';
import {
  TeamOutlined, DatabaseOutlined, TableOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

export const API_BASE = '/api/v1/datamart';

export const CATEGORY_COLORS: Record<string, string> = {
  'Clinical Data': 'blue',
  'Health System': 'green',
  'Derived': 'purple',
  'Cost & Payer': 'orange',
  'Unstructured': 'cyan',
  'Other': 'default',
};

export const DOMAIN_COLORS: Record<string, string> = {
  Clinical: '#006241',
  Imaging: '#0088FE',
  Admin: '#52A67D',
  Lab: '#FF6F00',
  Drug: '#8B5CF6',
};

export interface TableInfo {
  name: string;
  description: string;
  category: string;
  row_count: number;
  column_count: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  position: number;
}

export interface CdmSummary {
  table_stats: { name: string; row_count: number; category: string; description: string }[];
  demographics: { total_patients: number; male: number; female: number; min_birth_year: number; max_birth_year: number; avg_age: number };
  top_conditions: { snomed_code: string; name_kr: string; count: number; patient_count: number }[];
  visit_types: { type_id: number; type_name: string; count: number; patient_count: number }[];
  top_measurements: { code: string; count: number }[];
  yearly_activity: { year: number; total: number }[];
  quality: { domain: string; score: number; total: number; issues: number }[];
  total_records: number;
  total_tables: number;
}

export interface MappingExample {
  source: string;
  sourceField: string;
  cdmTable: string;
  cdmField: string;
  cdmValue: string;
  standard: string;
}

export const pythonCodeSnippet = (tableName: string) => `import psycopg2
import pandas as pd
import os

# OMOP CDM 데이터베이스 연결
conn = psycopg2.connect(
    host=os.environ.get("OMOP_HOST", "localhost"),
    port=int(os.environ.get("OMOP_PORT", "5436")),
    dbname=os.environ.get("OMOP_DB", "omop_cdm"),
    user=os.environ.get("OMOP_USER", "omopuser"),
    password=os.environ["OMOP_PASSWORD"]  # 환경변수 설정 필요
)

# 데이터 조회
df = pd.read_sql("SELECT * FROM ${tableName} LIMIT 100", conn)
print(df.head())
print(f"\\nShape: {df.shape}")

conn.close()
`;

export const rCodeSnippet = (tableName: string) => `library(DBI)
library(RPostgres)

# OMOP CDM 데이터베이스 연결
con <- dbConnect(
  Postgres(),
  host = Sys.getenv("OMOP_HOST", "localhost"),
  port = as.integer(Sys.getenv("OMOP_PORT", "5436")),
  dbname = Sys.getenv("OMOP_DB", "omop_cdm"),
  user = Sys.getenv("OMOP_USER", "omopuser"),
  password = Sys.getenv("OMOP_PASSWORD")  # 환경변수 설정 필요
)

# 데이터 조회
df <- dbGetQuery(con, "SELECT * FROM ${tableName} LIMIT 100")
head(df)
cat(sprintf("\\nRows: %d, Cols: %d\\n", nrow(df), ncol(df)))

dbDisconnect(con)
`;

export const LazyCodeBlock = React.lazy(() =>
  Promise.all([
    import('react-syntax-highlighter/dist/esm/prism-light'),
    import('react-syntax-highlighter/dist/esm/styles/prism/one-dark'),
    import('react-syntax-highlighter/dist/esm/languages/prism/python'),
    import('react-syntax-highlighter/dist/esm/languages/prism/r'),
  ]).then(([{ default: SyntaxHighlighter }, { default: oneDark }, { default: python }, { default: r }]) => {
    SyntaxHighlighter.registerLanguage('python', python);
    SyntaxHighlighter.registerLanguage('r', r);
    return {
      default: ({ language, children }: { language: string; children: string }) => (
        <SyntaxHighlighter language={language} style={oneDark} customStyle={{ borderRadius: '6px' }}>
          {children}
        </SyntaxHighlighter>
      ),
    };
  })
);

export const CONDITION_COLUMNS = [
  { title: '#', key: 'idx', width: 40, render: (_: any, __: any, i: number) => i + 1 },
  { title: 'SNOMED CT', dataIndex: 'snomed_code', key: 'snomed_code', width: 120, render: (v: string) => <Text code>{v}</Text> },
  { title: '진단명', dataIndex: 'name_kr', key: 'name_kr' },
  { title: '건수', dataIndex: 'count', key: 'count', width: 80, render: (v: number) => v.toLocaleString(), sorter: (a: any, b: any) => a.count - b.count },
  { title: '환자수', dataIndex: 'patient_count', key: 'patient_count', width: 80, render: (v: number) => v.toLocaleString() },
];

export const MAPPING_COLUMNS = [
  { title: '원본 데이터', dataIndex: 'source', key: 'source' },
  { title: 'CDM 테이블', dataIndex: 'cdmTable', key: 'cdmTable', render: (v: string) => <Tag color="blue">{v}</Tag> },
  { title: 'CDM 필드', dataIndex: 'cdmField', key: 'cdmField', render: (v: string) => <Text code>{v}</Text> },
  { title: '표준코드 값', dataIndex: 'cdmValue', key: 'cdmValue', render: (v: string) => <Text strong>{v}</Text> },
  { title: '표준체계', dataIndex: 'standard', key: 'standard', render: (v: string) => <Tag color="green">{v}</Tag> },
];

export const SCHEMA_COLUMNS = [
  { title: '#', dataIndex: 'position', key: 'position', width: 40 },
  { title: '컬럼명', dataIndex: 'name', key: 'name', render: (text: string) => <Text code>{text}</Text> },
  { title: '타입', dataIndex: 'type', key: 'type', render: (text: string) => <Tag color="blue">{text}</Tag> },
  { title: 'Nullable', dataIndex: 'nullable', key: 'nullable', width: 80, render: (val: boolean) => val ? <Tag>YES</Tag> : <Tag color="red">NO</Tag> },
];

export const SUMMARY_STATS = [
  { title: '총 환자 수', key: 'demographics.total_patients', suffix: '명', color: '#006241', borderColor: '#006241', prefix: <TeamOutlined /> },
  { title: '총 레코드', key: 'total_records', color: '#0088FE', borderColor: '#0088FE', prefix: <DatabaseOutlined />, format: true },
  { title: 'CDM 테이블', key: 'total_tables', suffix: '개', color: '#52A67D', borderColor: '#52A67D', prefix: <TableOutlined /> },
  { title: '평균 연령', key: 'demographics.avg_age', suffix: '세', color: '#FF6F00', borderColor: '#FF6F00' },
];

export function getQualityColor(score: number) {
  if (score >= 90) return '#52A67D';
  if (score >= 70) return '#FF6F00';
  return '#DC2626';
}

export const VISIT_TYPE_COLORS = ['#006241', '#FF6F00', '#DC2626'];
