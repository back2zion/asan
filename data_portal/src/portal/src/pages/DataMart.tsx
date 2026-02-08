import React, { useState, useMemo, useEffect, useCallback } from 'react';
import {
  Card,
  Table,
  Input,
  Tabs,
  Typography,
  Tag,
  Space,
  Row,
  Col,
  Descriptions,
  Empty,
  Button,
  Spin,
  Statistic,
  Progress,
  App,
} from 'antd';
import {
  DatabaseOutlined,
  TableOutlined,
  CodeOutlined,
  ReloadOutlined,
  SwapOutlined,
  TeamOutlined,
  ManOutlined,
  WomanOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  AreaChart, Area, CartesianGrid, PieChart, Pie,
} from 'recharts';

const { Title, Paragraph, Text } = Typography;
const { Search } = Input;

const API_BASE = '/api/v1/datamart';

// 카테고리별 색상
const CATEGORY_COLORS: Record<string, string> = {
  'Clinical Data': 'blue',
  'Health System': 'green',
  'Derived': 'purple',
  'Cost & Payer': 'orange',
  'Unstructured': 'cyan',
  'Other': 'default',
};

const DOMAIN_COLORS: Record<string, string> = {
  Clinical: '#006241',
  Imaging: '#0088FE',
  Admin: '#52A67D',
  Lab: '#FF6F00',
  Drug: '#8B5CF6',
};

interface TableInfo {
  name: string;
  description: string;
  category: string;
  row_count: number;
  column_count: number;
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  position: number;
}

interface CdmSummary {
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

const pythonCodeSnippet = (tableName: string) => `import psycopg2
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

const rCodeSnippet = (tableName: string) => `library(DBI)
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

// SNOMED → OMOP CDM 매핑 예시 테이블
const CDM_MAPPING_EXAMPLES = [
  { source: '환자 성별 "남/여"', sourceField: 'gender', cdmTable: 'person', cdmField: 'gender_source_value', cdmValue: 'M / F', standard: 'OMOP Gender' },
  { source: '진단명 "고혈압"', sourceField: 'diagnosis_code', cdmTable: 'condition_occurrence', cdmField: 'condition_source_value', cdmValue: '38341003', standard: 'SNOMED CT' },
  { source: '약물 "Metformin"', sourceField: 'drug_name', cdmTable: 'drug_exposure', cdmField: 'drug_source_value', cdmValue: 'RxNorm Code', standard: 'RxNorm' },
  { source: '검사 "HbA1c"', sourceField: 'lab_code', cdmTable: 'measurement', cdmField: 'measurement_source_value', cdmValue: 'LOINC Code', standard: 'LOINC' },
  { source: '내원 "외래"', sourceField: 'visit_type', cdmTable: 'visit_occurrence', cdmField: 'visit_concept_id', cdmValue: '9202', standard: 'OMOP Visit' },
  { source: '영상 "Chest X-ray"', sourceField: 'study_type', cdmTable: 'imaging_study', cdmField: 'finding_labels', cdmValue: 'Cardiomegaly etc.', standard: 'Custom' },
];

/* =============== CDM Summary Tab =============== */
const CdmSummaryTab: React.FC = () => {
  const { message } = App.useApp();
  const [summary, setSummary] = useState<CdmSummary | null>(null);
  const [loading, setLoading] = useState(true);

  const loadSummary = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/cdm-summary`);
      if (!res.ok) throw new Error('CDM 요약 로드 실패');
      setSummary(await res.json());
    } catch (e: any) {
      message.error(e.message || 'API 연결 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadSummary(); }, [loadSummary]);

  if (loading) return <Spin size="large" tip="CDM 데이터 분석 중..."><div style={{ textAlign: 'center', padding: 80 }} /></Spin>;
  if (!summary) return <Empty description="CDM 요약 데이터를 불러올 수 없습니다." />;

  const genderData = [
    { name: '남성', value: summary.demographics.male, color: '#006241' },
    { name: '여성', value: summary.demographics.female, color: '#FF6F00' },
  ];

  const conditionColumns = [
    { title: '#', key: 'idx', width: 40, render: (_: any, __: any, i: number) => i + 1 },
    { title: 'SNOMED CT', dataIndex: 'snomed_code', key: 'snomed_code', width: 120, render: (v: string) => <Text code>{v}</Text> },
    { title: '진단명', dataIndex: 'name_kr', key: 'name_kr' },
    { title: '건수', dataIndex: 'count', key: 'count', width: 80, render: (v: number) => v.toLocaleString(), sorter: (a: any, b: any) => a.count - b.count },
    { title: '환자수', dataIndex: 'patient_count', key: 'patient_count', width: 80, render: (v: number) => v.toLocaleString() },
  ];

  const mappingColumns = [
    { title: '원본 데이터', dataIndex: 'source', key: 'source' },
    { title: 'CDM 테이블', dataIndex: 'cdmTable', key: 'cdmTable', render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: 'CDM 필드', dataIndex: 'cdmField', key: 'cdmField', render: (v: string) => <Text code>{v}</Text> },
    { title: '표준코드 값', dataIndex: 'cdmValue', key: 'cdmValue', render: (v: string) => <Text strong>{v}</Text> },
    { title: '표준체계', dataIndex: 'standard', key: 'standard', render: (v: string) => <Tag color="green">{v}</Tag> },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* 요약 카드 */}
      <Row gutter={[16, 16]}>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #006241' }}>
            <Statistic title="총 환자 수" value={summary.demographics.total_patients} suffix="명" valueStyle={{ color: '#006241' }} prefix={<TeamOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #0088FE' }}>
            <Statistic title="총 레코드" value={summary.total_records} formatter={(v) => Number(v).toLocaleString()} valueStyle={{ color: '#0088FE' }} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #52A67D' }}>
            <Statistic title="CDM 테이블" value={summary.total_tables} suffix="개" valueStyle={{ color: '#52A67D' }} prefix={<TableOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #FF6F00' }}>
            <Statistic title="평균 연령" value={summary.demographics.avg_age} suffix="세" valueStyle={{ color: '#FF6F00' }} />
          </Card>
        </Col>
      </Row>

      {/* 환자 인구통계 + 방문유형 + 품질 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card title={<><TeamOutlined /> 환자 인구통계</>} size="small">
            <div style={{ height: 180 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={genderData} cx="50%" cy="50%" innerRadius={45} outerRadius={70} dataKey="value" label={({ name, value }) => `${name} ${value}`}>
                    {genderData.map((e, i) => <Cell key={i} fill={e.color} />)}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div style={{ textAlign: 'center', marginTop: 8 }}>
              <Space size="large">
                <Text><ManOutlined style={{ color: '#006241' }} /> 남 {summary.demographics.male}명</Text>
                <Text><WomanOutlined style={{ color: '#FF6F00' }} /> 여 {summary.demographics.female}명</Text>
              </Space>
            </div>
          </Card>
        </Col>

        <Col xs={24} md={8}>
          <Card title={<><SwapOutlined /> 방문 유형 분포</>} size="small">
            <div style={{ height: 180 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={summary.visit_types} layout="vertical">
                  <XAxis type="number" hide />
                  <YAxis dataKey="type_name" type="category" width={50} tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
                  <Tooltip formatter={(v: number) => v.toLocaleString() + '건'} />
                  <Bar dataKey="count" name="내원 건수" radius={[0, 6, 6, 0]} barSize={28}>
                    {summary.visit_types.map((_, i) => (
                      <Cell key={i} fill={['#006241', '#FF6F00', '#DC2626'][i] || '#A8A8A8'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div style={{ textAlign: 'center', marginTop: 8 }}>
              <Text type="secondary">총 {summary.visit_types.reduce((s, v) => s + v.count, 0).toLocaleString()}건</Text>
            </div>
          </Card>
        </Col>

        <Col xs={24} md={8}>
          <Card title={<><CheckCircleOutlined /> 도메인별 데이터 품질</>} size="small">
            <Space direction="vertical" size={12} style={{ width: '100%', padding: '8px 0' }}>
              {summary.quality.map((q) => (
                <div key={q.domain}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                    <Text style={{ fontSize: 12 }}>{q.domain}</Text>
                    <Text strong style={{ fontSize: 12, color: q.score >= 90 ? '#52A67D' : q.score >= 70 ? '#FF6F00' : '#DC2626' }}>
                      {q.score}%
                    </Text>
                  </div>
                  <Progress
                    percent={q.score}
                    showInfo={false}
                    size="small"
                    strokeColor={q.score >= 90 ? '#52A67D' : q.score >= 70 ? '#FF6F00' : '#DC2626'}
                  />
                  <Text type="secondary" style={{ fontSize: 10 }}>
                    {q.total.toLocaleString()}건{q.issues > 0 ? ` / ${q.issues.toLocaleString()} NULL` : ''}
                  </Text>
                </div>
              ))}
            </Space>
          </Card>
        </Col>
      </Row>

      {/* 연도별 활동 추이 */}
      <Card title="연도별 CDM 데이터 활동 추이" size="small">
        <div style={{ height: 250 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={summary.yearly_activity}>
              <defs>
                <linearGradient id="cdmGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#006241" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#006241" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" vertical={false} />
              <XAxis dataKey="year" tick={{ fontSize: 11, fill: '#666' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: '#666' }} axisLine={false} tickLine={false} />
              <Tooltip formatter={(v: number) => v.toLocaleString() + '건'} />
              <Area type="monotone" dataKey="total" name="전체 활동" stroke="#006241" strokeWidth={2} fillOpacity={1} fill="url(#cdmGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* 주요 진단 + 테이블 현황 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title="주요 진단 Top 15 (SNOMED CT → 한글명)" size="small">
            <Table
              columns={conditionColumns}
              dataSource={summary.top_conditions.map((c, i) => ({ ...c, key: i }))}
              pagination={false}
              size="small"
              scroll={{ y: 400 }}
            />
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card title="테이블별 레코드 분포" size="small">
            <div style={{ height: 400 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={summary.table_stats.slice(0, 10)} layout="vertical" margin={{ left: 20 }}>
                  <XAxis type="number" hide />
                  <YAxis dataKey="name" type="category" width={130} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                  <Tooltip formatter={(v: number) => v.toLocaleString() + '건'} />
                  <Bar dataKey="row_count" name="레코드 수" radius={[0, 4, 4, 0]} barSize={20}>
                    {summary.table_stats.slice(0, 10).map((t, i) => (
                      <Cell key={i} fill={DOMAIN_COLORS[Object.keys(DOMAIN_COLORS)[i % 5]] || '#006241'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Card>
        </Col>
      </Row>

      {/* Source → CDM 매핑 예시 */}
      <Card
        title={<><SwapOutlined /> Source → OMOP CDM 매핑 예시</>}
        size="small"
        extra={<Tag color="green">OMOP CDM V6.0</Tag>}
      >
        <Table
          columns={mappingColumns}
          dataSource={CDM_MAPPING_EXAMPLES.map((m, i) => ({ ...m, key: i }))}
          pagination={false}
          size="small"
        />
        <div style={{ marginTop: 12, padding: '8px 12px', background: '#f6ffed', borderRadius: 6, border: '1px solid #b7eb8f' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 4 }} />
            CMS Synthetic 데이터가 OMOP CDM V6.0 표준으로 변환 완료. SNOMED CT (진단), RxNorm (약물), LOINC (검사) 표준코드 체계 사용.
          </Text>
        </div>
      </Card>
    </Space>
  );
};

/* =============== Table Explorer Tab (기존) =============== */
const TableExplorerTab: React.FC = () => {
  const { message } = App.useApp();
  const [searchTerm, setSearchTerm] = useState('');
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [schema, setSchema] = useState<ColumnInfo[]>([]);
  const [sampleData, setSampleData] = useState<{ columns: string[]; rows: any[] }>({ columns: [], rows: [] });
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);

  const loadTables = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/tables`);
      if (!res.ok) throw new Error('테이블 목록 로드 실패');
      const data = await res.json();
      setTables(data.tables);
      if (data.tables.length > 0 && !selectedTable) {
        const defaultTable = data.tables.find((t: TableInfo) => t.name === 'person') || data.tables[0];
        loadTableDetail(defaultTable);
      }
    } catch (e: any) {
      message.error(e.message || 'OMOP CDM 연결 실패');
    } finally {
      setLoading(false);
    }
  };

  const loadTableDetail = async (table: TableInfo) => {
    setSelectedTable(table);
    setDetailLoading(true);
    try {
      const [schemaRes, sampleRes] = await Promise.all([
        fetch(`${API_BASE}/tables/${table.name}/schema`),
        fetch(`${API_BASE}/tables/${table.name}/sample?limit=5`),
      ]);
      if (schemaRes.ok) {
        const schemaData = await schemaRes.json();
        setSchema(schemaData.columns);
      }
      if (sampleRes.ok) {
        const sampleResult = await sampleRes.json();
        setSampleData({ columns: sampleResult.columns, rows: sampleResult.rows });
      }
    } catch {
      message.error('테이블 상세 정보 로드 실패');
    } finally {
      setDetailLoading(false);
    }
  };

  useEffect(() => { loadTables(); }, []);

  const filteredTables = useMemo(() =>
    tables.filter(t =>
      t.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      t.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
      t.category.toLowerCase().includes(searchTerm.toLowerCase())
    ),
    [searchTerm, tables]
  );

  const schemaColumns = [
    { title: '#', dataIndex: 'position', key: 'position', width: 40 },
    { title: '컬럼명', dataIndex: 'name', key: 'name', render: (text: string) => <Text code>{text}</Text> },
    { title: '타입', dataIndex: 'type', key: 'type', render: (text: string) => <Tag color="blue">{text}</Tag> },
    { title: 'Nullable', dataIndex: 'nullable', key: 'nullable', width: 80, render: (val: boolean) => val ? <Tag>YES</Tag> : <Tag color="red">NO</Tag> },
  ];

  const renderDetailView = () => {
    if (!selectedTable) {
      return <Card style={{ marginTop: 16 }}><Empty description="왼쪽 목록에서 테이블을 선택해주세요." /></Card>;
    }

    const sampleColumns = sampleData.columns.map(col => ({
      title: col,
      dataIndex: col,
      key: col,
      ellipsis: true,
      width: 150,
      render: (val: any) => val === null ? <Text type="secondary">NULL</Text> : String(val),
    }));

    return (
      <Spin spinning={detailLoading}>
        <Card title={<><DatabaseOutlined /> {selectedTable.name}</>} style={{ marginTop: 16 }}>
          <Descriptions bordered column={2} size="small">
            <Descriptions.Item label="설명">{selectedTable.description}</Descriptions.Item>
            <Descriptions.Item label="카테고리">
              <Tag color={CATEGORY_COLORS[selectedTable.category]}>{selectedTable.category}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="행 수">{selectedTable.row_count.toLocaleString()}</Descriptions.Item>
            <Descriptions.Item label="컬럼 수">{selectedTable.column_count}</Descriptions.Item>
          </Descriptions>

          <Tabs
            defaultActiveKey="1"
            style={{ marginTop: 20 }}
            items={[
              {
                key: '1',
                label: <><TableOutlined /> 스키마 정보</>,
                children: (
                  <Table
                    columns={schemaColumns}
                    dataSource={schema.map(c => ({ ...c, key: c.name }))}
                    pagination={false}
                    size="small"
                  />
                ),
              },
              {
                key: '2',
                label: '샘플 데이터',
                children: sampleData.rows.length > 0 ? (
                  <Table
                    columns={sampleColumns}
                    dataSource={sampleData.rows.map((d, i) => ({ ...d, _key: i }))}
                    rowKey="_key"
                    pagination={false}
                    size="small"
                    scroll={{ x: 'max-content' }}
                  />
                ) : (
                  <Empty description="데이터가 없습니다." />
                ),
              },
              {
                key: '3',
                label: <><CodeOutlined /> 사용 예제 코드</>,
                children: (
                  <Tabs
                    defaultActiveKey="python"
                    items={[
                      {
                        key: 'python',
                        label: 'Python',
                        children: (
                          <SyntaxHighlighter language="python" style={oneDark} customStyle={{ borderRadius: '6px' }}>
                            {pythonCodeSnippet(selectedTable.name)}
                          </SyntaxHighlighter>
                        ),
                      },
                      {
                        key: 'r',
                        label: 'R',
                        children: (
                          <SyntaxHighlighter language="r" style={oneDark} customStyle={{ borderRadius: '6px' }}>
                            {rCodeSnippet(selectedTable.name)}
                          </SyntaxHighlighter>
                        ),
                      },
                    ]}
                  />
                ),
              },
            ]}
          />
        </Card>
      </Spin>
    );
  };

  return (
    <Row gutter={[16, 16]}>
      <Col xs={24} xl={9}>
        <Card
          title="OMOP CDM 테이블 목록"
          size="small"
          extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadTables} loading={loading}>새로고침</Button>}
        >
          <Search
            placeholder="테이블명, 설명, 카테고리로 검색..."
            onSearch={value => setSearchTerm(value)}
            onChange={e => setSearchTerm(e.target.value)}
            style={{ marginBottom: 12 }}
            allowClear
          />
          <Spin spinning={loading}>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {filteredTables.map((table) => (
                <Card
                  key={table.name}
                  size="small"
                  hoverable
                  onClick={() => loadTableDetail(table)}
                  style={{
                    cursor: 'pointer',
                    borderColor: selectedTable?.name === table.name ? '#005BAC' : undefined,
                    background: selectedTable?.name === table.name ? '#f0f7ff' : undefined,
                  }}
                >
                  <Space direction="vertical" size={4} style={{ width: '100%' }}>
                    <Space>
                      <DatabaseOutlined style={{ color: '#005BAC' }} />
                      <Text strong style={{ fontSize: 13 }}>{table.name}</Text>
                      <Tag color={CATEGORY_COLORS[table.category] || 'default'} style={{ fontSize: 11, margin: 0 }}>
                        {table.category}
                      </Tag>
                    </Space>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {table.description}
                    </Text>
                    <Space size={16}>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {table.row_count.toLocaleString()} rows
                      </Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {table.column_count} cols
                      </Text>
                    </Space>
                  </Space>
                </Card>
              ))}
            </Space>
          </Spin>
        </Card>
      </Col>
      <Col xs={24} xl={15}>
        {renderDetailView()}
      </Col>
    </Row>
  );
};

/* =============== Main DataMart Page =============== */
const DataMart: React.FC = () => {
  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <DatabaseOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              데이터마트
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              OMOP CDM V6.0 기반 임상 데이터 탐색 환경 (CMS Synthetic Data)
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Tabs
        defaultActiveKey="summary"
        type="card"
        size="large"
        items={[
          {
            key: 'summary',
            label: <><SwapOutlined /> CDM 변환 요약</>,
            children: <CdmSummaryTab />,
          },
          {
            key: 'explorer',
            label: <><DatabaseOutlined /> 테이블 탐색</>,
            children: <TableExplorerTab />,
          },
        ]}
      />
    </Space>
  );
};

export default DataMart;
