import React, { useState, useMemo, useEffect } from 'react';
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
  message,
} from 'antd';
import {
  DatabaseOutlined,
  TableOutlined,
  CodeOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

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

const pythonCodeSnippet = (tableName: string) => `import psycopg2
import pandas as pd

# OMOP CDM 데이터베이스 연결
conn = psycopg2.connect(
    host="localhost",
    port=5436,
    dbname="omop_cdm",
    user="omopuser",
    password="omop"
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
  host = "localhost",
  port = 5436,
  dbname = "omop_cdm",
  user = "omopuser",
  password = "omop"
)

# 데이터 조회
df <- dbGetQuery(con, "SELECT * FROM ${tableName} LIMIT 100")
head(df)
cat(sprintf("\\nRows: %d, Cols: %d\\n", nrow(df), ncol(df)))

dbDisconnect(con)
`;

const DataMart: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [schema, setSchema] = useState<ColumnInfo[]>([]);
  const [sampleData, setSampleData] = useState<{ columns: string[]; rows: any[] }>({ columns: [], rows: [] });
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [dbInfo, setDbInfo] = useState<{ database: string; source: string; total_tables: number } | null>(null);

  // 테이블 목록 로드
  const loadTables = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/tables`);
      if (!res.ok) throw new Error('테이블 목록 로드 실패');
      const data = await res.json();
      setTables(data.tables);
      setDbInfo({ database: data.database, source: data.source, total_tables: data.total_tables });
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

  // 테이블 상세정보 로드 (스키마 + 샘플)
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

  // 통계
  const totalRows = useMemo(() => tables.reduce((sum, t) => sum + t.row_count, 0), [tables]);

  const schemaColumns = [
    {
      title: '#',
      dataIndex: 'position',
      key: 'position',
      width: 40,
    },
    {
      title: '컬럼명',
      dataIndex: 'name',
      key: 'name',
      render: (text: string) => <Text code>{text}</Text>,
    },
    {
      title: '타입',
      dataIndex: 'type',
      key: 'type',
      render: (text: string) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: 'Nullable',
      dataIndex: 'nullable',
      key: 'nullable',
      width: 80,
      render: (val: boolean) => val ? <Tag>YES</Tag> : <Tag color="red">NO</Tag>,
    },
  ];

  const renderDetailView = () => {
    if (!selectedTable) {
      return (
        <Card style={{ marginTop: 16 }}>
          <Empty description="왼쪽 목록에서 테이블을 선택해주세요." />
        </Card>
      );
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
        <Card
          title={<><DatabaseOutlined /> {selectedTable.name}</>}
          style={{ marginTop: 16 }}
        >
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
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <DatabaseOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              데이터마트
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              OMOP CDM V6.0 기반 임상 데이터 탐색 환경 ({dbInfo?.source || 'CMS Synthetic Data'})
            </Paragraph>
          </Col>
          <Col>
            <Space size="large">
              <Statistic title="테이블" value={dbInfo?.total_tables || 0} />
              <Statistic title="총 레코드" value={totalRows} formatter={(val) => Number(val).toLocaleString()} />
              <Button icon={<ReloadOutlined />} onClick={loadTables} loading={loading}>
                새로고침
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={9}>
          <Card title="OMOP CDM 테이블 목록" size="small">
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
    </Space>
  );
};

export default DataMart;
