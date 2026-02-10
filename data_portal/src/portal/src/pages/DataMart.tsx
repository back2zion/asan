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
  Tooltip,
  Modal,
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
  EditOutlined,
  DownloadOutlined,
  ClearOutlined,
  ToolOutlined,
} from '@ant-design/icons';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip as RechartsTooltip, ResponsiveContainer, Cell,
  AreaChart, Area, CartesianGrid, PieChart, Pie,
} from 'recharts';
import { fetchPost, fetchPut } from '../services/apiUtils';
import {
  API_BASE, CATEGORY_COLORS, DOMAIN_COLORS,
  TableInfo, ColumnInfo, CdmSummary, MappingExample,
  pythonCodeSnippet, rCodeSnippet, LazyCodeBlock,
  CONDITION_COLUMNS, MAPPING_COLUMNS, SCHEMA_COLUMNS,
  SUMMARY_STATS, getQualityColor, VISIT_TYPE_COLORS,
} from './DataMartHelpers';

const { Title, Paragraph, Text } = Typography;
const { Search } = Input;

const CdmSummaryTab: React.FC = () => {
  const { message } = App.useApp();
  const [summary, setSummary] = useState<CdmSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [mappingExamples, setMappingExamples] = useState<MappingExample[]>([]);

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

  useEffect(() => {
    fetch(`${API_BASE}/cdm-mapping-examples`)
      .then(res => { if (!res.ok) throw new Error(); return res.json(); })
      .then(d => setMappingExamples(d.examples || []))
      .catch(() => { /* 매핑 예시 로드 실패 — 빈 목록 유지 */ });
  }, []);

  if (loading) return <Spin size="large" tip="CDM 데이터 분석 중..."><div style={{ textAlign: 'center', padding: 80 }} /></Spin>;
  if (!summary) return <Empty description="CDM 요약 데이터를 불러올 수 없습니다." />;

  const genderData = [
    { name: '남성', value: summary.demographics.male, color: '#006241' },
    { name: '여성', value: summary.demographics.female, color: '#FF6F00' },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={[16, 16]}>
        {SUMMARY_STATS.map((s) => {
          const val = s.key.includes('.') ? s.key.split('.').reduce((o: any, k: string) => o?.[k], summary) : (summary as any)[s.key];
          return (
            <Col xs={12} sm={6} key={s.key}>
              <Card size="small" style={{ borderLeft: `4px solid ${s.borderColor}` }}>
                <Statistic title={s.title} value={val} suffix={s.suffix} valueStyle={{ color: s.color }}
                  prefix={s.prefix} formatter={s.format ? ((v) => Number(v).toLocaleString()) : undefined} />
              </Card>
            </Col>
          );
        })}
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
                  <RechartsTooltip />
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
                  <RechartsTooltip formatter={(v: number) => v.toLocaleString() + '건'} />
                  <Bar dataKey="count" name="내원 건수" radius={[0, 6, 6, 0]} barSize={28}>
                    {summary.visit_types.map((_, i) => (
                      <Cell key={i} fill={VISIT_TYPE_COLORS[i] || '#A8A8A8'} />
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
                    <Text strong style={{ fontSize: 12, color: getQualityColor(q.score) }}>
                      {q.score}%
                    </Text>
                  </div>
                  <Progress percent={q.score} showInfo={false} size="small" strokeColor={getQualityColor(q.score)} />
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
              <RechartsTooltip formatter={(v: number) => v.toLocaleString() + '건'} />
              <Area type="monotone" dataKey="total" name="전체 활동" stroke="#006241" strokeWidth={2} fillOpacity={1} fill="url(#cdmGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* 주요 진단 + 테이블 현황 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title="주요 진단 Top 15 (SNOMED CT)" size="small">
            <Table
              columns={CONDITION_COLUMNS}
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
                  <RechartsTooltip formatter={(v: number) => v.toLocaleString() + '건'} />
                  <Bar dataKey="row_count" name="레코드 수" radius={[0, 4, 4, 0]} barSize={20}>
                    {summary.table_stats.slice(0, 10).map((_, i) => (
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
        extra={<Tag color="green">OMOP CDM V5.4</Tag>}
      >
        <Table
          columns={MAPPING_COLUMNS}
          dataSource={mappingExamples.map((m, i) => ({ ...m, key: i }))}
          pagination={false}
          size="small"
        />
        <div style={{ marginTop: 12, padding: '8px 12px', background: '#f6ffed', borderRadius: 6, border: '1px solid #b7eb8f' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 4 }} />
            CMS Synthetic 데이터가 OMOP CDM V5.4 표준으로 변환 완료. SNOMED CT (진단), RxNorm (약물), LOINC (검사) 표준코드 체계 사용.
          </Text>
        </div>
      </Card>
    </Space>
  );
};

const TableExplorerTab: React.FC = () => {
  const { message } = App.useApp();
  const [searchTerm, setSearchTerm] = useState('');
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [schema, setSchema] = useState<ColumnInfo[]>([]);
  const [sampleData, setSampleData] = useState<{ columns: string[]; rows: any[] }>({ columns: [], rows: [] });
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [editDescOpen, setEditDescOpen] = useState(false);
  const [editDescValue, setEditDescValue] = useState('');
  const [editDescSaving, setEditDescSaving] = useState(false);

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

  const handleEditDescription = () => {
    if (!selectedTable) return;
    setEditDescValue(selectedTable.description);
    setEditDescOpen(true);
  };

  const handleSaveDescription = async () => {
    if (!selectedTable) return;
    setEditDescSaving(true);
    try {
      const res = await fetchPut(`${API_BASE}/tables/${selectedTable.name}/description`, { description: editDescValue });
      if (!res.ok) throw new Error();
      // Update local state
      setTables(prev => prev.map(t => t.name === selectedTable.name ? { ...t, description: editDescValue } : t));
      setSelectedTable(prev => prev ? { ...prev, description: editDescValue } : prev);
      setEditDescOpen(false);
      message.success('설명이 수정되었습니다');
    } catch {
      message.error('설명 수정 실패');
    } finally {
      setEditDescSaving(false);
    }
  };

  const handleExportCsv = (tableName: string) => {
    const a = document.createElement('a');
    a.href = `${API_BASE}/tables/${tableName}/export-csv?limit=10000`;
    a.download = `${tableName}.csv`;
    a.click();
    message.info(`${tableName}.csv 다운로드 시작`);
  };


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
        <Card
          title={<><DatabaseOutlined /> {selectedTable.name}</>}
          style={{ marginTop: 16 }}
          extra={
            <Space>
              <Tooltip title="설명 수정">
                <Button size="small" icon={<EditOutlined />} onClick={handleEditDescription}>설명 수정</Button>
              </Tooltip>
              <Tooltip title="CSV 내보내기">
                <Button size="small" icon={<DownloadOutlined />} onClick={() => handleExportCsv(selectedTable.name)}>CSV 내보내기</Button>
              </Tooltip>
            </Space>
          }
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
                    columns={SCHEMA_COLUMNS}
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
                  <React.Suspense fallback={<Spin size="small" />}>
                    <Tabs
                      defaultActiveKey="python"
                      items={[
                        {
                          key: 'python',
                          label: 'Python',
                          children: (
                            <LazyCodeBlock language="python">
                              {pythonCodeSnippet(selectedTable.name)}
                            </LazyCodeBlock>
                          ),
                        },
                        {
                          key: 'r',
                          label: 'R',
                          children: (
                            <LazyCodeBlock language="r">
                              {rCodeSnippet(selectedTable.name)}
                            </LazyCodeBlock>
                          ),
                        },
                      ]}
                    />
                  </React.Suspense>
                ),
              },
            ]}
          />
        </Card>
      </Spin>
    );
  };

  return (
    <>
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
                  <div
                    key={table.name}
                    onClick={() => loadTableDetail(table)}
                    style={{
                      cursor: 'pointer',
                      padding: '10px 12px',
                      borderRadius: 6,
                      border: `1px solid ${selectedTable?.name === table.name ? '#005BAC' : '#f0f0f0'}`,
                      background: selectedTable?.name === table.name ? '#f0f7ff' : '#fff',
                      transition: 'border-color 0.2s, background 0.2s',
                    }}
                  >
                    <Space>
                      <DatabaseOutlined style={{ color: '#005BAC' }} />
                      <Text strong style={{ fontSize: 13 }}>{table.name}</Text>
                      <Tag color={CATEGORY_COLORS[table.category] || 'default'} style={{ fontSize: 11, margin: 0 }}>
                        {table.category}
                      </Tag>
                    </Space>
                    <div style={{ fontSize: 12, color: '#8c8c8c', marginTop: 4 }}>
                      {table.description}
                    </div>
                    <div style={{ fontSize: 11, color: '#bfbfbf', marginTop: 2 }}>
                      {table.row_count.toLocaleString()} rows · {table.column_count} cols
                    </div>
                  </div>
                ))}
              </Space>
            </Spin>
          </Card>
        </Col>
        <Col xs={24} xl={15}>
          {renderDetailView()}
        </Col>
      </Row>

      {/* Edit Description Modal */}
      <Modal
        title="테이블 설명 수정"
        open={editDescOpen}
        onCancel={() => setEditDescOpen(false)}
        onOk={handleSaveDescription}
        confirmLoading={editDescSaving}
        okText="저장"
        cancelText="취소"
      >
        <div style={{ marginTop: 16 }}>
          <Text type="secondary">테이블: <Text code>{selectedTable?.name}</Text></Text>
          <Input.TextArea
            rows={3}
            value={editDescValue}
            onChange={e => setEditDescValue(e.target.value)}
            style={{ marginTop: 8 }}
            placeholder="테이블 설명을 입력하세요"
          />
        </div>
      </Modal>
    </>
  );
};

const DataMart: React.FC = () => {
  const { message } = App.useApp();
  const [cacheClearLoading, setCacheClearLoading] = useState(false);

  const handleCacheClear = async () => {
    setCacheClearLoading(true);
    try {
      const res = await fetchPost(`${API_BASE}/cache-clear`);
      if (!res.ok) throw new Error();
      message.success('캐시가 초기화되었습니다');
    } catch {
      message.error('캐시 초기화 실패');
    } finally {
      setCacheClearLoading(false);
    }
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
              OMOP CDM V5.4 기반 임상 데이터 탐색 및 관리 (CMS Synthetic Data)
            </Paragraph>
          </Col>
          <Col>
            <Space>
              <Tooltip title="CDM 요약/대시보드/매핑 캐시 초기화">
                <Button icon={<ClearOutlined />} onClick={handleCacheClear} loading={cacheClearLoading}>
                  캐시 초기화
                </Button>
              </Tooltip>
            </Space>
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
            label: <><ToolOutlined /> 테이블 관리</>,
            children: <TableExplorerTab />,
          },
        ]}
      />
    </Space>
  );
};

export default DataMart;
