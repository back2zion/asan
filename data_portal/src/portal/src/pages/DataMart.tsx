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
  CheckCircleOutlined,
  EditOutlined,
  DownloadOutlined,
  ClearOutlined,
  ToolOutlined,
  PictureOutlined,
  CloudUploadOutlined,
  ClockCircleOutlined,
  SafetyCertificateOutlined,
  HeartOutlined,
  ApiOutlined,
} from '@ant-design/icons';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip as RechartsTooltip, ResponsiveContainer, Cell,
} from 'recharts';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { RadarChart as ERadarChart, GraphChart, LineChart, BarChart as EBarChart, PieChart as EPieChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, RadarComponent, LegendComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { fetchPost, fetchPut } from '../services/apiUtils';
import {
  API_BASE, CATEGORY_COLORS, DOMAIN_COLORS,
  TableInfo, ColumnInfo, CdmSummary, MappingExample,
  pythonCodeSnippet, rCodeSnippet, LazyCodeBlock,
  MAPPING_COLUMNS, SCHEMA_COLUMNS,
  useCountUp,
  buildRadarOption, buildPatientJourneyOption,
  buildYearlyActivityOption, buildTopConditionsOption,
  buildTableDistributionOption,
  buildDataFlexibilityOption,
} from './DataMartHelpers';

echarts.use([ERadarChart, GraphChart, LineChart, EBarChart, EPieChart, GridComponent, TooltipComponent, RadarComponent, LegendComponent, SVGRenderer]);

const { Title, Paragraph, Text } = Typography;
const { Search } = Input;

// ── 히어로 카운터 카드 (글래스모피즘) ──
const HeroStatCard: React.FC<{ label: string; target: number; suffix: string; icon: React.ReactNode }> = ({ label, target, suffix, icon }) => {
  const animVal = useCountUp(target);
  return (
    <div style={{
      background: 'rgba(255,255,255,0.12)',
      backdropFilter: 'blur(10px)',
      borderRadius: 12,
      padding: '18px 20px',
      border: '1px solid rgba(255,255,255,0.18)',
      textAlign: 'center',
      minWidth: 0,
    }}>
      <div style={{ fontSize: 13, color: 'rgba(255,255,255,0.75)', marginBottom: 6, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6 }}>
        {icon}
        {label}
      </div>
      <div style={{ fontSize: 28, fontWeight: 700, color: '#fff', lineHeight: 1.2 }}>
        {animVal.toLocaleString()}<span style={{ fontSize: 14, fontWeight: 400, marginLeft: 2 }}>{suffix}</span>
      </div>
    </div>
  );
};

// ── 히어로 헤더 ──
const HeroBanner: React.FC<{ summary: CdmSummary | undefined; loading: boolean; onCacheClear: () => void; cacheClearLoading: boolean }> = ({ summary, loading, onCacheClear, cacheClearLoading }) => {
  const totalRecords = summary?.total_records || 0;
  const totalPatients = summary?.demographics.total_patients || 0;
  const totalTables = summary?.total_tables || 0;
  const avgQuality = summary?.quality.length
    ? Math.round(summary.quality.reduce((s, q) => s + q.score, 0) / summary.quality.length * 10) / 10
    : 0;

  return (
    <div style={{
      background: 'linear-gradient(135deg, #006241 0%, #004d33 50%, #003825 100%)',
      borderRadius: 16,
      padding: '32px 36px 28px',
      position: 'relative',
      overflow: 'hidden',
    }}>
      {/* 장식 원 */}
      <div style={{ position: 'absolute', top: -40, right: -40, width: 180, height: 180, borderRadius: '50%', background: 'rgba(255,255,255,0.04)' }} />
      <div style={{ position: 'absolute', bottom: -60, left: '30%', width: 240, height: 240, borderRadius: '50%', background: 'rgba(255,255,255,0.03)' }} />

      <div style={{ position: 'relative', zIndex: 1 }}>
        <Row align="middle" justify="space-between" style={{ marginBottom: 20 }}>
          <Col>
            <Title level={2} style={{ margin: 0, color: '#fff', fontWeight: 700, letterSpacing: '-0.5px' }}>
              <DatabaseOutlined style={{ marginRight: 12, fontSize: 30 }} />
              데이터마트
            </Title>
            <Paragraph style={{ margin: '8px 0 0 42px', fontSize: 14, color: 'rgba(255,255,255,0.7)' }}>
              OMOP CDM V5.4 기반 | 92M+ 임상 레코드 | 76K 환자 코호트
            </Paragraph>
          </Col>
          <Col>
            <Tooltip title="데이터 캐시 초기화">
              <Button
                icon={<ClearOutlined />}
                onClick={onCacheClear}
                loading={cacheClearLoading}
                style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.25)', color: '#fff' }}
              >
                캐시 초기화
              </Button>
            </Tooltip>
          </Col>
        </Row>

        <Row gutter={[16, 16]}>
          <Col xs={12} sm={6}>
            <HeroStatCard label="총 레코드" target={totalRecords} suffix="+" icon={<DatabaseOutlined />} />
          </Col>
          <Col xs={12} sm={6}>
            <HeroStatCard label="등록 환자" target={totalPatients} suffix="+" icon={<TeamOutlined />} />
          </Col>
          <Col xs={12} sm={6}>
            <HeroStatCard label="CDM 테이블" target={totalTables} suffix="개" icon={<TableOutlined />} />
          </Col>
          <Col xs={12} sm={6}>
            <HeroStatCard label="데이터 품질" target={avgQuality} suffix="%" icon={<SafetyCertificateOutlined />} />
          </Col>
        </Row>
      </div>
    </div>
  );
};

// ── CdmSummaryTab (ECharts 기반) ──
const CdmSummaryTab: React.FC<{ summary: CdmSummary }> = ({ summary }) => {
  const { message } = App.useApp();

  const { data: mappingExamples = [] } = useQuery<MappingExample[]>({
    queryKey: ['datamart', 'cdm-mapping-examples'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/cdm-mapping-examples`);
      if (!res.ok) throw new Error('매핑 예시 로드 실패');
      const d = await res.json();
      return d.examples || [];
    },
  });

  // ECharts: 성별 도넛
  const genderOption = useMemo(() => ({
    tooltip: { trigger: 'item', formatter: '{b}: {c}명 ({d}%)' },
    series: [{
      type: 'pie',
      radius: ['45%', '72%'],
      center: ['50%', '45%'],
      data: [
        { value: summary.demographics.male, name: '남성', itemStyle: { color: '#006241' } },
        { value: summary.demographics.female, name: '여성', itemStyle: { color: '#FF6F00' } },
      ],
      label: { show: true, formatter: '{b}\n{c}명', fontSize: 11, lineHeight: 16 },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.2)' } },
      animationDuration: 1200,
      animationEasing: 'cubicOut',
    }],
    graphic: [{
      type: 'text',
      left: 'center',
      top: '40%',
      style: {
        text: `${summary.demographics.total_patients.toLocaleString()}`,
        textAlign: 'center',
        fill: '#333',
        fontSize: 20,
        fontWeight: 700,
      },
    }, {
      type: 'text',
      left: 'center',
      top: '48%',
      style: { text: '총 환자', textAlign: 'center', fill: '#999', fontSize: 11 },
    }],
  }), [summary.demographics]);

  const radarOption = useMemo(() => buildRadarOption(summary.quality), [summary.quality]);
  const journeyOption = useMemo(() => buildPatientJourneyOption(summary), [summary]);
  const yearlyOption = useMemo(() => buildYearlyActivityOption(summary.yearly_activity), [summary.yearly_activity]);
  const conditionsOption = useMemo(() => buildTopConditionsOption(summary.top_conditions), [summary.top_conditions]);
  const tableDistOption = useMemo(() => buildTableDistributionOption(summary.table_stats), [summary.table_stats]);

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* 환자 인구통계 + 방문유형 + 품질 레이더 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card title={<><TeamOutlined /> 환자 인구통계</>} size="small">
            <ReactEChartsCore echarts={echarts} option={genderOption} style={{ height: 260 }} opts={{ renderer: 'svg' }} />
          </Card>
        </Col>

        <Col xs={24} md={8}>
          <Card title={<><SwapOutlined /> 방문 유형 분포</>} size="small">
            <div style={{ padding: '16px 0' }}>
              {summary.visit_types.map((v, i) => {
                const colors = ['#006241', '#FF6F00', '#DC2626'];
                const maxCount = Math.max(...summary.visit_types.map(vt => vt.count));
                const pct = (v.count / maxCount) * 100;
                return (
                  <div key={v.type_id} style={{ marginBottom: 16 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <Text strong style={{ fontSize: 13 }}>{v.type_name}</Text>
                      <Text style={{ fontSize: 12, color: '#666' }}>{v.count.toLocaleString()}건</Text>
                    </div>
                    <div style={{ height: 24, background: '#f5f5f5', borderRadius: 6, overflow: 'hidden' }}>
                      <div style={{
                        width: `${pct}%`,
                        height: '100%',
                        background: `linear-gradient(90deg, ${colors[i] || '#aaa'}, ${colors[i] || '#aaa'}cc)`,
                        borderRadius: 6,
                        transition: 'width 1.2s cubic-bezier(0.4,0,0.2,1)',
                        display: 'flex',
                        alignItems: 'center',
                        paddingLeft: 8,
                      }}>
                        <span style={{ fontSize: 11, color: '#fff', fontWeight: 600 }}>
                          {v.patient_count.toLocaleString()}명
                        </span>
                      </div>
                    </div>
                  </div>
                );
              })}
              <div style={{ textAlign: 'center', marginTop: 8 }}>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  총 {summary.visit_types.reduce((s, v) => s + v.count, 0).toLocaleString()}건
                </Text>
              </div>
            </div>
          </Card>
        </Col>

        <Col xs={24} md={8}>
          <Card title={<><CheckCircleOutlined /> 도메인별 데이터 품질</>} size="small">
            <ReactEChartsCore echarts={echarts} option={radarOption} style={{ height: 260 }} opts={{ renderer: 'svg' }} />
          </Card>
        </Col>
      </Row>

      {/* 환자 임상 여정 (Patient Journey) */}
      <Card title={<><HeartOutlined style={{ color: '#006241' }} /> 환자 임상 여정 (Patient Clinical Journey)</>} size="small">
        <ReactEChartsCore echarts={echarts} option={journeyOption} style={{ height: 220 }} opts={{ renderer: 'svg' }} />
      </Card>

      {/* 연도별 활동 추이 */}
      <Card title="연도별 임상 데이터 활동 추이" size="small">
        <ReactEChartsCore echarts={echarts} option={yearlyOption} style={{ height: 320 }} opts={{ renderer: 'svg' }} />
      </Card>

      {/* 주요 진단 Top 15 + 테이블 분포 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title="주요 진단 Top 15 (SNOMED CT)" size="small">
            <ReactEChartsCore echarts={echarts} option={conditionsOption} style={{ height: 480 }} opts={{ renderer: 'svg' }} />
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card title="테이블별 레코드 분포" size="small">
            <ReactEChartsCore echarts={echarts} option={tableDistOption} style={{ height: 400 }} opts={{ renderer: 'svg' }} />
          </Card>
        </Col>
      </Row>

      {/* Source → CDM 매핑 예시 */}
      <Card
        title={<><SwapOutlined /> 원천 → 표준 코드 매핑 예시</>}
        size="small"
        extra={<Tag color="green">국제표준 V5.4</Tag>}
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
            국제표준 코드 체계로 변환 완료 — SNOMED CT (진단), RxNorm (약물), LOINC (검사).
          </Text>
        </div>
      </Card>
    </Space>
  );
};

const TableExplorerTab: React.FC = () => {
  const { message } = App.useApp();
  const queryClient = useQueryClient();
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [editDescOpen, setEditDescOpen] = useState(false);
  const [editDescValue, setEditDescValue] = useState('');
  const [editDescSaving, setEditDescSaving] = useState(false);

  const { data: tablesData, isLoading: loading, error: tablesError, refetch: refetchTables } = useQuery<TableInfo[]>({
    queryKey: ['datamart', 'tables'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/tables`);
      if (!res.ok) throw new Error('테이블 목록 로드 실패');
      const data = await res.json();
      return data.tables;
    },
  });

  useEffect(() => {
    if (tablesError) message.error((tablesError as Error).message || '임상 DB 연결 실패');
  }, [tablesError, message]);

  const tables = tablesData || [];

  // Auto-select default table on initial load
  useEffect(() => {
    if (tables.length > 0 && !selectedTable) {
      const defaultTable = tables.find((t: TableInfo) => t.name === 'person') || tables[0];
      setSelectedTable(defaultTable);
    }
  }, [tables, selectedTable]);

  const { data: schemaData, isLoading: schemaLoading, error: detailError } = useQuery<ColumnInfo[]>({
    queryKey: ['datamart', 'table-schema', selectedTable?.name],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/tables/${selectedTable!.name}/schema`);
      if (!res.ok) throw new Error('스키마 정보 로드 실패');
      const data = await res.json();
      return data.columns;
    },
    enabled: !!selectedTable,
  });

  const { data: sampleResult, isLoading: sampleLoading } = useQuery<{ columns: string[]; rows: any[] }>({
    queryKey: ['datamart', 'table-sample', selectedTable?.name],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/tables/${selectedTable!.name}/sample?limit=5`);
      if (!res.ok) throw new Error('샘플 데이터 로드 실패');
      const data = await res.json();
      return { columns: data.columns, rows: data.rows };
    },
    enabled: !!selectedTable,
  });

  useEffect(() => {
    if (detailError) message.error((detailError as Error).message || '테이블 상세 정보 로드 실패');
  }, [detailError, message]);

  const schema = schemaData || [];
  const sampleData = sampleResult || { columns: [], rows: [] };
  const detailLoading = schemaLoading || sampleLoading;

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
      setSelectedTable(prev => prev ? { ...prev, description: editDescValue } : prev);
      queryClient.invalidateQueries({ queryKey: ['datamart', 'tables'] });
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
            title="임상 데이터 테이블 목록"
            size="small"
            extra={<Button icon={<ReloadOutlined />} size="small" onClick={() => refetchTables()} loading={loading}>새로고침</Button>}
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
                    onClick={() => setSelectedTable(table)}
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

// ── 의료 영상 데이터셋 탭 ──
interface ImagingDataset {
  key: string;
  label: string;
  source: string;
  modality: string;
  image_count: number;
  label_count: number;
  label_desc: string;
  status: string;
}
interface ImagingStats {
  bucket: string;
  total_images: number;
  total_labels: number;
  total_files: number;
  datasets: ImagingDataset[];
}

const BODY_PART_COLORS: Record<string, string> = {
  ChestPA: '#005BAC', Mammography: '#FF6F00', PNS: '#006241',
  FacialBone: '#722ED1', Foot: '#13C2C2', NIH: '#EB2F96',
};

const MedicalImagingTab: React.FC = () => {
  const { data: stats, isLoading } = useQuery<ImagingStats>({
    queryKey: ['datamart', 'imaging-stats'],
    queryFn: async () => {
      const res = await fetch('/api/v1/medical-imaging/minio-stats');
      if (!res.ok) throw new Error('영상 데이터 통계 로드 실패');
      return res.json();
    },
  });

  if (isLoading) return <Spin size="large" tip="영상 데이터 조회 중..."><div style={{ textAlign: 'center', padding: 80 }} /></Spin>;
  if (!stats) return <Empty description="영상 데이터 통계를 불러올 수 없습니다." />;

  const chartData = stats.datasets.map(d => ({
    name: d.label.split(' (')[0],
    images: d.image_count,
    labels: d.label_count,
    fill: BODY_PART_COLORS[d.key] || '#666',
  }));

  const columns = [
    {
      title: '부위',
      dataIndex: 'label',
      key: 'label',
      render: (label: string, r: ImagingDataset) => (
        <Space>
          <Tag color={BODY_PART_COLORS[r.key] || 'default'}>{label}</Tag>
        </Space>
      ),
    },
    { title: '출처', dataIndex: 'source', key: 'source', width: 80 },
    { title: 'Modality', dataIndex: 'modality', key: 'modality', width: 80 },
    {
      title: '이미지',
      dataIndex: 'image_count',
      key: 'image_count',
      width: 120,
      render: (v: number) => v > 0 ? <Text strong>{v.toLocaleString()}장</Text> : <Text type="secondary">-</Text>,
    },
    {
      title: '라벨',
      dataIndex: 'label_desc',
      key: 'label_desc',
      width: 180,
      render: (desc: string) => desc ? <Text>{desc}</Text> : <Text type="secondary">-</Text>,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => status === 'uploaded'
        ? <Tag icon={<CheckCircleOutlined />} color="success">적재 완료</Tag>
        : <Tag icon={<ClockCircleOutlined />} color="warning">대기</Tag>,
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={[16, 16]}>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #005BAC' }}>
            <Statistic title="전체 파일" value={stats.total_files} suffix="건" valueStyle={{ color: '#005BAC' }}
              prefix={<CloudUploadOutlined />} formatter={(v) => Number(v).toLocaleString()} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #006241' }}>
            <Statistic title="이미지" value={stats.total_images} suffix="장" valueStyle={{ color: '#006241' }}
              prefix={<PictureOutlined />} formatter={(v) => Number(v).toLocaleString()} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #FF6F00' }}>
            <Statistic title="라벨 (어노테이션)" value={stats.total_labels} suffix="건" valueStyle={{ color: '#FF6F00' }}
              prefix={<CodeOutlined />} formatter={(v) => Number(v).toLocaleString()} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small" style={{ borderLeft: '4px solid #722ED1' }}>
            <Statistic title="데이터셋" value={stats.datasets.length} suffix="종" valueStyle={{ color: '#722ED1' }}
              prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title={<><PictureOutlined /> 부위별 이미지 데이터셋</>} size="small">
            <Table
              columns={columns}
              dataSource={stats.datasets.map((d, i) => ({ ...d, key: d.key || i }))}
              pagination={false}
              size="small"
            />
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card title="부위별 이미지 분포" size="small">
            <div style={{ height: 320 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} layout="vertical" margin={{ left: 10 }}>
                  <XAxis type="number" hide />
                  <YAxis dataKey="name" type="category" width={100} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                  <RechartsTooltip formatter={(v: number) => v.toLocaleString() + '장'} />
                  <Bar dataKey="images" name="이미지" radius={[0, 4, 4, 0]} barSize={18}>
                    {chartData.map((d, i) => <Cell key={i} fill={d.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Card>
        </Col>
      </Row>

      <Card size="small" style={{ background: '#f6ffed', border: '1px solid #b7eb8f' }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 4 }} />
          AI Hub 합성 X-ray 5개 부위 + NIH Chest X-ray 공개 데이터셋 적재 완료.
          영상 뷰어 및 AI 학습 파이프라인과 연동 가능.
        </Text>
      </Card>
    </Space>
  );
};

const DataMart: React.FC = () => {
  const { message } = App.useApp();
  const [cacheClearLoading, setCacheClearLoading] = useState(false);

  const { data: summary, isLoading: summaryLoading, error: summaryError } = useQuery<CdmSummary>({
    queryKey: ['datamart', 'cdm-summary'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/cdm-summary`);
      if (!res.ok) throw new Error('임상 데이터 요약 로드 실패');
      return res.json();
    },
  });

  useEffect(() => {
    if (summaryError) message.error((summaryError as Error).message || 'API 연결 실패');
  }, [summaryError, message]);

  const flexibilityOption = useMemo(() => buildDataFlexibilityOption(), []);

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
      <HeroBanner summary={summary} loading={summaryLoading} onCacheClear={handleCacheClear} cacheClearLoading={cacheClearLoading} />

      {/* 데이터 제공 유연성 다이어그램 */}
      <Card
        title={<><ApiOutlined style={{ color: '#006241' }} /> 데이터 제공 유연성 — 통합 아키텍처</>}
        size="small"
        extra={<Tag color="green">5 접근방식 · 5 표준 · 5 인프라</Tag>}
      >
        <ReactEChartsCore echarts={echarts} option={flexibilityOption} style={{ height: 440 }} opts={{ renderer: 'svg' }} />
      </Card>

      <Tabs
        defaultActiveKey="imaging"
        type="line"
        size="large"
        destroyOnHidden
        items={[
          {
            key: 'summary',
            label: <><SwapOutlined /> 임상 데이터 현황</>,
            children: summaryLoading
              ? <Spin size="large" tip="임상 데이터 분석 중..."><div style={{ textAlign: 'center', padding: 80 }} /></Spin>
              : !summary
                ? <Empty description="임상 데이터 요약을 불러올 수 없습니다." />
                : <CdmSummaryTab summary={summary} />,
          },
          {
            key: 'imaging',
            label: <><PictureOutlined /> 의료 영상</>,
            children: <MedicalImagingTab />,
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
