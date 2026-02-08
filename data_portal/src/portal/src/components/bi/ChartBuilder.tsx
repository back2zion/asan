import React, { useState, useEffect, useCallback } from 'react';
import { Card, Steps, Button, Select, Input, Space, Row, Col, Table, Tag, Popconfirm, Typography, Empty, Spin, Radio, App } from 'antd';
import {
  BarChartOutlined, LineChartOutlined, PieChartOutlined, AreaChartOutlined,
  DotChartOutlined, TableOutlined, PlusOutlined, DeleteOutlined, EyeOutlined, EditOutlined,
} from '@ant-design/icons';
import { biApi } from '../../services/biApi';
import ResultChart from '../common/ResultChart';
import RawDataModal from './RawDataModal';

const { Text, Title } = Typography;

const CHART_TYPES = [
  { value: 'bar', label: '바 차트', icon: <BarChartOutlined /> },
  { value: 'line', label: '라인 차트', icon: <LineChartOutlined /> },
  { value: 'pie', label: '파이 차트', icon: <PieChartOutlined /> },
  { value: 'area', label: '영역 차트', icon: <AreaChartOutlined /> },
  { value: 'scatter', label: '산점도', icon: <DotChartOutlined /> },
  { value: 'table', label: '테이블', icon: <TableOutlined /> },
];

const ChartBuilder: React.FC = () => {
  const { message } = App.useApp();
  const [step, setStep] = useState(0);

  // Step 0: Data source
  const [sourceType, setSourceType] = useState<'table' | 'sql'>('table');
  const [tables, setTables] = useState<any[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [customSql, setCustomSql] = useState('');

  // Step 1: Chart type
  const [chartType, setChartType] = useState('bar');

  // Step 2: Column mapping
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [xField, setXField] = useState('');
  const [yField, setYField] = useState('');
  const [groupField, setGroupField] = useState<string | undefined>();

  // Step 3: Preview
  const [previewColumns, setPreviewColumns] = useState<string[]>([]);
  const [previewRows, setPreviewRows] = useState<any[][]>([]);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Chart metadata
  const [chartName, setChartName] = useState('');
  const [chartDesc, setChartDesc] = useState('');

  // Saved charts list
  const [charts, setCharts] = useState<any[]>([]);
  const [chartsLoading, setChartsLoading] = useState(true);

  // Raw data modal
  const [rawModal, setRawModal] = useState<{ open: boolean; name: string; sql: string; columns: string[]; rows: any[][] }>({
    open: false, name: '', sql: '', columns: [], rows: [],
  });

  useEffect(() => {
    biApi.getTables().then(setTables).catch(() => {});
    loadCharts();
  }, []);

  const loadCharts = async () => {
    setChartsLoading(true);
    try {
      const list = await biApi.getCharts();
      setCharts(list);
    } catch { /* ignore */ }
    setChartsLoading(false);
  };

  const buildSqlQuery = (): string => {
    if (sourceType === 'sql' && customSql.trim()) return customSql;
    if (sourceType === 'table' && selectedTable) {
      const cols = [xField, yField, groupField].filter(Boolean);
      if (cols.length === 0) return `SELECT * FROM ${selectedTable} LIMIT 100`;
      return `SELECT ${cols.join(', ')} FROM ${selectedTable} LIMIT 500`;
    }
    return '';
  };

  const fetchPreview = useCallback(async () => {
    const sqlQuery = buildSqlQuery();
    if (!sqlQuery) { message.warning('SQL이 비어 있습니다'); return; }
    setPreviewLoading(true);
    const hide = message.loading('데이터 미리보기를 불러오는 중입니다. 테이블 크기에 따라 시간이 소요될 수 있습니다...', 0);
    try {
      const result = await biApi.executeQuery(sqlQuery, 500);
      setPreviewColumns(result.columns || []);
      setPreviewRows(result.rows || []);
      if (result.columns?.length && !xField) setXField(result.columns[0]);
      if (result.columns?.length > 1 && !yField) setYField(result.columns[1]);
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '미리보기 실패');
    }
    hide();
    setPreviewLoading(false);
  }, [sourceType, selectedTable, customSql, xField, yField, groupField]);

  const handleLoadColumns = async () => {
    if (sourceType === 'table' && selectedTable) {
      try {
        const cols = await biApi.getColumns(selectedTable);
        setAvailableColumns(cols.map((c: any) => c.column_name));
      } catch { setAvailableColumns([]); }
    } else if (sourceType === 'sql' && customSql.trim()) {
      try {
        const result = await biApi.executeQuery(customSql, 5);
        setAvailableColumns(result.columns || []);
      } catch { setAvailableColumns([]); }
    }
  };

  const handleSaveChart = async () => {
    if (!chartName.trim()) { message.warning('차트 이름을 입력하세요'); return; }
    const sql = buildSqlQuery();
    if (!sql) { message.warning('SQL이 필요합니다'); return; }
    try {
      await biApi.createChart({
        name: chartName,
        chart_type: chartType,
        sql_query: sql,
        config: { xField, yField, groupField },
        description: chartDesc,
      });
      message.success('차트가 저장되었습니다');
      setStep(0);
      setChartName('');
      setChartDesc('');
      loadCharts();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '저장 실패');
    }
  };

  const handleDeleteChart = async (chartId: number) => {
    try {
      await biApi.deleteChart(chartId);
      setCharts(prev => prev.filter(c => c.chart_id !== chartId));
      message.success('차트가 삭제되었습니다');
    } catch { message.error('삭제 실패'); }
  };

  const handleViewRawData = async (chart: any) => {
    const hide = message.loading('원본 데이터를 조회합니다. 데이터 양에 따라 10초 이상 소요될 수 있습니다...', 0);
    try {
      const data = await biApi.getChartRawData(chart.chart_id, 500);
      setRawModal({ open: true, name: chart.name, sql: data.sql_query, columns: data.columns, rows: data.rows });
    } catch { message.error('원본 데이터 로드 실패'); }
    hide();
  };

  const columnOptions = availableColumns.map(c => ({ label: c, value: c }));

  const stepContent = [
    // Step 0: Data source
    <div key="s0">
      <Radio.Group value={sourceType} onChange={e => setSourceType(e.target.value)} style={{ marginBottom: 16 }}>
        <Radio.Button value="table">테이블 선택</Radio.Button>
        <Radio.Button value="sql">직접 SQL</Radio.Button>
      </Radio.Group>
      {sourceType === 'table' ? (
        <Select
          showSearch placeholder="테이블 선택" style={{ width: '100%' }}
          value={selectedTable || undefined}
          onChange={v => { setSelectedTable(v); setAvailableColumns([]); }}
          options={tables.map(t => ({ label: `${t.table_name} (${t.row_count > 1000000 ? `${(t.row_count / 1000000).toFixed(1)}M` : t.row_count}행)`, value: t.table_name }))}
          filterOption={(input, opt) => (opt?.label as string)?.toLowerCase().includes(input.toLowerCase())}
        />
      ) : (
        <Input.TextArea
          placeholder="SELECT ... FROM ... GROUP BY ..."
          value={customSql} onChange={e => setCustomSql(e.target.value)}
          rows={4} style={{ fontFamily: 'monospace', fontSize: 12 }}
        />
      )}
    </div>,

    // Step 1: Chart type
    <div key="s1">
      <Row gutter={[12, 12]}>
        {CHART_TYPES.map(ct => (
          <Col span={8} key={ct.value}>
            <Card
              hoverable
              size="small"
              onClick={() => setChartType(ct.value)}
              style={{
                border: chartType === ct.value ? '2px solid #006241' : '1px solid #d9d9d9',
                textAlign: 'center',
                cursor: 'pointer',
              }}
            >
              <div style={{ fontSize: 24, color: chartType === ct.value ? '#006241' : '#999' }}>{ct.icon}</div>
              <Text strong={chartType === ct.value}>{ct.label}</Text>
            </Card>
          </Col>
        ))}
      </Row>
    </div>,

    // Step 2: Column mapping
    <div key="s2">
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Select placeholder="X축 (카테고리/시간)" style={{ width: '100%' }} value={xField || undefined}
          onChange={setXField} options={columnOptions} allowClear showSearch />
        <Select placeholder="Y축 (값)" style={{ width: '100%' }} value={yField || undefined}
          onChange={setYField} options={columnOptions} allowClear showSearch />
        <Select placeholder="그룹 (선택)" style={{ width: '100%' }} value={groupField}
          onChange={setGroupField} options={columnOptions} allowClear showSearch />
      </Space>
    </div>,

    // Step 3: Preview & Save
    <div key="s3">
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Input placeholder="차트 이름 *" value={chartName} onChange={e => setChartName(e.target.value)} />
        <Input placeholder="설명 (선택)" value={chartDesc} onChange={e => setChartDesc(e.target.value)} />
        {previewLoading ? <Spin /> : previewColumns.length > 0 ? (
          <div style={{ background: '#fafafa', borderRadius: 8, padding: 16 }}>
            <ResultChart columns={previewColumns} results={previewRows} />
          </div>
        ) : (
          <Empty description="미리보기를 실행하세요" />
        )}
      </Space>
    </div>,
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, height: '100%' }}>
      {/* Wizard */}
      <Card size="small">
        <Steps
          current={step}
          size="small"
          items={[
            { title: '데이터 소스' },
            { title: '차트 타입' },
            { title: '컬럼 매핑' },
            { title: '미리보기 & 저장' },
          ]}
          style={{ marginBottom: 16 }}
        />
        <div style={{ minHeight: 150 }}>{stepContent[step]}</div>
        <div style={{ marginTop: 16, display: 'flex', justifyContent: 'space-between' }}>
          <Button disabled={step === 0} onClick={() => setStep(s => s - 1)}>이전</Button>
          <Space>
            {step === 2 && <Button onClick={handleLoadColumns}>컬럼 로드</Button>}
            {step === 3 && <Button onClick={fetchPreview} loading={previewLoading}>미리보기</Button>}
            {step === 3 ? (
              <Button type="primary" icon={<PlusOutlined />} onClick={handleSaveChart}>차트 저장</Button>
            ) : (
              <Button type="primary" onClick={() => { if (step === 0) handleLoadColumns(); setStep(s => s + 1); }}>다음</Button>
            )}
          </Space>
        </div>
      </Card>

      {/* Saved charts grid */}
      <Card size="small" title={<><BarChartOutlined /> 저장된 차트 ({charts.length})</>}>
        {chartsLoading ? <Spin /> : charts.length === 0 ? <Empty description="저장된 차트가 없습니다" /> : (
          <Row gutter={[12, 12]}>
            {charts.map(c => (
              <Col xs={24} sm={12} md={8} key={c.chart_id}>
                <Card size="small" hoverable
                  actions={[
                    <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDeleteChart(c.chart_id)} okText="삭제" cancelText="취소">
                      <DeleteOutlined />
                    </Popconfirm>,
                    <EyeOutlined onClick={() => handleViewRawData(c)} />,
                  ]}
                >
                  <Card.Meta
                    title={<Text ellipsis style={{ fontSize: 13 }}>{c.name}</Text>}
                    description={
                      <Space>
                        <Tag color="blue">{c.chart_type}</Tag>
                        <Text type="secondary" style={{ fontSize: 11 }}>{c.description?.substring(0, 30)}</Text>
                      </Space>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>
        )}
      </Card>

      <RawDataModal
        open={rawModal.open}
        onClose={() => setRawModal(prev => ({ ...prev, open: false }))}
        chartName={rawModal.name}
        sqlQuery={rawModal.sql}
        columns={rawModal.columns}
        rows={rawModal.rows}
      />
    </div>
  );
};

export default ChartBuilder;
