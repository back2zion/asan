import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Tabs, Empty, Popconfirm, Modal, message,
} from 'antd';
import {
  RocketOutlined, DatabaseOutlined, ExportOutlined, FundOutlined,
  ThunderboltOutlined, ReloadOutlined, EyeOutlined, PlayCircleOutlined,
  DeleteOutlined, SearchOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const PL_BASE = '/api/v1/pipeline';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body?: any) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const DataPipelineTab: React.FC = () => {
  const [dashboard, setDashboard] = useState<any>(null);
  const [lzTemplates, setLzTemplates] = useState<any[]>([]);
  const [queryHistory, setQueryHistory] = useState<any>(null);
  const [analysisResult, setAnalysisResult] = useState<any>(null);
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const [martDesigns, setMartDesigns] = useState<any[]>([]);
  const [exports, setExports] = useState<any[]>([]);
  const [executions, setExecutions] = useState<any>(null);
  const [previewModal, setPreviewModal] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const loadDashboard = useCallback(async () => {
    try { setDashboard(await fetchJSON(`${PL_BASE}/dashboard`)); } catch { /* ignore */ }
  }, []);

  const loadLZTemplates = useCallback(async () => {
    try { const d = await fetchJSON(`${PL_BASE}/landing-zone/templates`); setLzTemplates(d.templates || []); } catch { /* ignore */ }
  }, []);

  const loadExports = useCallback(async () => {
    try { const d = await fetchJSON(`${PL_BASE}/exports`); setExports(d.exports || []); } catch { /* ignore */ }
  }, []);

  const loadMartDesigns = useCallback(async () => {
    try { const d = await fetchJSON(`${PL_BASE}/mart-designs`); setMartDesigns(d.mart_designs || []); } catch { /* ignore */ }
  }, []);

  const loadExecutions = useCallback(async () => {
    try { setExecutions(await fetchJSON(`${PL_BASE}/executions`)); } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadDashboard(); loadLZTemplates(); loadExports(); loadMartDesigns(); loadExecutions(); },
    [loadDashboard, loadLZTemplates, loadExports, loadMartDesigns, loadExecutions]);

  const handleAutoGenerateLZ = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${PL_BASE}/landing-zone/templates/auto-generate`);
      message.success(`${data.count}개 Landing Zone 템플릿 자동 생성`);
      loadLZTemplates();
    } catch { message.error('자동 생성 실패'); }
    finally { setLoading(false); }
  };

  const handlePreviewLZ = async (templateId: string) => {
    try {
      const data = await postJSON(`${PL_BASE}/landing-zone/preview/${templateId}`);
      setPreviewModal(data);
    } catch { message.error('미리보기 실패'); }
  };

  const handleDeleteLZ = async (templateId: string) => {
    try {
      await deleteJSON(`${PL_BASE}/landing-zone/templates/${templateId}`);
      message.success('템플릿 삭제됨');
      loadLZTemplates();
    } catch (e: any) { message.error(e.message || '삭제 실패'); }
  };

  const handleLoadQueryHistory = async () => {
    setLoading(true);
    try { setQueryHistory(await fetchJSON(`${PL_BASE}/query-history`)); } catch { message.error('조회 이력 로드 실패'); }
    finally { setLoading(false); }
  };

  const handleAnalyze = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${PL_BASE}/query-history/analyze?period_days=30`);
      setAnalysisResult(data);
      message.success('분석 완료');
    } catch { message.error('분석 실패'); }
    finally { setLoading(false); }
  };

  const handleSuggestMarts = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${PL_BASE}/mart-suggestions`);
      setSuggestions(data.suggestions || []);
      message.success(`${data.suggestions?.length || 0}개 마트 추천`);
    } catch { message.error('추천 실패'); }
    finally { setLoading(false); }
  };

  const handleSaveMart = async (suggestion: any) => {
    try {
      await postJSON(`${PL_BASE}/mart-designs`, {
        name: suggestion.name, description: suggestion.description,
        source_tables: suggestion.source_tables, columns: suggestion.columns,
        indexes: suggestion.indexes, estimated_rows: suggestion.estimated_rows, build_sql: suggestion.build_sql,
      });
      message.success(`마트 '${suggestion.name}' 저장됨`);
      loadMartDesigns();
    } catch { message.error('저장 실패'); }
  };

  const handleExecuteExport = async (exportId: string) => {
    setLoading(true);
    try {
      const data = await postJSON(`${PL_BASE}/exports/${exportId}/execute`);
      message.success(`Export 실행 완료 (${data.summary?.total_rows?.toLocaleString()} rows)`);
      loadExecutions();
    } catch { message.error('실행 실패'); }
    finally { setLoading(false); }
  };

  const handlePreviewExport = async (exportId: string) => {
    try {
      const data = await fetchJSON(`${PL_BASE}/exports/${exportId}/preview`);
      setPreviewModal(data);
    } catch { message.error('미리보기 실패'); }
  };

  const handleDeleteExport = async (exportId: string) => {
    try {
      await deleteJSON(`${PL_BASE}/exports/${exportId}`);
      message.success('Export 삭제됨');
      loadExports();
    } catch (e: any) { message.error(e.message || '삭제 실패'); }
  };

  return (
    <div>
      {/* Dashboard stats */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={8} md={4}><Card size="small"><Statistic title="LZ 템플릿" value={dashboard?.landing_zone_templates || 0} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="Export" value={dashboard?.export_pipelines || 0} prefix={<ExportOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="마트 설계" value={dashboard?.mart_designs || 0} prefix={<FundOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="24h 실행" value={dashboard?.executions_24h || 0} prefix={<ThunderboltOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="24h 처리행" value={dashboard?.rows_processed_24h?.toLocaleString() || 0} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="성공" value={dashboard?.status_summary?.completed || 0} valueStyle={{ color: '#3f8600' }} /></Card></Col>
      </Row>

      <Tabs items={[
        { key: 'lz', label: <span><DatabaseOutlined /> Landing Zone</span>, children: (
          <Card size="small" title={<><DatabaseOutlined /> Landing Zone 템플릿</>}
            extra={<Button size="small" icon={<ThunderboltOutlined />} onClick={handleAutoGenerateLZ} loading={loading}>자동 생성</Button>}>
            {lzTemplates.length > 0 ? (
              <Table
                dataSource={lzTemplates.map(t => ({ ...t, key: t.id }))}
                columns={[
                  { title: '이름', dataIndex: 'name', key: 'name', render: (v: string, r: any) => (
                    <span><Text strong>{v}</Text>{r.id?.startsWith('lz-builtin') && <Tag color="cyan" style={{ marginLeft: 4 }}>내장</Tag>}</span>
                  )},
                  { title: '소스', dataIndex: 'source_system', key: 'source', width: 120 },
                  { title: '타겟 테이블', dataIndex: 'target_tables', key: 'targets', width: 180, render: (v: string[]) => v?.map(t => <Tag key={t}><Text code>{t}</Text></Tag>) },
                  { title: 'CDC 모드', key: 'cdc', width: 120, render: (_: any, r: any) => <Tag color="blue">{r.cdc_config?.capture_mode || '-'}</Tag> },
                  { title: '스케줄', dataIndex: 'schedule', key: 'schedule', width: 100 },
                  { title: '상태', dataIndex: 'enabled', key: 'enabled', width: 80, render: (v: boolean) => <Tag color={v !== false ? 'green' : 'default'}>{v !== false ? '활성' : '비활성'}</Tag> },
                  { title: '작업', key: 'action', width: 160, render: (_: any, r: any) => (
                    <Space size="small">
                      <Button size="small" icon={<EyeOutlined />} onClick={() => handlePreviewLZ(r.id)}>미리보기</Button>
                      {!r.id?.startsWith('lz-builtin') && (
                        <Popconfirm title="삭제?" onConfirm={() => handleDeleteLZ(r.id)}>
                          <Button size="small" icon={<DeleteOutlined />} danger />
                        </Popconfirm>
                      )}
                    </Space>
                  )},
                ]}
                size="small" pagination={false}
              />
            ) : <Empty description="Landing Zone 템플릿 없음" />}
          </Card>
        )},
        { key: 'cdw', label: <span><SearchOutlined /> CDW 분석</span>, children: (
          <Card size="small" title={<><SearchOutlined /> CDW 조회 이력 분석</>}
            extra={<Space><Button size="small" onClick={handleLoadQueryHistory} loading={loading}>이력 조회</Button>
              <Button size="small" icon={<SearchOutlined />} onClick={handleAnalyze} loading={loading}>패턴 분석</Button>
              <Button size="small" icon={<RocketOutlined />} type="primary" onClick={handleSuggestMarts} loading={loading}>마트 추천</Button></Space>}>
            {analysisResult ? (
              <Row gutter={16} style={{ marginBottom: 12 }}>
                <Col span={6}><Statistic title="유니크 쿼리" value={analysisResult.total_unique_queries} /></Col>
                <Col span={6}><Statistic title="총 실행" value={analysisResult.total_executions} /></Col>
                <Col span={6}><Statistic title="평균 시간(ms)" value={analysisResult.avg_query_time_ms} /></Col>
                <Col span={6}><Statistic title="분석 기간" value={`${analysisResult.period_days}일`} /></Col>
              </Row>
            ) : null}
            {analysisResult?.hot_tables?.length > 0 && (
              <div style={{ marginBottom: 12 }}>
                <Text strong>Hot Tables: </Text>
                {analysisResult.hot_tables.slice(0, 8).map((t: any) => <Tag key={t.table} color="volcano">{t.table} ({t.access_count})</Tag>)}
              </div>
            )}
            {suggestions.length > 0 && (
              <Table
                dataSource={suggestions.map((s, i) => ({ ...s, key: i }))}
                columns={[
                  { title: '#', dataIndex: 'rank', key: 'rank', width: 40 },
                  { title: '마트명', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                  { title: '소스 테이블', dataIndex: 'source_tables', key: 'tables', render: (v: string[]) => v?.map(t => <Tag key={t}>{t}</Tag>) },
                  { title: '신뢰도', dataIndex: 'confidence', key: 'conf', width: 90, render: (v: number) => <Tag color={v >= 0.7 ? 'green' : v >= 0.4 ? 'orange' : 'default'}>{(v * 100).toFixed(0)}%</Tag> },
                  { title: '조회수', dataIndex: 'total_access_count', key: 'access', width: 80 },
                  { title: '', key: 'save', width: 70, render: (_: any, r: any) => <Button size="small" type="primary" onClick={() => handleSaveMart(r)}>저장</Button> },
                ]}
                size="small" pagination={false}
              />
            )}
            {queryHistory?.items?.length > 0 && !suggestions.length && !analysisResult && (
              <Table
                dataSource={queryHistory.items.map((q: any, i: number) => ({ ...q, key: i }))}
                columns={[
                  { title: '쿼리', dataIndex: 'query_hash', key: 'hash', width: 100, render: (v: string) => <Text code>{v?.substring(0, 8)}</Text> },
                  { title: '테이블', dataIndex: 'tables_accessed', key: 'tables', render: (v: string[]) => v?.map(t => <Tag key={t}>{t}</Tag>) },
                  { title: '실행횟수', dataIndex: 'execution_count', key: 'exec', width: 90 },
                  { title: '총 시간(ms)', dataIndex: 'total_time_ms', key: 'time', width: 100 },
                ]}
                size="small" pagination={{ pageSize: 10 }}
              />
            )}
          </Card>
        )},
        { key: 'export', label: <span><ExportOutlined /> Export</span>, children: (
          <Card size="small" title={<><ExportOutlined /> Export Pipeline</>}>
            {exports.length > 0 ? (
              <Table
                dataSource={exports.map(e => ({ ...e, key: e.id }))}
                columns={[
                  { title: '이름', dataIndex: 'name', key: 'name', render: (v: string, r: any) => (
                    <span><Text strong>{v}</Text>{r.id?.startsWith('exp-builtin') && <Tag color="cyan" style={{ marginLeft: 4 }}>내장</Tag>}</span>
                  )},
                  { title: '대상 시스템', dataIndex: 'target_system', key: 'target', width: 120 },
                  { title: '포맷', dataIndex: 'format', key: 'format', width: 80, render: (v: string) => <Tag>{v}</Tag> },
                  { title: '소스 테이블', dataIndex: 'source_tables', key: 'tables', render: (v: string[]) => v?.map(t => <Tag key={t}>{t}</Tag>) },
                  { title: '비식별화', dataIndex: 'deidentify', key: 'deid', width: 90, render: (v: boolean) => v ? <Tag color="green">Y</Tag> : <Tag>N</Tag> },
                  { title: '스케줄', dataIndex: 'schedule', key: 'sched', width: 110 },
                  { title: '작업', key: 'action', width: 220, render: (_: any, r: any) => (
                    <Space size="small">
                      <Button size="small" icon={<PlayCircleOutlined />} onClick={() => handleExecuteExport(r.id)} loading={loading}>실행</Button>
                      <Button size="small" icon={<EyeOutlined />} onClick={() => handlePreviewExport(r.id)}>미리보기</Button>
                      {!r.id?.startsWith('exp-builtin') && (
                        <Popconfirm title="삭제?" onConfirm={() => handleDeleteExport(r.id)}>
                          <Button size="small" icon={<DeleteOutlined />} danger />
                        </Popconfirm>
                      )}
                    </Space>
                  )},
                ]}
                size="small" pagination={false}
              />
            ) : <Empty description="Export Pipeline 없음" />}
          </Card>
        )},
        { key: 'history', label: <span><FundOutlined /> 실행 이력</span>, children: (
          <Card size="small" title="실행 이력" extra={<Button size="small" icon={<ReloadOutlined />} onClick={loadExecutions}>새로고침</Button>}>
            {executions?.items?.length > 0 ? (
              <Table
                dataSource={executions.items.map((e: any, i: number) => ({ ...e, key: i }))}
                columns={[
                  { title: 'ID', dataIndex: 'exec_id', key: 'id', width: 60 },
                  { title: '파이프라인', dataIndex: 'pipeline_name', key: 'name' },
                  { title: '유형', dataIndex: 'pipeline_type', key: 'type', width: 90, render: (v: string) => <Tag>{v}</Tag> },
                  { title: '상태', dataIndex: 'status', key: 'status', width: 90, render: (v: string) => (
                    <Tag color={v === 'completed' ? 'green' : v === 'running' ? 'blue' : v === 'failed' ? 'red' : 'default'}>{v}</Tag>
                  )},
                  { title: '처리행', dataIndex: 'rows_processed', key: 'rows', width: 100, render: (v: number) => v?.toLocaleString() },
                  { title: '시작', dataIndex: 'started_at', key: 'start', width: 130, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
                ]}
                size="small" pagination={{ pageSize: 10 }}
              />
            ) : <Empty description="실행 이력 없음" />}
          </Card>
        )},
        { key: 'marts', label: <span><FundOutlined /> 마트 설계</span>, children: (
          <Card size="small" title="저장된 마트 설계">
            {martDesigns.length > 0 ? (
              <Table
                dataSource={martDesigns.map((m: any) => ({ ...m, key: m.mart_id || m.name }))}
                columns={[
                  { title: '이름', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                  { title: '설명', dataIndex: 'description', key: 'desc', ellipsis: true },
                  { title: '소스 테이블', dataIndex: 'source_tables', key: 'tables', render: (v: string[]) => v?.map(t => <Tag key={t}>{t}</Tag>) },
                  { title: '예상 행수', dataIndex: 'estimated_rows', key: 'rows', width: 110, render: (v: number) => v?.toLocaleString() },
                  { title: '상태', dataIndex: 'status', key: 'status', width: 90, render: (v: string) => <Tag color={v === 'built' ? 'green' : 'default'}>{v || 'draft'}</Tag> },
                ]}
                size="small" pagination={false}
              />
            ) : <Empty description="저장된 마트 설계 없음" />}
          </Card>
        )},
      ]} />

      {/* Preview Modal */}
      <Modal title="미리보기" open={!!previewModal} onCancel={() => setPreviewModal(null)} footer={null} width={800}>
        {previewModal?.preview?.length > 0 ? (
          <Table
            dataSource={previewModal.preview.map((r: any, i: number) => ({ ...r, key: i }))}
            columns={Object.keys(previewModal.preview[0] || {}).filter(k => k !== 'key').map(k => ({ title: k, dataIndex: k, key: k, ellipsis: true }))}
            size="small" scroll={{ x: true }} pagination={false}
          />
        ) : previewModal?.previews ? (
          Object.entries(previewModal.previews).map(([table, rows]: [string, any]) => (
            <Card key={table} size="small" title={<Text code>{table}</Text>} style={{ marginBottom: 8 }}>
              {rows?.length > 0 ? (
                <Table
                  dataSource={rows.map((r: any, i: number) => ({ ...r, key: i }))}
                  columns={Object.keys(rows[0] || {}).filter(k => k !== 'key').map(k => ({ title: k, dataIndex: k, key: k, ellipsis: true }))}
                  size="small" scroll={{ x: true }} pagination={false}
                />
              ) : <Empty description="데이터 없음" />}
            </Card>
          ))
        ) : <Empty description="미리보기 데이터 없음" />}
      </Modal>
    </div>
  );
};

export default DataPipelineTab;
