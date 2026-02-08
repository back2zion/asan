/**
 * DIT-002: 데이터 마트 생성 및 운영 체계
 * 마트 카탈로그, 스키마 변경, Dimension 관리, 표준 지표, 데이터 흐름, 연결 최적화
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Segmented, Badge, List,
  Tooltip, Descriptions, Drawer, Popconfirm, message, Steps, Timeline,
  Collapse, Progress, Tree,
} from 'antd';
import {
  DatabaseOutlined, AppstoreOutlined, SettingOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined, SearchOutlined,
  CheckCircleOutlined, CloseCircleOutlined, WarningOutlined,
  SyncOutlined, ReloadOutlined, BarChartOutlined, BranchesOutlined,
  ExperimentOutlined, BookOutlined, ThunderboltOutlined,
  RocketOutlined, EyeOutlined, SafetyCertificateOutlined,
  ApartmentOutlined, FundOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text, Title, Paragraph } = Typography;

const API_BASE = '/api/v1/data-mart-ops';

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
async function putJSON(url: string, body?: any) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const ZONE_COLORS: Record<string, string> = {
  silver: '#805AD5', gold: '#D69E2E', mart: '#38A169',
};
const CATEGORY_COLORS: Record<string, string> = {
  clinical: 'blue', operational: 'green', financial: 'gold', quality: 'purple', research: 'cyan',
};
const OPT_COLORS: Record<string, string> = {
  materialized_view: 'blue', index: 'green', partition: 'orange', denormalize: 'purple', cache: 'cyan',
};
const STAGE_COLORS: Record<string, string> = {
  ingest: '#E53E3E', cleanse: '#DD6B20', transform: '#805AD5', enrich: '#D69E2E', serve: '#38A169',
};

const DataMartOps: React.FC = () => {
  const [view, setView] = useState<string>('catalog');
  const [loading, setLoading] = useState(false);
  const [overview, setOverview] = useState<any>(null);

  // -- Catalog state --
  const [marts, setMarts] = useState<any[]>([]);
  const [martModal, setMartModal] = useState(false);
  const [martForm] = Form.useForm();
  const [editMartId, setEditMartId] = useState<number | null>(null);
  const [martDrawer, setMartDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });
  const [dupCheck, setDupCheck] = useState<any[] | null>(null);

  // -- Schema Changes --
  const [schemaChanges, setSchemaChanges] = useState<any[]>([]);

  // -- Dimensions --
  const [dimensions, setDimensions] = useState<any[]>([]);
  const [dimModal, setDimModal] = useState(false);
  const [dimForm] = Form.useForm();
  const [dimHierarchy, setDimHierarchy] = useState<{ open: boolean; data: any }>({ open: false, data: null });

  // -- Metrics --
  const [metrics, setMetrics] = useState<any[]>([]);
  const [metricModal, setMetricModal] = useState(false);
  const [metricForm] = Form.useForm();
  const [catalogSearch, setCatalogSearch] = useState('');
  const [catalogData, setCatalogData] = useState<any>(null);

  // -- Flow Stages --
  const [stages, setStages] = useState<any[]>([]);

  // -- Optimizations --
  const [optimizations, setOptimizations] = useState<any[]>([]);
  const [suggestions, setSuggestions] = useState<any[]>([]);

  const loadOverview = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/overview`);
      setOverview(data);
    } catch { /* ignore */ }
  }, []);

  const loadMarts = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/marts`);
      setMarts(data);
    } catch { message.error('마트 목록 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadSchemaChanges = useCallback(async () => {
    setLoading(true);
    try { setSchemaChanges(await fetchJSON(`${API_BASE}/schema-changes`)); }
    catch { message.error('스키마 변경 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadDimensions = useCallback(async () => {
    setLoading(true);
    try { setDimensions(await fetchJSON(`${API_BASE}/dimensions`)); }
    catch { message.error('Dimension 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadMetrics = useCallback(async () => {
    setLoading(true);
    try { setMetrics(await fetchJSON(`${API_BASE}/metrics`)); }
    catch { message.error('지표 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadStages = useCallback(async () => {
    setLoading(true);
    try { setStages(await fetchJSON(`${API_BASE}/flow-stages`)); }
    catch { message.error('흐름 단계 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadOptimizations = useCallback(async () => {
    setLoading(true);
    try {
      setOptimizations(await fetchJSON(`${API_BASE}/optimizations`));
      setSuggestions((await fetchJSON(`${API_BASE}/optimizations/suggestions`)).suggestions || []);
    } catch { message.error('최적화 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    loadOverview();
    if (view === 'catalog') loadMarts();
    else if (view === 'schema') loadSchemaChanges();
    else if (view === 'dimensions') loadDimensions();
    else if (view === 'metrics') loadMetrics();
    else if (view === 'flow') loadStages();
    else if (view === 'optimization') loadOptimizations();
  }, [view]);

  // -- Mart CRUD --
  const handleSaveMart = async () => {
    try {
      const vals = await martForm.validateFields();
      vals.source_tables = vals.source_tables_str ? vals.source_tables_str.split(',').map((s: string) => s.trim()).filter(Boolean) : [];
      delete vals.source_tables_str;
      if (vals.target_schema_str) {
        try { vals.target_schema = JSON.parse(vals.target_schema_str); } catch { vals.target_schema = null; }
      }
      delete vals.target_schema_str;

      if (editMartId) {
        await putJSON(`${API_BASE}/marts/${editMartId}`, vals);
        message.success('마트 수정 완료');
      } else {
        const result = await postJSON(`${API_BASE}/marts`, vals);
        if (result.duplicates?.length > 0) {
          setDupCheck(result.duplicates);
        }
        message.success('마트 생성 완료');
      }
      setMartModal(false);
      setEditMartId(null);
      martForm.resetFields();
      loadMarts();
    } catch { /* validation error */ }
  };

  const openMartDetail = async (martId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/marts/${martId}`);
      setMartDrawer({ open: true, data });
    } catch { message.error('마트 상세 로드 실패'); }
  };

  // -- Dimension hierarchy --
  const openHierarchy = async (dimId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/dimensions/${dimId}/hierarchy`);
      setDimHierarchy({ open: true, data });
    } catch { message.error('계층 로드 실패'); }
  };

  // -- Catalog search --
  const searchCatalog = async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/metrics/catalog${catalogSearch ? `?search=${encodeURIComponent(catalogSearch)}` : ''}`);
      setCatalogData(data);
    } catch { message.error('카탈로그 검색 실패'); }
  };

  // ── Render sections ──

  const renderOverviewCards = () => {
    if (!overview) return null;
    return (
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={3}><Card size="small"><Statistic title="활성 마트" value={overview.marts} valueStyle={{ color: '#006241' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="총 행 수" value={overview.total_rows} suffix="건" /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="대기 변경" value={overview.pending_schema_changes} valueStyle={{ color: overview.pending_schema_changes > 0 ? '#faad14' : undefined }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="Dimension" value={overview.dimensions} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="표준 지표" value={overview.metrics_total} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="카탈로그" value={overview.metrics_in_catalog} valueStyle={{ color: '#1890ff' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="적용 최적화" value={overview.optimizations_applied} valueStyle={{ color: '#3f8600' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="제안 최적화" value={overview.optimizations_proposed} valueStyle={{ color: '#722ed1' }} /></Card></Col>
      </Row>
    );
  };

  const renderCatalog = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { setEditMartId(null); martForm.resetFields(); setMartModal(true); }}>마트 생성</Button>
        <Button icon={<ReloadOutlined />} onClick={loadMarts}>새로고침</Button>
      </Space>
      <Table
        dataSource={marts} rowKey="mart_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'mart_id', width: 50 },
          { title: '마트명', dataIndex: 'mart_name', width: 160,
            render: (v: string, r: any) => <a onClick={() => openMartDetail(r.mart_id)}>{v}</a> },
          { title: '용도', dataIndex: 'purpose', width: 200, ellipsis: true },
          { title: '영역', dataIndex: 'zone', width: 80, render: (v: string) => <Tag color={ZONE_COLORS[v]}>{v}</Tag> },
          { title: '소스 테이블', dataIndex: 'source_tables', width: 200,
            render: (v: any) => {
              const arr = typeof v === 'string' ? JSON.parse(v) : v;
              return <Space wrap size={[4, 4]}>{(arr || []).map((t: string) => <Tag key={t} style={{ fontSize: 11 }}>{t}</Tag>)}</Space>;
            },
          },
          { title: '행 수', dataIndex: 'row_count', width: 100, render: (v: number) => v ? v.toLocaleString() : '-' },
          { title: '담당', dataIndex: 'owner', width: 90 },
          { title: '갱신 주기', dataIndex: 'refresh_schedule', width: 110 },
          { title: '상태', dataIndex: 'status', width: 70,
            render: (v: string) => <Tag color={v === 'active' ? 'green' : 'default'}>{v}</Tag> },
          { title: '대기 변경', dataIndex: 'pending_changes', width: 80,
            render: (v: number) => v > 0 ? <Badge count={v} /> : <Tag color="green">없음</Tag> },
          { title: '', width: 80, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="상세"><Button size="small" icon={<EyeOutlined />} onClick={() => openMartDetail(r.mart_id)} /></Tooltip>
              <Popconfirm title="아카이브?" onConfirm={async () => { await deleteJSON(`${API_BASE}/marts/${r.mart_id}`); loadMarts(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />

      {/* Duplicate check result */}
      {dupCheck && dupCheck.length > 0 && (
        <Card size="small" style={{ marginTop: 12, borderColor: '#faad14' }} title={<><WarningOutlined style={{ color: '#faad14' }} /> 유사 마트 감지</>}>
          <List size="small" dataSource={dupCheck} renderItem={(d: any) => (
            <List.Item>
              <Space>
                <Tag color="orange">{d.similarity}% 유사</Tag>
                <Text strong>{d.mart_name}</Text>
                {d.overlap_tables && <Text type="secondary">겹치는 테이블: {d.overlap_tables.join(', ')}</Text>}
                {d.reason === 'schema_hash_match' && <Tag color="red">스키마 동일</Tag>}
              </Space>
            </List.Item>
          )} />
          <Button size="small" onClick={() => setDupCheck(null)} style={{ marginTop: 8 }}>닫기</Button>
        </Card>
      )}

      {/* Mart Modal */}
      <Modal title={editMartId ? '마트 수정' : '마트 생성'} open={martModal} onOk={handleSaveMart}
        onCancel={() => { setMartModal(false); setEditMartId(null); }} width={600}>
        <Form form={martForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="mart_name" label="마트명" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="zone" label="영역" initialValue="gold">
              <Select options={[{ value: 'silver', label: 'Silver' }, { value: 'gold', label: 'Gold' }, { value: 'mart', label: 'Mart' }]} />
            </Form.Item></Col>
          </Row>
          <Form.Item name="purpose" label="용도" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="source_tables_str" label="소스 테이블 (쉼표 구분)"><Input placeholder="person,visit_occurrence,condition_occurrence" /></Form.Item>
          <Row gutter={16}>
            <Col span={8}><Form.Item name="owner" label="담당"><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="refresh_schedule" label="갱신 주기"><Input placeholder="0 2 * * *" /></Form.Item></Col>
            <Col span={8}><Form.Item name="retention_days" label="보관(일)" initialValue={365}><Input type="number" /></Form.Item></Col>
          </Row>
          <Form.Item name="target_schema_str" label="스키마 JSON (선택)"><Input.TextArea rows={2} placeholder='{"columns":["col1","col2"]}' /></Form.Item>
        </Form>
      </Modal>

      {/* Mart Detail Drawer */}
      <Drawer title={martDrawer.data?.mart_name} open={martDrawer.open} width={640}
        onClose={() => setMartDrawer({ open: false, data: null })}>
        {martDrawer.data && (<>
          <Descriptions column={2} size="small" bordered>
            <Descriptions.Item label="영역"><Tag color={ZONE_COLORS[martDrawer.data.zone]}>{martDrawer.data.zone}</Tag></Descriptions.Item>
            <Descriptions.Item label="상태"><Tag color={martDrawer.data.status === 'active' ? 'green' : 'default'}>{martDrawer.data.status}</Tag></Descriptions.Item>
            <Descriptions.Item label="용도" span={2}>{martDrawer.data.purpose}</Descriptions.Item>
            <Descriptions.Item label="담당">{martDrawer.data.owner}</Descriptions.Item>
            <Descriptions.Item label="행 수">{martDrawer.data.row_count?.toLocaleString()}</Descriptions.Item>
            <Descriptions.Item label="갱신 주기">{martDrawer.data.refresh_schedule}</Descriptions.Item>
            <Descriptions.Item label="보관">{martDrawer.data.retention_days}일</Descriptions.Item>
            <Descriptions.Item label="소스 테이블" span={2}>
              <Space wrap>{(typeof martDrawer.data.source_tables === 'string' ? JSON.parse(martDrawer.data.source_tables) : martDrawer.data.source_tables || []).map((t: string) => <Tag key={t}>{t}</Tag>)}</Space>
            </Descriptions.Item>
          </Descriptions>
          {martDrawer.data.schema_changes?.length > 0 && (
            <Card size="small" title="스키마 변경 이력" style={{ marginTop: 16 }}>
              <Timeline items={martDrawer.data.schema_changes.map((c: any) => ({
                color: c.status === 'applied' ? 'green' : c.status === 'pending' ? 'orange' : 'red',
                children: <><Tag color={c.status === 'applied' ? 'green' : 'orange'}>{c.status}</Tag> {c.change_type} — {c.impact_summary} <Text type="secondary">({dayjs(c.created_at).format('YYYY-MM-DD')})</Text></>,
              }))} />
            </Card>
          )}
          {martDrawer.data.optimizations?.length > 0 && (
            <Card size="small" title="적용된 최적화" style={{ marginTop: 12 }}>
              {martDrawer.data.optimizations.map((o: any) => (
                <div key={o.opt_id} style={{ marginBottom: 8 }}>
                  <Tag color={OPT_COLORS[o.opt_type]}>{o.opt_type}</Tag>
                  <Tag color={o.status === 'applied' ? 'green' : 'default'}>{o.status}</Tag>
                  <Text>{o.description}</Text>
                </div>
              ))}
            </Card>
          )}
          {martDrawer.data.metrics?.length > 0 && (
            <Card size="small" title="관련 표준 지표" style={{ marginTop: 12 }}>
              {martDrawer.data.metrics.map((m: any) => (
                <div key={m.metric_id} style={{ marginBottom: 6 }}>
                  <Tag color={CATEGORY_COLORS[m.category]}>{m.category}</Tag>
                  <Text strong>{m.logical_name}</Text> <Text type="secondary">({m.formula})</Text>
                </div>
              ))}
            </Card>
          )}
        </>)}
      </Drawer>
    </Spin>
  );

  const renderSchemaChanges = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadSchemaChanges} style={{ marginBottom: 16 }}>새로고침</Button>
      <Table
        dataSource={schemaChanges} rowKey="change_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'change_id', width: 50 },
          { title: '마트', dataIndex: 'mart_name', width: 140 },
          { title: '유형', dataIndex: 'change_type', width: 100,
            render: (v: string) => <Tag color={v === 'column_add' ? 'blue' : v === 'column_remove' ? 'red' : 'orange'}>{v}</Tag> },
          { title: '추가 컬럼', dataIndex: 'columns_added', width: 150,
            render: (v: any) => { const arr = typeof v === 'string' ? JSON.parse(v) : v; return arr?.length ? <Space wrap>{arr.map((c: string) => <Tag key={c} color="green">{c}</Tag>)}</Space> : '-'; } },
          { title: '삭제 컬럼', dataIndex: 'columns_removed', width: 150,
            render: (v: any) => { const arr = typeof v === 'string' ? JSON.parse(v) : v; return arr?.length ? <Space wrap>{arr.map((c: string) => <Tag key={c} color="red">{c}</Tag>)}</Space> : '-'; } },
          { title: '변경 컬럼', dataIndex: 'columns_modified', width: 150,
            render: (v: any) => { const arr = typeof v === 'string' ? JSON.parse(v) : v; return arr?.length ? <Space wrap>{arr.map((c: any, i: number) => <Tag key={i} color="orange">{c.column}</Tag>)}</Space> : '-'; } },
          { title: '영향', dataIndex: 'impact_summary', ellipsis: true },
          { title: '상태', dataIndex: 'status', width: 90,
            render: (v: string) => <Tag color={v === 'applied' ? 'green' : v === 'pending' ? 'orange' : 'red'}>{v}</Tag> },
          { title: '일시', dataIndex: 'created_at', width: 100, render: (v: string) => dayjs(v).format('MM-DD HH:mm') },
          { title: '', width: 120, render: (_: any, r: any) => r.status === 'pending' ? (
            <Space size={4}>
              <Popconfirm title="적용?" onConfirm={async () => { await putJSON(`${API_BASE}/schema-changes/${r.change_id}/apply`); loadSchemaChanges(); }}>
                <Button size="small" type="primary">적용</Button>
              </Popconfirm>
              <Popconfirm title="롤백?" onConfirm={async () => { await putJSON(`${API_BASE}/schema-changes/${r.change_id}/rollback`); loadSchemaChanges(); }}>
                <Button size="small" danger>롤백</Button>
              </Popconfirm>
            </Space>
          ) : null },
        ]}
      />
    </Spin>
  );

  const renderDimensions = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { dimForm.resetFields(); setDimModal(true); }}>Dimension 추가</Button>
        <Button icon={<ReloadOutlined />} onClick={loadDimensions}>새로고침</Button>
      </Space>
      <Table
        dataSource={dimensions} rowKey="dim_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'dim_id', width: 50 },
          { title: '물리명', dataIndex: 'dimension_name', width: 130 },
          { title: '논리명', dataIndex: 'logical_name', width: 130 },
          { title: '계층 수', dataIndex: 'hierarchy_levels', width: 80,
            render: (v: any) => (typeof v === 'string' ? JSON.parse(v) : v)?.length || 0 },
          { title: '계층 레벨', dataIndex: 'hierarchy_levels', width: 250,
            render: (v: any) => {
              const arr = typeof v === 'string' ? JSON.parse(v) : v;
              return <Space wrap size={[4, 4]}>{(arr || []).map((l: any, i: number) => <Tag key={i} color="blue">{l.level}</Tag>)}</Space>;
            },
          },
          { title: '속성 수', dataIndex: 'attributes', width: 80,
            render: (v: any) => (typeof v === 'string' ? JSON.parse(v) : v)?.length || 0 },
          { title: '연결 마트', dataIndex: 'mart_ids', width: 100,
            render: (v: any) => { const arr = typeof v === 'string' ? JSON.parse(v) : v; return <Tag>{arr?.length || 0}개</Tag>; } },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: '', width: 100, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="계층 보기"><Button size="small" icon={<ApartmentOutlined />} onClick={() => openHierarchy(r.dim_id)} /></Tooltip>
              <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/dimensions/${r.dim_id}`); loadDimensions(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />

      {/* Dimension Modal */}
      <Modal title="Dimension 추가" open={dimModal} onCancel={() => setDimModal(false)}
        onOk={async () => {
          try {
            const vals = await dimForm.validateFields();
            vals.hierarchy_levels = vals.hierarchy_str ? JSON.parse(vals.hierarchy_str) : [];
            vals.attributes = vals.attributes_str ? JSON.parse(vals.attributes_str) : [];
            vals.mart_ids = vals.mart_ids_str ? vals.mart_ids_str.split(',').map((s: string) => parseInt(s.trim())).filter(Boolean) : [];
            delete vals.hierarchy_str; delete vals.attributes_str; delete vals.mart_ids_str;
            await postJSON(`${API_BASE}/dimensions`, vals);
            message.success('Dimension 생성 완료');
            setDimModal(false);
            loadDimensions();
          } catch { /* validation */ }
        }} width={560}>
        <Form form={dimForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="dimension_name" label="물리명" rules={[{ required: true }]}><Input placeholder="dim_time" /></Form.Item></Col>
            <Col span={12}><Form.Item name="logical_name" label="논리명" rules={[{ required: true }]}><Input placeholder="시간 Dimension" /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="hierarchy_str" label='계층 JSON (예: [{"level":"year","key":"year"}])'>
            <Input.TextArea rows={2} placeholder='[{"level":"year","key":"year"},{"level":"month","key":"month"}]' />
          </Form.Item>
          <Form.Item name="attributes_str" label='속성 JSON (예: [{"name":"is_weekend","type":"boolean"}])'>
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="mart_ids_str" label="연결 마트 ID (쉼표 구분)"><Input placeholder="1,2,3" /></Form.Item>
        </Form>
      </Modal>

      {/* Hierarchy Drawer */}
      <Drawer title="Dimension 계층 구조" open={dimHierarchy.open} width={480}
        onClose={() => setDimHierarchy({ open: false, data: null })}>
        {dimHierarchy.data && (<>
          <Descriptions column={1} size="small" bordered style={{ marginBottom: 16 }}>
            <Descriptions.Item label="물리명">{dimHierarchy.data.dimension?.dimension_name}</Descriptions.Item>
            <Descriptions.Item label="논리명">{dimHierarchy.data.dimension?.logical_name}</Descriptions.Item>
            <Descriptions.Item label="설명">{dimHierarchy.data.dimension?.description}</Descriptions.Item>
          </Descriptions>
          <Card size="small" title="계층 트리">
            <Tree
              showLine
              defaultExpandAll
              treeData={[buildTreeData(dimHierarchy.data.tree)]}
            />
          </Card>
        </>)}
      </Drawer>
    </Spin>
  );

  const renderMetrics = () => (
    <Spin spinning={loading}>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col flex="auto">
          <Space>
            <Button icon={<PlusOutlined />} type="primary" onClick={() => { metricForm.resetFields(); setMetricModal(true); }}>지표 추가</Button>
            <Button icon={<ReloadOutlined />} onClick={loadMetrics}>새로고침</Button>
          </Space>
        </Col>
        <Col>
          <Space>
            <Input.Search placeholder="카탈로그 검색" value={catalogSearch} onChange={e => setCatalogSearch(e.target.value)}
              onSearch={searchCatalog} enterButton={<><BookOutlined /> 카탈로그</>} style={{ width: 320 }} />
          </Space>
        </Col>
      </Row>

      {catalogData && (
        <Card size="small" style={{ marginBottom: 16, borderColor: '#1890ff' }}
          title={<><BookOutlined /> 카탈로그 검색 결과 ({catalogData.total}건)</>}
          extra={<Button size="small" onClick={() => setCatalogData(null)}>닫기</Button>}>
          {Object.entries(catalogData.categories || {}).map(([cat, items]: [string, any]) => (
            <div key={cat} style={{ marginBottom: 12 }}>
              <Tag color={CATEGORY_COLORS[cat]}>{cat}</Tag>
              <span style={{ fontWeight: 600 }}>{(items as any[]).length}건</span>
              <Table size="small" dataSource={items as any[]} rowKey="metric_id" pagination={false} style={{ marginTop: 4 }}
                columns={[
                  { title: '지표명', dataIndex: 'logical_name', width: 120 },
                  { title: '산식', dataIndex: 'formula', ellipsis: true },
                  { title: '단위', dataIndex: 'unit', width: 60 },
                  { title: '마트', dataIndex: 'mart_name', width: 120 },
                ]}
              />
            </div>
          ))}
        </Card>
      )}

      <Table
        dataSource={metrics} rowKey="metric_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'metric_id', width: 50 },
          { title: '물리명', dataIndex: 'metric_name', width: 140 },
          { title: '논리명', dataIndex: 'logical_name', width: 120 },
          { title: '산식', dataIndex: 'formula', ellipsis: true },
          { title: '단위', dataIndex: 'unit', width: 60 },
          { title: '분류', dataIndex: 'category', width: 90,
            render: (v: string) => <Tag color={CATEGORY_COLORS[v]}>{v}</Tag> },
          { title: '마트', dataIndex: 'mart_name', width: 120 },
          { title: '카탈로그', dataIndex: 'catalog_visible', width: 80,
            render: (v: boolean) => v ? <Tag color="blue">공개</Tag> : <Tag>비공개</Tag> },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: '', width: 60, render: (_: any, r: any) => (
            <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/metrics/${r.metric_id}`); loadMetrics(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />

      {/* Metric Modal */}
      <Modal title="표준 지표 추가" open={metricModal} onCancel={() => setMetricModal(false)}
        onOk={async () => {
          try {
            const vals = await metricForm.validateFields();
            vals.dimension_ids = vals.dimension_ids_str ? vals.dimension_ids_str.split(',').map((s: string) => parseInt(s.trim())).filter(Boolean) : [];
            delete vals.dimension_ids_str;
            await postJSON(`${API_BASE}/metrics`, vals);
            message.success('지표 생성 완료');
            setMetricModal(false);
            loadMetrics();
          } catch { /* validation */ }
        }} width={560}>
        <Form form={metricForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="metric_name" label="물리명" rules={[{ required: true }]}><Input placeholder="patient_count" /></Form.Item></Col>
            <Col span={12}><Form.Item name="logical_name" label="논리명" rules={[{ required: true }]}><Input placeholder="환자 수" /></Form.Item></Col>
          </Row>
          <Form.Item name="formula" label="산식" rules={[{ required: true }]}><Input placeholder="COUNT(DISTINCT person_id)" /></Form.Item>
          <Row gutter={16}>
            <Col span={8}><Form.Item name="unit" label="단위"><Input placeholder="명" /></Form.Item></Col>
            <Col span={8}><Form.Item name="category" label="분류" initialValue="clinical">
              <Select options={['clinical', 'operational', 'financial', 'quality', 'research'].map(c => ({ value: c, label: c }))} />
            </Form.Item></Col>
            <Col span={8}><Form.Item name="mart_id" label="마트 ID"><Input type="number" /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="dimension_ids_str" label="연결 Dimension ID (쉼표 구분)"><Input placeholder="1,2" /></Form.Item>
          <Form.Item name="catalog_visible" label="카탈로그 공개" initialValue={true}>
            <Select options={[{ value: true, label: '공개' }, { value: false, label: '비공개' }]} />
          </Form.Item>
        </Form>
      </Modal>
    </Spin>
  );

  const renderFlowStages = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadStages} style={{ marginBottom: 16 }}>새로고침</Button>

      {/* Pipeline visualization */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 0, overflowX: 'auto', padding: '16px 0' }}>
          {stages.map((s, i) => (
            <React.Fragment key={s.stage_id}>
              <div style={{
                minWidth: 200, padding: 16, borderRadius: 12, textAlign: 'center',
                background: `${STAGE_COLORS[s.stage_type]}15`,
                border: `2px solid ${STAGE_COLORS[s.stage_type]}`,
              }}>
                <div style={{ fontSize: 20, marginBottom: 4 }}>
                  {s.stage_type === 'ingest' && <DatabaseOutlined />}
                  {s.stage_type === 'cleanse' && <SafetyCertificateOutlined />}
                  {s.stage_type === 'transform' && <SyncOutlined />}
                  {s.stage_type === 'enrich' && <ExperimentOutlined />}
                  {s.stage_type === 'serve' && <RocketOutlined />}
                </div>
                <div style={{ fontWeight: 700, fontSize: 14, color: STAGE_COLORS[s.stage_type] }}>{s.stage_name}</div>
                <Tag color={STAGE_COLORS[s.stage_type]} style={{ marginTop: 4 }}>{s.stage_type}</Tag>
                <div style={{ marginTop: 8, fontSize: 11, color: '#666' }}>
                  <div>{s.storage_type} / {s.file_format}</div>
                </div>
              </div>
              {i < stages.length - 1 && (
                <div style={{ fontSize: 24, color: '#999', padding: '0 8px' }}>→</div>
              )}
            </React.Fragment>
          ))}
        </div>
      </Card>

      {/* Stage details */}
      <Collapse
        items={stages.map((s: any) => {
          const rules = typeof s.processing_rules === 'string' ? JSON.parse(s.processing_rules) : s.processing_rules;
          const meta = typeof s.meta_config === 'string' ? JSON.parse(s.meta_config) : s.meta_config;
          return {
            key: s.stage_id,
            label: <Space><Tag color={STAGE_COLORS[s.stage_type]}>{s.stage_type}</Tag><Text strong>{s.stage_name}</Text></Space>,
            children: (
              <Row gutter={16}>
                <Col span={12}>
                  <Descriptions column={1} size="small" bordered>
                    <Descriptions.Item label="저장소">{s.storage_type}</Descriptions.Item>
                    <Descriptions.Item label="파일 포맷">{s.file_format}</Descriptions.Item>
                    <Descriptions.Item label="설명">{s.description}</Descriptions.Item>
                  </Descriptions>
                </Col>
                <Col span={6}>
                  <Card size="small" title="처리 규칙">
                    {rules ? Object.entries(rules).map(([k, v]) => (
                      <div key={k}><Text code>{k}</Text>: <Text>{String(v)}</Text></div>
                    )) : <Empty description="없음" />}
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small" title="메타 구성">
                    {meta ? Object.entries(meta).map(([k, v]) => (
                      <div key={k}><Text code>{k}</Text>: <Text>{String(v)}</Text></div>
                    )) : <Empty description="없음" />}
                  </Card>
                </Col>
              </Row>
            ),
          };
        })}
      />

      {/* Flow description */}
      <Card size="small" style={{ marginTop: 16 }} title="비정형→정형 / 비표준→표준 변환 Flow">
        <Steps
          direction="vertical" size="small"
          items={[
            { title: '원천 수집 (Source)', description: '원본 데이터 수집 — EHR/EMR, Lab, 병리, 영상, 수술기록. 원본 형태 그대로 보존.', status: 'process' },
            { title: '정제/클렌징 (Bronze→Silver)', description: '비표준 용어→표준 용어 매핑 (SNOMED CT, LOINC). 결측치 처리, 중복 제거, 타입 정규화.', status: 'process' },
            { title: '변환/표준화 (Silver→Gold)', description: '비정형→정형 구조화: 병리보고서 NLP 추출, 영상 DICOM 메타데이터 구조화, 수술기록 엔티티 추출. OMOP CDM 매핑.', status: 'process' },
            { title: '통합/강화 (Gold)', description: 'Dimension 결합, 표준 지표 산출, 집계 테이블 생성. 데이터 마트 구성.', status: 'process' },
            { title: '서빙/제공 (Mart)', description: '사용자 서비스: MV/인덱스 최적화, 카탈로그 등록, 접근 제어, BI/분석 도구 연동.', status: 'process' },
          ]}
        />
      </Card>
    </Spin>
  );

  const renderOptimization = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadOptimizations} style={{ marginBottom: 16 }}>새로고침</Button>

      {/* Suggestions */}
      {suggestions.length > 0 && (
        <Card size="small" style={{ marginBottom: 16, borderColor: '#722ed1' }}
          title={<><ThunderboltOutlined style={{ color: '#722ed1' }} /> 최적화 제안 ({suggestions.length}건)</>}>
          <Table size="small" dataSource={suggestions} rowKey={(_, i) => String(i)} pagination={false}
            columns={[
              { title: '마트', dataIndex: 'mart_name', width: 140 },
              { title: '유형', dataIndex: 'opt_type', width: 120, render: (v: string) => <Tag color={OPT_COLORS[v]}>{v}</Tag> },
              { title: '사유', dataIndex: 'reason', ellipsis: true },
              { title: '우선순위', dataIndex: 'priority', width: 80,
                render: (v: string) => <Tag color={v === 'high' ? 'red' : v === 'medium' ? 'orange' : 'default'}>{v}</Tag> },
            ]}
          />
        </Card>
      )}

      {/* Applied / proposed */}
      <Table
        dataSource={optimizations} rowKey="opt_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'opt_id', width: 50 },
          { title: '마트', dataIndex: 'mart_name', width: 140 },
          { title: '유형', dataIndex: 'opt_type', width: 120, render: (v: string) => <Tag color={OPT_COLORS[v]}>{v}</Tag> },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: '상태', dataIndex: 'status', width: 80,
            render: (v: string) => <Tag color={v === 'applied' ? 'green' : v === 'proposed' ? 'purple' : 'default'}>{v}</Tag> },
          { title: '적용일', dataIndex: 'applied_at', width: 100, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
          { title: '', width: 120, render: (_: any, r: any) => (
            <Space size={4}>
              {r.status === 'proposed' && (
                <Popconfirm title="적용?" onConfirm={async () => { await putJSON(`${API_BASE}/optimizations/${r.opt_id}/apply`); loadOptimizations(); }}>
                  <Button size="small" type="primary">적용</Button>
                </Popconfirm>
              )}
              <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/optimizations/${r.opt_id}`); loadOptimizations(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />
    </Spin>
  );

  return (
    <div>
      {renderOverviewCards()}
      <Segmented
        block
        value={view}
        onChange={(v) => setView(v as string)}
        options={[
          { label: '마트 카탈로그', value: 'catalog', icon: <AppstoreOutlined /> },
          { label: '스키마 변경', value: 'schema', icon: <BranchesOutlined /> },
          { label: 'Dimension 관리', value: 'dimensions', icon: <ApartmentOutlined /> },
          { label: '표준 지표', value: 'metrics', icon: <FundOutlined /> },
          { label: '데이터 흐름', value: 'flow', icon: <RocketOutlined /> },
          { label: '연결 최적화', value: 'optimization', icon: <ThunderboltOutlined /> },
        ]}
        style={{ marginBottom: 16 }}
      />
      {view === 'catalog' && renderCatalog()}
      {view === 'schema' && renderSchemaChanges()}
      {view === 'dimensions' && renderDimensions()}
      {view === 'metrics' && renderMetrics()}
      {view === 'flow' && renderFlowStages()}
      {view === 'optimization' && renderOptimization()}
    </div>
  );
};

function buildTreeData(node: any): any {
  if (!node) return { title: '-', key: '-' };
  return {
    title: node.name + (node.key ? ` (${node.key})` : ''),
    key: node.name + (node.key || ''),
    children: (node.children || []).map(buildTreeData),
  };
}

export default DataMartOps;
