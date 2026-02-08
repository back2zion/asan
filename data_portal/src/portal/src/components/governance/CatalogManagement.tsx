/**
 * DGR-003: 데이터 카탈로그 관리
 * 종합 카탈로그, 오너쉽/공유, 비테이블 리소스, 작업 이력, 메타 동기화, 연관 데이터
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Segmented, Badge, List,
  Tooltip, Descriptions, Drawer, Popconfirm, Timeline,
  Collapse, Progress, App,
} from 'antd';
import {
  DatabaseOutlined, ApiOutlined, FileOutlined, CloudOutlined,
  PlusOutlined, DeleteOutlined, SearchOutlined, ReloadOutlined,
  EyeOutlined, SwapOutlined, HistoryOutlined, SyncOutlined,
  TeamOutlined, LockOutlined, ShareAltOutlined, AppstoreOutlined,
  CodeOutlined, LinkOutlined, UserOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API = '/api/v1/data-catalog';

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

const TYPE_ICONS: Record<string, React.ReactNode> = {
  table: <DatabaseOutlined />, view: <CodeOutlined />, api: <ApiOutlined />,
  sql_query: <CodeOutlined />, json_schema: <FileOutlined />, xml_schema: <FileOutlined />,
  file: <FileOutlined />, stream: <CloudOutlined />,
};
const TYPE_COLORS: Record<string, string> = {
  table: 'green', view: 'purple', api: 'blue', sql_query: 'orange',
  json_schema: 'gold', xml_schema: 'red', file: 'cyan', stream: 'magenta',
};
const ACCESS_COLORS: Record<string, string> = {
  public: 'green', internal: 'blue', restricted: 'orange', confidential: 'red',
};
const OWNER_COLORS: Record<string, string> = {
  primary: 'green', steward: 'blue', consumer: 'orange', shared: 'purple',
};
const ACTION_COLORS: Record<string, string> = {
  create: 'green', update: 'blue', schema_change: 'orange', quality_check: 'cyan',
  etl_run: 'purple', cohort_extract: 'magenta', query: 'default', export: 'gold', share: 'lime', archive: 'red',
};
const RELATION_COLORS: Record<string, string> = {
  feeds_into: 'green', derives_from: 'orange', references: 'blue',
  same_entity: 'purple', aggregates: 'gold', supplements: 'cyan',
};
const RESOURCE_COLORS: Record<string, string> = {
  rest_api: 'blue', graphql: 'purple', sql_view: 'orange', stored_proc: 'red',
  json_file: 'gold', xml_file: 'volcano', csv_file: 'green', parquet_file: 'cyan', stream_topic: 'magenta',
};

const CatalogManagement: React.FC = () => {
  const { message } = App.useApp();
  const [view, setView] = useState<string>('catalog');
  const [loading, setLoading] = useState(false);
  const [overview, setOverview] = useState<any>(null);

  const [entries, setEntries] = useState<any[]>([]);
  const [entryDrawer, setEntryDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });
  const [searchText, setSearchText] = useState('');
  const [filterType, setFilterType] = useState<string | undefined>();

  const [ownerships, setOwnerships] = useState<any[]>([]);
  const [ownerMatrix, setOwnerMatrix] = useState<any[]>([]);
  const [ownerModal, setOwnerModal] = useState(false);
  const [ownerForm] = Form.useForm();

  const [resources, setResources] = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [syncLogs, setSyncLogs] = useState<any[]>([]);
  const [relations, setRelations] = useState<any[]>([]);

  const loadOverview = useCallback(async () => {
    try { setOverview(await fetchJSON(`${API}/overview`)); } catch { /* */ }
  }, []);

  const loadEntries = useCallback(async () => {
    setLoading(true);
    try {
      let url = `${API}/entries?`;
      if (searchText) url += `search=${encodeURIComponent(searchText)}&`;
      if (filterType) url += `entry_type=${filterType}&`;
      setEntries(await fetchJSON(url));
    } catch { message.error('카탈로그 로드 실패'); }
    finally { setLoading(false); }
  }, [searchText, filterType]);

  const loadOwnerships = useCallback(async () => {
    setLoading(true);
    try {
      setOwnerships(await fetchJSON(`${API}/ownerships`));
      setOwnerMatrix(await fetchJSON(`${API}/ownerships/matrix`));
    } catch { message.error('오너쉽 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadResources = useCallback(async () => {
    setLoading(true);
    try { setResources(await fetchJSON(`${API}/resources`)); }
    catch { message.error('리소스 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadHistory = useCallback(async () => {
    setLoading(true);
    try { setHistory(await fetchJSON(`${API}/history?limit=100`)); }
    catch { message.error('이력 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadSync = useCallback(async () => {
    setLoading(true);
    try { setSyncLogs(await fetchJSON(`${API}/sync/logs`)); }
    catch { message.error('동기화 로그 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadRelations = useCallback(async () => {
    setLoading(true);
    try { setRelations(await fetchJSON(`${API}/relations`)); }
    catch { message.error('연관 데이터 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    loadOverview();
    if (view === 'catalog') loadEntries();
    else if (view === 'ownership') loadOwnerships();
    else if (view === 'resources') loadResources();
    else if (view === 'history') loadHistory();
    else if (view === 'sync') loadSync();
    else if (view === 'relations') loadRelations();
  }, [view]);

  useEffect(() => { if (view === 'catalog') loadEntries(); }, [searchText, filterType]);

  const openDetail = async (entryId: number) => {
    try {
      const data = await fetchJSON(`${API}/entries/${entryId}`);
      setEntryDrawer({ open: true, data });
    } catch { message.error('상세 로드 실패'); }
  };

  // ── Overview Cards ──
  const renderOverviewCards = () => {
    if (!overview) return null;
    const bt = overview.by_type || {};
    return (
      <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
        <Col span={3}><Card size="small"><Statistic title="전체" value={overview.total_entries} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="테이블" value={bt.table || 0} valueStyle={{ color: '#006241' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="API/뷰/기타" value={(bt.api || 0) + (bt.view || 0) + (bt.file || 0) + (bt.stream || 0) + (bt.json_schema || 0)} valueStyle={{ color: '#1890ff' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="소유 부서" value={overview.owner_depts} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="공유" value={overview.shared_count} valueStyle={{ color: '#722ed1' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="연관관계" value={overview.relations} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="리소스" value={overview.resources} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="작업이력" value={overview.history_records} /></Card></Col>
      </Row>
    );
  };

  // ══════════════════════════════════════════
  //  1. Catalog
  // ══════════════════════════════════════════
  const renderCatalog = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }} wrap>
        <Input.Search placeholder="검색 (이름, 설명)" value={searchText}
          onChange={e => setSearchText(e.target.value)} onSearch={loadEntries} style={{ width: 280 }} allowClear />
        <Select placeholder="유형" allowClear value={filterType} onChange={setFilterType} style={{ width: 120 }}
          options={['table', 'view', 'api', 'sql_query', 'json_schema', 'file', 'stream'].map(v => ({ value: v, label: v }))} />
        <Button icon={<ReloadOutlined />} onClick={loadEntries}>새로고침</Button>
      </Space>

      <Table dataSource={entries} rowKey="entry_id" size="small" pagination={{ pageSize: 15 }}
        columns={[
          { title: 'ID', dataIndex: 'entry_id', width: 45 },
          { title: '유형', dataIndex: 'entry_type', width: 90,
            render: (v: string) => <Tag icon={TYPE_ICONS[v]} color={TYPE_COLORS[v]}>{v}</Tag> },
          { title: '카탈로그명', dataIndex: 'business_name', width: 180,
            render: (v: string, r: any) => <a onClick={() => openDetail(r.entry_id)}>{v}</a> },
          { title: '물리명', dataIndex: 'physical_name', width: 140,
            render: (v: string) => v ? <Text code>{v}</Text> : '-' },
          { title: '도메인', dataIndex: 'domain', width: 70, render: (v: string) => <Tag>{v}</Tag> },
          { title: '출처', dataIndex: 'source_system', width: 90 },
          { title: '소유 부서', dataIndex: 'owner_dept', width: 100 },
          { title: '접근', dataIndex: 'access_level', width: 80,
            render: (v: string) => <Tag color={ACCESS_COLORS[v]}>{v}</Tag> },
          { title: '행 수', dataIndex: 'row_count', width: 100,
            render: (v: number) => v > 0 ? v.toLocaleString() : '-' },
          { title: '품질', dataIndex: 'quality_score', width: 65,
            render: (v: number) => v ? <Tag color={v >= 90 ? 'green' : v >= 80 ? 'orange' : 'red'}>{v}%</Tag> : '-' },
          { title: '공유', dataIndex: 'share_count', width: 55,
            render: (v: number) => v > 0 ? <Badge count={v} /> : '-' },
          { title: '연관', dataIndex: 'relation_count', width: 55, render: (v: number) => v || '-' },
          { title: '', width: 60, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="상세"><Button size="small" icon={<EyeOutlined />} onClick={() => openDetail(r.entry_id)} /></Tooltip>
            </Space>
          )},
        ]}
      />

      {/* Detail Drawer */}
      <Drawer title={entryDrawer.data?.business_name} open={entryDrawer.open} width={680}
        onClose={() => setEntryDrawer({ open: false, data: null })}>
        {entryDrawer.data && (<>
          <Descriptions column={2} size="small" bordered>
            <Descriptions.Item label="유형"><Tag icon={TYPE_ICONS[entryDrawer.data.entry_type]} color={TYPE_COLORS[entryDrawer.data.entry_type]}>{entryDrawer.data.entry_type}</Tag></Descriptions.Item>
            <Descriptions.Item label="접근"><Tag color={ACCESS_COLORS[entryDrawer.data.access_level]}>{entryDrawer.data.access_level}</Tag></Descriptions.Item>
            <Descriptions.Item label="물리명">{entryDrawer.data.physical_name || '-'}</Descriptions.Item>
            <Descriptions.Item label="도메인"><Tag>{entryDrawer.data.domain}</Tag></Descriptions.Item>
            <Descriptions.Item label="출처">{entryDrawer.data.source_system}</Descriptions.Item>
            <Descriptions.Item label="행 수">{entryDrawer.data.row_count?.toLocaleString() || '-'}</Descriptions.Item>
            <Descriptions.Item label="소유 부서">{entryDrawer.data.owner_dept}</Descriptions.Item>
            <Descriptions.Item label="담당자">{entryDrawer.data.owner_person}</Descriptions.Item>
            <Descriptions.Item label="품질 점수" span={2}>
              {entryDrawer.data.quality_score ? <Progress percent={entryDrawer.data.quality_score} size="small" /> : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="태그" span={2}>
              <Space wrap>{(typeof entryDrawer.data.tags === 'string' ? JSON.parse(entryDrawer.data.tags) : entryDrawer.data.tags || []).map((t: string) => <Tag key={t}>{t}</Tag>)}</Space>
            </Descriptions.Item>
          </Descriptions>

          {entryDrawer.data.description && (
            <Card size="small" title="설명" style={{ marginTop: 12 }}><Text>{entryDrawer.data.description}</Text></Card>
          )}
          {entryDrawer.data.usage_guide && (
            <Card size="small" title="사용 가이드" style={{ marginTop: 12 }}><Text>{entryDrawer.data.usage_guide}</Text></Card>
          )}
          {entryDrawer.data.policy_notes && (
            <Card size="small" title="정책/규정" style={{ marginTop: 12 }}><Text>{entryDrawer.data.policy_notes}</Text></Card>
          )}

          {/* Ownership */}
          {entryDrawer.data.ownerships?.length > 0 && (
            <Card size="small" title={<><TeamOutlined /> 오너쉽/공유 ({entryDrawer.data.ownerships.length})</>} style={{ marginTop: 12 }}>
              {entryDrawer.data.ownerships.map((o: any) => (
                <div key={o.ownership_id} style={{ marginBottom: 6 }}>
                  <Tag color={OWNER_COLORS[o.owner_type]}>{o.owner_type}</Tag>
                  <Text strong>{o.dept}</Text> {o.person && <Text type="secondary">({o.person})</Text>}
                  <Tag style={{ marginLeft: 8 }}>{o.access_scope}</Tag>
                  {o.share_purpose && <Text type="secondary" style={{ marginLeft: 8 }}>— {o.share_purpose}</Text>}
                </div>
              ))}
            </Card>
          )}

          {/* Relations */}
          {entryDrawer.data.relations?.length > 0 && (
            <Card size="small" title={<><LinkOutlined /> 연관 데이터 ({entryDrawer.data.relations.length})</>} style={{ marginTop: 12 }}>
              {entryDrawer.data.relations.map((r: any) => (
                <div key={r.relation_id} style={{ marginBottom: 6 }}>
                  <Tag color={RELATION_COLORS[r.relation_type]}>{r.relation_type}</Tag>
                  <Text>{r.source_biz}</Text> → <Text>{r.target_biz}</Text>
                  {r.description && <Text type="secondary" style={{ marginLeft: 8, fontSize: 11 }}>{r.description}</Text>}
                </div>
              ))}
            </Card>
          )}

          {/* Resources */}
          {entryDrawer.data.resources?.length > 0 && (
            <Card size="small" title={<><ApiOutlined /> 리소스 ({entryDrawer.data.resources.length})</>} style={{ marginTop: 12 }}>
              {entryDrawer.data.resources.map((r: any) => (
                <div key={r.resource_id} style={{ marginBottom: 6 }}>
                  <Tag color={RESOURCE_COLORS[r.resource_type]}>{r.resource_type}</Tag>
                  {r.method && <Tag>{r.method}</Tag>}
                  {r.endpoint_url && <Text code>{r.endpoint_url}</Text>}
                  {r.file_path && <Text code>{r.file_path}</Text>}
                  {r.description && <Text type="secondary" style={{ marginLeft: 8 }}>{r.description}</Text>}
                </div>
              ))}
            </Card>
          )}

          {/* History */}
          {entryDrawer.data.history?.length > 0 && (
            <Card size="small" title={<><HistoryOutlined /> 최근 이력</>} style={{ marginTop: 12 }}>
              <Timeline items={entryDrawer.data.history.slice(0, 8).map((h: any) => ({
                color: ACTION_COLORS[h.action_type] || 'blue',
                children: <>
                  <Tag color={ACTION_COLORS[h.action_type]}>{h.action_type}</Tag>
                  <Text>{h.actor}</Text>
                  {h.job_id && <Tag style={{ marginLeft: 4 }}>Job: {h.job_id}</Tag>}
                  {h.cohort_id && <Tag color="magenta" style={{ marginLeft: 4 }}>Cohort: {h.cohort_id}</Tag>}
                  {h.row_count != null && <Text type="secondary"> ({h.row_count.toLocaleString()}건)</Text>}
                  <Text type="secondary" style={{ marginLeft: 8 }}>{dayjs(h.created_at).format('MM-DD HH:mm')}</Text>
                </>,
              }))} />
            </Card>
          )}
        </>)}
      </Drawer>
    </Spin>
  );

  // ══════════════════════════════════════════
  //  2. Ownership
  // ══════════════════════════════════════════
  const renderOwnership = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { ownerForm.resetFields(); setOwnerModal(true); }}>공유 추가</Button>
        <Button icon={<ReloadOutlined />} onClick={loadOwnerships}>새로고침</Button>
      </Space>

      {/* Matrix */}
      <Card size="small" title="오너쉽 매트릭스" style={{ marginBottom: 16 }}>
        <Table dataSource={ownerMatrix} rowKey="entry_id" size="small" pagination={false} scroll={{ x: 900 }}
          columns={[
            { title: '데이터', dataIndex: 'business_name', width: 160, fixed: 'left',
              render: (v: string, r: any) => <><Tag color={TYPE_COLORS[r.entry_type]}>{r.entry_type}</Tag> {v}</> },
            { title: '접근', dataIndex: 'access_level', width: 80, render: (v: string) => <Tag color={ACCESS_COLORS[v]}>{v}</Tag> },
            { title: 'Primary (소유)', dataIndex: 'primary_depts', width: 140,
              render: (v: string[]) => v?.filter(Boolean).map(d => <Tag key={d} color="green">{d}</Tag>) || '-' },
            { title: 'Steward (관리)', dataIndex: 'steward_depts', width: 140,
              render: (v: string[]) => v?.filter(Boolean).map(d => <Tag key={d} color="blue">{d}</Tag>) || '-' },
            { title: 'Consumer (사용)', dataIndex: 'consumer_depts', width: 140,
              render: (v: string[]) => v?.filter(Boolean).map(d => <Tag key={d} color="orange">{d}</Tag>) || '-' },
            { title: 'Shared (공유)', dataIndex: 'shared_depts', width: 140,
              render: (v: string[]) => v?.filter(Boolean).map(d => <Tag key={d} color="purple">{d}</Tag>) || '-' },
            { title: '이해관계자', dataIndex: 'total_stakeholders', width: 80 },
          ]}
        />
      </Card>

      {/* Detail list */}
      <Table dataSource={ownerships} rowKey="ownership_id" size="small" pagination={false}
        columns={[
          { title: '데이터', width: 160, render: (_: any, r: any) => <><Tag color={TYPE_COLORS[r.entry_type]}>{r.entry_type}</Tag> {r.business_name}</> },
          { title: '역할', dataIndex: 'owner_type', width: 80, render: (v: string) => <Tag color={OWNER_COLORS[v]}>{v}</Tag> },
          { title: '부서', dataIndex: 'dept', width: 120 },
          { title: '담당자', dataIndex: 'person', width: 80, render: (v: string) => v || '-' },
          { title: '범위', dataIndex: 'access_scope', width: 70, render: (v: string) => <Tag>{v}</Tag> },
          { title: '공유 목적', dataIndex: 'share_purpose', ellipsis: true },
          { title: '부여일', dataIndex: 'granted_at', width: 100, render: (v: string) => dayjs(v).format('MM-DD HH:mm') },
          { title: '', width: 50, render: (_: any, r: any) => (
            <Popconfirm title="철회?" onConfirm={async () => { await deleteJSON(`${API}/ownerships/${r.ownership_id}`); loadOwnerships(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />

      <Modal title="공유/오너쉽 추가" open={ownerModal} onCancel={() => setOwnerModal(false)}
        onOk={async () => {
          try {
            const vals = await ownerForm.validateFields();
            await postJSON(`${API}/ownerships`, vals);
            message.success('추가 완료');
            setOwnerModal(false);
            loadOwnerships();
          } catch { /* */ }
        }} width={480}>
        <Form form={ownerForm} layout="vertical" size="small">
          <Form.Item name="entry_id" label="대상 데이터 ID" rules={[{ required: true }]}><Input type="number" /></Form.Item>
          <Form.Item name="owner_type" label="역할" rules={[{ required: true }]}>
            <Select options={[
              { value: 'primary', label: 'Primary (소유)' }, { value: 'steward', label: 'Steward (관리)' },
              { value: 'consumer', label: 'Consumer (사용)' }, { value: 'shared', label: 'Shared (공유)' },
            ]} />
          </Form.Item>
          <Row gutter={16}>
            <Col span={12}><Form.Item name="dept" label="부서" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="person" label="담당자"><Input /></Form.Item></Col>
          </Row>
          <Form.Item name="access_scope" label="접근 범위" initialValue="read">
            <Select options={['read', 'write', 'admin', 'full'].map(v => ({ value: v, label: v }))} />
          </Form.Item>
          <Form.Item name="share_purpose" label="공유 목적"><Input.TextArea rows={2} /></Form.Item>
        </Form>
      </Modal>
    </Spin>
  );

  // ══════════════════════════════════════════
  //  3. Resources
  // ══════════════════════════════════════════
  const renderResources = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadResources} style={{ marginBottom: 16 }}>새로고침</Button>
      <Table dataSource={resources} rowKey="resource_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'resource_id', width: 45 },
          { title: '카탈로그', dataIndex: 'business_name', width: 160 },
          { title: '리소스 유형', dataIndex: 'resource_type', width: 110,
            render: (v: string) => <Tag color={RESOURCE_COLORS[v]}>{v}</Tag> },
          { title: '메소드', dataIndex: 'method', width: 60, render: (v: string) => v ? <Tag>{v}</Tag> : '-' },
          { title: 'URL/경로', width: 250,
            render: (_: any, r: any) => r.endpoint_url ? <Text code style={{ fontSize: 11 }}>{r.endpoint_url}</Text>
              : r.file_path ? <Text code style={{ fontSize: 11 }}>{r.file_path}</Text> : '-' },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: 'SQL/스키마', width: 80, render: (_: any, r: any) => (
            <Space size={4}>
              {r.query_text && <Tooltip title={r.query_text}><Tag color="orange">SQL</Tag></Tooltip>}
              {r.request_schema && <Tooltip title={JSON.stringify(r.request_schema, null, 2)}><Tag color="blue">Req</Tag></Tooltip>}
              {r.response_schema && <Tooltip title={JSON.stringify(r.response_schema, null, 2)}><Tag color="green">Res</Tag></Tooltip>}
            </Space>
          )},
          { title: '', width: 40, render: (_: any, r: any) => (
            <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API}/resources/${r.resource_id}`); loadResources(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />
    </Spin>
  );

  // ══════════════════════════════════════════
  //  4. Work History
  // ══════════════════════════════════════════
  const renderHistory = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadHistory} style={{ marginBottom: 16 }}>새로고침</Button>
      <Table dataSource={history} rowKey="history_id" size="small" pagination={{ pageSize: 20 }}
        columns={[
          { title: 'ID', dataIndex: 'history_id', width: 55 },
          { title: '데이터', dataIndex: 'business_name', width: 150 },
          { title: '작업', dataIndex: 'action_type', width: 110,
            render: (v: string) => <Tag color={ACTION_COLORS[v]}>{v}</Tag> },
          { title: '수행자', dataIndex: 'actor', width: 80 },
          { title: 'Job ID', dataIndex: 'job_id', width: 120, render: (v: string) => v ? <Text code style={{ fontSize: 11 }}>{v}</Text> : '-' },
          { title: 'Cohort ID', dataIndex: 'cohort_id', width: 120, render: (v: string) => v ? <Tag color="magenta">{v}</Tag> : '-' },
          { title: '처리 건수', dataIndex: 'row_count', width: 100, render: (v: number) => v != null ? v.toLocaleString() : '-' },
          { title: '소요(ms)', dataIndex: 'duration_ms', width: 80, render: (v: number) => v != null ? v.toLocaleString() : '-' },
          { title: '상태', dataIndex: 'status', width: 70,
            render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'failed' ? 'red' : 'orange'}>{v}</Tag> },
          { title: '상세', dataIndex: 'action_detail', ellipsis: true,
            render: (v: any) => {
              if (!v) return '-';
              const d = typeof v === 'string' ? JSON.parse(v) : v;
              return <Tooltip title={JSON.stringify(d, null, 2)}><Text type="secondary" style={{ fontSize: 11 }}>{JSON.stringify(d).slice(0, 80)}</Text></Tooltip>;
            },
          },
          { title: '일시', dataIndex: 'created_at', width: 100, render: (v: string) => dayjs(v).format('MM-DD HH:mm') },
        ]}
      />
    </Spin>
  );

  // ══════════════════════════════════════════
  //  5. Sync
  // ══════════════════════════════════════════
  const renderSync = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<SyncOutlined />} type="primary" onClick={async () => {
          try {
            const res = await postJSON(`${API}/sync/detect-changes`);
            message.info(`스키마 변경 감지 완료: ${res.total}건`);
            loadSync();
          } catch { message.error('동기화 실패'); }
        }}>스키마 변경 감지</Button>
        <Button icon={<ReloadOutlined />} onClick={loadSync}>새로고침</Button>
      </Space>

      <Table dataSource={syncLogs} rowKey="sync_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'sync_id', width: 50 },
          { title: '유형', dataIndex: 'sync_type', width: 100, render: (v: string) => <Tag>{v}</Tag> },
          { title: '감지된 변경', dataIndex: 'changes_detected', ellipsis: true,
            render: (v: any) => {
              if (!v) return '-';
              const arr = typeof v === 'string' ? JSON.parse(v) : v;
              if (!Array.isArray(arr) || arr.length === 0) return <Tag color="green">변경 없음</Tag>;
              return <Space wrap size={[4, 4]}>
                {arr.map((c: any, i: number) => <Tag key={i} color={c.type === 'new_table' ? 'green' : 'orange'}>{c.type}: {c.table}</Tag>)}
              </Space>;
            },
          },
          { title: '상태', dataIndex: 'status', width: 70, render: (v: string) => <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag> },
          { title: '동기화 일시', dataIndex: 'synced_at', width: 140, render: (v: string) => dayjs(v).format('YYYY-MM-DD HH:mm:ss') },
        ]}
      />
    </Spin>
  );

  // ══════════════════════════════════════════
  //  6. Relations
  // ══════════════════════════════════════════
  const renderRelations = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadRelations} style={{ marginBottom: 16 }}>새로고침</Button>

      {/* Legend */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          {Object.entries(RELATION_COLORS).map(([k, c]) => <Tag key={k} color={c}>{k}</Tag>)}
        </Space>
      </Card>

      <Table dataSource={relations} rowKey="relation_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'relation_id', width: 45 },
          { title: '소스', width: 180, render: (_: any, r: any) => <><Tag color={TYPE_COLORS[r.source_type]}>{r.source_type}</Tag> {r.source_biz}</> },
          { title: '관계', dataIndex: 'relation_type', width: 120,
            render: (v: string) => <Tag color={RELATION_COLORS[v]}>{v}</Tag> },
          { title: '대상', width: 180, render: (_: any, r: any) => <><Tag color={TYPE_COLORS[r.target_type]}>{r.target_type}</Tag> {r.target_biz}</> },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: '자동감지', dataIndex: 'auto_detected', width: 70, render: (v: boolean) => v ? <Tag color="blue">자동</Tag> : <Tag>수동</Tag> },
          { title: '', width: 40, render: (_: any, r: any) => (
            <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API}/relations/${r.relation_id}`); loadRelations(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />
    </Spin>
  );

  return (
    <div>
      {renderOverviewCards()}
      <Segmented block value={view}
        onChange={(v) => setView(v as string)}
        options={[
          { label: '카탈로그 종합', value: 'catalog', icon: <AppstoreOutlined /> },
          { label: '오너쉽/공유', value: 'ownership', icon: <TeamOutlined /> },
          { label: '리소스(API/뷰)', value: 'resources', icon: <ApiOutlined /> },
          { label: '작업 이력', value: 'history', icon: <HistoryOutlined /> },
          { label: '메타 동기화', value: 'sync', icon: <SyncOutlined /> },
          { label: '연관 데이터', value: 'relations', icon: <LinkOutlined /> },
        ]}
        style={{ marginBottom: 16 }}
      />
      {view === 'catalog' && renderCatalog()}
      {view === 'ownership' && renderOwnership()}
      {view === 'resources' && renderResources()}
      {view === 'history' && renderHistory()}
      {view === 'sync' && renderSync()}
      {view === 'relations' && renderRelations()}
    </div>
  );
};

export default CatalogManagement;
