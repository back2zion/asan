/**
 * AI 인프라 & 아키텍처 — 운영 관리 페이지
 *
 * 5 탭: S/W 아키텍처 | MCP 도구 관리 | 컨테이너 관리 | GPU 자원 | 헬스체크
 * 읽기전용 대시보드 → 실제 CRUD/액션 운영 도구로 전환
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Space, Row, Col, Tag, Badge, Statistic, Tabs, Table,
  Spin, Alert, Descriptions, Button, Tooltip, Popconfirm, Drawer, Form,
  Input, Select, Switch, Modal, App,
} from 'antd';
import {
  ApiOutlined, CloudServerOutlined, DatabaseOutlined, RobotOutlined,
  CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined,
  ReloadOutlined, ThunderboltOutlined, ClusterOutlined, HddOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined,
  StopOutlined, FileTextOutlined, SettingOutlined,
} from '@ant-design/icons';
import {
  aiArchitectureApi,
  type ArchitectureOverview,
  type HealthCheck,
  type ContainerInfo,
  type McpTool,
  type SwComponent,
} from '../services/aiArchitectureApi';

const { Title, Paragraph, Text } = Typography;

const STATUS_MAP: Record<string, { color: string; icon: React.ReactNode }> = {
  healthy: { color: 'green', icon: <CheckCircleOutlined /> },
  connected: { color: 'green', icon: <CheckCircleOutlined /> },
  initializing: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  degraded: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  not_available: { color: 'default', icon: <ExclamationCircleOutlined /> },
  disconnected: { color: 'red', icon: <CloseCircleOutlined /> },
  error: { color: 'red', icon: <CloseCircleOutlined /> },
  unavailable: { color: 'red', icon: <CloseCircleOutlined /> },
};

const TOOL_CATEGORIES = [
  { value: 'data', label: 'Data', color: '#52c41a' },
  { value: 'sql', label: 'SQL', color: '#fa8c16' },
  { value: 'search', label: 'Search', color: '#1890ff' },
  { value: 'governance', label: 'Governance', color: '#722ed1' },
  { value: 'imaging', label: 'Imaging', color: '#eb2f96' },
  { value: 'interop', label: 'Interop', color: '#13c2c2' },
  { value: 'knowledge', label: 'Knowledge', color: '#2f54eb' },
  { value: 'workflow', label: 'Workflow', color: '#faad14' },
  { value: 'emr', label: 'EMR', color: '#f5222d' },
  { value: 'clinical', label: 'Clinical', color: '#a0d911' },
  { value: 'research', label: 'Research', color: '#597ef7' },
  { value: 'custom', label: 'Custom', color: '#8c8c8c' },
];

const PHASE_MAP: Record<string, { label: string; color: string }> = {
  deployed: { label: '운영 중', color: 'green' },
  phase1: { label: '1단계', color: 'blue' },
  phase2: { label: '2단계', color: 'orange' },
  future: { label: '향후', color: 'default' },
};

const PRIORITY_MAP: Record<string, { label: string; color: string }> = {
  high: { label: '높음', color: 'red' },
  medium: { label: '중간', color: 'orange' },
  low: { label: '낮음', color: 'default' },
};

const catColor = (cat: string) => TOOL_CATEGORIES.find(c => c.value === cat)?.color || '#8c8c8c';

// ── MCP 도구 관리 탭 ─────────────────────────────────────

const McpToolsTab: React.FC = () => {
  const { message } = App.useApp();
  const [tools, setTools] = useState<McpTool[]>([]);
  const [summary, setSummary] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editTool, setEditTool] = useState<McpTool | null>(null);
  const [testing, setTesting] = useState<number | null>(null);
  const [testResults, setTestResults] = useState<Record<number, { success: boolean; latency_ms: number; error?: string }>>({});
  const [phaseFilter, setPhaseFilter] = useState<string>('all');
  const [form] = Form.useForm();

  const loadTools = useCallback(async () => {
    setLoading(true);
    try {
      const data = await aiArchitectureApi.getMcpTopology();
      setTools(data.tools || []);
      setSummary(data.summary || null);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { loadTools(); }, [loadTools]);

  const handleAdd = () => {
    setEditTool(null);
    form.resetFields();
    form.setFieldsValue({ category: 'custom', enabled: true, priority: 'medium', phase: 'future' });
    setDrawerOpen(true);
  };

  const handleEdit = (tool: McpTool) => {
    setEditTool(tool);
    form.setFieldsValue(tool);
    setDrawerOpen(true);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      if (editTool) {
        await aiArchitectureApi.updateMcpTool(editTool.id, values);
        message.success('도구 설정이 수정되었습니다');
      } else {
        await aiArchitectureApi.createMcpTool(values);
        message.success('새 MCP 도구가 등록되었습니다');
      }
      setDrawerOpen(false);
      loadTools();
    } catch (e: any) {
      if (e.errorFields) return;
      message.error(e.message || '저장 실패');
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await aiArchitectureApi.deleteMcpTool(id);
      message.success('도구가 삭제되었습니다');
      loadTools();
    } catch { message.error('삭제 실패'); }
  };

  const handleTest = async (tool: McpTool) => {
    setTesting(tool.id);
    try {
      const result = await aiArchitectureApi.testMcpTool(tool.id);
      setTestResults(prev => ({ ...prev, [tool.id]: result }));
      if (result.success) message.success(`${tool.name}: ${result.latency_ms}ms 응답`);
      else message.warning(`${tool.name}: 연결 실패 — ${result.error || ''}`);
    } catch { message.error('테스트 실패'); }
    setTesting(null);
  };

  const handleToggle = async (tool: McpTool, enabled: boolean) => {
    try {
      await aiArchitectureApi.updateMcpTool(tool.id, { enabled });
      setTools(prev => prev.map(t => t.id === tool.id ? { ...t, enabled } : t));
    } catch { message.error('상태 변경 실패'); }
  };

  const filteredTools = phaseFilter === 'all' ? tools : tools.filter(t => t.phase === phaseFilter);

  const columns = [
    {
      title: '도구명', dataIndex: 'name', key: 'name', width: 170,
      render: (v: string, r: McpTool) => (
        <Space>
          <Badge status={r.enabled ? 'success' : 'default'} />
          <span>
            <Text code strong>{v}</Text>
            {r.reference && (
              <a href={r.reference} target="_blank" rel="noopener noreferrer" style={{ marginLeft: 4, fontSize: 11 }}
                 title={r.reference}>
                <ApiOutlined />
              </a>
            )}
          </span>
        </Space>
      ),
    },
    {
      title: '카테고리', dataIndex: 'category', key: 'category', width: 100,
      filters: TOOL_CATEGORIES.map(c => ({ text: c.label, value: c.value })),
      onFilter: (value: any, record: McpTool) => record.category === value,
      render: (v: string) => <Tag color={catColor(v)}>{v.toUpperCase()}</Tag>,
    },
    { title: '설명', dataIndex: 'description', key: 'description', ellipsis: true },
    {
      title: '단계', dataIndex: 'phase', key: 'phase', width: 80,
      render: (v: string) => {
        const p = PHASE_MAP[v] || PHASE_MAP.future;
        return <Tag color={p.color}>{p.label}</Tag>;
      },
    },
    {
      title: '우선순위', dataIndex: 'priority', key: 'priority', width: 80,
      render: (v: string) => {
        const p = PRIORITY_MAP[v] || PRIORITY_MAP.medium;
        return <Tag color={p.color}>{p.label}</Tag>;
      },
    },
    {
      title: '연결 상태', key: 'test', width: 90,
      render: (_: any, r: McpTool) => {
        const result = testResults[r.id];
        if (testing === r.id) return <Spin size="small" />;
        if (result) {
          return result.success
            ? <Tag color="green">{result.latency_ms}ms</Tag>
            : <Tag color="red">실패</Tag>;
        }
        return <Text type="secondary" style={{ fontSize: 11 }}>미확인</Text>;
      },
    },
    {
      title: '활성', key: 'enabled', width: 60,
      render: (_: any, r: McpTool) => (
        <Switch size="small" checked={r.enabled} onChange={(v) => handleToggle(r, v)} />
      ),
    },
    {
      title: '작업', key: 'action', width: 180,
      render: (_: any, r: McpTool) => (
        <Space size="small">
          <Tooltip title="연결 테스트">
            <Button size="small" icon={<ThunderboltOutlined />} loading={testing === r.id} onClick={() => handleTest(r)} />
          </Tooltip>
          <Tooltip title="편집">
            <Button size="small" icon={<EditOutlined />} onClick={() => handleEdit(r)} />
          </Tooltip>
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(r.id)}>
            <Tooltip title="삭제">
              <Button size="small" icon={<DeleteOutlined />} danger />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* Summary cards */}
      {summary && (
        <Row gutter={12}>
          <Col span={4}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="전체 도구" value={summary.total} valueStyle={{ color: '#1890ff', fontSize: 22 }} />
            </Card>
          </Col>
          <Col span={4}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="활성화" value={summary.enabled} valueStyle={{ color: '#52c41a', fontSize: 22 }} suffix={`/ ${summary.total}`} />
            </Card>
          </Col>
          {Object.entries(summary.by_phase as Record<string, number>).map(([phase, count]) => {
            const p = PHASE_MAP[phase] || { label: phase, color: 'default' };
            return (
              <Col span={4} key={phase}>
                <Card size="small" style={{ textAlign: 'center', cursor: 'pointer', border: phaseFilter === phase ? '2px solid #1890ff' : undefined }}
                      onClick={() => setPhaseFilter(phaseFilter === phase ? 'all' : phase)}>
                  <Statistic title={p.label} value={count as number} valueStyle={{ fontSize: 22 }} suffix="개" />
                </Card>
              </Col>
            );
          })}
        </Row>
      )}

      {/* Toolbar */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={loadTools}>새로고침</Button>
          <Button onClick={() => {
            setTestResults({});
            const enabledTools = tools.filter(t => t.enabled);
            enabledTools.forEach(t => handleTest(t));
          }}>활성 도구 테스트</Button>
          {phaseFilter !== 'all' && (
            <Tag closable onClose={() => setPhaseFilter('all')} color="blue">
              필터: {PHASE_MAP[phaseFilter]?.label || phaseFilter}
            </Tag>
          )}
        </Space>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>MCP 도구 추가</Button>
      </div>

      {/* Table */}
      <Table
        dataSource={filteredTools}
        columns={columns}
        rowKey="id"
        size="small"
        loading={loading}
        pagination={false}
        expandable={{
          expandedRowRender: (record: McpTool) => (
            <Descriptions size="small" column={3} bordered>
              <Descriptions.Item label="백엔드 서비스">{record.backend_service || '-'}</Descriptions.Item>
              <Descriptions.Item label="데이터 소스">{record.data_source || '-'}</Descriptions.Item>
              <Descriptions.Item label="엔드포인트">
                <Text code style={{ fontSize: 11 }}>{record.endpoint || '-'}</Text>
              </Descriptions.Item>
              {record.reference && (
                <Descriptions.Item label="참조" span={3}>
                  <a href={record.reference} target="_blank" rel="noopener noreferrer">{record.reference}</a>
                </Descriptions.Item>
              )}
            </Descriptions>
          ),
        }}
      />

      {/* Drawer */}
      <Drawer
        title={editTool ? 'MCP 도구 편집' : 'MCP 도구 추가'}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={520}
        extra={<Button type="primary" onClick={handleSave}>저장</Button>}
      >
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={16}>
              <Form.Item name="name" label="도구명 (Tool Name)" rules={[{ required: true }]}>
                <Input placeholder="e.g. dicom_mcp" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="enabled" label="활성화" valuePropName="checked">
                <Switch />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={12}>
            <Col span={8}>
              <Form.Item name="category" label="카테고리">
                <Select options={TOOL_CATEGORIES} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="priority" label="우선순위">
                <Select options={[
                  { value: 'high', label: '높음' },
                  { value: 'medium', label: '중간' },
                  { value: 'low', label: '낮음' },
                ]} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="phase" label="도입 단계">
                <Select options={[
                  { value: 'deployed', label: '운영 중' },
                  { value: 'phase1', label: '1단계 (즉시)' },
                  { value: 'phase2', label: '2단계 (중기)' },
                  { value: 'future', label: '향후 확장' },
                ]} />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="도구의 기능 설명" />
          </Form.Item>
          <Form.Item name="backend_service" label="백엔드 서비스">
            <Input placeholder="e.g. dicom-mcp (Orthanc/DICOMweb)" />
          </Form.Item>
          <Form.Item name="data_source" label="데이터 소스">
            <Input placeholder="e.g. PACS (DICOM RT, CT, MRI, PET)" />
          </Form.Item>
          <Form.Item name="endpoint" label="API 엔드포인트">
            <Input placeholder="e.g. /dicom/query_patients" />
          </Form.Item>
          <Form.Item name="reference" label="참조 URL">
            <Input placeholder="e.g. https://github.com/christianhinge/dicom-mcp" />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

// ── 컨테이너 관리 탭 ──────────────────────────────────────

const ContainersTab: React.FC = () => {
  const { message } = App.useApp();
  const [containers, setContainers] = useState<ContainerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [acting, setActing] = useState<string | null>(null);
  const [logDrawer, setLogDrawer] = useState<{ open: boolean; name: string; logs: string }>({ open: false, name: '', logs: '' });
  const [overview, setOverview] = useState<any>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [c, o] = await Promise.all([
        aiArchitectureApi.getContainers(),
        aiArchitectureApi.getOverview(),
      ]);
      setContainers(c.containers || []);
      setOverview(o.container_infrastructure);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const doAction = async (name: string, action: 'restart' | 'stop' | 'start') => {
    setActing(`${name}-${action}`);
    try {
      const result = await aiArchitectureApi.containerAction(name, action);
      if (result.success) message.success(`${name}: ${action} 완료`);
      else message.error(`${name}: ${result.error || '실패'}`);
      load();
    } catch { message.error('작업 실패'); }
    setActing(null);
  };

  const showLogs = async (name: string) => {
    try {
      const data = await aiArchitectureApi.getContainerLogs(name, 200);
      setLogDrawer({ open: true, name, logs: data.logs });
    } catch { message.error('로그 조회 실패'); }
  };

  const columns = [
    {
      title: '컨테이너', dataIndex: 'name', key: 'name', width: 250,
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: '상태', dataIndex: 'status', key: 'status', width: 180,
      render: (v: string) => <Tag color={v?.includes('Up') ? 'green' : 'red'}>{v}</Tag>,
    },
    {
      title: '이미지', dataIndex: 'image', key: 'image', ellipsis: true,
      render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text>,
    },
    {
      title: '포트', dataIndex: 'ports', key: 'ports', width: 200,
      render: (v: string) => <Text style={{ fontSize: 11 }}>{v || '-'}</Text>,
    },
    {
      title: '작업', key: 'action', width: 280,
      render: (_: any, r: ContainerInfo) => {
        const isUp = r.status?.includes('Up');
        return (
          <Space size="small">
            <Button size="small" icon={<ReloadOutlined />}
              loading={acting === `${r.name}-restart`}
              onClick={() => doAction(r.name, 'restart')}
            >재시작</Button>
            {isUp ? (
              <Popconfirm title={`${r.name}을 중지하시겠습니까?`} onConfirm={() => doAction(r.name, 'stop')}>
                <Button size="small" icon={<StopOutlined />} danger
                  loading={acting === `${r.name}-stop`}
                >중지</Button>
              </Popconfirm>
            ) : (
              <Button size="small" icon={<PlayCircleOutlined />} type="primary"
                loading={acting === `${r.name}-start`}
                onClick={() => doAction(r.name, 'start')}
              >시작</Button>
            )}
            <Button size="small" icon={<FileTextOutlined />} onClick={() => showLogs(r.name)}>로그</Button>
          </Space>
        );
      },
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* Stack overview */}
      {overview?.stacks && (
        <Row gutter={[12, 12]}>
          {overview.stacks.map((stack: any) => (
            <Col span={6} key={stack.stack}>
              <Card size="small" title={<><DatabaseOutlined /> {stack.stack}</>} extra={<Tag>{stack.compose}</Tag>}>
                {stack.services.map((svc: string) => {
                  const container = containers.find(c => c.name === svc);
                  const running = container?.status?.includes('Up');
                  return (
                    <div key={svc} style={{ marginBottom: 4 }}>
                      <Badge status={running ? 'success' : 'default'} />
                      <Text style={{ fontSize: 12, marginLeft: 4 }}>{svc}</Text>
                    </div>
                  );
                })}
              </Card>
            </Col>
          ))}
        </Row>
      )}

      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Text type="secondary">실행 중: {containers.filter(c => c.status?.includes('Up')).length} / {containers.length}개</Text>
      </div>
      <Table dataSource={containers} columns={columns} rowKey="name" size="small" loading={loading} pagination={false} />

      <Drawer
        title={`컨테이너 로그: ${logDrawer.name}`}
        open={logDrawer.open}
        onClose={() => setLogDrawer(p => ({ ...p, open: false }))}
        width={700}
      >
        <pre style={{ fontSize: 11, background: '#1e1e1e', color: '#d4d4d4', padding: 16, borderRadius: 8, maxHeight: 'calc(100vh - 200px)', overflow: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {logDrawer.logs || '(empty)'}
        </pre>
      </Drawer>
    </Space>
  );
};

// ── S/W 아키텍처 탭 (서비스 설정 + 연결 테스트) ─────────

const SwArchitectureTab: React.FC = () => {
  const { message } = App.useApp();
  const [overview, setOverview] = useState<ArchitectureOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, { success: boolean; latency_ms: number }>>({});
  const [editModal, setEditModal] = useState<{ open: boolean; comp: SwComponent | null }>({ open: false, comp: null });
  const [editEndpoint, setEditEndpoint] = useState('');
  const [editDesc, setEditDesc] = useState('');

  const TYPE_COLORS: Record<string, string> = { protocol: '#722ed1', pipeline: '#1890ff', agent: '#52c41a', model: '#fa8c16' };

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const data = await aiArchitectureApi.getOverview();
      setOverview(data);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleTest = async (comp: SwComponent) => {
    setTesting(comp.id);
    try {
      const result = await aiArchitectureApi.testService(comp.id);
      setTestResults(prev => ({ ...prev, [comp.id]: result }));
      if (result.success) message.success(`${comp.name}: ${result.latency_ms}ms`);
      else message.warning(`${comp.name}: 연결 실패`);
    } catch { message.error('테스트 실패'); }
    setTesting(null);
  };

  const openEdit = (comp: SwComponent) => {
    setEditModal({ open: true, comp });
    setEditEndpoint(comp.endpoint || '');
    setEditDesc(comp.description);
  };

  const handleSaveEdit = async () => {
    if (!editModal.comp) return;
    try {
      await aiArchitectureApi.updateService(editModal.comp.id, { endpoint: editEndpoint, description: editDesc });
      message.success('설정 저장됨');
      setEditModal({ open: false, comp: null });
      load();
    } catch { message.error('저장 실패'); }
  };

  if (loading || !overview) return <Spin />;
  const sw = overview.sw_architecture;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Card title="아키텍처 패턴" size="small">
        <Space wrap>
          {sw.patterns.map((p, i) => (
            <Tag key={i} color="blue" style={{ padding: '4px 8px' }}><CheckCircleOutlined /> {p}</Tag>
          ))}
        </Space>
      </Card>
      <Row gutter={[12, 12]}>
        {sw.components.map(comp => {
          const result = testResults[comp.id];
          return (
            <Col span={8} key={comp.id}>
              <Card
                size="small"
                title={<Space><Tag color={TYPE_COLORS[comp.type] || '#999'}>{comp.type}</Tag><Text strong>{comp.name}</Text></Space>}
                extra={
                  <Space size={4}>
                    {result && (result.success ? <Tag color="green">{result.latency_ms}ms</Tag> : <Tag color="red">실패</Tag>)}
                    <Button size="small" icon={<SettingOutlined />} onClick={() => openEdit(comp)} />
                  </Space>
                }
              >
                <Text type="secondary" style={{ fontSize: 13 }}>{comp.description}</Text>
                {comp.endpoint && <div style={{ marginTop: 8 }}><Tag color="blue">{comp.endpoint}</Tag></div>}
                {comp.model && <div style={{ marginTop: 4 }}><Text code style={{ fontSize: 11 }}>{comp.model}</Text></div>}
                {comp.providers && <div style={{ marginTop: 4 }}>{comp.providers.map((p, i) => <Tag key={i} style={{ fontSize: 11, marginTop: 2 }}>{p}</Tag>)}</div>}
                {comp.nodes && <div style={{ marginTop: 4 }}>{comp.nodes.map((n, i) => <Tag key={i} color="green" style={{ fontSize: 11, marginTop: 2 }}>{n}</Tag>)}</div>}
                <div style={{ marginTop: 12, borderTop: '1px solid #f0f0f0', paddingTop: 8 }}>
                  <Button size="small" icon={<ThunderboltOutlined />} loading={testing === comp.id} onClick={() => handleTest(comp)} block>
                    연결 테스트
                  </Button>
                </div>
              </Card>
            </Col>
          );
        })}
      </Row>
      <Modal title="서비스 설정 편집" open={editModal.open} onOk={handleSaveEdit} onCancel={() => setEditModal({ open: false, comp: null })} okText="저장">
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <div><Text strong>{editModal.comp?.name}</Text></div>
          <Input addonBefore="엔드포인트" value={editEndpoint} onChange={e => setEditEndpoint(e.target.value)} />
          <Input.TextArea rows={2} value={editDesc} onChange={e => setEditDesc(e.target.value)} placeholder="설명" />
        </Space>
      </Modal>
    </Space>
  );
};

// ── 헬스체크 탭 (per-component 재검사 + 액션) ───────────

const HealthCheckTab: React.FC = () => {
  const { message } = App.useApp();
  const [health, setHealth] = useState<HealthCheck | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const data = await aiArchitectureApi.getHealth();
      setHealth(data);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading || !health) return <Spin />;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Alert
          message={`전체 상태: ${health.overall}`}
          description={`마지막 확인: ${health.checked_at}`}
          type={health.overall === 'healthy' ? 'success' : 'warning'}
          showIcon
          style={{ flex: 1, marginRight: 12 }}
        />
        <Button type="primary" icon={<ReloadOutlined />} onClick={load} loading={loading}>전체 재검사</Button>
      </div>
      <Row gutter={[12, 12]}>
        {Object.entries(health.components).map(([key, val]) => {
          const st = STATUS_MAP[val.status] || STATUS_MAP.error;
          return (
            <Col span={6} key={key}>
              <Card size="small">
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Row justify="space-between" align="middle">
                    <Text strong style={{ fontSize: 13 }}>{key}</Text>
                    <Tag color={st.color} icon={st.icon}>{val.status}</Tag>
                  </Row>
                  {val.execution_time_ms !== undefined && <Text type="secondary" style={{ fontSize: 11 }}>응답: {val.execution_time_ms.toFixed(0)}ms</Text>}
                  {val.port !== undefined && <Text type="secondary" style={{ fontSize: 11 }}>포트: {val.port}</Text>}
                  {val.tools !== undefined && <Text type="secondary" style={{ fontSize: 11 }}>Tools: {val.tools}개</Text>}
                  {val.error && <Text type="danger" style={{ fontSize: 11 }}>{val.error}</Text>}
                </Space>
              </Card>
            </Col>
          );
        })}
      </Row>
    </Space>
  );
};

// ── GPU 자원 탭 ─────────────────────────────────────────

const GpuTab: React.FC<{ hw: any }> = ({ hw }) => {
  if (!hw) return <Spin />;
  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={[12, 12]}>
        {hw.gpu_resources.map((gpu: any) => (
          <Col span={8} key={gpu.id}>
            <Card size="small" title={<><HddOutlined /> {gpu.name}</>}>
              <Descriptions column={1} size="small">
                <Descriptions.Item label="디바이스">{gpu.device}</Descriptions.Item>
                <Descriptions.Item label="모델">{gpu.model_loaded}</Descriptions.Item>
                <Descriptions.Item label="메모리">{gpu.memory_gb} GB</Descriptions.Item>
                <Descriptions.Item label="포트">{String(gpu.port)}</Descriptions.Item>
              </Descriptions>
            </Card>
          </Col>
        ))}
      </Row>
      <Card title="GPU 메모리 할당 현황" size="small">
        {hw.gpu_resources.map((gpu: any) => {
          const pct = Math.min(100, (gpu.memory_gb / (hw.total_gpu_memory_gb || 1)) * 100);
          return (
            <div key={gpu.id} style={{ marginBottom: 12 }}>
              <Row justify="space-between"><Text strong>{gpu.model_loaded}</Text><Text>{gpu.memory_gb} GB</Text></Row>
              <div style={{ background: '#f0f0f0', borderRadius: 4, height: 20, marginTop: 4, overflow: 'hidden' }}>
                <div style={{ background: '#fa8c16', height: '100%', width: `${pct}%`, borderRadius: 4, transition: 'width 0.3s' }} />
              </div>
            </div>
          );
        })}
      </Card>
    </Space>
  );
};

// ── Main Component ──────────────────────────────────────

const AIArchitecture: React.FC = () => {
  const [overview, setOverview] = useState<ArchitectureOverview | null>(null);
  const [health, setHealth] = useState<HealthCheck | null>(null);
  const [loading, setLoading] = useState(true);

  const [mcpSummary, setMcpSummary] = useState<{ total: number; enabled: number } | null>(null);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const [o, h, mcp] = await Promise.all([
          aiArchitectureApi.getOverview(),
          aiArchitectureApi.getHealth(),
          aiArchitectureApi.getMcpTopology(),
        ]);
        setOverview(o);
        setHealth(h);
        setMcpSummary(mcp.summary || null);
      } catch { /* */ }
      setLoading(false);
    };
    load();
  }, []);

  if (loading) {
    return <Card><div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /><div style={{ marginTop: 16 }}>아키텍처 정보 로딩 중...</div></div></Card>;
  }

  const sw = overview?.sw_architecture;
  const hw = overview?.hw_infrastructure;
  const infra = overview?.container_infrastructure;

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* Header */}
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: 600 }}>
              <ClusterOutlined style={{ color: '#722ed1', marginRight: 12, fontSize: 28 }} />
              AI 인프라 & 아키텍처
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 15 }}>
              MCP 도구 관리, 서비스 설정, 컨테이너 운영, GPU 자원, 헬스체크
            </Paragraph>
          </Col>
          <Col>
            {health && (
              <Badge
                status={health.overall === 'healthy' ? 'success' : 'warning'}
                text={<span style={{ fontWeight: 500 }}>{health.overall === 'healthy' ? '전체 정상' : '일부 이상'}</span>}
              />
            )}
          </Col>
        </Row>
      </Card>

      {/* Summary */}
      <Row gutter={16}>
        <Col span={6}><Card><Statistic title="S/W 컴포넌트" value={sw?.components.length || 0} prefix={<ApiOutlined />} suffix="개" valueStyle={{ color: '#722ed1' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="GPU 메모리" value={hw?.total_gpu_memory_gb?.toFixed(1) || 0} prefix={<ThunderboltOutlined />} suffix="GB" valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="컨테이너" value={infra?.total_services || 0} prefix={<CloudServerOutlined />} suffix="개" valueStyle={{ color: '#1890ff' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="MCP Tools" value={mcpSummary?.total || 0} prefix={<RobotOutlined />} suffix={<span style={{ fontSize: 14, color: '#52c41a' }}>({mcpSummary?.enabled || 0} 활성)</span>} valueStyle={{ color: '#52c41a' }} /></Card></Col>
      </Row>

      {/* Tabs */}
      <Card>
        <Tabs
          defaultActiveKey="mcp"
          size="large"
          items={[
            { key: 'mcp', label: <span><RobotOutlined /> MCP 도구 관리</span>, children: <McpToolsTab /> },
            { key: 'sw', label: <span><ApiOutlined /> S/W 아키텍처</span>, children: <SwArchitectureTab /> },
            { key: 'containers', label: <span><CloudServerOutlined /> 컨테이너 관리</span>, children: <ContainersTab /> },
            { key: 'gpu', label: <span><ThunderboltOutlined /> GPU 자원</span>, children: <GpuTab hw={hw} /> },
            { key: 'health', label: <span><CheckCircleOutlined /> 헬스체크</span>, children: <HealthCheckTab /> },
          ]}
        />
      </Card>
    </Space>
  );
};

export default AIArchitecture;
