import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Space, Row, Col, Tag, Badge, Statistic, Table,
  Spin, Alert, Descriptions, Button, Tooltip, Modal, Input, App,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined,
  ReloadOutlined, ThunderboltOutlined, HddOutlined, SettingOutlined,
} from '@ant-design/icons';
import {
  aiArchitectureApi,
  type ArchitectureOverview,
  type HealthCheck,
  type SwComponent,
} from '../services/aiArchitectureApi';

const { Text } = Typography;

export const STATUS_MAP: Record<string, { color: string; icon: React.ReactNode }> = {
  healthy: { color: 'green', icon: <CheckCircleOutlined /> },
  connected: { color: 'green', icon: <CheckCircleOutlined /> },
  initializing: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  degraded: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  not_available: { color: 'default', icon: <ExclamationCircleOutlined /> },
  disconnected: { color: 'red', icon: <CloseCircleOutlined /> },
  error: { color: 'red', icon: <CloseCircleOutlined /> },
  unavailable: { color: 'red', icon: <CloseCircleOutlined /> },
};

export const TOOL_CATEGORIES = [
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

export const PHASE_MAP: Record<string, { label: string; color: string }> = {
  deployed: { label: '운영 중', color: 'green' },
  phase1: { label: '1단계', color: 'blue' },
  phase2: { label: '2단계', color: 'orange' },
  future: { label: '향후', color: 'default' },
};

export const PRIORITY_MAP: Record<string, { label: string; color: string }> = {
  high: { label: '높음', color: 'red' },
  medium: { label: '중간', color: 'orange' },
  low: { label: '낮음', color: 'default' },
};

export const catColor = (cat: string) => TOOL_CATEGORIES.find(c => c.value === cat)?.color || '#8c8c8c';

const TYPE_COLORS: Record<string, string> = { protocol: '#722ed1', pipeline: '#1890ff', agent: '#52c41a', model: '#fa8c16' };

// ── S/W 아키텍처 탭 (서비스 설정 + 연결 테스트) ─────────

export const SwArchitectureTab: React.FC = () => {
  const { message } = App.useApp();
  const [overview, setOverview] = useState<ArchitectureOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, { success: boolean; latency_ms: number }>>({});
  const [editModal, setEditModal] = useState<{ open: boolean; comp: SwComponent | null }>({ open: false, comp: null });
  const [editEndpoint, setEditEndpoint] = useState('');
  const [editDesc, setEditDesc] = useState('');

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

export const HealthCheckTab: React.FC = () => {
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

export const GpuTab: React.FC<{ hw: any }> = ({ hw }) => {
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
