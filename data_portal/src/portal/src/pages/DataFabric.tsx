/**
 * 데이터 패브릭 관리 — 소스 CRUD, 흐름 CRUD, 연결 테스트, 품질 지표
 * /api/v1/portal-ops/fabric-* 연동
 */
import React, { useEffect, useState, useCallback } from 'react';
import {
  Card, Row, Col, Statistic, Badge, Tag, Table, Tooltip, Spin, Typography,
  Button, Switch, Modal, Form, Input, InputNumber, Select, Space, App, Popconfirm,
} from 'antd';
import {
  CloudServerOutlined, CheckCircleOutlined, WarningOutlined,
  CloseCircleOutlined, ReloadOutlined, PlusOutlined,
  ThunderboltOutlined, DeleteOutlined, EditOutlined,
  PlayCircleOutlined, ClearOutlined, ApiOutlined,
} from '@ant-design/icons';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip as RTooltip, ResponsiveContainer, Cell,
} from 'recharts';
import { fetchPost, fetchPut, fetchDelete } from '../services/apiUtils';

const { Title, Text, Paragraph } = Typography;

interface SourceStats { [key: string]: string | number }

interface DataSource {
  id: string;
  name: string;
  type: string;
  host: string;
  port: number;
  enabled: boolean;
  check_method: string;
  check_url: string;
  description: string;
  config: Record<string, any>;
  status: 'healthy' | 'degraded' | 'error' | 'disabled';
  latency_ms: number;
  stats: SourceStats;
}

interface Flow {
  id: number;
  from: string;
  to: string;
  label: string;
  enabled: boolean;
  description: string;
}

interface FabricData {
  sources: DataSource[];
  flows: Flow[];
  summary: { total: number; healthy: number; degraded: number; error: number };
  quality_data: { domain: string; score: number; issues: number }[];
  source_count: number;
}

const STATUS_CONFIG: Record<string, { color: string; text: string; badge: any }> = {
  healthy:  { color: '#52c41a', text: '정상', badge: 'success' },
  degraded: { color: '#faad14', text: '경고', badge: 'warning' },
  error:    { color: '#ff4d4f', text: '장애', badge: 'error' },
  disabled: { color: '#d9d9d9', text: '비활성', badge: 'default' },
};

const getScoreColor = (score: number) => {
  if (score > 90) return '#52c41a';
  if (score > 80) return '#ff6600';
  return '#ff4d4f';
};

const API = '/api/v1/portal-ops';


const DataFabric: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<FabricData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [testingAll, setTestingAll] = useState(false);
  const [testingId, setTestingId] = useState<string | null>(null);

  // 소스 모달
  const [srcModalOpen, setSrcModalOpen] = useState(false);
  const [srcEditing, setSrcEditing] = useState<DataSource | null>(null);
  const [srcForm] = Form.useForm();
  const [srcSaving, setSrcSaving] = useState(false);

  // 흐름 모달
  const [flowModalOpen, setFlowModalOpen] = useState(false);
  const [flowEditing, setFlowEditing] = useState<Flow | null>(null);
  const [flowForm] = Form.useForm();
  const [flowSaving, setFlowSaving] = useState(false);

  // ── 데이터 로딩 ──
  const fetchData = useCallback((isRefresh = false) => {
    if (isRefresh) setRefreshing(true); else setLoading(true);
    fetch(`${API}/fabric-stats`)
      .then(r => { if (!r.ok) throw new Error(); return r.json(); })
      .then(d => setData(d))
      .catch(() => { if (isRefresh) message.error('데이터 로딩 실패'); })
      .finally(() => { setLoading(false); setRefreshing(false); });
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  // ── 소스 관리 ──
  const handleTestSource = async (sourceId: string) => {
    setTestingId(sourceId);
    try {
      const r = await fetchPost(`${API}/fabric-test/${sourceId}`);
      const result = await r.json();
      if (result.status === 'healthy') {
        message.success(`${result.name}: 정상 (${result.latency_ms}ms)`);
      } else if (result.status === 'degraded') {
        message.warning(`${result.name}: 경고 (${result.latency_ms}ms)`);
      } else {
        message.error(`${result.name}: 연결 실패`);
      }
      fetchData(true);
    } catch { message.error('테스트 실패'); }
    finally { setTestingId(null); }
  };

  const handleTestAll = async () => {
    setTestingAll(true);
    try {
      const r = await fetchPost(`${API}/fabric-test-all`);
      const result = await r.json();
      const ok = result.sources?.filter((s: any) => s.status === 'healthy').length ?? 0;
      const total = result.sources?.length ?? 0;
      message.info(`전체 테스트 완료: ${ok}/${total} 정상`);
      // 캐시 삭제 + 재로딩
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { message.error('전체 테스트 실패'); }
    finally { setTestingAll(false); }
  };

  const handleClearCache = async () => {
    try {
      await fetchPost(`${API}/fabric-cache-clear`);
      message.success('캐시 삭제 완료');
      fetchData(true);
    } catch { message.error('캐시 삭제 실패'); }
  };

  const handleToggleSource = async (sourceId: string, enabled: boolean) => {
    try {
      await fetchPut(`${API}/fabric-source/${sourceId}`, { enabled });
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { message.error('변경 실패'); }
  };

  const openSrcModal = (source?: DataSource) => {
    if (source) {
      setSrcEditing(source);
      srcForm.setFieldsValue({
        source_id: source.id,
        name: source.name,
        source_type: source.type,
        host: source.host,
        port: source.port,
        check_method: source.check_method,
        check_url: source.check_url,
        description: source.description,
      });
    } else {
      setSrcEditing(null);
      srcForm.resetFields();
    }
    setSrcModalOpen(true);
  };

  const handleSaveSrc = async () => {
    try {
      const values = await srcForm.validateFields();
      setSrcSaving(true);
      if (srcEditing) {
        // 수정
        const { source_id, ...updates } = values;
        await fetchPut(`${API}/fabric-source/${srcEditing.id}`, updates);
        message.success('소스 수정 완료');
      } else {
        // 신규
        const r = await fetchPost(`${API}/fabric-source`, values);
        if (!r.ok) {
          const err = await r.json();
          message.error(err.detail || '소스 추가 실패');
          return;
        }
        message.success('소스 추가 완료');
      }
      setSrcModalOpen(false);
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { /* validation error */ }
    finally { setSrcSaving(false); }
  };

  const handleDeleteSrc = async (sourceId: string) => {
    try {
      await fetchDelete(`${API}/fabric-source/${sourceId}`);
      message.success('소스 삭제 완료');
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { message.error('삭제 실패'); }
  };

  // ── 흐름 관리 ──
  const handleToggleFlow = async (flowId: number, enabled: boolean) => {
    try {
      await fetchPut(`${API}/fabric-flow/${flowId}`, { enabled });
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { message.error('변경 실패'); }
  };

  const openFlowModal = (flow?: Flow) => {
    if (flow) {
      setFlowEditing(flow);
      flowForm.setFieldsValue({
        source_from: flow.from,
        source_to: flow.to,
        label: flow.label,
        description: flow.description,
      });
    } else {
      setFlowEditing(null);
      flowForm.resetFields();
    }
    setFlowModalOpen(true);
  };

  const handleSaveFlow = async () => {
    try {
      const values = await flowForm.validateFields();
      setFlowSaving(true);
      if (flowEditing) {
        await fetchPut(`${API}/fabric-flow/${flowEditing.id}`, values);
        message.success('흐름 수정 완료');
      } else {
        const r = await fetchPost(`${API}/fabric-flow`, values);
        if (!r.ok) {
          const err = await r.json();
          message.error(err.detail || '흐름 추가 실패');
          return;
        }
        message.success('흐름 추가 완료');
      }
      setFlowModalOpen(false);
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { /* validation */ }
    finally { setFlowSaving(false); }
  };

  const handleDeleteFlow = async (flowId: number) => {
    try {
      await fetchDelete(`${API}/fabric-flow/${flowId}`);
      message.success('흐름 삭제 완료');
      await fetchPost(`${API}/fabric-cache-clear`);
      fetchData(true);
    } catch { message.error('삭제 실패'); }
  };

  // ── 테이블 렌더 ──
  const nameMap: Record<string, string> = {};
  data?.sources.forEach(s => { nameMap[s.id] = s.name; });

  const sourceColumns = [
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (status: string) => {
        const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.disabled;
        return <Badge status={cfg.badge} text={cfg.text} />;
      },
    },
    {
      title: '소스명',
      dataIndex: 'name',
      key: 'name',
      render: (name: string, record: DataSource) => (
        <div>
          <Text strong>{name}</Text>
          <br />
          <Text type="secondary" style={{ fontSize: 12 }}>{record.description}</Text>
        </div>
      ),
    },
    {
      title: '유형',
      dataIndex: 'type',
      key: 'type',
      width: 140,
      render: (v: string) => <Tag>{v}</Tag>,
    },
    {
      title: 'Host:Port',
      key: 'endpoint',
      width: 180,
      render: (_: any, record: DataSource) => (
        <Text code style={{ fontSize: 12 }}>{record.host}:{record.port}</Text>
      ),
    },
    {
      title: '응답시간',
      dataIndex: 'latency_ms',
      key: 'latency',
      width: 100,
      render: (ms: number, record: DataSource) => {
        if (record.status === 'disabled' || record.status === 'error') return <Text type="secondary">-</Text>;
        return (
          <Tag color={ms < 50 ? 'green' : ms < 500 ? 'orange' : 'red'}>
            {ms}ms
          </Tag>
        );
      },
    },
    {
      title: '활성',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 70,
      render: (enabled: boolean, record: DataSource) => (
        <Switch
          size="small"
          checked={enabled}
          onChange={(v) => handleToggleSource(record.id, v)}
        />
      ),
    },
    {
      title: '관리',
      key: 'actions',
      width: 180,
      render: (_: any, record: DataSource) => (
        <Space size="small">
          <Tooltip title="연결 테스트">
            <Button
              size="small"
              icon={<ThunderboltOutlined />}
              loading={testingId === record.id}
              onClick={() => handleTestSource(record.id)}
              disabled={!record.enabled}
            />
          </Tooltip>
          <Tooltip title="수정">
            <Button
              size="small"
              icon={<EditOutlined />}
              onClick={() => openSrcModal(record)}
            />
          </Tooltip>
          <Popconfirm
            title="이 소스를 삭제하시겠습니까?"
            description="관련된 데이터 흐름도 함께 삭제됩니다."
            onConfirm={() => handleDeleteSrc(record.id)}
            okText="삭제"
            cancelText="취소"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="삭제">
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const flowColumns = [
    {
      title: '소스',
      dataIndex: 'from',
      key: 'from',
      render: (id: string) => <Tag color="blue">{nameMap[id] || id}</Tag>,
    },
    {
      title: '',
      key: 'arrow',
      width: 40,
      render: () => <span style={{ color: '#999', fontSize: 16 }}>→</span>,
    },
    {
      title: '대상',
      dataIndex: 'to',
      key: 'to',
      render: (id: string) => <Tag color="green">{nameMap[id] || id}</Tag>,
    },
    {
      title: '연결 유형',
      dataIndex: 'label',
      key: 'label',
      render: (v: string) => <Text strong>{v}</Text>,
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
      render: (v: string) => <Text type="secondary" style={{ fontSize: 12 }}>{v}</Text>,
    },
    {
      title: '활성',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 70,
      render: (enabled: boolean, record: Flow) => (
        <Switch
          size="small"
          checked={enabled}
          onChange={(v) => handleToggleFlow(record.id, v)}
        />
      ),
    },
    {
      title: '관리',
      key: 'actions',
      width: 120,
      render: (_: any, record: Flow) => (
        <Space size="small">
          <Tooltip title="수정">
            <Button size="small" icon={<EditOutlined />} onClick={() => openFlowModal(record)} />
          </Tooltip>
          <Popconfirm
            title="이 흐름을 삭제하시겠습니까?"
            onConfirm={() => handleDeleteFlow(record.id)}
            okText="삭제"
            cancelText="취소"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="삭제">
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (loading) {
    return (
      <div style={{ textAlign: 'center' }}>
        <Spin size="large" tip="데이터 소스 상태 확인 중..."><div style={{ padding: 80 }} /></Spin>
      </div>
    );
  }

  const summary = data?.summary ?? { total: 0, healthy: 0, degraded: 0, error: 0 };
  const sources = data?.sources ?? [];
  const qualityData = data?.quality_data ?? [];
  const flows = data?.flows ?? [];
  const sourceOptions = sources.map(s => ({ label: s.name, value: s.id }));

  return (
    <div>
      {/* 헤더 */}
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <ApiOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
              데이터 패브릭 관리
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 14, color: '#6c757d' }}>
              소스 연결 · 데이터 흐름 · 연결 테스트 · 품질 지표
            </Paragraph>
          </Col>
          <Col>
            <Space wrap>
              <Button icon={<PlusOutlined />} type="primary" onClick={() => openSrcModal()}>
                소스 추가
              </Button>
              <Button icon={<PlayCircleOutlined />} loading={testingAll} onClick={handleTestAll}>
                전체 테스트
              </Button>
              <Button icon={<ClearOutlined />} onClick={handleClearCache}>
                캐시 삭제
              </Button>
              <Button icon={<ReloadOutlined spin={refreshing} />} onClick={() => fetchData(true)} loading={refreshing}>
                새로고침
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 요약 카드 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 20 }}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="활성 소스" value={summary.total} suffix="개" valueStyle={{ color: '#1890ff', fontSize: 28 }} prefix={<CloudServerOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="정상" value={summary.healthy} suffix="개" valueStyle={{ color: '#52c41a', fontSize: 28 }} prefix={<CheckCircleOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="경고" value={summary.degraded} suffix="개" valueStyle={{ color: '#faad14', fontSize: 28 }} prefix={<WarningOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="장애" value={summary.error} suffix="개" valueStyle={{ color: '#ff4d4f', fontSize: 28 }} prefix={<CloseCircleOutlined />} />
          </Card>
        </Col>
      </Row>

      {/* 소스 관리 테이블 */}
      <Card
        title="소스 관리"
        style={{ marginBottom: 20 }}
        extra={<Text type="secondary">{sources.length}개 등록</Text>}
      >
        <Table
          dataSource={sources.map(s => ({ ...s, key: s.id }))}
          columns={sourceColumns}
          pagination={false}
          size="middle"
          scroll={{ x: 900 }}
        />
      </Card>

      {/* 하단: 흐름 관리 + 품질 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card
            title="데이터 흐름 관리"
            style={{ height: 420 }}
            extra={
              <Button size="small" icon={<PlusOutlined />} onClick={() => openFlowModal()}>
                흐름 추가
              </Button>
            }
          >
            <Table
              dataSource={flows.map(f => ({ ...f, key: f.id }))}
              columns={flowColumns}
              pagination={false}
              size="small"
              scroll={{ y: 300 }}
            />
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card title="도메인별 품질 점수" style={{ height: 420 }}>
            {qualityData.length > 0 ? (
              <ResponsiveContainer width="99%" height={320}>
                <BarChart data={qualityData} layout="vertical">
                  <XAxis type="number" domain={[0, 100]} unit="%" />
                  <YAxis dataKey="domain" type="category" width={60} />
                  <RTooltip formatter={(value: number) => [`${value}%`, '품질 점수']} />
                  <Bar dataKey="score" barSize={22} radius={[0, 4, 4, 0]}>
                    {qualityData.map((entry) => (
                      <Cell key={entry.domain} fill={getScoreColor(entry.score)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>품질 데이터 없음</div>
            )}
          </Card>
        </Col>
      </Row>

      {/* 소스 추가/수정 모달 */}
      <Modal
        title={srcEditing ? `소스 수정: ${srcEditing.name}` : '새 소스 추가'}
        open={srcModalOpen}
        onCancel={() => setSrcModalOpen(false)}
        onOk={handleSaveSrc}
        confirmLoading={srcSaving}
        okText={srcEditing ? '수정' : '추가'}
        cancelText="취소"
        width={560}
        destroyOnHidden
      >
        <Form form={srcForm} layout="vertical" style={{ marginTop: 16 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="source_id" label="소스 ID" rules={[{ required: true, message: '필수' }]}>
                <Input placeholder="my-source" disabled={!!srcEditing} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="name" label="소스명" rules={[{ required: true, message: '필수' }]}>
                <Input placeholder="My Source" />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="source_type" label="유형" rules={[{ required: true, message: '필수' }]}>
                <Input placeholder="PostgreSQL, Redis, HTTP API ..." />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="check_method" label="체크 방식" initialValue="http">
                <Select options={[
                  { label: 'HTTP', value: 'http' },
                  { label: 'TCP', value: 'tcp' },
                  { label: 'AsyncPG', value: 'asyncpg' },
                  { label: 'Psycopg2', value: 'psycopg2' },
                ]} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={16}>
              <Form.Item name="host" label="Host" initialValue="localhost">
                <Input />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="port" label="Port" rules={[{ required: true, message: '필수' }]}>
                <InputNumber style={{ width: '100%' }} min={1} max={65535} />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="check_url" label="헬스체크 URL (HTTP 방식)">
            <Input placeholder="http://localhost:8080/health" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="소스에 대한 설명" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 흐름 추가/수정 모달 */}
      <Modal
        title={flowEditing ? `흐름 수정` : '새 흐름 추가'}
        open={flowModalOpen}
        onCancel={() => setFlowModalOpen(false)}
        onOk={handleSaveFlow}
        confirmLoading={flowSaving}
        okText={flowEditing ? '수정' : '추가'}
        cancelText="취소"
        width={480}
        destroyOnHidden
      >
        <Form form={flowForm} layout="vertical" style={{ marginTop: 16 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="source_from" label="소스 (From)" rules={[{ required: true, message: '필수' }]}>
                <Select options={sourceOptions} placeholder="소스 선택" showSearch />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="source_to" label="대상 (To)" rules={[{ required: true, message: '필수' }]}>
                <Select options={sourceOptions} placeholder="대상 선택" showSearch />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="label" label="연결 유형" rules={[{ required: true, message: '필수' }]}>
            <Input placeholder="ETL, RAG, Schema ..." />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="흐름에 대한 설명" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default DataFabric;
