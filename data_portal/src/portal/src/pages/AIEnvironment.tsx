import React, { useState, useEffect, useCallback } from 'react';
import { Card, Tabs, Button, Space, Table, Tag, Progress, Row, Col, Statistic, Typography, Tooltip, Modal, Form, Input, Select, InputNumber, message, Spin, Empty } from 'antd';
import {
  CodeOutlined,
  CloudServerOutlined,
  ThunderboltOutlined,
  RobotOutlined,
  ExperimentOutlined,
  FundProjectionScreenOutlined,
  ShareAltOutlined,
  DownloadOutlined,
  PlayCircleOutlined,
  StopOutlined,
  DeleteOutlined,
  ReloadOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  LinkOutlined,
} from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const API_BASE = '/api/v1/ai-environment';

const ICON_MAP: Record<string, React.ReactNode> = {
  FundProjectionScreenOutlined: <FundProjectionScreenOutlined />,
  ThunderboltOutlined: <ThunderboltOutlined />,
  RobotOutlined: <RobotOutlined />,
  ExperimentOutlined: <ExperimentOutlined />,
};

interface ContainerInfo {
  id: string;
  full_id: string;
  name: string;
  status: string;
  image: string;
  cpu: string;
  memory: string;
  ports: Record<string, string>;
  access_url: string | null;
  created: string;
  is_protected: boolean;
}

interface SystemResources {
  cpu: { percent: number; cores: number; used_cores: number };
  memory: { total_gb: number; used_gb: number; available_gb: number; percent: number };
  disk: { total_gb: number; used_gb: number; free_gb: number; percent: number };
}

interface GpuInfo {
  index: number;
  name: string;
  utilization_percent: number;
  memory_used_mb: number;
  memory_total_mb: number;
  memory_percent: number;
  temperature: number;
}

interface TemplateInfo {
  id: string;
  name: string;
  description: string;
  libraries: string[];
  icon: string;
  default_memory: string;
  default_cpu: number;
}

interface NotebookInfo {
  filename: string;
  name: string;
  size_kb: number;
  modified: string;
  cell_count: number;
}

const AIEnvironment: React.FC = () => {
  const [containers, setContainers] = useState<ContainerInfo[]>([]);
  const [templates, setTemplates] = useState<TemplateInfo[]>([]);
  const [systemRes, setSystemRes] = useState<SystemResources | null>(null);
  const [gpus, setGpus] = useState<GpuInfo[]>([]);
  const [gpuAvailable, setGpuAvailable] = useState(false);
  const [notebooks, setNotebooks] = useState<NotebookInfo[]>([]);
  const [loading, setLoading] = useState({ containers: true, resources: true, templates: true, notebooks: true });
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [createForm] = Form.useForm();

  // --- Data fetching ---

  const fetchContainers = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/containers`);
      const data = await res.json();
      setContainers(data.containers || []);
    } catch {
      console.error('컨테이너 목록 로드 실패');
    } finally {
      setLoading(prev => ({ ...prev, containers: false }));
    }
  }, []);

  const fetchResources = useCallback(async () => {
    try {
      const [sysRes, gpuRes] = await Promise.all([
        fetch(`${API_BASE}/resources/system`).then(r => r.json()),
        fetch(`${API_BASE}/resources/gpu`).then(r => r.json()),
      ]);
      setSystemRes(sysRes);
      setGpus(gpuRes.gpus || []);
      setGpuAvailable(gpuRes.available || false);
    } catch {
      console.error('리소스 정보 로드 실패');
    } finally {
      setLoading(prev => ({ ...prev, resources: false }));
    }
  }, []);

  const fetchTemplates = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/templates`);
      const data = await res.json();
      setTemplates(data.templates || []);
    } catch {
      console.error('템플릿 로드 실패');
    } finally {
      setLoading(prev => ({ ...prev, templates: false }));
    }
  }, []);

  const fetchNotebooks = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/shared`);
      const data = await res.json();
      setNotebooks(data.notebooks || []);
    } catch {
      console.error('노트북 목록 로드 실패');
    } finally {
      setLoading(prev => ({ ...prev, notebooks: false }));
    }
  }, []);

  useEffect(() => {
    fetchContainers();
    fetchResources();
    fetchTemplates();
    fetchNotebooks();
  }, [fetchContainers, fetchResources, fetchTemplates, fetchNotebooks]);

  // 리소스 모니터링 자동 갱신 (10초)
  useEffect(() => {
    const interval = setInterval(fetchResources, 10000);
    return () => clearInterval(interval);
  }, [fetchResources]);

  // --- Actions ---

  const handleStartContainer = async (containerId: string) => {
    setActionLoading(containerId);
    try {
      const res = await fetch(`${API_BASE}/containers/${containerId}/start`, { method: 'POST' });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '시작 실패');
      }
      message.success('컨테이너가 시작되었습니다');
      fetchContainers();
    } catch (e: any) {
      message.error(e.message);
    } finally {
      setActionLoading(null);
    }
  };

  const handleStopContainer = async (containerId: string) => {
    setActionLoading(containerId);
    try {
      const res = await fetch(`${API_BASE}/containers/${containerId}/stop`, { method: 'POST' });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '중지 실패');
      }
      message.success('컨테이너가 중지되었습니다');
      fetchContainers();
    } catch (e: any) {
      message.error(e.message);
    } finally {
      setActionLoading(null);
    }
  };

  const handleDeleteContainer = async (containerId: string, containerName: string) => {
    Modal.confirm({
      title: '컨테이너 삭제',
      content: `'${containerName}' 컨테이너를 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.`,
      okText: '삭제',
      okType: 'danger',
      cancelText: '취소',
      onOk: async () => {
        setActionLoading(containerId);
        try {
          const res = await fetch(`${API_BASE}/containers/${containerId}`, { method: 'DELETE' });
          if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || '삭제 실패');
          }
          message.success('컨테이너가 삭제되었습니다');
          fetchContainers();
        } catch (e: any) {
          message.error(e.message);
        } finally {
          setActionLoading(null);
        }
      },
    });
  };

  const handleCreateContainer = async (values: any) => {
    try {
      const res = await fetch(`${API_BASE}/containers`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: values.name,
          cpu_limit: values.cpu_limit,
          memory_limit: values.memory_limit,
          template_id: values.template_id || null,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '생성 실패');
      }
      message.success('컨테이너가 생성되었습니다');
      setCreateModalOpen(false);
      createForm.resetFields();
      fetchContainers();
    } catch (e: any) {
      message.error(e.message);
    }
  };

  const handleTemplateCreate = (template: TemplateInfo) => {
    createForm.setFieldsValue({
      name: `${template.id}-${Date.now().toString(36)}`,
      cpu_limit: template.default_cpu,
      memory_limit: template.default_memory,
      template_id: template.id,
    });
    setCreateModalOpen(true);
  };

  // --- Stats ---
  const runningCount = containers.filter(c => c.status === 'running').length;

  // --- Columns ---
  const containerColumns = [
    {
      title: '컨테이너명',
      dataIndex: 'name',
      key: 'name',
      render: (text: string, record: ContainerInfo) => (
        <Space>
          <strong>{text}</strong>
          {record.is_protected && <Tag color="gold">인프라</Tag>}
        </Space>
      ),
    },
    {
      title: '이미지',
      dataIndex: 'image',
      key: 'image',
      ellipsis: true,
      render: (text: string) => <span style={{ fontSize: 12, color: '#888' }}>{text?.split('@')[0] || text}</span>,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const colorMap: Record<string, string> = { running: 'green', exited: 'red', created: 'blue', paused: 'orange' };
        return <Tag color={colorMap[status] || 'default'}>{status.toUpperCase()}</Tag>;
      },
    },
    {
      title: 'CPU',
      dataIndex: 'cpu',
      key: 'cpu',
    },
    {
      title: 'Memory',
      dataIndex: 'memory',
      key: 'memory',
    },
    {
      title: '생성일',
      dataIndex: 'created',
      key: 'created',
    },
    {
      title: '작업',
      key: 'action',
      render: (_: any, record: ContainerInfo) => (
        <Space size="small">
          {record.access_url && record.status === 'running' && (
            <Tooltip title="JupyterLab 열기">
              <Button size="small" type="primary" icon={<LinkOutlined />} onClick={() => window.open(record.access_url!, '_blank')}>
                열기
              </Button>
            </Tooltip>
          )}
          {record.status !== 'running' ? (
            <Tooltip title="시작">
              <Button size="small" icon={<PlayCircleOutlined />} loading={actionLoading === record.id} onClick={() => handleStartContainer(record.full_id)} />
            </Tooltip>
          ) : (
            <Tooltip title="중지">
              <Button size="small" icon={<StopOutlined />} loading={actionLoading === record.id} disabled={record.is_protected} onClick={() => handleStopContainer(record.full_id)} />
            </Tooltip>
          )}
          <Tooltip title="삭제">
            <Button size="small" danger icon={<DeleteOutlined />} disabled={record.is_protected} onClick={() => handleDeleteContainer(record.full_id, record.name)} />
          </Tooltip>
        </Space>
      ),
    },
  ];

  const notebookColumns = [
    {
      title: '파일명',
      dataIndex: 'name',
      key: 'name',
      render: (text: string) => (
        <Space><CodeOutlined style={{ color: '#fa8c16' }} /><strong>{text}</strong></Space>
      ),
    },
    {
      title: '파일크기',
      dataIndex: 'size_kb',
      key: 'size_kb',
      render: (v: number) => `${v} KB`,
    },
    {
      title: '셀 수',
      dataIndex: 'cell_count',
      key: 'cell_count',
    },
    {
      title: '최종 수정',
      dataIndex: 'modified',
      key: 'modified',
    },
    {
      title: '작업',
      key: 'action',
      render: (_: any, record: NotebookInfo) => (
        <Tooltip title="다운로드">
          <Button size="small" icon={<DownloadOutlined />} onClick={() => window.open(`${API_BASE}/shared/${record.filename}/download`, '_blank')} />
        </Tooltip>
      ),
    },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <RobotOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              AI 데이터 분석환경
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              컨테이너 기반 AI 분석환경 및 리소스 관리
            </Paragraph>
          </Col>
        </Row>
      </Card>

      {/* 상단 통계 카드 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="활성 컨테이너"
              value={runningCount}
              suffix={`/ ${containers.length}`}
              prefix={<CloudServerOutlined />}
              valueStyle={{ color: '#3f8600' }}
              loading={loading.containers}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="CPU 사용률"
              value={systemRes?.cpu.percent ?? '-'}
              suffix="%"
              prefix={<ThunderboltOutlined />}
              valueStyle={{ color: '#1890ff' }}
              loading={loading.resources}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="메모리"
              value={systemRes ? `${systemRes.memory.used_gb} / ${systemRes.memory.total_gb} GB` : '-'}
              prefix={<CloudServerOutlined />}
              loading={loading.resources}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="디스크"
              value={systemRes ? `${systemRes.disk.used_gb} / ${systemRes.disk.total_gb} GB` : '-'}
              prefix={<CodeOutlined />}
              loading={loading.resources}
            />
          </Card>
        </Col>
      </Row>

      <Card title="AI 데이터 분석환경">
        <Tabs
          defaultActiveKey="containers"
          items={[
            {
              key: 'containers',
              label: '컨테이너 관리',
              children: (
                <div>
                  <Space style={{ marginBottom: 16 }}>
                    <Button type="primary" icon={<CloudServerOutlined />} onClick={() => setCreateModalOpen(true)}>
                      새 컨테이너 생성
                    </Button>
                    <Button icon={<ReloadOutlined />} onClick={fetchContainers}>
                      새로고침
                    </Button>
                  </Space>

                  <Table
                    columns={containerColumns}
                    dataSource={containers}
                    pagination={false}
                    rowKey="id"
                    loading={loading.containers}
                    locale={{ emptyText: <Empty description="Docker 컨테이너가 없습니다" /> }}
                  />
                </div>
              ),
            },
            {
              key: 'templates',
              label: '분석 템플릿',
              children: loading.templates ? (
                <Spin tip="로딩 중..." />
              ) : (
                <Row gutter={[16, 16]}>
                  {templates.map((template) => (
                    <Col span={12} key={template.id}>
                      <Card>
                        <Space direction="vertical" style={{ width: '100%' }}>
                          <Space>
                            {ICON_MAP[template.icon] || <ExperimentOutlined />}
                            <strong>{template.name}</strong>
                          </Space>
                          <p>{template.description}</p>
                          <Space wrap>
                            {template.libraries.map((lib, libIdx) => (
                              <Tag key={libIdx}>{lib}</Tag>
                            ))}
                          </Space>
                          <div style={{ fontSize: 12, color: '#888' }}>
                            기본 설정: CPU {template.default_cpu} cores, 메모리 {template.default_memory}
                          </div>
                          <Button type="primary" block onClick={() => handleTemplateCreate(template)}>
                            이 템플릿으로 시작
                          </Button>
                        </Space>
                      </Card>
                    </Col>
                  ))}
                </Row>
              ),
            },
            {
              key: 'monitoring',
              label: '리소스 모니터링',
              children: loading.resources ? (
                <Spin tip="리소스 정보 로딩 중..." />
              ) : (
                <div>
                  <Space style={{ marginBottom: 16 }}>
                    <Button icon={<ReloadOutlined />} onClick={fetchResources}>새로고침</Button>
                    <Tag icon={<CheckCircleOutlined />} color="success">10초 자동 갱신</Tag>
                  </Space>
                  <Row gutter={[16, 16]}>
                    <Col span={12}>
                      <Card title="CPU 사용률">
                        <Progress percent={systemRes?.cpu.percent ?? 0} status="active" />
                        <p>{systemRes?.cpu.used_cores ?? 0} cores / {systemRes?.cpu.cores ?? 0} cores 사용 중</p>
                      </Card>
                    </Col>
                    <Col span={12}>
                      <Card title="메모리 사용률">
                        <Progress percent={systemRes?.memory.percent ?? 0} status="active" />
                        <p>{systemRes?.memory.used_gb ?? 0} GB / {systemRes?.memory.total_gb ?? 0} GB 사용 중</p>
                      </Card>
                    </Col>
                    <Col span={12}>
                      <Card title="디스크 사용률">
                        <Progress percent={systemRes?.disk.percent ?? 0} status="active" />
                        <p>{systemRes?.disk.used_gb ?? 0} GB / {systemRes?.disk.total_gb ?? 0} GB 사용 중</p>
                      </Card>
                    </Col>
                    {gpuAvailable ? (
                      gpus.map((gpu) => (
                        <Col span={12} key={gpu.index}>
                          <Card title={`GPU #${gpu.index}: ${gpu.name}`}>
                            <Progress percent={gpu.utilization_percent} strokeColor="#52c41a" />
                            <p>VRAM: {Math.round(gpu.memory_used_mb)} MB / {Math.round(gpu.memory_total_mb)} MB ({gpu.memory_percent}%)</p>
                            <p>온도: {gpu.temperature}°C</p>
                          </Card>
                        </Col>
                      ))
                    ) : (
                      <Col span={12}>
                        <Card title="GPU">
                          <Tag icon={<CloseCircleOutlined />} color="default">GPU를 감지할 수 없습니다 (nvidia-smi 없음)</Tag>
                        </Card>
                      </Col>
                    )}
                  </Row>
                </div>
              ),
            },
            {
              key: 'sharing',
              label: '분석 작업 공유',
              children: (
                <div>
                  <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
                    <Col span={8}>
                      <Card size="small">
                        <Statistic title="공유된 노트북" value={notebooks.length} prefix={<ShareAltOutlined />} valueStyle={{ color: '#1890ff' }} loading={loading.notebooks} />
                      </Card>
                    </Col>
                    <Col span={8}>
                      <Card size="small">
                        <Statistic
                          title="총 셀 수"
                          value={notebooks.reduce((sum, n) => sum + n.cell_count, 0)}
                          prefix={<CodeOutlined />}
                          loading={loading.notebooks}
                        />
                      </Card>
                    </Col>
                    <Col span={8}>
                      <Card size="small">
                        <Statistic
                          title="총 크기"
                          value={`${notebooks.reduce((sum, n) => sum + n.size_kb, 0).toFixed(1)} KB`}
                          prefix={<DownloadOutlined />}
                          loading={loading.notebooks}
                        />
                      </Card>
                    </Col>
                  </Row>

                  <Space style={{ marginBottom: 16 }}>
                    <Button icon={<ReloadOutlined />} onClick={fetchNotebooks}>새로고침</Button>
                  </Space>

                  <Table
                    columns={notebookColumns}
                    dataSource={notebooks}
                    pagination={false}
                    rowKey="filename"
                    size="small"
                    loading={loading.notebooks}
                    locale={{ emptyText: <Empty description="공유된 노트북이 없습니다. notebooks/ 디렉토리에 .ipynb 파일을 추가하세요." /> }}
                  />
                </div>
              ),
            },
          ]}
        />
      </Card>

      {/* 컨테이너 생성 모달 */}
      <Modal
        title="새 JupyterLab 컨테이너 생성"
        open={createModalOpen}
        onCancel={() => { setCreateModalOpen(false); createForm.resetFields(); }}
        onOk={() => createForm.submit()}
        okText="생성"
        cancelText="취소"
      >
        <Form form={createForm} layout="vertical" onFinish={handleCreateContainer} initialValues={{ cpu_limit: 2, memory_limit: '4g' }}>
          <Form.Item name="name" label="컨테이너 이름" rules={[{ required: true, message: '이름을 입력하세요' }, { pattern: /^[a-z0-9][a-z0-9_.-]*$/, message: '소문자, 숫자, -, _, .만 사용 가능' }]}>
            <Input prefix="asan-" placeholder="my-analysis" />
          </Form.Item>
          <Form.Item name="cpu_limit" label="CPU 제한 (cores)">
            <InputNumber min={1} max={16} step={1} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="memory_limit" label="메모리 제한">
            <Select options={[{ value: '2g', label: '2 GB' }, { value: '4g', label: '4 GB' }, { value: '8g', label: '8 GB' }, { value: '16g', label: '16 GB' }]} />
          </Form.Item>
          <Form.Item name="template_id" label="템플릿 (선택)">
            <Select allowClear placeholder="템플릿 없음" options={templates.map(t => ({ value: t.id, label: t.name }))} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default AIEnvironment;
