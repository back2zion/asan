import React, { useState, useEffect, useCallback } from 'react';
import { Button, Space, Table, Tag, Tooltip, Modal, Form, Input, Select, InputNumber, App, Empty } from 'antd';
import {
  CloudServerOutlined,
  PlayCircleOutlined,
  StopOutlined,
  DeleteOutlined,
  ReloadOutlined,
  LinkOutlined,
} from '@ant-design/icons';

const API_BASE = '/api/v1/ai-environment';
const JUPYTER_DIRECT_URL = 'http://localhost:18888';

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

interface TemplateInfo {
  id: string;
  name: string;
  description: string;
  libraries: string[];
  icon: string;
  default_memory: string;
  default_cpu: number;
}

interface ContainerManagerProps {
  onStatsChange?: (running: number, total: number) => void;
}

const ContainerManager: React.FC<ContainerManagerProps> = ({ onStatsChange }) => {
  const { message, modal } = App.useApp();
  const [containers, setContainers] = useState<ContainerInfo[]>([]);
  const [templates, setTemplates] = useState<TemplateInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [createForm] = Form.useForm();

  const fetchContainers = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/containers`);
      const data = await res.json();
      const list: ContainerInfo[] = data.containers || [];
      setContainers(list);
      const running = list.filter(c => c.status === 'running').length;
      onStatsChange?.(running, list.length);
    } catch {
      console.error('컨테이너 목록 로드 실패');
    } finally {
      setLoading(false);
    }
  }, [onStatsChange]);

  const fetchTemplates = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/templates`);
      const data = await res.json();
      setTemplates(data.templates || []);
    } catch {
      console.error('템플릿 로드 실패');
    }
  }, []);

  useEffect(() => {
    fetchContainers();
    fetchTemplates();
  }, [fetchContainers, fetchTemplates]);

  const handleStartContainer = async (containerId: string) => {
    setActionLoading(containerId);
    try {
      const res = await fetch(`${API_BASE}/containers/${containerId}/start`, { method: 'POST' });
      if (!res.ok) { const err = await res.json(); throw new Error(err.detail || '시작 실패'); }
      message.success('컨테이너가 시작되었습니다');
      fetchContainers();
    } catch (e: any) { message.error(e.message); }
    finally { setActionLoading(null); }
  };

  const handleStopContainer = async (containerId: string) => {
    setActionLoading(containerId);
    try {
      const res = await fetch(`${API_BASE}/containers/${containerId}/stop`, { method: 'POST' });
      if (!res.ok) { const err = await res.json(); throw new Error(err.detail || '중지 실패'); }
      message.success('컨테이너가 중지되었습니다');
      fetchContainers();
    } catch (e: any) { message.error(e.message); }
    finally { setActionLoading(null); }
  };

  const handleDeleteContainer = async (containerId: string, containerName: string) => {
    modal.confirm({
      title: '컨테이너 삭제',
      content: `'${containerName}' 컨테이너를 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.`,
      okText: '삭제', okType: 'danger', cancelText: '취소',
      onOk: async () => {
        setActionLoading(containerId);
        try {
          const res = await fetch(`${API_BASE}/containers/${containerId}`, { method: 'DELETE' });
          if (!res.ok) { const err = await res.json(); throw new Error(err.detail || '삭제 실패'); }
          message.success('컨테이너가 삭제되었습니다');
          fetchContainers();
        } catch (e: any) { message.error(e.message); }
        finally { setActionLoading(null); }
      },
    });
  };

  const handleCreateContainer = async (values: any) => {
    try {
      const res = await fetch(`${API_BASE}/containers`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: values.name, cpu_limit: values.cpu_limit,
          memory_limit: values.memory_limit, template_id: values.template_id || null,
        }),
      });
      if (!res.ok) { const err = await res.json(); throw new Error(err.detail || '생성 실패'); }
      message.success('컨테이너가 생성되었습니다');
      setCreateModalOpen(false); createForm.resetFields(); fetchContainers();
    } catch (e: any) { message.error(e.message); }
  };

  const containerColumns = [
    {
      title: '컨테이너명', dataIndex: 'name', key: 'name',
      render: (text: string, record: ContainerInfo) => (
        <Space>
          <strong>{text}</strong>
          {record.is_protected && <Tag color="gold">인프라</Tag>}
        </Space>
      ),
    },
    {
      title: '이미지', dataIndex: 'image', key: 'image', ellipsis: true,
      render: (text: string) => <span style={{ fontSize: 12, color: '#888' }}>{text?.split('@')[0] || text}</span>,
    },
    {
      title: '상태', dataIndex: 'status', key: 'status',
      render: (status: string) => {
        const colorMap: Record<string, string> = { running: 'green', exited: 'red', created: 'blue', paused: 'orange' };
        return <Tag color={colorMap[status] || 'default'}>{status.toUpperCase()}</Tag>;
      },
    },
    { title: 'CPU', dataIndex: 'cpu', key: 'cpu' },
    { title: 'Memory', dataIndex: 'memory', key: 'memory' },
    { title: '생성일', dataIndex: 'created', key: 'created' },
    {
      title: '작업', key: 'action',
      render: (_: any, record: ContainerInfo) => (
        <Space size="small">
          {record.access_url && record.status === 'running' && (
            <Tooltip title="열기">
              <Button size="small" type="primary" icon={<LinkOutlined />} onClick={() => {
                const url = record.access_url!.startsWith('/jupyter')
                  ? record.access_url!.replace(/^\/jupyter/, JUPYTER_DIRECT_URL) : record.access_url!;
                window.open(url, '_blank');
              }}>열기</Button>
            </Tooltip>
          )}
          {record.status !== 'running' ? (
            <Tooltip title="시작"><Button size="small" icon={<PlayCircleOutlined />} loading={actionLoading === record.id} onClick={() => handleStartContainer(record.full_id)} /></Tooltip>
          ) : (
            <Tooltip title="중지"><Button size="small" icon={<StopOutlined />} loading={actionLoading === record.id} disabled={record.is_protected} onClick={() => handleStopContainer(record.full_id)} /></Tooltip>
          )}
          <Tooltip title="삭제"><Button size="small" danger icon={<DeleteOutlined />} disabled={record.is_protected} onClick={() => handleDeleteContainer(record.full_id, record.name)} /></Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<CloudServerOutlined />} onClick={() => setCreateModalOpen(true)}>새 컨테이너 생성</Button>
        <Button icon={<ReloadOutlined />} onClick={fetchContainers}>새로고침</Button>
      </Space>
      <Table columns={containerColumns} dataSource={containers} pagination={false} rowKey="id" loading={loading} locale={{ emptyText: <Empty description="Docker 컨테이너가 없습니다" /> }} />

      {/* 컨테이너 생성 모달 */}
      <Modal title="새 JupyterLab 컨테이너 생성" open={createModalOpen} onCancel={() => { setCreateModalOpen(false); createForm.resetFields(); }} onOk={() => createForm.submit()} okText="생성" cancelText="취소">
        <Form form={createForm} layout="vertical" onFinish={handleCreateContainer} initialValues={{ cpu_limit: 2, memory_limit: '4g' }}>
          <Form.Item name="name" label="컨테이너 이름" rules={[{ required: true, message: '이름을 입력하세요' }, { pattern: /^[a-z0-9][a-z0-9_.-]*$/, message: '소문자, 숫자, -, _, .만 사용 가능' }]}>
            <Input prefix="asan-" placeholder="my-analysis" />
          </Form.Item>
          <Form.Item name="cpu_limit" label="CPU 제한 (cores)"><InputNumber min={1} max={16} step={1} style={{ width: '100%' }} /></Form.Item>
          <Form.Item name="memory_limit" label="메모리 제한">
            <Select options={[{ value: '2g', label: '2 GB' }, { value: '4g', label: '4 GB' }, { value: '8g', label: '8 GB' }, { value: '16g', label: '16 GB' }]} />
          </Form.Item>
          <Form.Item name="template_id" label="템플릿 (선택)"><Select allowClear placeholder="템플릿 없음" options={templates.map(t => ({ value: t.id, label: t.name }))} /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ContainerManager;
