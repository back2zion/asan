import React, { useState, useEffect, useCallback } from 'react';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Badge, InputNumber, Form, Input, Select, Steps, Radio, Popconfirm,
  Drawer,
} from 'antd';
import {
  DatabaseOutlined, FileOutlined, ApiOutlined, CloudServerOutlined,
  ThunderboltOutlined, EditOutlined, DeleteOutlined, PlusOutlined,
  ReloadOutlined, ClusterOutlined, CloudOutlined, FileTextOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function putJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const SOURCE_TYPE_ICON: Record<string, React.ReactNode> = {
  rdbms: <DatabaseOutlined style={{ color: '#005BAC' }} />,
  nosql: <ClusterOutlined style={{ color: '#13c2c2' }} />,
  file: <FileOutlined style={{ color: '#52c41a' }} />,
  api: <ApiOutlined style={{ color: '#722ed1' }} />,
  cloud: <CloudOutlined style={{ color: '#1890ff' }} />,
  log: <FileTextOutlined style={{ color: '#faad14' }} />,
};
const STATUS_COLOR: Record<string, string> = {
  connected: 'green', available: 'blue', configured: 'default', error: 'red', unavailable: 'orange',
};

const HeterogeneousSourcesTab: React.FC = () => {
  const { message } = App.useApp();
  const [sources, setSources] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState<string | null>(null);
  const [typeRegistry, setTypeRegistry] = useState<any>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editSource, setEditSource] = useState<any>(null);
  const [addStep, setAddStep] = useState(0);
  const [selectedType, setSelectedType] = useState<string>('');
  const [selectedSubtype, setSelectedSubtype] = useState<string>('');
  const [form] = Form.useForm();

  const loadSources = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/sources`);
      setSources(data.sources || []);
    } catch { message.error('소스 목록 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadTypeRegistry = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/sources/types`);
      setTypeRegistry(data.types || {});
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadSources(); loadTypeRegistry(); }, [loadSources, loadTypeRegistry]);

  const testConnection = async (sourceId: string) => {
    setTesting(sourceId);
    try {
      const data = await postJSON(`${API_BASE}/sources/test`, { source_id: sourceId });
      if (data.success) message.success(data.message);
      else message.warning(data.message);
      loadSources();
    } catch { message.error('연결 테스트 실패'); }
    finally { setTesting(null); }
  };

  const handleDelete = async (sourceId: string) => {
    try {
      await deleteJSON(`${API_BASE}/sources/${sourceId}`);
      message.success('커넥터 삭제됨');
      loadSources();
    } catch (e: any) {
      message.error(e.message || '삭제 실패');
    }
  };

  const handleAddOpen = () => {
    setEditSource(null);
    setAddStep(0);
    setSelectedType('');
    setSelectedSubtype('');
    form.resetFields();
    setDrawerOpen(true);
  };

  const handleEditOpen = (source: any) => {
    setEditSource(source);
    setSelectedType(source.type);
    setSelectedSubtype(source.subtype);
    form.setFieldsValue({
      name: source.name,
      host: source.host,
      port: source.port,
      database: source.database,
      username: source.username,
      password: source.password,
      path: source.path,
      url: source.url,
      description: source.description,
    });
    setAddStep(2);
    setDrawerOpen(true);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      if (editSource) {
        await putJSON(`${API_BASE}/sources/${editSource.id}`, values);
        message.success('커넥터 수정됨');
      } else {
        await postJSON(`${API_BASE}/sources`, {
          ...values,
          type: selectedType,
          subtype: selectedSubtype,
        });
        message.success('커넥터 생성됨');
      }
      setDrawerOpen(false);
      loadSources();
    } catch (e: any) {
      if (e.errorFields) return;
      message.error(e.message || '저장 실패');
    }
  };

  const getSubtypeInfo = () => {
    if (!typeRegistry || !selectedType || !selectedSubtype) return null;
    return typeRegistry[selectedType]?.subtypes?.[selectedSubtype];
  };

  const columns = [
    {
      title: '소스', key: 'name', width: 260,
      render: (_: any, r: any) => (
        <Space>
          {SOURCE_TYPE_ICON[r.type] || <DatabaseOutlined />}
          <div>
            <Text strong>{r.name}</Text>
            <div><Text type="secondary" style={{ fontSize: 11 }}>{r.description}</Text></div>
          </div>
        </Space>
      ),
    },
    { title: '유형', dataIndex: 'type', key: 'type', width: 80, render: (v: string) => <Tag>{v.toUpperCase()}</Tag> },
    { title: '서브타입', dataIndex: 'subtype', key: 'subtype', width: 100, render: (v: string) => <Tag color="blue">{v}</Tag> },
    {
      title: '상태', dataIndex: 'status', key: 'status', width: 100,
      render: (v: string) => <Badge status={v === 'connected' ? 'success' : v === 'error' ? 'error' : 'default'} text={<Tag color={STATUS_COLOR[v]}>{v}</Tag>} />,
    },
    { title: '테이블 수', dataIndex: 'tables', key: 'tables', width: 90, render: (v: number) => v > 0 ? v : '-' },
    {
      title: '마지막 동기화', dataIndex: 'last_sync', key: 'last_sync', width: 140,
      render: (v: string | null) => v ? dayjs(v).format('MM-DD HH:mm') : <Text type="secondary">미동기화</Text>,
    },
    {
      title: '작업', key: 'action', width: 240,
      render: (_: any, r: any) => (
        <Space size="small">
          <Button size="small" icon={<ThunderboltOutlined />} loading={testing === r.id} onClick={() => testConnection(r.id)}>
            테스트
          </Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => handleEditOpen(r)}>편집</Button>
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(r.id)} disabled={r.id === 'omop-cdm'}>
            <Button size="small" icon={<DeleteOutlined />} danger disabled={r.id === 'omop-cdm'}>삭제</Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const typeSummary = sources.reduce((acc: Record<string, number>, s) => { acc[s.type] = (acc[s.type] || 0) + 1; return acc; }, {});

  const subtypeInfo = getSubtypeInfo();

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={[16, 16]}>
        <Col xs={8} md={4}>
          <Card size="small"><Statistic title="전체 소스" value={sources.length} prefix={<DatabaseOutlined />} /></Card>
        </Col>
        <Col xs={8} md={4}>
          <Card size="small"><Statistic title="연결됨" value={sources.filter(s => s.status === 'connected').length} valueStyle={{ color: '#52c41a' }} /></Card>
        </Col>
        <Col xs={8} md={16}>
          <Card size="small">
            <Space wrap>
              {Object.entries(typeSummary).map(([t, c]) => (
                <Tag key={t} icon={SOURCE_TYPE_ICON[t]}>{t.toUpperCase()}: {c as React.ReactNode}</Tag>
              ))}
            </Space>
          </Card>
        </Col>
      </Row>

      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Button icon={<ReloadOutlined />} onClick={loadSources}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAddOpen}>소스 추가</Button>
      </div>

      <Table dataSource={sources} columns={columns} rowKey="id" size="small" loading={loading} pagination={false} />

      <Drawer
        title={editSource ? '소스 편집' : '소스 추가'}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={500}
        extra={addStep >= 2 || editSource ? <Button type="primary" onClick={handleSave}>저장</Button> : undefined}
      >
        {!editSource && (
          <Steps current={addStep} size="small" style={{ marginBottom: 16 }} items={[
            { title: '유형 선택' }, { title: '서브타입' }, { title: '설정' },
          ]} />
        )}

        {!editSource && addStep === 0 && typeRegistry && (
          <Radio.Group onChange={e => { setSelectedType(e.target.value); setAddStep(1); }} style={{ width: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              {Object.entries(typeRegistry).map(([key, val]: [string, any]) => (
                <Radio.Button key={key} value={key} style={{ width: '100%', height: 'auto', padding: 12 }}>
                  <Space>{SOURCE_TYPE_ICON[key]}<div><Text strong>{val.label}</Text><br /><Text type="secondary" style={{ fontSize: 11 }}>{val.description}</Text></div></Space>
                </Radio.Button>
              ))}
            </Space>
          </Radio.Group>
        )}

        {!editSource && addStep === 1 && typeRegistry?.[selectedType] && (
          <Radio.Group onChange={e => { setSelectedSubtype(e.target.value); setAddStep(2); }} style={{ width: '100%' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              {Object.entries(typeRegistry[selectedType].subtypes || {}).map(([key, val]: [string, any]) => (
                <Radio.Button key={key} value={key} style={{ width: '100%', height: 'auto', padding: 12 }}>
                  <Text strong>{val.label || key}</Text>
                </Radio.Button>
              ))}
            </Space>
          </Radio.Group>
        )}

        {(addStep >= 2 || editSource) && (
          <Form form={form} layout="vertical">
            <Form.Item name="name" label="커넥터 이름" rules={[{ required: true }]}>
              <Input placeholder="e.g. AMIS PostgreSQL" />
            </Form.Item>
            <Form.Item name="host" label="호스트">
              <Input placeholder="localhost" />
            </Form.Item>
            <Row gutter={16}>
              <Col span={12}><Form.Item name="port" label="포트"><InputNumber style={{ width: '100%' }} /></Form.Item></Col>
              <Col span={12}><Form.Item name="database" label="데이터베이스"><Input /></Form.Item></Col>
            </Row>
            <Form.Item name="username" label="사용자명"><Input /></Form.Item>
            <Form.Item name="password" label="비밀번호"><Input.Password /></Form.Item>
            <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          </Form>
        )}
      </Drawer>
    </Space>
  );
};

export default HeterogeneousSourcesTab;
