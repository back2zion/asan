/**
 * SecurityManagement sub-component: Masking Rules, Access Audit Log
 * Sections 7-8 of DGR-005
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Button, Modal, Form, Input, Select, Tag, Space,
  Statistic, Row, Col, Switch, Typography, Timeline, Popconfirm, message,
} from 'antd';
import {
  PlusOutlined, ReloadOutlined, AuditOutlined,
} from '@ant-design/icons';

const { TextArea } = Input;
const { Text } = Typography;
const BASE = '/api/v1/security-mgmt';

const fetchJson = async (url: string) => { const r = await fetch(url); return r.json(); };
const postJson = async (url: string, body?: any) => {
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  return r.json();
};
const deleteJson = async (url: string) => { await fetch(url, { method: 'DELETE' }); };

const RESULT_COLORS: Record<string, string> = { allowed: 'green', denied: 'red', masked: 'orange', filtered: 'blue' };
const MASKING_LEVEL_COLORS: Record<string, string> = { row: 'red', column: 'orange', cell: 'blue' };

// ---------------------------------------------------------------
// 7. Masking Rules (마스킹 규칙)
// ---------------------------------------------------------------
export const MaskingRulesView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/masking-rules`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const columns = [
    { title: '규칙명', dataIndex: 'name', width: 200, ellipsis: true },
    { title: '마스킹 단계', dataIndex: 'masking_level', width: 100, render: (v: string) => <Tag color={MASKING_LEVEL_COLORS[v]}>{v.toUpperCase()}</Tag> },
    { title: '테이블', dataIndex: 'target_table', width: 160 },
    { title: '컬럼', dataIndex: 'target_column', width: 160, render: (v: string) => v || <Text type="secondary">전체</Text> },
    { title: '방법', dataIndex: 'masking_method', width: 120 },
    { title: '조건', dataIndex: 'condition', width: 200, ellipsis: true, render: (v: any) => v ? <Text code style={{ fontSize: 11 }}>{typeof v === 'string' ? v : JSON.stringify(v)}</Text> : '-' },
    { title: '활성', dataIndex: 'enabled', width: 60, render: (v: boolean) => <Switch checked={v} size="small" disabled /> },
    {
      title: '', width: 60, render: (_: any, r: any) => (
        <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/masking-rules/${r.rule_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
      ),
    },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Card><Statistic title="Row-Level 규칙" value={data.filter(d => d.masking_level === 'row').length} valueStyle={{ color: '#f5222d' }} /></Card>
        </Col>
        <Col span={8}>
          <Card><Statistic title="Column-Level 규칙" value={data.filter(d => d.masking_level === 'column').length} valueStyle={{ color: '#fa8c16' }} /></Card>
        </Col>
        <Col span={8}>
          <Card><Statistic title="Cell-Level 규칙" value={data.filter(d => d.masking_level === 'cell').length} valueStyle={{ color: '#1890ff' }} /></Card>
        </Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>마스킹 규칙 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="rule_id" size="small" loading={loading} pagination={false} scroll={{ x: 1100 }} />
      <Modal title="마스킹 규칙 추가" open={modalOpen} onOk={async () => {
        const vals = await form.validateFields();
        if (vals.condition_str) { try { vals.condition = JSON.parse(vals.condition_str); } catch { vals.condition = null; } }
        if (vals.parameters_str) { try { vals.parameters = JSON.parse(vals.parameters_str); } catch { vals.parameters = null; } }
        delete vals.condition_str; delete vals.parameters_str;
        await postJson(`${BASE}/masking-rules`, vals);
        setModalOpen(false); load(); message.success('생성 완료');
      }} onCancel={() => setModalOpen(false)} width={550}>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="규칙명" rules={[{ required: true }]}><Input /></Form.Item>
          <Row gutter={12}>
            <Col span={8}><Form.Item name="masking_level" label="마스킹 단계" rules={[{ required: true }]}><Select options={['row', 'column', 'cell'].map(v => ({ value: v, label: v.toUpperCase() }))} /></Form.Item></Col>
            <Col span={8}><Form.Item name="target_table" label="테이블" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="target_column" label="컬럼"><Input /></Form.Item></Col>
          </Row>
          <Form.Item name="masking_method" label="마스킹 방법" rules={[{ required: true }]}>
            <Select options={['hash', 'code_map', 'rounding', 'partial_mask', 'regex_mask', 'range_bucket', 'row_filter', 'redact'].map(v => ({ value: v, label: v }))} />
          </Form.Item>
          <Form.Item name="condition_str" label="조건 (JSON, 선택)"><TextArea rows={2} placeholder='{"user_role":"분석가"}' /></Form.Item>
          <Form.Item name="parameters_str" label="파라미터 (JSON, 선택)"><TextArea rows={2} placeholder='{"algorithm":"SHA-256"}' /></Form.Item>
          <Form.Item name="enabled" label="활성" valuePropName="checked" initialValue={true}><Switch /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ---------------------------------------------------------------
// 8. Access Audit Log (접근 로그)
// ---------------------------------------------------------------
export const AccessLogsView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [loading, setLoading] = useState(false);
  const [filterResult, setFilterResult] = useState<string | undefined>(undefined);

  const load = useCallback(async (result?: string) => {
    setLoading(true);
    try {
      const params = result ? `?result=${result}` : '';
      const [logs, st] = await Promise.all([
        fetchJson(`${BASE}/access-logs${params}`),
        fetchJson(`${BASE}/access-logs/stats`),
      ]);
      setData(logs);
      setStats(st);
    } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const columns = [
    { title: '시간', dataIndex: 'created_at', width: 150, render: (v: string) => new Date(v).toLocaleString() },
    { title: '사용자', dataIndex: 'user_name', width: 80 },
    { title: 'ID', dataIndex: 'user_id', width: 70 },
    { title: '액션', dataIndex: 'action', width: 70, render: (v: string) => <Tag>{v}</Tag> },
    { title: '테이블', dataIndex: 'target_table', width: 160 },
    { title: '컬럼', dataIndex: 'target_column', width: 140, render: (v: string) => v || '-' },
    { title: '결과', dataIndex: 'result', width: 80, render: (v: string) => <Tag color={RESULT_COLORS[v]}>{v}</Tag> },
    { title: 'IP', dataIndex: 'ip_address', width: 110 },
    { title: '컨텍스트', dataIndex: 'context', width: 200, ellipsis: true, render: (v: any) => <Text code style={{ fontSize: 11 }}>{typeof v === 'string' ? v : JSON.stringify(v)}</Text> },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={5}><Card><Statistic title="전체 로그" value={stats.total || 0} prefix={<AuditOutlined />} /></Card></Col>
        <Col span={5}><Card><Statistic title="허용" value={stats.by_result?.find((r: any) => r.result === 'allowed')?.cnt || 0} valueStyle={{ color: '#52c41a' }} /></Card></Col>
        <Col span={5}><Card><Statistic title="차단" value={stats.denied_count || 0} valueStyle={{ color: '#f5222d' }} /></Card></Col>
        <Col span={5}><Card><Statistic title="마스킹" value={stats.by_result?.find((r: any) => r.result === 'masked')?.cnt || 0} valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="필터링" value={stats.by_result?.find((r: any) => r.result === 'filtered')?.cnt || 0} valueStyle={{ color: '#1890ff' }} /></Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between' }}>
        <Space>
          <Select value={filterResult} onChange={(v) => { setFilterResult(v); load(v); }} allowClear placeholder="결과 필터" style={{ width: 120 }}
            options={[{ value: 'allowed', label: '허용' }, { value: 'denied', label: '차단' }, { value: 'masked', label: '마스킹' }, { value: 'filtered', label: '필터링' }]} />
        </Space>
        <Button icon={<ReloadOutlined />} onClick={() => load(filterResult)}>새로고침</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="log_id" size="small" loading={loading} pagination={{ pageSize: 20 }} scroll={{ x: 1200 }} />
      {stats.recent_denied?.length > 0 && (
        <Card title="최근 차단 이벤트" size="small" style={{ marginTop: 16 }}>
          <Timeline items={stats.recent_denied.map((d: any) => ({
            color: 'red',
            children: (
              <span>
                <Text strong>{d.user_name}</Text> ({d.user_id}) - <Tag color="red">{d.action}</Tag> {d.target_table}
                {d.target_column && `.${d.target_column}`}
                <Text type="secondary" style={{ marginLeft: 8 }}>{new Date(d.created_at).toLocaleString()}</Text>
              </span>
            ),
          }))} />
        </Card>
      )}
    </div>
  );
};
