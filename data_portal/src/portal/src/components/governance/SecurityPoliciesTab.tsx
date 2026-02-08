/**
 * SecurityManagement sub-component: Policies, Term-based Security, Biz Metadata Security
 * Sections 1-3 of DGR-005
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Button, Modal, Form, Input, Select, Tag, Space,
  Statistic, Row, Col, Badge, Switch, Typography, Alert, Popconfirm, App,
} from 'antd';
import {
  PlusOutlined, ReloadOutlined, ThunderboltOutlined,
  CheckCircleOutlined, CloseCircleOutlined,
} from '@ant-design/icons';
import { fetchPost, fetchPut, fetchDelete } from '../../services/apiUtils';

const { TextArea } = Input;
const { Text } = Typography;
const BASE = '/api/v1/security-mgmt';

const fetchJson = async (url: string) => { const r = await fetch(url); return r.json(); };
const postJson = async (url: string, body?: any) => {
  const r = await fetchPost(url, body);
  return r.json();
};
const putJson = async (url: string, body?: any) => {
  const r = await fetchPut(url, body);
  return r.json();
};
const deleteJson = async (url: string) => { await fetchDelete(url); };

const LEVEL_COLORS: Record<string, string> = { '극비': 'red', '민감': 'orange', '일반': 'blue', '공개': 'green' };

// ---------------------------------------------------------------
// 1. Security Policies (보안 정책)
// ---------------------------------------------------------------
export const SecurityPoliciesView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [overview, setOverview] = useState<any>({});
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [policies, ov] = await Promise.all([
        fetchJson(`${BASE}/policies`),
        fetchJson(`${BASE}/policies/overview`),
      ]);
      setData(policies);
      setOverview(ov);
    } finally { setLoading(false); }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleSave = async () => {
    const vals = await form.validateFields();
    if (editItem) {
      await putJson(`${BASE}/policies/${editItem.policy_id}`, vals);
    } else {
      await postJson(`${BASE}/policies`, vals);
    }
    setModalOpen(false);
    form.resetFields();
    setEditItem(null);
    load();
    message.success(editItem ? '수정 완료' : '생성 완료');
  };

  const columns = [
    { title: '대상', dataIndex: 'target_type', width: 70, render: (v: string) => <Tag color={v === 'table' ? 'purple' : 'cyan'}>{v}</Tag> },
    { title: '테이블', dataIndex: 'target_table', width: 160 },
    { title: '컬럼', dataIndex: 'target_column', width: 160, render: (v: string) => v || <Text type="secondary">전체</Text> },
    { title: '보안유형', dataIndex: 'security_type', width: 110 },
    { title: '등급', dataIndex: 'security_level', width: 70, render: (v: string) => <Tag color={LEVEL_COLORS[v]}>{v}</Tag> },
    { title: '법적근거', dataIndex: 'legal_basis', width: 160, ellipsis: true },
    { title: '비식별방법', dataIndex: 'deident_method', width: 120 },
    { title: '적용시점', dataIndex: 'deident_timing', width: 90 },
    { title: '활성', dataIndex: 'enabled', width: 60, render: (v: boolean) => v ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#ccc' }} /> },
    {
      title: '', width: 100, render: (_: any, r: any) => (
        <Space>
          <Button size="small" onClick={() => { setEditItem(r); form.setFieldsValue(r); setModalOpen(true); }}>편집</Button>
          <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/policies/${r.policy_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={4}><Card><Statistic title="전체 정책" value={overview.total || 0} /></Card></Col>
        <Col span={4}><Card><Statistic title="극비" value={overview.by_level?.find((l: any) => l.security_level === '극비')?.cnt || 0} valueStyle={{ color: '#f5222d' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="민감" value={overview.by_level?.find((l: any) => l.security_level === '민감')?.cnt || 0} valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="일반" value={overview.by_level?.find((l: any) => l.security_level === '일반')?.cnt || 0} valueStyle={{ color: '#1890ff' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="테이블 단위" value={overview.by_target?.find((t: any) => t.target_type === 'table')?.cnt || 0} /></Card></Col>
        <Col span={4}><Card><Statistic title="컬럼 단위" value={overview.by_target?.find((t: any) => t.target_type === 'column')?.cnt || 0} /></Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setEditItem(null); form.resetFields(); setModalOpen(true); }}>정책 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="policy_id" size="small" loading={loading} pagination={{ pageSize: 15 }} scroll={{ x: 1200 }} />
      <Modal title={editItem ? '보안 정책 수정' : '보안 정책 추가'} open={modalOpen} onOk={handleSave} onCancel={() => setModalOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={8}><Form.Item name="target_type" label="대상 유형" rules={[{ required: true }]}><Select options={[{ value: 'table', label: '테이블' }, { value: 'column', label: '컬럼' }]} /></Form.Item></Col>
            <Col span={8}><Form.Item name="target_table" label="테이블명" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="target_column" label="컬럼명"><Input placeholder="전체(테이블 단위)" /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={8}><Form.Item name="security_type" label="보안유형" rules={[{ required: true }]}><Select options={['PII', '준식별자', '민감의료정보', '진단정보', '처방정보', '검사결과', '방문기록', '자유텍스트', '관찰기록'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={8}><Form.Item name="security_level" label="등급" rules={[{ required: true }]}><Select options={['극비', '민감', '일반', '공개'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={8}><Form.Item name="deident_method" label="비식별방법"><Select allowClear options={['SHA-256 해시', '코드변환', '라운딩', '패턴마스킹', '범위변환', '접근제한', 'Row-level 필터링', '날짜 이동', '접근통제'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="legal_basis" label="법적근거"><Input placeholder="개인정보보호법 제23조" /></Form.Item></Col>
            <Col span={12}><Form.Item name="deident_timing" label="적용시점"><Select allowClear options={['수집 즉시', '분석 시', '조회 시', '상시'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><TextArea rows={2} /></Form.Item>
          <Form.Item name="enabled" label="활성" valuePropName="checked" initialValue={true}><Switch /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ---------------------------------------------------------------
// 2. Term-based Security Rules (용어 기반 보안)
// ---------------------------------------------------------------
export const TermSecurityView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [propagating, setPropagating] = useState(false);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/term-rules`)); } finally { setLoading(false); }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handlePropagate = async () => {
    setPropagating(true);
    try {
      const result = await postJson(`${BASE}/term-rules/propagate`);
      message.success(`${result.total_new}개 보안 정책 자동 전파 완료`);
      load();
    } finally { setPropagating(false); }
  };

  const columns = [
    { title: '표준 용어', dataIndex: 'term_name', width: 100 },
    { title: '보안유형', dataIndex: 'security_type', width: 100 },
    { title: '등급', dataIndex: 'security_level', width: 70, render: (v: string) => <Tag color={LEVEL_COLORS[v]}>{v}</Tag> },
    { title: '비식별방법', dataIndex: 'deident_method', width: 130 },
    { title: '자동전파', dataIndex: 'auto_propagate', width: 80, render: (v: boolean) => v ? <Tag color="green">활성</Tag> : <Tag>비활성</Tag> },
    { title: '적용 수', dataIndex: 'applied_count', width: 80, render: (v: number) => <Badge count={v} showZero style={{ backgroundColor: v > 0 ? '#52c41a' : '#d9d9d9' }} /> },
    { title: '설명', dataIndex: 'description', ellipsis: true },
    {
      title: '', width: 60, render: (_: any, r: any) => (
        <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/term-rules/${r.rule_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
      ),
    },
  ];

  return (
    <div>
      <Alert message="표준 용어 메타데이터 기반으로 같은 용도의 컬럼에 동일 보안 규정을 자동 전파합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ThunderboltOutlined />} loading={propagating} onClick={handlePropagate} type="primary" ghost>전체 자동 전파</Button>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { form.resetFields(); setModalOpen(true); }}>규칙 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="rule_id" size="small" loading={loading} pagination={false} />
      <Modal title="용어 보안 규칙 추가" open={modalOpen} onCancel={() => setModalOpen(false)} onOk={async () => {
        const vals = await form.validateFields();
        await postJson(`${BASE}/term-rules`, vals);
        setModalOpen(false); load(); message.success('생성 완료');
      }}>
        <Form form={form} layout="vertical">
          <Form.Item name="term_name" label="표준 용어" rules={[{ required: true }]}><Input placeholder="환자, 진단, 처방 등" /></Form.Item>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="security_type" label="보안유형" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="security_level" label="등급" rules={[{ required: true }]}><Select options={['극비', '민감', '일반', '공개'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="deident_method" label="비식별방법"><Input /></Form.Item>
          <Form.Item name="auto_propagate" label="자동전파" valuePropName="checked" initialValue={true}><Switch /></Form.Item>
          <Form.Item name="description" label="설명"><TextArea rows={2} /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ---------------------------------------------------------------
// 3. Biz Metadata Security (Biz 메타 보안)
// ---------------------------------------------------------------
export const BizSecurityView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/biz-security`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleSave = async () => {
    const vals = await form.validateFields();
    if (editItem) { await putJson(`${BASE}/biz-security/${editItem.meta_id}`, vals); }
    else { await postJson(`${BASE}/biz-security`, vals); }
    setModalOpen(false); form.resetFields(); setEditItem(null); load();
    message.success(editItem ? '수정 완료' : '생성 완료');
  };

  const columns = [
    { title: '테이블', dataIndex: 'table_name', width: 160 },
    { title: '컬럼', dataIndex: 'column_name', width: 160, render: (v: string) => v || '-' },
    { title: '보안유형', dataIndex: 'security_type', width: 100 },
    { title: '비식별 대상', dataIndex: 'deident_target', width: 90, render: (v: boolean) => v ? <Tag color="red">대상</Tag> : <Tag>비대상</Tag> },
    { title: '시점', dataIndex: 'deident_timing', width: 80 },
    { title: '수준', dataIndex: 'deident_level', width: 90 },
    { title: '방법', dataIndex: 'deident_method', width: 100 },
    { title: '보존기간', dataIndex: 'retention_period', width: 80 },
    { title: '접근범위', dataIndex: 'access_scope', width: 80 },
    {
      title: '', width: 100, render: (_: any, r: any) => (
        <Space>
          <Button size="small" onClick={() => { setEditItem(r); form.setFieldsValue(r); setModalOpen(true); }}>편집</Button>
          <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/biz-security/${r.meta_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Alert message="Biz 메타데이터에서 보안 유형, 비식별 처리 대상/시점/수준/방법 등 관리 속성을 정의합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setEditItem(null); form.resetFields(); setModalOpen(true); }}>추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="meta_id" size="small" loading={loading} pagination={{ pageSize: 15 }} scroll={{ x: 1200 }} />
      <Modal title={editItem ? 'Biz 보안 메타 수정' : 'Biz 보안 메타 추가'} open={modalOpen} onOk={handleSave} onCancel={() => setModalOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={8}><Form.Item name="table_name" label="테이블" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="column_name" label="컬럼"><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="security_type" label="보안유형" rules={[{ required: true }]}><Select options={['PII', '준식별자', '민감의료정보', '진단정보', '처방정보', '검사결과', '자유텍스트', '관찰기록', '방문기록'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={6}><Form.Item name="deident_target" label="비식별 대상" valuePropName="checked"><Switch /></Form.Item></Col>
            <Col span={6}><Form.Item name="deident_timing" label="시점"><Select allowClear options={['수집 즉시', '분석 시', '조회 시', '상시', '해당없음'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={6}><Form.Item name="deident_level" label="수준"><Select allowClear options={['완전비식별', '부분비식별', '접근제한', '해당없음'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={6}><Form.Item name="deident_method" label="방법"><Select allowClear options={['SHA-256 해시', '코드변환', '라운딩(5년)', '패턴마스킹', '접근통제', '날짜 이동', '해당없음'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="retention_period" label="보존기간"><Select allowClear options={['영구보존', '10년', '5년', '3년', '1년'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="access_scope" label="접근범위"><Select allowClear options={['관리자', '임상의+', '연구자+', '분석가+', '전체'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><TextArea rows={2} /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};
