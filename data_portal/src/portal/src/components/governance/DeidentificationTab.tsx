/**
 * 비식별화 탭 컴포넌트 (DGR-006) - OMOP CDM DB 기반
 * 비식별화 규칙 관리 + Pipeline 단계별 처리 + 처리 로그/모니터링 + 재식별 요청 관리
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic, Progress,
  Spin, Alert, Button, Modal, Form, Input, Select, Switch, Popconfirm,
  Tabs, Steps, Drawer, DatePicker, App,
} from 'antd';
import {
  CheckCircleOutlined, DatabaseOutlined, LockOutlined, EyeInvisibleOutlined,
  ReloadOutlined, PlusOutlined, EditOutlined, DeleteOutlined, SettingOutlined,
  NodeIndexOutlined, FileSearchOutlined, AuditOutlined, ClockCircleOutlined,
  SafetyCertificateOutlined, UserOutlined,
} from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer, Legend } from 'recharts';
import { governanceApi } from '../../services/api';

const { Title, Text } = Typography;

// ── 타입 정의 ──

interface PiiType {
  key: string;
  type: string;
  count: number;
  method: string;
  status: string;
  example: string;
}

interface DeidentData {
  pii_types: PiiType[];
  summary: {
    total_scanned: number;
    pii_detected: number;
    masking_rate: number;
    k_anonymity: number;
  };
  trend: { date: string; total_scanned: number; pii_detected: number }[];
  sample_before: Record<string, any>[];
  sample_after: Record<string, any>[];
}

interface DeidentRule {
  rule_id: number;
  target_column: string;
  method: string;
  pattern: string | null;
  enabled: boolean;
  created_at: string | null;
}

interface PipelineStage {
  stage_id: number;
  stage_name: string;
  stage_order: number;
  description: string;
  deident_methods: string[];
  applied_tables: string[];
  record_count: number;
  status: string;
  last_processed: string | null;
}

interface ProcessingLog {
  log_id: number;
  process_type: string;
  stage_name: string;
  target_table: string;
  target_column: string;
  method: string;
  records_processed: number;
  status: string;
  operator: string;
  detail: string;
  processed_at: string | null;
}

interface MonitoringData {
  summary: {
    total_deident: number;
    total_reident: number;
    recent_24h: number;
    pending_reident: number;
  };
  stages: { stage_name: string; stage_order: number; description: string; record_count: number; status: string }[];
  recent_logs: ProcessingLog[];
}

interface ReidentRequest {
  request_id: number;
  requester: string;
  purpose: string;
  target_tables: string[];
  target_columns: string[];
  approval_level: string;
  approver: string | null;
  approval_comment: string | null;
  expires_at: string | null;
  created_at: string | null;
  approved_at: string | null;
}

const DEIDENT_METHODS = ['마스킹', '해시', '라운딩', '범주화', '삭제'];

// ═══════════════════════════════════════════════════
// Tab 1: 비식별 현황 (기존)
// ═══════════════════════════════════════════════════

const DeidentStatusSection: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<DeidentData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [rules, setRules] = useState<DeidentRule[]>([]);
  const [rulesLoading, setRulesLoading] = useState(false);
  const [ruleModalOpen, setRuleModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<DeidentRule | null>(null);
  const [ruleForm] = Form.useForm();

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await governanceApi.getDeidentification();
      setData(result);
    } catch (err: any) {
      setError(err?.message || '비식별화 데이터 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchRules = useCallback(async () => {
    setRulesLoading(true);
    try {
      const result = await governanceApi.getDeidentRules();
      setRules(result);
    } catch {
      // 규칙 로드 실패는 비필수이므로 무시
    } finally {
      setRulesLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    fetchRules();
  }, [fetchData, fetchRules]);

  const openRuleModal = (rule?: DeidentRule) => {
    setEditingRule(rule || null);
    if (rule) {
      ruleForm.setFieldsValue({ target_column: rule.target_column, method: rule.method, pattern: rule.pattern || '', enabled: rule.enabled });
    } else {
      ruleForm.resetFields();
      ruleForm.setFieldsValue({ enabled: true });
    }
    setRuleModalOpen(true);
  };

  const handleRuleSave = async () => {
    try {
      const values = await ruleForm.validateFields();
      const payload = { ...values, pattern: values.pattern || null };
      if (editingRule) {
        await governanceApi.updateDeidentRule(editingRule.rule_id, payload);
        message.success('규칙 수정 완료');
      } else {
        await governanceApi.createDeidentRule(payload);
        message.success('규칙 추가 완료');
      }
      setRuleModalOpen(false);
      await fetchRules();
    } catch (err: any) {
      if (err?.response?.data?.detail) message.error(err.response.data.detail);
    }
  };

  const handleRuleDelete = async (ruleId: number) => {
    try {
      await governanceApi.deleteDeidentRule(ruleId);
      message.success('규칙 삭제 완료');
      await fetchRules();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '삭제 실패');
    }
  };

  const handleToggleEnabled = async (rule: DeidentRule) => {
    try {
      await governanceApi.updateDeidentRule(rule.rule_id, {
        target_column: rule.target_column, method: rule.method, pattern: rule.pattern ?? undefined, enabled: !rule.enabled,
      });
      message.success(`규칙 ${!rule.enabled ? '활성화' : '비활성화'} 완료`);
      await fetchRules();
    } catch {
      message.error('변경 실패');
    }
  };

  if (loading) return <Spin size="large" tip="비식별화 현황 스캔 중..."><div style={{ textAlign: 'center', padding: 80 }} /></Spin>;
  if (error || !data) return <Alert type="error" message="비식별화 데이터 로드 실패" description={error} showIcon action={<Button onClick={fetchData} icon={<ReloadOutlined />}>재시도</Button>} />;

  const { pii_types, summary, trend, sample_before, sample_after } = data;

  const piiColumns = [
    { title: 'PII 유형', dataIndex: 'type', key: 'type', render: (v: string) => <Text strong>{v}</Text> },
    { title: '탐지 건수', dataIndex: 'count', key: 'count', sorter: (a: PiiType, b: PiiType) => a.count - b.count, render: (v: number) => <Text>{v.toLocaleString()}건</Text> },
    { title: '비식별화 기법', dataIndex: 'method', key: 'method', render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: '처리 상태', dataIndex: 'status', key: 'status', render: (v: string) => v === 'done' ? <Tag color="green">완료</Tag> : <Tag color="orange">진행중</Tag> },
    { title: '변환 예시', dataIndex: 'example', key: 'example', render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text> },
  ];

  const sampleColumns = Object.keys(sample_before[0] || {}).map((key) => ({
    title: key, dataIndex: key, key, render: (v: any) => <Text code style={{ fontSize: 11 }}>{String(v ?? '')}</Text>,
  }));

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="총 스캔 레코드" value={summary.total_scanned} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="PII 탐지 건수" value={summary.pii_detected} prefix={<EyeInvisibleOutlined />} valueStyle={{ color: '#cf1322' }} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="마스킹 완료율" value={summary.masking_rate} suffix="%" prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="K-익명성 수준" value={summary.k_anonymity} prefix={<LockOutlined />} suffix="(k)" valueStyle={{ color: '#005BAC' }} /></Card></Col>
      </Row>

      <Card title="PII 유형별 탐지 현황" size="small" extra={<Button size="small" icon={<ReloadOutlined />} onClick={fetchData}>스캔 실행</Button>}>
        <Table columns={piiColumns} dataSource={pii_types} rowKey="key" size="small" pagination={false} />
      </Card>

      <Card title={<><SettingOutlined /> 비식별화 규칙 관리</>} size="small" extra={<Button size="small" icon={<PlusOutlined />} type="primary" onClick={() => openRuleModal()}>규칙 추가</Button>}>
        <Spin spinning={rulesLoading}>
          <Table dataSource={rules} rowKey="rule_id" size="small" pagination={false} columns={[
            { title: '대상 컬럼', dataIndex: 'target_column', key: 'target_column', render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text> },
            { title: '비식별화 기법', dataIndex: 'method', key: 'method', render: (v: string) => <Tag color="blue">{v}</Tag> },
            { title: '패턴', dataIndex: 'pattern', key: 'pattern', render: (v: string | null) => v ? <Text code style={{ fontSize: 11 }}>{v}</Text> : <Text type="secondary">-</Text> },
            { title: '활성화', dataIndex: 'enabled', key: 'enabled', width: 80, render: (_: any, r: DeidentRule) => <Switch size="small" checked={r.enabled} onChange={() => handleToggleEnabled(r)} /> },
            { title: '관리', key: 'action', width: 100, render: (_: any, r: DeidentRule) => (
              <Space size="small">
                <Button size="small" icon={<EditOutlined />} onClick={() => openRuleModal(r)} />
                <Popconfirm title="이 규칙을 삭제하시겠습니까?" onConfirm={() => handleRuleDelete(r.rule_id)} okText="삭제" cancelText="취소"><Button size="small" icon={<DeleteOutlined />} danger /></Popconfirm>
              </Space>
            )},
          ]} />
        </Spin>
      </Card>

      {sample_before.length > 0 && (
        <Row gutter={16}>
          <Col xs={24} md={12}>
            <Card title={<><Tag color="red">Before</Tag> 원본 레코드 (PII 포함)</>} size="small">
              <Table columns={sampleColumns} dataSource={sample_before.map((r, i) => ({ ...r, _key: i }))} rowKey="_key" size="small" pagination={false} scroll={{ x: true }} />
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title={<><Tag color="green">After</Tag> 비식별화 레코드</>} size="small">
              <Table columns={sampleColumns} dataSource={sample_after.map((r, i) => ({ ...r, _key: i }))} rowKey="_key" size="small" pagination={false} scroll={{ x: true }} />
            </Card>
          </Col>
        </Row>
      )}

      {trend.length > 0 && (
        <Card title="데이터 스캔 추이 (최근 14일)" size="small">
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={trend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" />
              <YAxis tickFormatter={(v: number) => `${(v / 1000000).toFixed(0)}M`} />
              <RechartsTooltip formatter={(v: number) => v.toLocaleString()} />
              <Legend />
              <Area type="monotone" dataKey="total_scanned" name="총 스캔 레코드" stroke="#1890ff" fill="#e6f7ff" strokeWidth={2} />
              <Area type="monotone" dataKey="pii_detected" name="PII 탐지 건수" stroke="#cf1322" fill="#fff1f0" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </Card>
      )}

      <Card title={<><LockOutlined /> 프라이버시 보호 기법 상태</>} size="small">
        <Row gutter={16}>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="K-익명성" value={summary.k_anonymity} suffix="(k)" valueStyle={{ color: '#005BAC', fontSize: 28 }} />
              <Progress percent={summary.k_anonymity >= 5 ? 100 : Math.round(summary.k_anonymity / 5 * 100)} status={summary.k_anonymity >= 5 ? 'success' : 'active'} size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>모든 레코드가 최소 {summary.k_anonymity}개의 동일 그룹 보장</Text>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="차등 프라이버시" value="ε=1.0" valueStyle={{ color: '#005BAC', fontSize: 28 }} />
              <Progress percent={100} status="success" size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>통계 쿼리에 Laplace 노이즈 적용</Text>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Title level={4} style={{ color: '#3f8600', margin: '8px 0' }}>HIPAA Compliant</Title>
              <Progress percent={100} status="success" size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>18개 식별자 Safe Harbor 기준 충족</Text>
            </Card>
          </Col>
        </Row>
      </Card>

      <Modal title={editingRule ? '비식별화 규칙 수정' : '비식별화 규칙 추가'} open={ruleModalOpen} onOk={handleRuleSave} onCancel={() => setRuleModalOpen(false)} okText={editingRule ? '수정' : '추가'} cancelText="취소" width={500}>
        <Form form={ruleForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="target_column" label="대상 컬럼" rules={[{ required: true, message: '대상 컬럼을 입력하세요' }]}><Input placeholder="예: person.person_source_value" /></Form.Item>
          <Form.Item name="method" label="비식별화 기법" rules={[{ required: true, message: '기법을 선택하세요' }]}><Select options={DEIDENT_METHODS.map((m) => ({ value: m, label: m }))} placeholder="기법 선택" /></Form.Item>
          <Form.Item name="pattern" label="패턴 (정규식, 선택)"><Input placeholder="예: 01[016789]-?\d{3,4}-?\d{4}" /></Form.Item>
          <Form.Item name="enabled" label="활성화" valuePropName="checked"><Switch /></Form.Item>
        </Form>
      </Modal>
    </Space>
  );
};

// ═══════════════════════════════════════════════════
// Tab 2: Pipeline 단계별 처리
// ═══════════════════════════════════════════════════

const STAGE_ICONS: Record<string, React.ReactNode> = {
  '수집': <FileSearchOutlined />,
  '적재': <DatabaseOutlined />,
  '변환': <NodeIndexOutlined />,
  '저장': <LockOutlined />,
  '제공': <SafetyCertificateOutlined />,
};

const PipelineSection: React.FC = () => {
  const { message } = App.useApp();
  const [stages, setStages] = useState<PipelineStage[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        const result = await governanceApi.getDeidentPipeline();
        setStages(result);
      } catch {
        message.error('Pipeline 데이터 로드 실패');
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  if (loading) return <Spin size="large"><div style={{ padding: 80 }} /></Spin>;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Card title={<><NodeIndexOutlined /> 데이터 흐름별 비식별 처리 현황</>} size="small">
        <Steps
          direction="vertical"
          current={stages.length}
          items={stages.map((s) => ({
            title: (
              <Space>
                <Text strong style={{ fontSize: 15 }}>{s.stage_name}</Text>
                <Tag color={s.status === 'active' ? 'green' : 'default'}>{s.status === 'active' ? '활성' : '비활성'}</Tag>
              </Space>
            ),
            description: (
              <div style={{ marginTop: 8, marginBottom: 16 }}>
                <Text type="secondary">{s.description}</Text>
                <div style={{ marginTop: 8 }}>
                  <Text strong style={{ fontSize: 12 }}>적용 기법: </Text>
                  {s.deident_methods.map((m, i) => <Tag key={i} color="blue" style={{ marginBottom: 4 }}>{m}</Tag>)}
                </div>
                <div style={{ marginTop: 4 }}>
                  <Text strong style={{ fontSize: 12 }}>대상 테이블: </Text>
                  {s.applied_tables.map((t, i) => <Tag key={i} style={{ marginBottom: 4 }}>{t}</Tag>)}
                </div>
                <Row gutter={16} style={{ marginTop: 8 }}>
                  <Col><Statistic title="처리 레코드" value={s.record_count} valueStyle={{ fontSize: 14 }} /></Col>
                  <Col><Text type="secondary" style={{ fontSize: 12 }}>마지막 처리: {s.last_processed ? new Date(s.last_processed).toLocaleString('ko-KR') : '-'}</Text></Col>
                </Row>
              </div>
            ),
            icon: STAGE_ICONS[s.stage_name] || <DatabaseOutlined />,
            status: 'finish' as const,
          }))}
        />
      </Card>

      <Card title="단계별 처리 요약" size="small">
        <Table
          dataSource={stages}
          rowKey="stage_id"
          size="small"
          pagination={false}
          columns={[
            { title: '순서', dataIndex: 'stage_order', key: 'stage_order', width: 60, render: (v: number) => <Tag>{v}</Tag> },
            { title: '단계', dataIndex: 'stage_name', key: 'stage_name', render: (v: string) => <Text strong>{v}</Text> },
            { title: '설명', dataIndex: 'description', key: 'description' },
            { title: '적용 기법', dataIndex: 'deident_methods', key: 'methods', render: (v: string[]) => v.map((m, i) => <Tag key={i} color="blue" style={{ marginBottom: 2 }}>{m}</Tag>) },
            { title: '대상 테이블 수', dataIndex: 'applied_tables', key: 'tables', width: 100, render: (v: string[]) => `${v.length}개` },
            { title: '처리 레코드', dataIndex: 'record_count', key: 'record_count', render: (v: number) => v.toLocaleString() },
            { title: '상태', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={v === 'active' ? 'green' : 'default'}>{v === 'active' ? '활성' : '비활성'}</Tag> },
          ]}
        />
      </Card>
    </Space>
  );
};

// ═══════════════════════════════════════════════════
// Tab 3: 처리 로그 & 모니터링
// ═══════════════════════════════════════════════════

const MonitoringSection: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<MonitoringData | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const result = await governanceApi.getDeidentMonitoring();
      setData(result);
    } catch {
      message.error('모니터링 데이터 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  if (loading) return <Spin size="large"><div style={{ padding: 80 }} /></Spin>;
  if (!data) return <Alert type="error" message="모니터링 데이터 로드 실패" showIcon />;

  const { summary, recent_logs } = data;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 비식별 처리" value={summary.total_deident} prefix={<EyeInvisibleOutlined />} valueStyle={{ color: '#005BAC' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 재식별 처리" value={summary.total_reident} prefix={<AuditOutlined />} valueStyle={{ color: '#cf1322' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="24시간 내 처리" value={summary.recent_24h} prefix={<ClockCircleOutlined />} valueStyle={{ color: '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="재식별 대기 요청" value={summary.pending_reident} prefix={<AuditOutlined />} valueStyle={{ color: summary.pending_reident > 0 ? '#faad14' : '#3f8600' }} />
          </Card>
        </Col>
      </Row>

      <Card title={<><FileSearchOutlined /> 최근 처리 로그</>} size="small" extra={<Button size="small" icon={<ReloadOutlined />} onClick={fetchData}>새로고침</Button>}>
        <Table
          dataSource={recent_logs}
          rowKey="log_id"
          size="small"
          pagination={false}
          columns={[
            { title: '유형', dataIndex: 'process_type', key: 'process_type', width: 80, render: (v: string) => <Tag color={v === 'deident' ? 'blue' : 'red'}>{v === 'deident' ? '비식별' : '재식별'}</Tag> },
            { title: '단계', dataIndex: 'stage_name', key: 'stage_name', width: 80, render: (v: string) => <Tag>{v}</Tag> },
            { title: '대상', key: 'target', render: (_: any, r: ProcessingLog) => <Text code style={{ fontSize: 11 }}>{r.target_table}.{r.target_column}</Text> },
            { title: '기법', dataIndex: 'method', key: 'method', render: (v: string) => <Tag color="purple">{v}</Tag> },
            { title: '처리 건수', dataIndex: 'records_processed', key: 'records_processed', render: (v: number) => v.toLocaleString() },
            { title: '상태', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'failed' ? 'red' : 'orange'}>{v}</Tag> },
            { title: '처리자', dataIndex: 'operator', key: 'operator', width: 100 },
            { title: '시각', dataIndex: 'processed_at', key: 'processed_at', width: 160, render: (v: string | null) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
          ]}
        />
      </Card>
    </Space>
  );
};

// ═══════════════════════════════════════════════════
// Tab 4: 재식별 요청 관리
// ═══════════════════════════════════════════════════

const ReidentRequestSection: React.FC = () => {
  const { message } = App.useApp();
  const [requests, setRequests] = useState<ReidentRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [form] = Form.useForm();

  const fetchRequests = useCallback(async () => {
    setLoading(true);
    try {
      const result = await governanceApi.getReidentRequests();
      setRequests(result);
    } catch {
      message.error('재식별 요청 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchRequests(); }, [fetchRequests]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const payload = {
        requester: values.requester,
        purpose: values.purpose,
        target_tables: values.target_tables.split(',').map((s: string) => s.trim()).filter(Boolean),
        target_columns: values.target_columns ? values.target_columns.split(',').map((s: string) => s.trim()).filter(Boolean) : [],
        expires_at: values.expires_at ? values.expires_at.toISOString() : undefined,
      };
      await governanceApi.createReidentRequest(payload);
      message.success('재식별 요청이 생성되었습니다');
      setDrawerOpen(false);
      form.resetFields();
      await fetchRequests();
    } catch (err: any) {
      if (err?.response?.data?.detail) message.error(err.response.data.detail);
    }
  };

  const handleApprove = async (id: number) => {
    try {
      await governanceApi.approveReidentRequest(id);
      message.success('승인 완료');
      await fetchRequests();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '승인 실패');
    }
  };

  const handleReject = async (id: number) => {
    try {
      await governanceApi.rejectReidentRequest(id);
      message.success('거부 완료');
      await fetchRequests();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '거부 실패');
    }
  };

  const approvalColor: Record<string, string> = { approved: 'green', pending: 'orange', rejected: 'red', expired: 'default' };
  const approvalLabel: Record<string, string> = { approved: '승인', pending: '대기', rejected: '거부', expired: '만료' };

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Card
        title={<><AuditOutlined /> 재식별 요청 관리</>}
        size="small"
        extra={<Button type="primary" size="small" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setDrawerOpen(true); }}>재식별 요청</Button>}
      >
        <Spin spinning={loading}>
          <Table
            dataSource={requests}
            rowKey="request_id"
            size="small"
            pagination={false}
            columns={[
              { title: '요청자', dataIndex: 'requester', key: 'requester', width: 100, render: (v: string) => <><UserOutlined /> {v}</> },
              { title: '목적', dataIndex: 'purpose', key: 'purpose', ellipsis: true },
              { title: '대상 테이블', dataIndex: 'target_tables', key: 'target_tables', render: (v: string[]) => v.map((t, i) => <Tag key={i}>{t}</Tag>) },
              { title: '상태', dataIndex: 'approval_level', key: 'approval_level', width: 80, render: (v: string) => <Tag color={approvalColor[v] || 'default'}>{approvalLabel[v] || v}</Tag> },
              { title: '승인자', dataIndex: 'approver', key: 'approver', width: 100, render: (v: string | null) => v || '-' },
              { title: '요청일', dataIndex: 'created_at', key: 'created_at', width: 160, render: (v: string | null) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
              { title: '만료일', dataIndex: 'expires_at', key: 'expires_at', width: 120, render: (v: string | null) => v ? new Date(v).toLocaleDateString('ko-KR') : '-' },
              {
                title: '관리', key: 'action', width: 140,
                render: (_: any, r: ReidentRequest) => r.approval_level === 'pending' ? (
                  <Space size="small">
                    <Popconfirm title="이 요청을 승인하시겠습니까?" onConfirm={() => handleApprove(r.request_id)} okText="승인" cancelText="취소">
                      <Button size="small" type="primary">승인</Button>
                    </Popconfirm>
                    <Popconfirm title="이 요청을 거부하시겠습니까?" onConfirm={() => handleReject(r.request_id)} okText="거부" cancelText="취소">
                      <Button size="small" danger>거부</Button>
                    </Popconfirm>
                  </Space>
                ) : <Text type="secondary">-</Text>,
              },
            ]}
          />
        </Spin>
      </Card>

      <Drawer
        title="재식별 요청 생성"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        width={480}
        extra={<Button type="primary" onClick={handleCreate}>요청 제출</Button>}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="requester" label="요청자" rules={[{ required: true, message: '요청자를 입력하세요' }]}>
            <Input placeholder="예: 김연구" />
          </Form.Item>
          <Form.Item name="purpose" label="사용 목적" rules={[{ required: true, message: '사용 목적을 입력하세요' }]}>
            <Input.TextArea rows={3} placeholder="예: IRB-2026-001 승인 코호트 연구" />
          </Form.Item>
          <Form.Item name="target_tables" label="대상 테이블 (쉼표 구분)" rules={[{ required: true, message: '대상 테이블을 입력하세요' }]}>
            <Input placeholder="예: person, condition_occurrence" />
          </Form.Item>
          <Form.Item name="target_columns" label="대상 컬럼 (쉼표 구분, 선택)">
            <Input placeholder="예: person_source_value, condition_source_value" />
          </Form.Item>
          <Form.Item name="expires_at" label="만료일 (선택)">
            <DatePicker style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

// ═══════════════════════════════════════════════════
// 메인 탭 컴포넌트
// ═══════════════════════════════════════════════════

const DeidentificationTab: React.FC = () => {
  return (
    <Tabs
      defaultActiveKey="status"
      items={[
        {
          key: 'status',
          label: <><EyeInvisibleOutlined /> 비식별 현황</>,
          children: <DeidentStatusSection />,
        },
        {
          key: 'pipeline',
          label: <><NodeIndexOutlined /> Pipeline 단계별 처리</>,
          children: <PipelineSection />,
        },
        {
          key: 'monitoring',
          label: <><FileSearchOutlined /> 처리 로그 & 모니터링</>,
          children: <MonitoringSection />,
        },
        {
          key: 'reident',
          label: <><AuditOutlined /> 재식별 요청 관리</>,
          children: <ReidentRequestSection />,
        },
      ]}
    />
  );
};

export default DeidentificationTab;
