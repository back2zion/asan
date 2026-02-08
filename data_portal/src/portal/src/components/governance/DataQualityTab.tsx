/**
 * DGR-004: 데이터 품질관리 체계
 * 5-section 품질관리 허브 (대시보드, 규칙 관리, 검증 이력, 알림 설정, 스키마 모니터링)
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Tabs, Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Alert, Progress, Button, Drawer, Form, Input, InputNumber, Select,
  Switch, Popconfirm, DatePicker, Descriptions, Spin, Modal, App,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, ReloadOutlined,
  PlusOutlined, PlayCircleOutlined, BellOutlined,
  SafetyCertificateOutlined, DatabaseOutlined, HistoryOutlined,
  SettingOutlined, CameraOutlined, WarningOutlined,
  EditOutlined, DeleteOutlined, ThunderboltOutlined,
} from '@ant-design/icons';
import { metadataMgmtApi, schemaMonitorApi } from '../../services/api';

const { Text } = Typography;
const { RangePicker } = DatePicker;

// ═══════════════════════════════════════════
//  Section 1: 품질 대시보드
// ═══════════════════════════════════════════

const DashboardSection: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [checking, setChecking] = useState(false);
  const [data, setData] = useState<any>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await metadataMgmtApi.getQualityDashboard();
      setData(res);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const doCheckAll = async () => {
    setChecking(true);
    const hide = message.loading('전체 데이터 품질 검증을 실행합니다. 테이블 수와 데이터 양에 따라 수 분이 소요될 수 있습니다...', 0);
    try {
      const res = await metadataMgmtApi.checkAllQualityRules();
      message.success(`검증 완료 (run: ${res.run_id}) — 통과율 ${res.pass_rate}%`);
      load();
    } catch {
      message.error('검증 실행 실패');
    }
    hide();
    setChecking(false);
  };

  const runCheckAll = () => {
    Modal.confirm({
      title: '전체 품질 검증 실행',
      content: '등록된 모든 품질 규칙에 대해 검증을 실행합니다. 대량 데이터 처리로 인해 10초 이상 소요될 수 있습니다. 검증은 백그라운드에서 진행되며 다른 작업을 계속하실 수 있습니다.',
      okText: '검증 시작',
      cancelText: '취소',
      onOk: doCheckAll,
    });
  };

  if (loading && !data) return <Spin tip="로딩 중..."><div style={{ minHeight: 200 }} /></Spin>;

  const rs = data?.rules_summary || {};
  const lr = data?.last_run;
  const scores = data?.table_scores || [];
  const trend = data?.trend || [];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={4}>
          <Card size="small"><Statistic title="전체 규칙" value={rs.total || 0} prefix={<SafetyCertificateOutlined />} /></Card>
        </Col>
        <Col xs={12} sm={4}>
          <Card size="small"><Statistic title="활성 규칙" value={rs.enabled || 0} prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} /></Card>
        </Col>
        <Col xs={12} sm={4}>
          <Card size="small">
            <Statistic title="최근 통과율" value={lr?.pass_rate ?? '-'} suffix="%" prefix={<ThunderboltOutlined />}
              valueStyle={{ color: (lr?.pass_rate ?? 100) >= 80 ? '#3f8600' : '#cf1322' }} />
          </Card>
        </Col>
        <Col xs={12} sm={4}>
          <Card size="small"><Statistic title="위반 건수" value={lr?.failed ?? 0} prefix={<CloseCircleOutlined />} valueStyle={{ color: '#cf1322' }} /></Card>
        </Col>
        <Col xs={12} sm={4}>
          <Card size="small"><Statistic title="알림 설정" value={data?.alerts_active ?? 0} prefix={<BellOutlined />} /></Card>
        </Col>
        <Col xs={12} sm={4}>
          <Card size="small">
            <Statistic title="마지막 검증" value={lr?.checked_at ? new Date(lr.checked_at).toLocaleString('ko-KR') : '없음'} valueStyle={{ fontSize: 14 }} />
          </Card>
        </Col>
      </Row>

      <Card title="테이블별 품질 점수" extra={
        <Button type="primary" icon={<PlayCircleOutlined />} loading={checking} onClick={runCheckAll}>전체 검증 실행</Button>
      }>
        <Table dataSource={scores} rowKey="table_name" size="small" pagination={false}
          columns={[
            { title: '테이블', dataIndex: 'table_name', render: (v: string) => <Text code>{v}</Text> },
            { title: '규칙 수', dataIndex: 'rule_count', width: 80 },
            { title: '통과', dataIndex: 'pass_count', width: 80, render: (v: number) => <Text type="success">{v}</Text> },
            { title: '실패', dataIndex: 'fail_count', width: 80, render: (v: number) => v > 0 ? <Text type="danger">{v}</Text> : <Text>{v}</Text> },
            { title: '점수', dataIndex: 'score', width: 200,
              render: (v: number) => <Progress percent={Number(v)} size="small" status={v >= 80 ? 'success' : v >= 60 ? 'normal' : 'exception'} /> },
          ]}
        />
      </Card>

      {trend.length > 0 && (
        <Card title="검증 트렌드 (최근 실행)">
          <Space wrap>
            {trend.map((t: any, i: number) => (
              <Tag key={i} color={t.pass_rate >= 90 ? 'green' : t.pass_rate >= 70 ? 'orange' : 'red'}>
                {t.run_id?.slice(0, 8)} : {t.pass_rate}%
              </Tag>
            ))}
          </Space>
        </Card>
      )}
    </Space>
  );
};

// ═══════════════════════════════════════════
//  Section 2: 품질 규칙 관리
// ═══════════════════════════════════════════

const RulesSection: React.FC = () => {
  const { message } = App.useApp();
  const [rules, setRules] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<any>(null);
  const [filterTable, setFilterTable] = useState<string | undefined>();
  const [filterSeverity, setFilterSeverity] = useState<string | undefined>();
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await metadataMgmtApi.getQualityRules({ table_name: filterTable, severity: filterSeverity });
      setRules(res);
    } catch { /* ignore */ }
    setLoading(false);
  }, [filterTable, filterSeverity]);

  useEffect(() => { load(); }, [load]);

  const tables = [...new Set(rules.map((r: any) => r.table_name))];

  const openCreate = () => {
    setEditingRule(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  const openEdit = (rule: any) => {
    setEditingRule(rule);
    form.setFieldsValue({
      ...rule,
      enum_values: rule.enum_values ? (Array.isArray(rule.enum_values) ? rule.enum_values.join(',') : String(rule.enum_values)) : undefined,
    });
    setDrawerOpen(true);
  };

  const onSave = async () => {
    try {
      const values = await form.validateFields();
      const payload = {
        ...values,
        enum_values: values.enum_values ? String(values.enum_values).split(',').map((s: string) => s.trim()) : null,
      };
      if (editingRule) {
        await metadataMgmtApi.updateQualityRule(editingRule.rule_id, payload);
        message.success('규칙 수정 완료');
      } else {
        await metadataMgmtApi.createQualityRule(payload);
        message.success('규칙 생성 완료');
      }
      setDrawerOpen(false);
      load();
    } catch { /* validation failed */ }
  };

  const onDelete = async (id: number) => {
    await metadataMgmtApi.deleteQualityRule(id);
    message.success('삭제 완료');
    load();
  };

  const onCheck = async (id: number) => {
    try {
      const res = await metadataMgmtApi.checkQualityRule(id);
      message.info(`검증 결과: ${res.status} (위반 ${res.violations}건)`);
      load();
    } catch { message.error('검증 실패'); }
  };

  const severityColor: Record<string, string> = { info: 'blue', warning: 'orange', error: 'red', critical: 'magenta' };
  const ruleTypeColor: Record<string, string> = { not_null: 'purple', range: 'cyan', enum: 'geekblue', regex: 'gold', unique: 'lime', custom: 'volcano', referential: 'green' };

  const columns = [
    { title: '규칙명', dataIndex: 'rule_name', key: 'rule_name', ellipsis: true },
    { title: '테이블.컬럼', key: 'target', render: (_: any, r: any) => <Text code>{r.table_name}.{r.column_name}</Text> },
    { title: '유형', dataIndex: 'rule_type', key: 'rule_type', width: 100,
      render: (v: string) => <Tag color={ruleTypeColor[v] || 'default'}>{v}</Tag> },
    { title: '심각도', dataIndex: 'severity', key: 'severity', width: 90,
      render: (v: string) => <Tag color={severityColor[v] || 'default'}>{v}</Tag> },
    { title: '활성', dataIndex: 'enabled', key: 'enabled', width: 60,
      render: (v: boolean) => v ? <Tag color="green">ON</Tag> : <Tag>OFF</Tag> },
    { title: '마지막 검증', dataIndex: 'last_checked', key: 'last_checked', width: 160,
      render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
    { title: '결과', dataIndex: 'last_result', key: 'last_result', width: 90,
      render: (v: any) => {
        if (!v) return '-';
        const parsed = typeof v === 'string' ? JSON.parse(v) : v;
        return <Tag color={parsed.status === 'pass' ? 'green' : 'red'}>{parsed.status}</Tag>;
      }},
    { title: '작업', key: 'actions', width: 140,
      render: (_: any, r: any) => (
        <Space size="small">
          <Button size="small" icon={<PlayCircleOutlined />} onClick={() => onCheck(r.rule_id)} title="단건 실행" />
          <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(r)} title="편집" />
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => onDelete(r.rule_id)}>
            <Button size="small" danger icon={<DeleteOutlined />} title="삭제" />
          </Popconfirm>
        </Space>
      )},
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={8}>
        <Col>
          <Select allowClear placeholder="테이블" style={{ width: 180 }} value={filterTable} onChange={setFilterTable}
            options={tables.map(t => ({ label: t, value: t }))} />
        </Col>
        <Col>
          <Select allowClear placeholder="심각도" style={{ width: 120 }} value={filterSeverity} onChange={setFilterSeverity}
            options={['info','warning','error','critical'].map(s => ({ label: s, value: s }))} />
        </Col>
        <Col flex="auto" />
        <Col>
          <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        </Col>
        <Col>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>규칙 추가</Button>
        </Col>
      </Row>

      <Table dataSource={rules} rowKey="rule_id" size="small" loading={loading}
        columns={columns} pagination={{ pageSize: 15, showSizeChanger: true }}
        expandable={{
          expandedRowRender: (r: any) => (
            <Descriptions size="small" column={2} bordered>
              {r.condition_expr && <Descriptions.Item label="조건식" span={2}><Text code>{r.condition_expr}</Text></Descriptions.Item>}
              {r.normal_min != null && <Descriptions.Item label="정상 최소">{r.normal_min}</Descriptions.Item>}
              {r.normal_max != null && <Descriptions.Item label="정상 최대">{r.normal_max}</Descriptions.Item>}
              {r.enum_values && <Descriptions.Item label="허용값" span={2}>{JSON.stringify(r.enum_values)}</Descriptions.Item>}
              {r.regex_pattern && <Descriptions.Item label="정규식" span={2}><Text code>{r.regex_pattern}</Text></Descriptions.Item>}
              {r.description && <Descriptions.Item label="설명" span={2}>{r.description}</Descriptions.Item>}
            </Descriptions>
          ),
        }}
      />

      <Drawer title={editingRule ? '규칙 편집' : '규칙 추가'} open={drawerOpen} onClose={() => setDrawerOpen(false)}
        width={480} extra={<Button type="primary" onClick={onSave}>저장</Button>}>
        <Form form={form} layout="vertical"
          initialValues={{ rule_type: 'not_null', severity: 'warning', enabled: true }}>
          <Form.Item name="table_name" label="테이블" rules={[{ required: true }]}>
            <Input placeholder="예: person" />
          </Form.Item>
          <Form.Item name="column_name" label="컬럼" rules={[{ required: true }]}>
            <Input placeholder="예: year_of_birth" />
          </Form.Item>
          <Form.Item name="rule_name" label="규칙명" rules={[{ required: true }]}>
            <Input placeholder="예: 출생연도 범위 검증" />
          </Form.Item>
          <Form.Item name="rule_type" label="유형" rules={[{ required: true }]}>
            <Select options={['not_null','range','enum','regex','unique','referential','custom'].map(v => ({ label: v, value: v }))} />
          </Form.Item>
          <Form.Item name="severity" label="심각도">
            <Select options={['info','warning','error','critical'].map(v => ({ label: v, value: v }))} />
          </Form.Item>
          <Form.Item name="condition_expr" label="조건식 (custom/not_null)">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Row gutter={16}>
            <Col span={12}><Form.Item name="normal_min" label="정상 최소"><InputNumber style={{ width: '100%' }} /></Form.Item></Col>
            <Col span={12}><Form.Item name="normal_max" label="정상 최대"><InputNumber style={{ width: '100%' }} /></Form.Item></Col>
          </Row>
          <Form.Item name="enum_values" label="허용값 (콤마 구분)">
            <Input placeholder="M,F" />
          </Form.Item>
          <Form.Item name="regex_pattern" label="정규식">
            <Input placeholder="^[A-Z0-9]{6,20}$" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="enabled" label="활성" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

// ═══════════════════════════════════════════
//  Section 3: 검증 이력
// ═══════════════════════════════════════════

const HistorySection: React.FC = () => {
  const [runs, setRuns] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [detailMap, setDetailMap] = useState<Record<string, any[]>>({});

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await metadataMgmtApi.getQualityCheckRuns();
      setRuns(res);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const loadRunDetail = async (runId: string) => {
    if (detailMap[runId]) return;
    try {
      const res = await metadataMgmtApi.getQualityCheckHistory({ run_id: runId });
      setDetailMap(prev => ({ ...prev, [runId]: res }));
    } catch { /* ignore */ }
  };

  const columns = [
    { title: 'Run ID', dataIndex: 'run_id', key: 'run_id', width: 120,
      render: (v: string) => <Text code>{v?.slice(0, 8)}</Text> },
    { title: '검증 시각', dataIndex: 'checked_at', key: 'checked_at', width: 180,
      render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
    { title: '전체', dataIndex: 'total_rules', key: 'total_rules', width: 70 },
    { title: '통과', dataIndex: 'passed', key: 'passed', width: 70,
      render: (v: number) => <Text type="success">{String(v)}</Text> },
    { title: '실패', dataIndex: 'failed', key: 'failed', width: 70,
      render: (v: number) => Number(v) > 0 ? <Text type="danger">{String(v)}</Text> : <Text>{String(v)}</Text> },
    { title: '통과율', dataIndex: 'pass_rate', key: 'pass_rate', width: 200,
      render: (v: number) => <Progress percent={Number(v)} size="small"
        status={Number(v) >= 80 ? 'success' : Number(v) >= 60 ? 'normal' : 'exception'} /> },
  ];

  const detailColumns = [
    { title: '규칙명', dataIndex: 'rule_name', key: 'rule_name' },
    { title: '테이블', dataIndex: 'table_name', key: 'table_name', render: (v: string) => <Text code>{v}</Text> },
    { title: '컬럼', dataIndex: 'column_name', key: 'column_name' },
    { title: '상태', dataIndex: 'status', key: 'status',
      render: (v: string) => <Tag color={v === 'pass' ? 'green' : 'red'}>{v}</Tag> },
    { title: '전체 행', dataIndex: 'total_rows', key: 'total_rows',
      render: (v: number) => (v ?? 0).toLocaleString() },
    { title: '위반 건', dataIndex: 'violations', key: 'violations',
      render: (v: number) => (v ?? 0).toLocaleString() },
    { title: '위반율', dataIndex: 'violation_rate', key: 'violation_rate',
      render: (v: number) => `${v ?? 0}%` },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row justify="end">
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
      </Row>
      <Table dataSource={runs} rowKey="run_id" size="small" loading={loading}
        columns={columns} pagination={{ pageSize: 10, showSizeChanger: true }}
        expandable={{
          onExpand: (expanded, record) => { if (expanded) loadRunDetail(record.run_id); },
          expandedRowRender: (record: any) => {
            const details = detailMap[record.run_id];
            if (!details) return <Spin size="small" />;
            return <Table dataSource={details} rowKey="history_id" size="small" columns={detailColumns} pagination={false} />;
          },
        }}
      />
    </Space>
  );
};

// ═══════════════════════════════════════════
//  Section 4: 알림 설정
// ═══════════════════════════════════════════

const AlertsSection: React.FC = () => {
  const { message } = App.useApp();
  const [alerts, setAlerts] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editingAlert, setEditingAlert] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await metadataMgmtApi.getQualityAlerts();
      setAlerts(res);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const openCreate = () => {
    setEditingAlert(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  const openEdit = (alert: any) => {
    setEditingAlert(alert);
    form.setFieldsValue(alert);
    setDrawerOpen(true);
  };

  const onSave = async () => {
    try {
      const values = await form.validateFields();
      if (editingAlert) {
        await metadataMgmtApi.updateQualityAlert(editingAlert.alert_id, values);
        message.success('알림 수정 완료');
      } else {
        await metadataMgmtApi.createQualityAlert(values);
        message.success('알림 생성 완료');
      }
      setDrawerOpen(false);
      load();
    } catch { /* validation failed */ }
  };

  const onDelete = async (id: number) => {
    await metadataMgmtApi.deleteQualityAlert(id);
    message.success('삭제 완료');
    load();
  };

  const onToggle = async (alert: any, enabled: boolean) => {
    await metadataMgmtApi.updateQualityAlert(alert.alert_id, { ...alert, enabled });
    load();
  };

  const condTypeLabel: Record<string, string> = {
    rule_fail: '규칙 실패', violation_threshold: '위반율 초과', batch_fail_rate: '통과율 미달',
  };
  const channelColor: Record<string, string> = { log: 'default', webhook: 'blue', email: 'green' };

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row justify="end" gutter={8}>
        <Col><Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button></Col>
        <Col><Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>알림 추가</Button></Col>
      </Row>

      <Table dataSource={alerts} rowKey="alert_id" size="small" loading={loading}
        pagination={false}
        columns={[
          { title: '알림명', dataIndex: 'name', key: 'name' },
          { title: '조건', dataIndex: 'condition_type', key: 'condition_type', width: 140,
            render: (v: string) => <Tag color="purple">{condTypeLabel[v] || v}</Tag> },
          { title: '임계값', dataIndex: 'threshold', key: 'threshold', width: 90,
            render: (v: number | null) => v != null ? `${v}%` : '-' },
          { title: '채널', dataIndex: 'channel', key: 'channel', width: 100,
            render: (v: string) => <Tag color={channelColor[v] || 'default'}>{v}</Tag> },
          { title: '활성', dataIndex: 'enabled', key: 'enabled', width: 80,
            render: (v: boolean, r: any) => <Switch size="small" checked={v} onChange={(c) => onToggle(r, c)} /> },
          { title: '작업', key: 'actions', width: 100,
            render: (_: any, r: any) => (
              <Space size="small">
                <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(r)} />
                <Popconfirm title="삭제하시겠습니까?" onConfirm={() => onDelete(r.alert_id)}>
                  <Button size="small" danger icon={<DeleteOutlined />} />
                </Popconfirm>
              </Space>
            )},
        ]}
      />

      <Drawer title={editingAlert ? '알림 편집' : '알림 추가'} open={drawerOpen} onClose={() => setDrawerOpen(false)}
        width={420} extra={<Button type="primary" onClick={onSave}>저장</Button>}>
        <Form form={form} layout="vertical"
          initialValues={{ condition_type: 'rule_fail', channel: 'log', enabled: true }}>
          <Form.Item name="name" label="알림명" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="condition_type" label="조건 유형" rules={[{ required: true }]}>
            <Select options={[
              { label: '규칙 실패 시', value: 'rule_fail' },
              { label: '위반율 초과 시', value: 'violation_threshold' },
              { label: '일괄 통과율 미달 시', value: 'batch_fail_rate' },
            ]} />
          </Form.Item>
          <Form.Item name="threshold" label="임계값 (%)">
            <InputNumber style={{ width: '100%' }} min={0} max={100} />
          </Form.Item>
          <Form.Item name="channel" label="알림 채널">
            <Select options={[
              { label: '로그', value: 'log' },
              { label: 'Webhook', value: 'webhook' },
              { label: 'Email', value: 'email' },
            ]} />
          </Form.Item>
          <Form.Item name="enabled" label="활성" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

// ═══════════════════════════════════════════
//  Section 5: 스키마 모니터링
// ═══════════════════════════════════════════

const SchemaMonitorSection: React.FC = () => {
  const { message } = App.useApp();
  const [changes, setChanges] = useState<any[]>([]);
  const [policies, setPolicies] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [snapshotting, setSnapshotting] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [histRes, polRes] = await Promise.all([
        schemaMonitorApi.getHistory(20),
        schemaMonitorApi.getPolicies(),
      ]);
      setChanges(histRes || []);
      setPolicies(polRes?.policies || []);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const takeSnapshot = async () => {
    setSnapshotting(true);
    try {
      const res = await schemaMonitorApi.captureSnapshot();
      message.success(`스냅샷 v${res.version} 캡처 완료 (변경 ${res.changes?.total_changes ?? 0}건)`);
      load();
    } catch { message.error('스냅샷 실패'); }
    setSnapshotting(false);
  };

  const changeTypeColor: Record<string, string> = {
    table_added: 'green', table_removed: 'red',
    column_added: 'cyan', column_removed: 'magenta',
    column_type_changed: 'orange', column_nullable_changed: 'gold',
  };

  const policyActionColor: Record<string, string> = { auto: 'green', manual: 'orange' };

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Card title="스키마 변경 이력" extra={
        <Button icon={<CameraOutlined />} loading={snapshotting} onClick={takeSnapshot}>스냅샷 갱신</Button>
      }>
        <Table dataSource={changes} rowKey="id" size="small" loading={loading}
          pagination={{ pageSize: 10 }}
          columns={[
            { title: '버전', dataIndex: 'version', key: 'version', width: 70,
              render: (v: number) => <Tag>v{v}</Tag> },
            { title: '시각', dataIndex: 'created_at', key: 'created_at', width: 180,
              render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
            { title: '라벨', dataIndex: 'label', key: 'label', ellipsis: true },
            { title: '변경 수', key: 'total', width: 80,
              render: (_: any, r: any) => r.summary?.total_changes ?? 0 },
            { title: '고위험', key: 'high', width: 80,
              render: (_: any, r: any) => {
                const h = r.summary?.high_risk ?? 0;
                return h > 0 ? <Tag color="red">{h}</Tag> : <Text type="secondary">0</Text>;
              }},
            { title: '자동적용', key: 'auto', width: 80,
              render: (_: any, r: any) => r.summary?.auto_applicable ?? 0 },
          ]}
        />
      </Card>

      <Card title="자동 반영 정책">
        <Table dataSource={policies} rowKey="key" size="small" pagination={false}
          columns={[
            { title: '정책', dataIndex: 'key', key: 'key',
              render: (v: string) => <Text code>{v}</Text> },
            { title: '동작', key: 'action', width: 100,
              render: (_: any, r: any) => {
                const action = r.value?.action;
                return action ? <Tag color={policyActionColor[action] || 'default'}>{action}</Tag> : <Tag>설정</Tag>;
              }},
            { title: '알림', key: 'notify', width: 80,
              render: (_: any, r: any) => r.value?.notify ? <Tag color="blue">ON</Tag> : <Tag>OFF</Tag> },
            { title: '설명', dataIndex: 'description', key: 'description', ellipsis: true },
          ]}
        />
      </Card>
    </Space>
  );
};

// ═══════════════════════════════════════════
//  Main Component
// ═══════════════════════════════════════════

const DataQualityTab: React.FC = () => {
  return (
    <Tabs defaultActiveKey="dashboard" items={[
      {
        key: 'dashboard',
        label: <span><SafetyCertificateOutlined /> 품질 대시보드</span>,
        children: <DashboardSection />,
      },
      {
        key: 'rules',
        label: <span><SettingOutlined /> 품질 규칙 관리</span>,
        children: <RulesSection />,
      },
      {
        key: 'history',
        label: <span><HistoryOutlined /> 검증 이력</span>,
        children: <HistorySection />,
      },
      {
        key: 'alerts',
        label: <span><BellOutlined /> 알림 설정</span>,
        children: <AlertsSection />,
      },
      {
        key: 'schema',
        label: <span><DatabaseOutlined /> 스키마 모니터링</span>,
        children: <SchemaMonitorSection />,
      },
    ]} />
  );
};

export default DataQualityTab;
