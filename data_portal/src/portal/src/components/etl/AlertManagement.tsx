/**
 * DIR-001: 알림 설정/이력 탭
 * 알림 규칙 CRUD + 알림 이력 테이블 + 확인 처리
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Switch, Segmented,
  Checkbox, Badge, Tooltip, message, Popconfirm,
} from 'antd';
import {
  BellOutlined, PlusOutlined, EditOutlined, DeleteOutlined,
  CheckOutlined, ReloadOutlined, WarningOutlined,
  ExclamationCircleOutlined, InfoCircleOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

interface AlertRule {
  rule_id: number;
  name: string;
  condition_type: string;
  threshold: Record<string, any>;
  job_id: number | null;
  job_name: string | null;
  channels: string[];
  webhook_url: string | null;
  enabled: boolean;
}

interface AlertHistory {
  alert_id: number;
  rule_id: number | null;
  rule_name: string | null;
  job_id: number | null;
  job_name: string | null;
  severity: string;
  title: string;
  message: string;
  acknowledged: boolean;
  acknowledged_by: string | null;
  acknowledged_at: string | null;
  created_at: string | null;
}

interface Job {
  job_id: number;
  name: string;
}

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

const CONDITION_TYPES: Record<string, { label: string; description: string }> = {
  job_failure: { label: 'Job 실패', description: '특정 횟수 이상 연속 실패 시 알림' },
  duration_exceeded: { label: '실행 시간 초과', description: '설정한 시간을 초과하면 알림' },
  row_anomaly: { label: '처리 건수 이상', description: '평소 대비 편차가 크면 알림' },
  error_count: { label: '에러 건수 초과', description: '에러 건수가 임계치 초과 시 알림' },
};

const SEVERITY_CONFIG: Record<string, { color: string; icon: React.ReactNode }> = {
  critical: { color: 'red', icon: <ExclamationCircleOutlined /> },
  warning: { color: 'orange', icon: <WarningOutlined /> },
  info: { color: 'blue', icon: <InfoCircleOutlined /> },
};

const AlertManagement: React.FC = () => {
  const [section, setSection] = useState<string>('rules');
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [alerts, setAlerts] = useState<AlertHistory[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [ruleModalOpen, setRuleModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null);
  const [unreadCount, setUnreadCount] = useState(0);
  const [ruleForm] = Form.useForm();

  const loadRules = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/alert-rules`);
      setRules(data.rules || []);
    } catch { message.error('알림 규칙 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadAlerts = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/alerts`);
      setAlerts(data.alerts || []);
    } catch { message.error('알림 이력 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadUnread = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/alerts/unread-count`);
      setUnreadCount(data.unread_count || 0);
    } catch { /* ignore */ }
  }, []);

  const loadJobs = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/jobs`);
      setJobs((data.jobs || []).map((j: any) => ({ job_id: j.job_id, name: j.name })));
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadJobs(); loadUnread(); }, [loadJobs, loadUnread]);
  useEffect(() => {
    if (section === 'rules') loadRules();
    else loadAlerts();
  }, [section, loadRules, loadAlerts]);

  // Rule CRUD
  const openRuleModal = (rule?: AlertRule) => {
    setEditingRule(rule || null);
    if (rule) {
      ruleForm.setFieldsValue({
        name: rule.name,
        condition_type: rule.condition_type,
        job_id: rule.job_id,
        channels: rule.channels,
        webhook_url: rule.webhook_url,
        enabled: rule.enabled,
        threshold_value: rule.threshold?.max_failures || rule.threshold?.max_minutes || rule.threshold?.deviation_pct || rule.threshold?.max_errors || 0,
      });
    } else {
      ruleForm.setFieldsValue({
        name: '', condition_type: 'job_failure', job_id: null,
        channels: ['email'], webhook_url: '', enabled: true, threshold_value: 1,
      });
    }
    setRuleModalOpen(true);
  };

  const handleRuleSave = async () => {
    const values = await ruleForm.validateFields();
    const thresholdKey: Record<string, string> = {
      job_failure: 'max_failures',
      duration_exceeded: 'max_minutes',
      row_anomaly: 'deviation_pct',
      error_count: 'max_errors',
    };
    const body = {
      name: values.name,
      condition_type: values.condition_type,
      threshold: { [thresholdKey[values.condition_type]]: values.threshold_value },
      job_id: values.job_id || null,
      channels: values.channels || [],
      webhook_url: values.webhook_url || null,
      enabled: values.enabled,
    };
    try {
      if (editingRule) {
        await putJSON(`${API_BASE}/alert-rules/${editingRule.rule_id}`, body);
        message.success('규칙 수정 완료');
      } else {
        await postJSON(`${API_BASE}/alert-rules`, body);
        message.success('규칙 생성 완료');
      }
      setRuleModalOpen(false);
      loadRules();
    } catch { message.error('저장 실패'); }
  };

  const handleRuleDelete = async (ruleId: number) => {
    try {
      await deleteJSON(`${API_BASE}/alert-rules/${ruleId}`);
      message.success('규칙 삭제 완료');
      loadRules();
    } catch { message.error('삭제 실패'); }
  };

  const handleAcknowledge = async (alertId: number) => {
    try {
      await putJSON(`${API_BASE}/alerts/${alertId}/acknowledge`, {});
      message.success('확인 처리 완료');
      loadAlerts();
      loadUnread();
    } catch { message.error('확인 처리 실패'); }
  };

  // Rule columns
  const ruleColumns = [
    { title: '규칙명', dataIndex: 'name', key: 'name', width: 200, render: (v: string) => <Text strong>{v}</Text> },
    {
      title: '조건 유형', dataIndex: 'condition_type', key: 'ctype', width: 140,
      render: (v: string) => <Tag>{CONDITION_TYPES[v]?.label || v}</Tag>,
    },
    {
      title: '임계값', key: 'threshold', width: 120,
      render: (_: any, r: AlertRule) => {
        const t = r.threshold;
        if (t.max_failures) return `${t.max_failures}회 실패`;
        if (t.max_minutes) return `${t.max_minutes}분 초과`;
        if (t.deviation_pct) return `${t.deviation_pct}% 편차`;
        if (t.max_errors) return `${t.max_errors}건 에러`;
        return '-';
      },
    },
    { title: '대상 Job', dataIndex: 'job_name', key: 'job', width: 160, render: (v: string | null) => v || <Text type="secondary">전체</Text> },
    {
      title: '채널', dataIndex: 'channels', key: 'channels', width: 150,
      render: (v: string[]) => <Space wrap size={4}>{(v || []).map(c => <Tag key={c}>{c}</Tag>)}</Space>,
    },
    {
      title: '활성', dataIndex: 'enabled', key: 'enabled', width: 70,
      render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? 'ON' : 'OFF'}</Tag>,
    },
    {
      title: '', key: 'actions', width: 100,
      render: (_: any, r: AlertRule) => (
        <Space size="small">
          <Button size="small" icon={<EditOutlined />} onClick={() => openRuleModal(r)} />
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleRuleDelete(r.rule_id)} okText="삭제" cancelText="취소">
            <Button size="small" icon={<DeleteOutlined />} danger />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  // Alert history columns
  const alertColumns = [
    {
      title: '심각도', dataIndex: 'severity', key: 'severity', width: 90,
      render: (v: string) => {
        const cfg = SEVERITY_CONFIG[v] || SEVERITY_CONFIG.info;
        return <Tag icon={cfg.icon} color={cfg.color}>{v}</Tag>;
      },
    },
    { title: '제목', dataIndex: 'title', key: 'title', width: 250, render: (v: string) => <Text strong>{v}</Text> },
    { title: 'Job', dataIndex: 'job_name', key: 'job', width: 160 },
    { title: '규칙', dataIndex: 'rule_name', key: 'rule', width: 140 },
    { title: '메시지', dataIndex: 'message', key: 'msg', ellipsis: true },
    {
      title: '발생', dataIndex: 'created_at', key: 'created', width: 140,
      render: (v: string | null) => v ? dayjs(v).format('MM-DD HH:mm') : '-',
    },
    {
      title: '확인', key: 'ack', width: 80,
      render: (_: any, r: AlertHistory) => r.acknowledged ? (
        <Tooltip title={`${r.acknowledged_by} (${r.acknowledged_at ? dayjs(r.acknowledged_at).format('MM-DD HH:mm') : ''})`}>
          <Tag icon={<CheckOutlined />} color="green">확인</Tag>
        </Tooltip>
      ) : (
        <Button size="small" type="primary" onClick={() => handleAcknowledge(r.alert_id)}>확인</Button>
      ),
    },
  ];

  return (
    <div>
      <Segmented
        block
        options={[
          { label: '알림 규칙', value: 'rules', icon: <BellOutlined /> },
          {
            label: (
              <Badge count={unreadCount} offset={[10, 0]} size="small">
                <span>알림 이력</span>
              </Badge>
            ),
            value: 'history',
            icon: <WarningOutlined />,
          },
        ]}
        value={section}
        onChange={(v) => setSection(v as string)}
        style={{ marginBottom: 16 }}
      />

      {section === 'rules' ? (
        <Card
          size="small"
          title={<><BellOutlined /> 알림 규칙</>}
          extra={
            <Space>
              <Button icon={<PlusOutlined />} type="primary" onClick={() => openRuleModal()}>규칙 추가</Button>
              <Button icon={<ReloadOutlined />} onClick={loadRules}>새로고침</Button>
            </Space>
          }
        >
          <Spin spinning={loading}>
            {rules.length > 0 ? (
              <Table
                dataSource={rules.map(r => ({ ...r, key: r.rule_id }))}
                columns={ruleColumns}
                size="small"
                pagination={false}
              />
            ) : !loading ? <Empty description="알림 규칙이 없습니다" /> : null}
          </Spin>
        </Card>
      ) : (
        <>
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={8}>
              <Card size="small">
                <Statistic title="총 알림" value={alerts.length} prefix={<BellOutlined />} />
              </Card>
            </Col>
            <Col xs={8}>
              <Card size="small">
                <Statistic title="미확인" value={unreadCount} valueStyle={{ color: unreadCount > 0 ? '#cf1322' : undefined }} prefix={<WarningOutlined />} />
              </Card>
            </Col>
            <Col xs={8}>
              <Card size="small">
                <Statistic
                  title="Critical"
                  value={alerts.filter(a => a.severity === 'critical').length}
                  valueStyle={{ color: '#cf1322' }}
                  prefix={<ExclamationCircleOutlined />}
                />
              </Card>
            </Col>
          </Row>

          <Card
            size="small"
            title="알림 이력"
            extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadAlerts}>새로고침</Button>}
          >
            <Spin spinning={loading}>
              {alerts.length > 0 ? (
                <Table
                  dataSource={alerts.map(a => ({ ...a, key: a.alert_id }))}
                  columns={alertColumns}
                  size="small"
                  pagination={{ pageSize: 20 }}
                  scroll={{ x: 1000 }}
                  rowClassName={(r: AlertHistory) => !r.acknowledged ? 'ant-table-row-selected' : ''}
                />
              ) : !loading ? <Empty description="알림 이력이 없습니다" /> : null}
            </Spin>
          </Card>
        </>
      )}

      {/* Rule Modal */}
      <Modal
        title={editingRule ? '알림 규칙 수정' : '알림 규칙 추가'}
        open={ruleModalOpen}
        onOk={handleRuleSave}
        onCancel={() => setRuleModalOpen(false)}
        okText="저장"
        cancelText="취소"
        width={600}
      >
        <Form form={ruleForm} layout="vertical">
          <Form.Item name="name" label="규칙명" rules={[{ required: true, message: '규칙명을 입력하세요' }]}>
            <Input />
          </Form.Item>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="condition_type" label="조건 유형" rules={[{ required: true }]}>
                <Select options={Object.entries(CONDITION_TYPES).map(([k, v]) => ({ value: k, label: v.label }))} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="threshold_value" label="임계값">
                <Input type="number" />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="job_id" label="대상 Job (비우면 전체)">
            <Select allowClear placeholder="전체" options={jobs.map(j => ({ value: j.job_id, label: j.name }))} />
          </Form.Item>
          <Form.Item name="channels" label="알림 채널">
            <Select
              mode="multiple"
              options={[
                { value: 'email', label: 'Email' },
                { value: 'webhook', label: 'Webhook' },
                { value: 'slack', label: 'Slack' },
                { value: 'asanworks', label: '아산웍스' },
              ]}
            />
          </Form.Item>
          <Form.Item name="webhook_url" label="Webhook URL">
            <Input placeholder="https://..." />
          </Form.Item>
          <Form.Item name="enabled" label="활성화" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default AlertManagement;
