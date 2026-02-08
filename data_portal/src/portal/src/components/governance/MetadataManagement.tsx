/**
 * DGR-002: 메타데이터 관리 체계
 * 변경 워크플로우, 품질관리 기준, 원천↔통합 매핑, 규정 준수 모니터링, Pipeline Biz 현황
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Segmented, Badge,
  Tooltip, Descriptions, Drawer, Popconfirm, Steps, Timeline,
  Progress, Switch, List, App,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, WarningOutlined,
  SyncOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined,
  SendOutlined, AuditOutlined, SafetyCertificateOutlined,
  ExperimentOutlined, PlayCircleOutlined, EyeOutlined,
  SwapOutlined, BarChartOutlined, RocketOutlined,
  FileSearchOutlined, EditOutlined, ThunderboltOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API = '/api/v1/metadata-mgmt';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body?: any) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function putJSON(url: string, body?: any) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const STATUS_COLORS: Record<string, string> = {
  draft: 'default', submitted: 'blue', reviewing: 'orange', approved: 'green', rejected: 'red', applied: 'cyan',
};
const PRIORITY_COLORS: Record<string, string> = {
  low: 'default', normal: 'blue', high: 'orange', critical: 'red',
};
const SEVERITY_COLORS: Record<string, string> = {
  info: 'blue', warning: 'orange', error: 'red', critical: 'magenta',
};
const CATEGORY_COLORS: Record<string, string> = {
  privacy: 'red', security: 'magenta', quality: 'blue', retention: 'green', access: 'orange', audit: 'purple',
};
const JOB_COLORS: Record<string, string> = {
  batch: 'blue', stream: 'green', cdc: 'orange',
};

const MetadataManagement: React.FC = () => {
  const { message } = App.useApp();
  const [view, setView] = useState<string>('workflow');
  const [loading, setLoading] = useState(false);
  const [overview, setOverview] = useState<any>(null);

  // Workflow
  const [requests, setRequests] = useState<any[]>([]);
  const [reqModal, setReqModal] = useState(false);
  const [reqForm] = Form.useForm();
  const [reqDrawer, setReqDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });

  // Quality Rules
  const [rules, setRules] = useState<any[]>([]);
  const [ruleModal, setRuleModal] = useState(false);
  const [ruleForm] = Form.useForm();
  const [checkResults, setCheckResults] = useState<any>(null);

  // Source Mappings
  const [mappings, setMappings] = useState<any[]>([]);
  const [mappingOverview, setMappingOverview] = useState<any>(null);

  // Compliance
  const [compliance, setCompliance] = useState<any>(null);

  // Pipeline Biz
  const [pipelineDash, setPipelineDash] = useState<any>(null);

  const loadOverview = useCallback(async () => {
    try { setOverview(await fetchJSON(`${API}/overview`)); } catch { /* */ }
  }, []);

  const loadRequests = useCallback(async () => {
    setLoading(true);
    try { setRequests(await fetchJSON(`${API}/change-requests`)); }
    catch { message.error('변경요청 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadRules = useCallback(async () => {
    setLoading(true);
    try { setRules(await fetchJSON(`${API}/quality-rules`)); }
    catch { message.error('품질규칙 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadMappings = useCallback(async () => {
    setLoading(true);
    try {
      setMappings(await fetchJSON(`${API}/source-mappings`));
      setMappingOverview(await fetchJSON(`${API}/source-mappings/overview`));
    } catch { message.error('매핑 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadCompliance = useCallback(async () => {
    setLoading(true);
    try { setCompliance(await fetchJSON(`${API}/compliance-dashboard`)); }
    catch { message.error('규정준수 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadPipeline = useCallback(async () => {
    setLoading(true);
    try { setPipelineDash(await fetchJSON(`${API}/pipeline-biz/dashboard`)); }
    catch { message.error('파이프라인 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    loadOverview();
    if (view === 'workflow') loadRequests();
    else if (view === 'quality') loadRules();
    else if (view === 'mapping') loadMappings();
    else if (view === 'compliance') loadCompliance();
    else if (view === 'pipeline') loadPipeline();
  }, [view]);

  // ── Overview Cards ──
  const renderOverviewCards = () => {
    if (!overview) return null;
    const cr = overview.change_requests || {};
    const pending = (cr.submitted || 0) + (cr.reviewing || 0);
    return (
      <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
        <Col span={3}><Card size="small"><Statistic title="변경 대기" value={pending} valueStyle={{ color: pending > 0 ? '#faad14' : '#3f8600' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="품질 규칙" value={overview.quality_rules} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="매핑 수" value={overview.source_mappings} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="원천 시스템" value={overview.source_systems} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="규정 준수율" value={overview.compliance_rate} suffix="%" valueStyle={{ color: overview.compliance_rate >= 80 ? '#3f8600' : '#cf1322' }} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="규정 통과" value={`${overview.compliance_pass}/${overview.compliance_total}`} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="Pipeline" value={overview.pipelines_total} /></Card></Col>
        <Col span={3}><Card size="small"><Statistic title="실행 중" value={overview.pipelines_running} valueStyle={{ color: '#1890ff' }} /></Card></Col>
      </Row>
    );
  };

  // ══════════════════════════════════════════
  //  1. Change Request Workflow
  // ══════════════════════════════════════════
  const renderWorkflow = () => {
    const statusOrder = ['draft', 'submitted', 'reviewing', 'approved', 'applied', 'rejected'];
    return (
      <Spin spinning={loading}>
        <Space style={{ marginBottom: 16 }}>
          <Button icon={<PlusOutlined />} type="primary" onClick={() => { reqForm.resetFields(); setReqModal(true); }}>변경 신청</Button>
          <Button icon={<ReloadOutlined />} onClick={loadRequests}>새로고침</Button>
        </Space>

        {/* Workflow Steps */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Steps size="small" items={[
            { title: '작성', description: 'Draft', icon: <EditOutlined /> },
            { title: '제출', description: 'Submitted', icon: <SendOutlined /> },
            { title: '검토', description: 'Reviewing', icon: <FileSearchOutlined /> },
            { title: '승인', description: 'Approved', icon: <CheckCircleOutlined /> },
            { title: '적용', description: 'Applied', icon: <RocketOutlined /> },
          ]} />
        </Card>

        <Table dataSource={requests} rowKey="request_id" size="small" pagination={false}
          columns={[
            { title: 'ID', dataIndex: 'request_id', width: 50 },
            { title: '유형', dataIndex: 'request_type', width: 110,
              render: (v: string) => <Tag>{v.replace('_', ' ')}</Tag> },
            { title: '제목', dataIndex: 'title', width: 200,
              render: (v: string, r: any) => <a onClick={() => setReqDrawer({ open: true, data: r })}>{v}</a> },
            { title: '대상 테이블', dataIndex: 'target_table', width: 140,
              render: (v: string, r: any) => <>{v}{r.target_column && <Text type="secondary">.{r.target_column}</Text>}</> },
            { title: '신청자', dataIndex: 'requester', width: 80 },
            { title: '우선순위', dataIndex: 'priority', width: 80,
              render: (v: string) => <Tag color={PRIORITY_COLORS[v]}>{v}</Tag> },
            { title: '상태', dataIndex: 'status', width: 90,
              render: (v: string) => <Tag color={STATUS_COLORS[v]}>{v}</Tag> },
            { title: '검토자', dataIndex: 'reviewer', width: 80, render: (v: string) => v || '-' },
            { title: '승인자', dataIndex: 'approver', width: 80, render: (v: string) => v || '-' },
            { title: '일시', dataIndex: 'created_at', width: 100, render: (v: string) => dayjs(v).format('MM-DD HH:mm') },
            { title: '', width: 180, render: (_: any, r: any) => (
              <Space size={4} wrap>
                {r.status === 'draft' && (
                  <Popconfirm title="제출?" onConfirm={async () => { await putJSON(`${API}/change-requests/${r.request_id}/submit`); loadRequests(); }}>
                    <Button size="small" icon={<SendOutlined />}>제출</Button>
                  </Popconfirm>
                )}
                {r.status === 'submitted' && (
                  <Popconfirm title="검토 시작?" onConfirm={async () => { await putJSON(`${API}/change-requests/${r.request_id}/review`); loadRequests(); }}>
                    <Button size="small" icon={<FileSearchOutlined />}>검토</Button>
                  </Popconfirm>
                )}
                {(r.status === 'submitted' || r.status === 'reviewing') && (<>
                  <Popconfirm title="승인?" onConfirm={async () => { await putJSON(`${API}/change-requests/${r.request_id}/approve`); loadRequests(); }}>
                    <Button size="small" type="primary" icon={<CheckCircleOutlined />}>승인</Button>
                  </Popconfirm>
                  <Popconfirm title="반려?" onConfirm={async () => { await putJSON(`${API}/change-requests/${r.request_id}/reject`); loadRequests(); }}>
                    <Button size="small" danger icon={<CloseCircleOutlined />}>반려</Button>
                  </Popconfirm>
                </>)}
                {r.status === 'approved' && (
                  <Popconfirm title="적용?" onConfirm={async () => { await putJSON(`${API}/change-requests/${r.request_id}/apply`); loadRequests(); }}>
                    <Button size="small" type="primary" icon={<RocketOutlined />}>적용</Button>
                  </Popconfirm>
                )}
              </Space>
            )},
          ]}
        />

        {/* Request Modal */}
        <Modal title="변경 신청" open={reqModal} onCancel={() => setReqModal(false)} width={560}
          onOk={async () => {
            try {
              const vals = await reqForm.validateFields();
              if (vals.change_detail_str) {
                try { vals.change_detail = JSON.parse(vals.change_detail_str); } catch { vals.change_detail = null; }
              }
              delete vals.change_detail_str;
              await postJSON(`${API}/change-requests`, vals);
              message.success('변경 신청 완료');
              setReqModal(false);
              loadRequests();
            } catch { /* validation */ }
          }}>
          <Form form={reqForm} layout="vertical" size="small">
            <Form.Item name="request_type" label="유형" rules={[{ required: true }]}>
              <Select options={[
                { value: 'schema_change', label: '스키마 변경' },
                { value: 'column_add', label: '컬럼 추가' },
                { value: 'column_remove', label: '컬럼 삭제' },
                { value: 'column_modify', label: '컬럼 수정' },
                { value: 'table_add', label: '테이블 생성' },
                { value: 'metadata_update', label: '메타데이터 수정' },
              ]} />
            </Form.Item>
            <Row gutter={16}>
              <Col span={12}><Form.Item name="target_table" label="대상 테이블" rules={[{ required: true }]}><Input /></Form.Item></Col>
              <Col span={12}><Form.Item name="target_column" label="대상 컬럼"><Input /></Form.Item></Col>
            </Row>
            <Form.Item name="title" label="제목" rules={[{ required: true }]}><Input /></Form.Item>
            <Form.Item name="description" label="설명" rules={[{ required: true }]}><Input.TextArea rows={3} /></Form.Item>
            <Row gutter={16}>
              <Col span={12}><Form.Item name="requester" label="신청자" initialValue="사용자"><Input /></Form.Item></Col>
              <Col span={12}><Form.Item name="priority" label="우선순위" initialValue="normal">
                <Select options={['low', 'normal', 'high', 'critical'].map(v => ({ value: v, label: v }))} />
              </Form.Item></Col>
            </Row>
            <Form.Item name="change_detail_str" label="변경 상세 JSON"><Input.TextArea rows={2} placeholder='{"old_type":"varchar","new_type":"integer"}' /></Form.Item>
          </Form>
        </Modal>

        {/* Request Drawer */}
        <Drawer title={reqDrawer.data?.title} open={reqDrawer.open} width={560}
          onClose={() => setReqDrawer({ open: false, data: null })}>
          {reqDrawer.data && (<>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="유형"><Tag>{reqDrawer.data.request_type}</Tag></Descriptions.Item>
              <Descriptions.Item label="상태"><Tag color={STATUS_COLORS[reqDrawer.data.status]}>{reqDrawer.data.status}</Tag></Descriptions.Item>
              <Descriptions.Item label="대상">{reqDrawer.data.target_table}{reqDrawer.data.target_column && `.${reqDrawer.data.target_column}`}</Descriptions.Item>
              <Descriptions.Item label="우선순위"><Tag color={PRIORITY_COLORS[reqDrawer.data.priority]}>{reqDrawer.data.priority}</Tag></Descriptions.Item>
              <Descriptions.Item label="신청자">{reqDrawer.data.requester}</Descriptions.Item>
              <Descriptions.Item label="신청일">{dayjs(reqDrawer.data.created_at).format('YYYY-MM-DD HH:mm')}</Descriptions.Item>
            </Descriptions>
            <Card size="small" title="설명" style={{ marginTop: 12 }}>
              <Text>{reqDrawer.data.description}</Text>
            </Card>
            {reqDrawer.data.change_detail && (
              <Card size="small" title="변경 상세" style={{ marginTop: 12 }}>
                <pre style={{ fontSize: 12, margin: 0 }}>{JSON.stringify(
                  typeof reqDrawer.data.change_detail === 'string' ? JSON.parse(reqDrawer.data.change_detail) : reqDrawer.data.change_detail,
                  null, 2
                )}</pre>
              </Card>
            )}
            <Card size="small" title="워크플로우 이력" style={{ marginTop: 12 }}>
              <Timeline items={[
                { color: 'blue', children: <><Text strong>신청</Text> {reqDrawer.data.requester} — {dayjs(reqDrawer.data.created_at).format('YYYY-MM-DD HH:mm')}</> },
                ...(reqDrawer.data.reviewer ? [{ color: 'orange', children: <><Text strong>검토</Text> {reqDrawer.data.reviewer} {reqDrawer.data.review_comment && `— ${reqDrawer.data.review_comment}`}</> }] : []),
                ...(reqDrawer.data.approver ? [{
                  color: reqDrawer.data.status === 'rejected' ? 'red' : 'green',
                  children: <><Text strong>{reqDrawer.data.status === 'rejected' ? '반려' : '승인'}</Text> {reqDrawer.data.approver} {reqDrawer.data.approval_comment && `— ${reqDrawer.data.approval_comment}`}</>
                }] : []),
                ...(reqDrawer.data.applied_at ? [{ color: 'cyan', children: <><Text strong>적용</Text> {dayjs(reqDrawer.data.applied_at).format('YYYY-MM-DD HH:mm')}</> }] : []),
              ]} />
            </Card>
          </>)}
        </Drawer>
      </Spin>
    );
  };

  // ══════════════════════════════════════════
  //  2. Quality Rules
  // ══════════════════════════════════════════
  const renderQualityRules = () => (
    <Spin spinning={loading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { ruleForm.resetFields(); setRuleModal(true); }}>규칙 추가</Button>
        <Button icon={<PlayCircleOutlined />} onClick={async () => {
          message.loading('전체 규칙 검증 실행 중...');
          try {
            const res = await postJSON(`${API}/quality-rules/check-all`);
            setCheckResults(res);
            message.success(`검증 완료: ${res.passed}/${res.total_rules} 통과 (${res.pass_rate}%)`);
            loadRules();
          } catch { message.error('검증 실패'); }
        }}>전체 검증</Button>
        <Button icon={<ReloadOutlined />} onClick={loadRules}>새로고침</Button>
      </Space>

      {checkResults && (
        <Card size="small" style={{ marginBottom: 16, borderColor: checkResults.pass_rate >= 80 ? '#52c41a' : '#faad14' }}>
          <Row gutter={16}>
            <Col span={6}><Statistic title="총 규칙" value={checkResults.total_rules} /></Col>
            <Col span={6}><Statistic title="통과" value={checkResults.passed} valueStyle={{ color: '#3f8600' }} /></Col>
            <Col span={6}><Statistic title="실패" value={checkResults.failed} valueStyle={{ color: checkResults.failed > 0 ? '#cf1322' : undefined }} /></Col>
            <Col span={6}><Progress type="circle" percent={checkResults.pass_rate} size={60} status={checkResults.pass_rate >= 80 ? 'success' : 'exception'} /></Col>
          </Row>
        </Card>
      )}

      <Table dataSource={rules} rowKey="rule_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'rule_id', width: 45 },
          { title: '테이블', dataIndex: 'table_name', width: 140 },
          { title: '컬럼', dataIndex: 'column_name', width: 150 },
          { title: '유형', dataIndex: 'rule_type', width: 80,
            render: (v: string) => <Tag>{v}</Tag> },
          { title: '규칙명', dataIndex: 'rule_name', width: 150 },
          { title: '정상 범위', width: 100, render: (_: any, r: any) => {
            if (r.normal_min != null && r.normal_max != null) return `${r.normal_min}~${r.normal_max}`;
            if (r.enum_values) {
              const arr = typeof r.enum_values === 'string' ? JSON.parse(r.enum_values) : r.enum_values;
              return <Space wrap size={[2, 2]}>{arr?.map((v: any) => <Tag key={v} style={{ fontSize: 10 }}>{String(v)}</Tag>)}</Space>;
            }
            return '-';
          }},
          { title: '오류 범위', width: 100, render: (_: any, r: any) =>
            r.error_min != null ? `< ${r.error_min} or > ${r.error_max}` : '-' },
          { title: '심각도', dataIndex: 'severity', width: 70,
            render: (v: string) => <Tag color={SEVERITY_COLORS[v]}>{v}</Tag> },
          { title: '활성', dataIndex: 'enabled', width: 55,
            render: (v: boolean) => v ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#ccc' }} /> },
          { title: '최근 결과', dataIndex: 'last_result', width: 100,
            render: (v: any) => {
              if (!v) return '-';
              const res = typeof v === 'string' ? JSON.parse(v) : v;
              return <Tag color={res.status === 'pass' ? 'green' : res.status === 'fail' ? 'red' : 'orange'}>
                {res.status} ({res.violations || 0}건)
              </Tag>;
            },
          },
          { title: '', width: 100, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="단건 검증">
                <Button size="small" icon={<PlayCircleOutlined />} onClick={async () => {
                  try {
                    const res = await postJSON(`${API}/quality-rules/${r.rule_id}/check`);
                    message.info(`${r.rule_name}: ${res.status} (위반 ${res.violations}건/${res.total}건, ${res.violation_rate}%)`);
                    loadRules();
                  } catch { message.error('검증 실패'); }
                }} />
              </Tooltip>
              <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API}/quality-rules/${r.rule_id}`); loadRules(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />

      {/* Rule Modal */}
      <Modal title="품질 규칙 추가" open={ruleModal} onCancel={() => setRuleModal(false)} width={560}
        onOk={async () => {
          try {
            const vals = await ruleForm.validateFields();
            if (vals.enum_values_str) vals.enum_values = vals.enum_values_str.split(',').map((s: string) => s.trim());
            delete vals.enum_values_str;
            await postJSON(`${API}/quality-rules`, vals);
            message.success('규칙 생성 완료');
            setRuleModal(false);
            loadRules();
          } catch { /* validation */ }
        }}>
        <Form form={ruleForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="table_name" label="테이블" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="column_name" label="컬럼" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}><Form.Item name="rule_type" label="유형" rules={[{ required: true }]}>
              <Select options={['not_null', 'range', 'enum', 'regex', 'unique', 'referential', 'custom'].map(v => ({ value: v, label: v }))} />
            </Form.Item></Col>
            <Col span={12}><Form.Item name="rule_name" label="규칙명" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Row gutter={16}>
            <Col span={6}><Form.Item name="normal_min" label="정상 Min"><Input type="number" /></Form.Item></Col>
            <Col span={6}><Form.Item name="normal_max" label="정상 Max"><Input type="number" /></Form.Item></Col>
            <Col span={6}><Form.Item name="error_min" label="오류 Min"><Input type="number" /></Form.Item></Col>
            <Col span={6}><Form.Item name="error_max" label="오류 Max"><Input type="number" /></Form.Item></Col>
          </Row>
          <Form.Item name="enum_values_str" label="허용값 (쉼표 구분)"><Input placeholder="M,F" /></Form.Item>
          <Form.Item name="regex_pattern" label="정규식 패턴"><Input placeholder="^[A-Z0-9]{6,20}$" /></Form.Item>
          <Form.Item name="condition_expr" label="조건식 (custom)"><Input placeholder="start_date <= end_date" /></Form.Item>
          <Row gutter={16}>
            <Col span={12}><Form.Item name="severity" label="심각도" initialValue="warning">
              <Select options={['info', 'warning', 'error', 'critical'].map(v => ({ value: v, label: v }))} />
            </Form.Item></Col>
            <Col span={12}><Form.Item name="enabled" label="활성" valuePropName="checked" initialValue={true}><Switch /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
        </Form>
      </Modal>
    </Spin>
  );

  // ══════════════════════════════════════════
  //  3. Source ↔ Integrated Mapping
  // ══════════════════════════════════════════
  const renderMapping = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadMappings} style={{ marginBottom: 16 }}>새로고침</Button>

      {mappingOverview && (
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Card size="small">
              <Statistic title="총 매핑" value={mappingOverview.total_mappings} />
            </Card>
          </Col>
          <Col span={18}>
            <Card size="small" title="원천 시스템별 매핑 현황">
              <Row gutter={16}>
                {mappingOverview.by_source_system?.map((s: any) => (
                  <Col span={6} key={s.source_system}>
                    <Card size="small" style={{ borderLeft: '3px solid #1890ff' }}>
                      <Text strong>{s.source_system}</Text>
                      <div style={{ fontSize: 12, color: '#666' }}>{s.cnt}건 매핑 · {s.tables} 테이블 → {s.targets} 대상</div>
                    </Card>
                  </Col>
                ))}
              </Row>
            </Card>
          </Col>
        </Row>
      )}

      {mappingOverview?.by_target_table && (
        <Card size="small" title="통합 테이블별 원천 시스템" style={{ marginBottom: 16 }}>
          <Space wrap>
            {mappingOverview.by_target_table.map((t: any) => (
              <Tag key={t.target_table} color="blue">
                {t.target_table} ({t.cnt}건) ← {(t.systems || []).join(', ')}
              </Tag>
            ))}
          </Space>
        </Card>
      )}

      <Table dataSource={mappings} rowKey="mapping_id" size="small" pagination={{ pageSize: 20 }}
        columns={[
          { title: 'ID', dataIndex: 'mapping_id', width: 45 },
          { title: '원천 시스템', dataIndex: 'source_system', width: 100,
            render: (v: string) => <Tag color="orange">{v}</Tag> },
          { title: '원천 테이블', dataIndex: 'source_table', width: 120 },
          { title: '원천 컬럼', dataIndex: 'source_column', width: 120 },
          { title: '타입', dataIndex: 'source_type', width: 80, render: (v: string) => <Text code>{v}</Text> },
          { title: '', width: 30, render: () => <SwapOutlined style={{ color: '#1890ff' }} /> },
          { title: '통합 테이블', dataIndex: 'target_table', width: 150,
            render: (v: string) => <Tag color="green">{v}</Tag> },
          { title: '통합 컬럼', dataIndex: 'target_column', width: 150 },
          { title: '타입', dataIndex: 'target_type', width: 100, render: (v: string) => <Text code>{v}</Text> },
          { title: '변환 규칙', dataIndex: 'transform_rule', ellipsis: true,
            render: (v: string) => v ? <Tooltip title={v}><Text code style={{ fontSize: 11 }}>{v}</Text></Tooltip> : '-' },
          { title: '', width: 40, render: (_: any, r: any) => (
            <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API}/source-mappings/${r.mapping_id}`); loadMappings(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />
    </Spin>
  );

  // ══════════════════════════════════════════
  //  4. Compliance Monitoring
  // ══════════════════════════════════════════
  const renderCompliance = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadCompliance} style={{ marginBottom: 16 }}>새로고침</Button>

      {compliance && (<>
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={5}><Card size="small"><Statistic title="총 규정" value={compliance.total} /></Card></Col>
          <Col span={5}><Card size="small"><Statistic title="통과" value={compliance.passed} valueStyle={{ color: '#3f8600' }} /></Card></Col>
          <Col span={5}><Card size="small"><Statistic title="경고" value={compliance.warnings} valueStyle={{ color: '#faad14' }} /></Card></Col>
          <Col span={5}><Card size="small"><Statistic title="미충족" value={compliance.failed} valueStyle={{ color: compliance.failed > 0 ? '#cf1322' : undefined }} /></Card></Col>
          <Col span={4}><Card size="small"><Progress type="circle" percent={compliance.compliance_rate} size={60} status={compliance.compliance_rate >= 80 ? 'success' : 'exception'} /></Card></Col>
        </Row>

        {/* By category */}
        <Card size="small" title="카테고리별 준수 현황" style={{ marginBottom: 16 }}>
          <Row gutter={16}>
            {Object.entries(compliance.by_category || {}).map(([cat, data]: [string, any]) => (
              <Col span={4} key={cat}>
                <Card size="small" style={{ textAlign: 'center', borderTop: `3px solid ${CATEGORY_COLORS[cat] === 'red' ? '#f5222d' : CATEGORY_COLORS[cat] === 'magenta' ? '#eb2f96' : '#1890ff'}` }}>
                  <Tag color={CATEGORY_COLORS[cat]}>{cat}</Tag>
                  <div style={{ marginTop: 8 }}>
                    <Text strong style={{ color: '#3f8600' }}>{data.pass}</Text> / {data.total}
                    {data.warning > 0 && <Tag color="orange" style={{ marginLeft: 4 }}>{data.warning} 경고</Tag>}
                  </div>
                </Card>
              </Col>
            ))}
          </Row>
        </Card>

        {/* Rules table */}
        <Table dataSource={compliance.rules} rowKey="rule_id" size="small" pagination={false}
          columns={[
            { title: 'ID', dataIndex: 'rule_id', width: 45 },
            { title: '규정명', dataIndex: 'rule_name', width: 160 },
            { title: '관련 법규', dataIndex: 'regulation', width: 140 },
            { title: '분류', dataIndex: 'category', width: 80,
              render: (v: string) => <Tag color={CATEGORY_COLORS[v]}>{v}</Tag> },
            { title: '설명', dataIndex: 'description', ellipsis: true },
            { title: '심각도', dataIndex: 'severity', width: 70,
              render: (v: string) => <Tag color={SEVERITY_COLORS[v]}>{v}</Tag> },
            { title: '결과', dataIndex: 'result', width: 80,
              render: (v: string) => <Tag color={v === 'pass' ? 'green' : v === 'warning' ? 'orange' : 'red'}>{v || 'N/A'}</Tag> },
            { title: '실측값', dataIndex: 'actual_value', width: 70,
              render: (v: number) => v != null ? v : '-' },
            { title: '상세', dataIndex: 'detail', ellipsis: true },
            { title: '점검일', dataIndex: 'check_time', width: 90,
              render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
          ]}
        />
      </>)}
    </Spin>
  );

  // ══════════════════════════════════════════
  //  5. Pipeline Biz Status
  // ══════════════════════════════════════════
  const renderPipeline = () => (
    <Spin spinning={loading}>
      <Button icon={<ReloadOutlined />} onClick={loadPipeline} style={{ marginBottom: 16 }}>새로고침</Button>

      {pipelineDash && (<>
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={3}><Card size="small"><Statistic title="총 Pipeline" value={pipelineDash.total_pipelines} /></Card></Col>
          <Col span={3}><Card size="small"><Statistic title="실행 중" value={pipelineDash.running} valueStyle={{ color: '#1890ff' }} /></Card></Col>
          <Col span={3}><Card size="small"><Statistic title="성공" value={pipelineDash.success} valueStyle={{ color: '#3f8600' }} /></Card></Col>
          <Col span={3}><Card size="small"><Statistic title="대기" value={pipelineDash.idle} /></Card></Col>
          <Col span={3}><Card size="small"><Statistic title="처리 행수" value={pipelineDash.total_rows_processed?.toLocaleString()} /></Card></Col>
          <Col span={3}><Card size="small"><Statistic title="Biz 메타" value={pipelineDash.unique_biz_metadata} /></Card></Col>
          <Col span={6}>
            <Card size="small" title="Job 유형별">
              <Space>
                {Object.entries(pipelineDash.by_job_type || {}).map(([k, v]: [string, any]) => (
                  <Tag key={k} color={JOB_COLORS[k]}>{k}: {v}</Tag>
                ))}
              </Space>
            </Card>
          </Col>
        </Row>

        {/* Biz metadata list */}
        <Card size="small" title="적용된 Biz 메타데이터 목록" style={{ marginBottom: 16 }}>
          <Space wrap>
            {pipelineDash.biz_metadata_list?.map((b: string) => (
              <Tag key={b} color="blue">{b}</Tag>
            ))}
          </Space>
        </Card>

        {/* Pipeline table */}
        <Table dataSource={pipelineDash.pipelines} rowKey="link_id" size="small" pagination={false}
          columns={[
            { title: 'ID', dataIndex: 'link_id', width: 45 },
            { title: 'Pipeline', dataIndex: 'pipeline_name', width: 170 },
            { title: 'Job 유형', dataIndex: 'job_type', width: 80,
              render: (v: string) => <Tag color={JOB_COLORS[v]}>{v}</Tag> },
            { title: 'Biz 메타데이터', dataIndex: 'biz_metadata_ids', width: 200,
              render: (v: any) => {
                const arr = typeof v === 'string' ? JSON.parse(v) : v;
                return <Space wrap size={[2, 2]}>{(arr || []).map((b: string) => <Tag key={b} color="blue" style={{ fontSize: 11 }}>{b}</Tag>)}</Space>;
              },
            },
            { title: '소스', dataIndex: 'source_tables', width: 150,
              render: (v: any) => {
                const arr = typeof v === 'string' ? JSON.parse(v) : v;
                return <Space wrap size={[2, 2]}>{(arr || []).map((t: string) => <Tag key={t} style={{ fontSize: 11 }}>{t}</Tag>)}</Space>;
              },
            },
            { title: '대상', dataIndex: 'target_table', width: 140,
              render: (v: string) => <Tag color="green">{v}</Tag> },
            { title: '주기', dataIndex: 'schedule', width: 80 },
            { title: '담당', dataIndex: 'owner', width: 80 },
            { title: '상태', dataIndex: 'last_status', width: 70,
              render: (v: string) => <Tag color={v === 'running' ? 'processing' : v === 'success' ? 'green' : v === 'failed' ? 'red' : 'default'}>{v}</Tag> },
            { title: '처리 행수', dataIndex: 'row_count', width: 110,
              render: (v: number) => v ? v.toLocaleString() : '-' },
          ]}
        />
      </>)}
    </Spin>
  );

  return (
    <div>
      {renderOverviewCards()}
      <Segmented
        block value={view}
        onChange={(v) => setView(v as string)}
        options={[
          { label: '변경 워크플로우', value: 'workflow', icon: <AuditOutlined /> },
          { label: '품질관리 기준', value: 'quality', icon: <SafetyCertificateOutlined /> },
          { label: '원천↔통합 매핑', value: 'mapping', icon: <SwapOutlined /> },
          { label: '규정 준수', value: 'compliance', icon: <ExperimentOutlined /> },
          { label: 'Pipeline Biz', value: 'pipeline', icon: <RocketOutlined /> },
        ]}
        style={{ marginBottom: 16 }}
      />
      {view === 'workflow' && renderWorkflow()}
      {view === 'quality' && renderQualityRules()}
      {view === 'mapping' && renderMapping()}
      {view === 'compliance' && renderCompliance()}
      {view === 'pipeline' && renderPipeline()}
    </div>
  );
};

export default MetadataManagement;
