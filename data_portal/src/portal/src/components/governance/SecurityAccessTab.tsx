/**
 * SecurityManagement sub-component: Reidentification Requests, User Attributes, Dynamic Policies
 * Sections 4-6 of DGR-005
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Button, Modal, Form, Input, Select, Tag, Space,
  Statistic, Row, Col, Switch, Typography, InputNumber, Descriptions,
  Steps, Drawer, Popconfirm, Alert, message,
} from 'antd';
import {
  PlusOutlined, ReloadOutlined, SendOutlined, FileProtectOutlined,
  CheckCircleOutlined, CloseCircleOutlined,
} from '@ant-design/icons';

const { TextArea } = Input;
const { Text } = Typography;
const BASE = '/api/v1/security-mgmt';

const fetchJson = async (url: string) => { const r = await fetch(url); return r.json(); };
const postJson = async (url: string, body?: any) => {
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  return r.json();
};
const putJson = async (url: string, body?: any) => {
  const r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  return r.json();
};
const deleteJson = async (url: string) => { await fetch(url, { method: 'DELETE' }); };

const STATUS_COLORS: Record<string, string> = { submitted: 'blue', reviewing: 'orange', approved: 'green', rejected: 'red', expired: 'default', revoked: 'purple', draft: 'default' };

// ---------------------------------------------------------------
// 4. Reidentification Requests (재식별 허용 신청)
// ---------------------------------------------------------------
export const ReidRequestsView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [requests, st] = await Promise.all([
        fetchJson(`${BASE}/reid-requests`),
        fetchJson(`${BASE}/reid-requests/stats`),
      ]);
      setData(requests);
      setStats(st);
    } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleCreate = async () => {
    const vals = await form.validateFields();
    vals.target_tables = vals.target_tables_str?.split(',').map((s: string) => s.trim()) || [];
    vals.target_columns = vals.target_columns_str?.split(',').map((s: string) => s.trim()) || [];
    delete vals.target_tables_str;
    delete vals.target_columns_str;
    await postJson(`${BASE}/reid-requests`, vals);
    setModalOpen(false); form.resetFields(); load();
    message.success('재식별 허용 신청 제출 완료');
  };

  const handleApprove = async (id: number) => {
    const item = data.find(d => d.request_id === id);
    await putJson(`${BASE}/reid-requests/${id}/approve?reviewer_comment=승인`, {
      scope_tables: item?.target_tables || [],
      scope_columns: item?.target_columns || [],
      row_filter: item?.row_filter,
      max_rows: 1000,
    });
    load(); message.success('승인 완료');
  };

  const STATUS_STEP: Record<string, number> = { draft: 0, submitted: 1, reviewing: 2, approved: 3, rejected: 3 };

  const columns = [
    { title: 'ID', dataIndex: 'request_id', width: 50 },
    { title: '신청자', dataIndex: 'requester', width: 80 },
    { title: '부서', dataIndex: 'department', width: 110 },
    { title: '목적', dataIndex: 'purpose', width: 200, ellipsis: true },
    { title: '상태', dataIndex: 'status', width: 80, render: (v: string) => <Tag color={STATUS_COLORS[v]}>{v}</Tag> },
    { title: '기간(일)', dataIndex: 'duration_days', width: 70, render: (v: number) => `${v}일` },
    { title: '만료', dataIndex: 'expires_at', width: 100, render: (v: string) => v ? new Date(v).toLocaleDateString() : '-' },
    {
      title: '', width: 180, render: (_: any, r: any) => (
        <Space>
          <Button size="small" onClick={() => { setSelectedItem(r); setDetailOpen(true); }}>상세</Button>
          {r.status === 'submitted' && <Button size="small" type="primary" ghost onClick={() => putJson(`${BASE}/reid-requests/${r.request_id}/review`).then(load)}>검토</Button>}
          {(r.status === 'submitted' || r.status === 'reviewing') && (
            <>
              <Button size="small" type="primary" onClick={() => handleApprove(r.request_id)}>승인</Button>
              <Button size="small" danger onClick={() => putJson(`${BASE}/reid-requests/${r.request_id}/reject?reviewer_comment=반려`).then(load)}>반려</Button>
            </>
          )}
          {r.status === 'approved' && <Button size="small" danger onClick={() => putJson(`${BASE}/reid-requests/${r.request_id}/revoke`).then(load)}>취소</Button>}
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}><Card><Statistic title="전체 신청" value={stats.total || 0} prefix={<FileProtectOutlined />} /></Card></Col>
        <Col span={6}><Card><Statistic title="대기중" value={stats.pending_count || 0} valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="승인 활성" value={stats.active_count || 0} valueStyle={{ color: '#52c41a' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="만료" value={stats.expired_count || 0} valueStyle={{ color: '#999' }} /></Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<SendOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>재식별 허용 신청</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="request_id" size="small" loading={loading} pagination={false} />

      <Modal title="재식별 허용 신청" open={modalOpen} onOk={handleCreate} onCancel={() => setModalOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="requester" label="신청자" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="department" label="부서" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Form.Item name="purpose" label="목적" rules={[{ required: true }]}><TextArea rows={2} /></Form.Item>
          <Form.Item name="justification" label="사유 (IRB 번호 포함)" rules={[{ required: true }]}><TextArea rows={3} /></Form.Item>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="target_tables_str" label="대상 테이블 (쉼표 구분)" rules={[{ required: true }]}><Input placeholder="person,condition_occurrence" /></Form.Item></Col>
            <Col span={12}><Form.Item name="target_columns_str" label="대상 컬럼 (쉼표 구분)" rules={[{ required: true }]}><Input placeholder="person_source_value" /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={16}><Form.Item name="row_filter" label="행 필터 (SQL WHERE)"><Input placeholder="condition_concept_id = 44054006" /></Form.Item></Col>
            <Col span={8}><Form.Item name="duration_days" label="기간(일)" rules={[{ required: true }]}><InputNumber min={1} max={365} style={{ width: '100%' }} /></Form.Item></Col>
          </Row>
        </Form>
      </Modal>

      <Drawer title="재식별 신청 상세" open={detailOpen} onClose={() => setDetailOpen(false)} width={600}>
        {selectedItem && (
          <>
            <Steps current={STATUS_STEP[selectedItem.status] || 0} status={selectedItem.status === 'rejected' ? 'error' : undefined} style={{ marginBottom: 24 }} items={[
              { title: '작성' }, { title: '제출' }, { title: '검토' },
              { title: selectedItem.status === 'rejected' ? '반려' : '승인' },
            ]} />
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="신청자">{selectedItem.requester}</Descriptions.Item>
              <Descriptions.Item label="부서">{selectedItem.department}</Descriptions.Item>
              <Descriptions.Item label="목적">{selectedItem.purpose}</Descriptions.Item>
              <Descriptions.Item label="사유">{selectedItem.justification}</Descriptions.Item>
              <Descriptions.Item label="대상 테이블">{selectedItem.target_tables?.join(', ')}</Descriptions.Item>
              <Descriptions.Item label="대상 컬럼">{selectedItem.target_columns?.join(', ')}</Descriptions.Item>
              <Descriptions.Item label="행 필터">{selectedItem.row_filter || '-'}</Descriptions.Item>
              <Descriptions.Item label="기간">{selectedItem.duration_days}일</Descriptions.Item>
              <Descriptions.Item label="상태"><Tag color={STATUS_COLORS[selectedItem.status]}>{selectedItem.status}</Tag></Descriptions.Item>
              <Descriptions.Item label="검토자">{selectedItem.reviewer || '-'}</Descriptions.Item>
              <Descriptions.Item label="검토 코멘트">{selectedItem.reviewer_comment || '-'}</Descriptions.Item>
              {selectedItem.status === 'approved' && (
                <>
                  <Descriptions.Item label="승인 범위 테이블">{selectedItem.scope_tables?.join(', ') || '-'}</Descriptions.Item>
                  <Descriptions.Item label="승인 범위 컬럼">{selectedItem.scope_columns?.join(', ') || '-'}</Descriptions.Item>
                  <Descriptions.Item label="최대 행수">{selectedItem.scope_max_rows || '제한 없음'}</Descriptions.Item>
                  <Descriptions.Item label="만료일">{selectedItem.expires_at ? new Date(selectedItem.expires_at).toLocaleString() : '-'}</Descriptions.Item>
                </>
              )}
            </Descriptions>
          </>
        )}
      </Drawer>
    </div>
  );
};

// ---------------------------------------------------------------
// 5. User Attributes (사용자 속성)
// ---------------------------------------------------------------
export const UserAttributesView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/user-attributes`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleSave = async () => {
    const vals = await form.validateFields();
    if (typeof vals.attributes === 'string') {
      try { vals.attributes = JSON.parse(vals.attributes); } catch { vals.attributes = {}; }
    }
    if (editItem) { await putJson(`${BASE}/user-attributes/${editItem.attr_id}`, vals); }
    else { await postJson(`${BASE}/user-attributes`, vals); }
    setModalOpen(false); form.resetFields(); setEditItem(null); load();
    message.success(editItem ? '수정 완료' : '생성 완료');
  };

  const ROLE_NAMES: Record<number, string> = { 1: '관리자', 2: '연구자', 3: '임상의', 4: '분석가' };

  const columns = [
    { title: 'ID', dataIndex: 'user_id', width: 80 },
    { title: '이름', dataIndex: 'user_name', width: 80 },
    { title: '부서', dataIndex: 'department', width: 120 },
    { title: '연구분야', dataIndex: 'research_field', width: 120, render: (v: string) => v || '-' },
    { title: '직급', dataIndex: 'rank', width: 90 },
    { title: '역할', dataIndex: 'role_id', width: 80, render: (v: number) => <Tag color="blue">{ROLE_NAMES[v] || '-'}</Tag> },
    { title: '속성', dataIndex: 'attributes', width: 200, ellipsis: true, render: (v: any) => <Text code style={{ fontSize: 11 }}>{typeof v === 'string' ? v : JSON.stringify(v)}</Text> },
    { title: '활성', dataIndex: 'active', width: 60, render: (v: boolean) => v ? <Tag color="green">활성</Tag> : <Tag>비활성</Tag> },
    {
      title: '', width: 100, render: (_: any, r: any) => (
        <Space>
          <Button size="small" onClick={() => {
            setEditItem(r);
            form.setFieldsValue({ ...r, attributes: typeof r.attributes === 'string' ? r.attributes : JSON.stringify(r.attributes || {}) });
            setModalOpen(true);
          }}>편집</Button>
          <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/user-attributes/${r.attr_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Alert message="진료과, 연구분야, 직급 등 사용자 속성 기반으로 동적 접근 제어 정책을 적용합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setEditItem(null); form.resetFields(); setModalOpen(true); }}>사용자 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="attr_id" size="small" loading={loading} pagination={false} scroll={{ x: 1000 }} />
      <Modal title={editItem ? '사용자 속성 수정' : '사용자 속성 추가'} open={modalOpen} onOk={handleSave} onCancel={() => setModalOpen(false)} width={550}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="user_id" label="사용자 ID" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="user_name" label="이름" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={8}><Form.Item name="department" label="부서" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="research_field" label="연구분야"><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="rank" label="직급" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Form.Item name="role_id" label="역할"><Select allowClear options={Object.entries(ROLE_NAMES).map(([k, v]) => ({ value: Number(k), label: v }))} /></Form.Item>
          <Form.Item name="attributes" label="추가 속성 (JSON)"><TextArea rows={3} placeholder='{"irb_certified":true,"clearance":"top_secret"}' /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ---------------------------------------------------------------
// 6. Dynamic Policies (동적 보안 정책)
// ---------------------------------------------------------------
export const DynamicPoliciesView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [evalResult, setEvalResult] = useState<any>(null);
  const [form] = Form.useForm();

  const POLICY_TYPE_LABELS: Record<string, string> = {
    row_level: 'Row-Level', column_level: 'Column-Level', cell_level: 'Cell-Level', context_based: '컨텍스트',
  };
  const POLICY_TYPE_COLORS: Record<string, string> = {
    row_level: 'red', column_level: 'orange', cell_level: 'blue', context_based: 'purple',
  };

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/dynamic-policies`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleEvaluate = async (dpId: number, userId: string) => {
    const result = await postJson(`${BASE}/dynamic-policies/${dpId}/evaluate?user_id=${userId}`);
    setEvalResult(result);
  };

  const columns = [
    { title: '우선순위', dataIndex: 'priority', width: 70, sorter: (a: any, b: any) => a.priority - b.priority },
    { title: '정책명', dataIndex: 'name', width: 220, ellipsis: true },
    { title: '유형', dataIndex: 'policy_type', width: 100, render: (v: string) => <Tag color={POLICY_TYPE_COLORS[v]}>{POLICY_TYPE_LABELS[v] || v}</Tag> },
    { title: '대상 테이블', dataIndex: 'target_tables', width: 200, render: (v: string[]) => v?.map((t: string) => <Tag key={t}>{t}</Tag>) || '-' },
    { title: '활성', dataIndex: 'enabled', width: 60, render: (v: boolean) => <Switch checked={v} size="small" disabled /> },
    { title: '평가 수', dataIndex: 'evaluated_count', width: 70 },
    {
      title: '', width: 160, render: (_: any, r: any) => (
        <Space>
          <Button size="small" type="primary" ghost onClick={() => handleEvaluate(r.dp_id, 'res01')}>시뮬레이션</Button>
          <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/dynamic-policies/${r.dp_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Alert message="사용자 속성, 시간, 위치, 목적 등 컨텍스트 기반으로 Row/Column/Cell 3단계 접근 제어를 동적으로 적용합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>정책 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="dp_id" size="small" loading={loading} pagination={false}
        expandable={{
          expandedRowRender: (r: any) => {
            const cond = typeof r.condition === 'string' ? JSON.parse(r.condition) : r.condition;
            const act = typeof r.action === 'string' ? JSON.parse(r.action) : r.action;
            return (
              <Row gutter={16}>
                <Col span={12}><Card size="small" title="조건"><pre style={{ fontSize: 11, margin: 0 }}>{JSON.stringify(cond, null, 2)}</pre></Card></Col>
                <Col span={12}><Card size="small" title="액션"><pre style={{ fontSize: 11, margin: 0 }}>{JSON.stringify(act, null, 2)}</pre></Card></Col>
              </Row>
            );
          },
        }}
      />
      {evalResult && (
        <Card title="시뮬레이션 결과" style={{ marginTop: 16 }} extra={<Button size="small" onClick={() => setEvalResult(null)}>닫기</Button>}>
          <Descriptions bordered column={2} size="small">
            <Descriptions.Item label="정책">{evalResult.policy_name}</Descriptions.Item>
            <Descriptions.Item label="유형">{evalResult.policy_type}</Descriptions.Item>
            <Descriptions.Item label="사용자">{evalResult.user_name} ({evalResult.user_id})</Descriptions.Item>
            <Descriptions.Item label="매칭">
              {evalResult.condition_matched
                ? <Tag color="green" icon={<CheckCircleOutlined />}>매칭</Tag>
                : <Tag color="red" icon={<CloseCircleOutlined />}>불일치</Tag>}
            </Descriptions.Item>
          </Descriptions>
          {evalResult.evaluation_details?.map((d: any, i: number) => (
            <Tag key={i} color={d.match ? 'green' : 'red'} style={{ margin: '8px 4px 0' }}>
              {d.condition}: {String(d.expected)} {d.match ? '=' : '!='} {String(d.actual)}
            </Tag>
          ))}
          {evalResult.action_to_apply && (
            <Card size="small" title="적용할 액션" style={{ marginTop: 12 }}>
              <pre style={{ fontSize: 11, margin: 0 }}>{JSON.stringify(evalResult.action_to_apply, null, 2)}</pre>
            </Card>
          )}
        </Card>
      )}
      <Modal title="동적 보안 정책 추가" open={modalOpen} onOk={async () => {
        const vals = await form.validateFields();
        try { vals.condition = JSON.parse(vals.condition_str); } catch { message.error('조건 JSON 형식 오류'); return; }
        try { vals.action = JSON.parse(vals.action_str); } catch { message.error('액션 JSON 형식 오류'); return; }
        if (vals.target_tables_str) { vals.target_tables = vals.target_tables_str.split(',').map((s: string) => s.trim()); }
        delete vals.condition_str; delete vals.action_str; delete vals.target_tables_str;
        await postJson(`${BASE}/dynamic-policies`, vals);
        setModalOpen(false); load(); message.success('생성 완료');
      }} onCancel={() => setModalOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="정책명" rules={[{ required: true }]}><Input /></Form.Item>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="policy_type" label="유형" rules={[{ required: true }]}><Select options={Object.entries(POLICY_TYPE_LABELS).map(([k, v]) => ({ value: k, label: v }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="priority" label="우선순위" initialValue={100}><InputNumber min={1} max={1000} style={{ width: '100%' }} /></Form.Item></Col>
          </Row>
          <Form.Item name="condition_str" label="조건 (JSON)" rules={[{ required: true }]}><TextArea rows={3} placeholder='{"user_role":"연구자","has_irb":true}' /></Form.Item>
          <Form.Item name="action_str" label="액션 (JSON)" rules={[{ required: true }]}><TextArea rows={3} placeholder='{"filter":"person_id IN (...)"}' /></Form.Item>
          <Form.Item name="target_tables_str" label="대상 테이블 (쉼표 구분)"><Input placeholder="condition_occurrence,drug_exposure" /></Form.Item>
          <Form.Item name="enabled" label="활성" valuePropName="checked" initialValue={true}><Switch /></Form.Item>
          <Form.Item name="description" label="설명"><TextArea rows={2} /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};
