/**
 * DGR-007: 데이터 권한 관리 구축
 * 데이터세트 권한 | 역할 할당 | 역할 파라미터 | EAM 연계 | EDW 이관 | 권한 감사
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Button, Modal, Form, Input, Select, Tag, Space, Segmented,
  Statistic, Row, Col, Badge, Drawer, Descriptions, Steps,
  Popconfirm, Alert, InputNumber, Switch, Typography, Tooltip, Timeline, App,
} from 'antd';
import {
  PlusOutlined, ReloadOutlined, TeamOutlined, KeyOutlined, SyncOutlined,
  SwapOutlined, AuditOutlined, DatabaseOutlined, CheckCircleOutlined,
  CloseCircleOutlined, EyeOutlined, LinkOutlined,
} from '@ant-design/icons';

const { TextArea } = Input;
const { Text } = Typography;
const BASE = '/api/v1/permission-mgmt';

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

const DS_TYPE_COLORS: Record<string, string> = { table_set: 'blue', column_set: 'cyan', row_filtered: 'orange', composite: 'purple' };
const GRANT_COLORS: Record<string, string> = { select: 'green', insert: 'blue', update: 'orange', delete: 'red', execute: 'purple', reidentify: 'magenta', export: 'cyan', admin: 'gold' };
const ASSIGN_COLORS: Record<string, string> = { primary: 'blue', secondary: 'green', temporary: 'orange', delegated: 'purple' };
const SYNC_COLORS: Record<string, string> = { synced: 'green', pending: 'orange', error: 'red', disabled: 'default' };
const MIG_STATUS_COLORS: Record<string, string> = { pending: 'orange', mapped: 'blue', migrated: 'green', verified: 'cyan', failed: 'red', skipped: 'default' };
const ROLE_NAMES: Record<number, string> = { 1: '관리자', 2: '연구자', 3: '임상의', 4: '분석가' };

// ═══════════════════════════════════════════════════════════
// 1. Datasets & Grants (데이터세트 권한)
// ═══════════════════════════════════════════════════════════
const DatasetGrantsView: React.FC = () => {
  const { message } = App.useApp();
  const [datasets, setDatasets] = useState<any[]>([]);
  const [grants, setGrants] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [dsModalOpen, setDsModalOpen] = useState(false);
  const [grantModalOpen, setGrantModalOpen] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [selectedDs, setSelectedDs] = useState<any>(null);
  const [effectiveOpen, setEffectiveOpen] = useState(false);
  const [effectiveData, setEffectiveData] = useState<any>(null);
  const [dsForm] = Form.useForm();
  const [grantForm] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [ds, gr] = await Promise.all([fetchJson(`${BASE}/datasets`), fetchJson(`${BASE}/grants`)]);
      setDatasets(ds);
      setGrants(gr);
    } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleShowEffective = async (userId: string) => {
    const data = await fetchJson(`${BASE}/grants/effective/${userId}`);
    setEffectiveData(data);
    setEffectiveOpen(true);
  };

  const dsColumns = [
    { title: 'ID', dataIndex: 'dataset_id', width: 45 },
    { title: '데이터세트', dataIndex: 'name', width: 180 },
    { title: '유형', dataIndex: 'dataset_type', width: 100, render: (v: string) => <Tag color={DS_TYPE_COLORS[v]}>{v}</Tag> },
    { title: '테이블', dataIndex: 'tables', width: 200, render: (v: string[]) => v?.slice(0, 3).map((t: string) => <Tag key={t}>{t}</Tag>) },
    { title: '등급', dataIndex: 'classification', width: 70, render: (v: string) => <Tag color={v === '극비' ? 'red' : v === '민감' ? 'orange' : 'blue'}>{v}</Tag> },
    { title: '소유자', dataIndex: 'owner', width: 100 },
    {
      title: '', width: 130, render: (_: any, r: any) => (
        <Space>
          <Button size="small" onClick={async () => { const d = await fetchJson(`${BASE}/datasets/${r.dataset_id}`); setSelectedDs(d); setDetailOpen(true); }}><EyeOutlined /></Button>
          <Popconfirm title="비활성?" onConfirm={async () => { await deleteJson(`${BASE}/datasets/${r.dataset_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
        </Space>
      ),
    },
  ];

  const grantColumns = [
    { title: '권한', dataIndex: 'grant_type', width: 90, render: (v: string) => <Tag color={GRANT_COLORS[v]}>{v.toUpperCase()}</Tag> },
    { title: '대상', render: (_: any, r: any) => r.dataset_name || r.target_table || '-', width: 160 },
    { title: '컬럼', dataIndex: 'target_columns', width: 150, render: (v: string[]) => v?.length ? v.join(', ') : <Text type="secondary">전체</Text> },
    { title: '행필터', dataIndex: 'row_filter', width: 150, ellipsis: true, render: (v: string) => v || '-' },
    { title: '수혜자', render: (_: any, r: any) => <span><Tag>{r.grantee_type}</Tag>{r.grantee_id}</span>, width: 130 },
    { title: '출처역할', dataIndex: 'source_role_id', width: 80, render: (v: number) => v ? <Tag color="blue">{ROLE_NAMES[v] || v}</Tag> : '-' },
    { title: '활성', dataIndex: 'active', width: 50, render: (v: boolean) => v ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#ccc' }} /> },
    {
      title: '', width: 80, render: (_: any, r: any) => r.active ? (
        <Popconfirm title="회수?" onConfirm={async () => { await putJson(`${BASE}/grants/${r.grant_id}/revoke`); load(); }}><Button size="small" danger>회수</Button></Popconfirm>
      ) : null,
    },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}><Card><Statistic title="데이터세트" value={datasets.length} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col span={6}><Card><Statistic title="활성 권한" value={grants.filter(g => g.active).length} valueStyle={{ color: '#52c41a' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="재식별 권한" value={grants.filter(g => g.grant_type === 'reidentify').length} valueStyle={{ color: '#eb2f96' }} /></Card></Col>
        <Col span={6}><Card>
          <div style={{ marginBottom: 8 }}><Text type="secondary">실효 권한 조회</Text></div>
          <Space.Compact style={{ width: '100%' }}>
            <Select defaultValue="res01" id="eff-user" style={{ width: '60%' }} options={['admin01', 'res01', 'doc01', 'ana01', 'doc02'].map(v => ({ value: v, label: v }))} />
            <Button type="primary" onClick={() => { const sel = (document.getElementById('eff-user') as any)?.querySelector('.ant-select-selection-item')?.title || 'res01'; handleShowEffective(sel); }}>조회</Button>
          </Space.Compact>
        </Card></Col>
      </Row>
      <Card title="데이터세트" size="small" extra={
        <Space>
          <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => { dsForm.resetFields(); setDsModalOpen(true); }}>데이터세트 추가</Button>
        </Space>
      } style={{ marginBottom: 16 }}>
        <Table dataSource={datasets} columns={dsColumns} rowKey="dataset_id" size="small" loading={loading} pagination={false} />
      </Card>
      <Card title="권한 부여 목록" size="small" extra={
        <Button type="primary" icon={<KeyOutlined />} onClick={() => { grantForm.resetFields(); setGrantModalOpen(true); }}>권한 부여</Button>
      }>
        <Table dataSource={grants} columns={grantColumns} rowKey="grant_id" size="small" loading={loading} pagination={{ pageSize: 15 }} scroll={{ x: 1100 }} />
      </Card>

      {/* Dataset create modal */}
      <Modal title="데이터세트 추가" open={dsModalOpen} onOk={async () => {
        const vals = await dsForm.validateFields();
        vals.tables = vals.tables_str?.split(',').map((s: string) => s.trim()) || [];
        if (vals.columns_str) { try { vals.columns = JSON.parse(vals.columns_str); } catch { vals.columns = null; } }
        delete vals.tables_str; delete vals.columns_str;
        await postJson(`${BASE}/datasets`, vals);
        setDsModalOpen(false); load(); message.success('생성 완료');
      }} onCancel={() => setDsModalOpen(false)} width={550}>
        <Form form={dsForm} layout="vertical">
          <Form.Item name="name" label="이름" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="description" label="설명"><TextArea rows={2} /></Form.Item>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="dataset_type" label="유형" rules={[{ required: true }]}><Select options={Object.entries(DS_TYPE_COLORS).map(([k]) => ({ value: k, label: k }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="classification" label="등급"><Select options={['극비', '민감', '일반', '공개'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="tables_str" label="테이블 (쉼표 구분)" rules={[{ required: true }]}><Input placeholder="person,visit_occurrence" /></Form.Item>
          <Form.Item name="columns_str" label="컬럼 (JSON, column_set용)"><TextArea rows={2} placeholder='{"person":["person_id","year_of_birth"]}' /></Form.Item>
          <Form.Item name="row_filter" label="행 필터 (SQL WHERE)"><Input placeholder="condition_concept_id = 44054006" /></Form.Item>
          <Form.Item name="owner" label="소유자"><Input /></Form.Item>
        </Form>
      </Modal>

      {/* Grant create modal */}
      <Modal title="권한 부여" open={grantModalOpen} onOk={async () => {
        const vals = await grantForm.validateFields();
        if (vals.target_columns_str) { vals.target_columns = vals.target_columns_str.split(',').map((s: string) => s.trim()); }
        delete vals.target_columns_str;
        await postJson(`${BASE}/grants`, vals);
        setGrantModalOpen(false); load(); message.success('권한 부여 완료');
      }} onCancel={() => setGrantModalOpen(false)} width={550}>
        <Form form={grantForm} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="dataset_id" label="데이터세트"><Select allowClear options={datasets.map(d => ({ value: d.dataset_id, label: `${d.dataset_id}: ${d.name}` }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="grant_type" label="권한 유형" rules={[{ required: true }]}><Select options={Object.keys(GRANT_COLORS).map(k => ({ value: k, label: k.toUpperCase() }))} /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={8}><Form.Item name="grantee_type" label="수혜자 유형" rules={[{ required: true }]}><Select options={[{ value: 'role', label: '역할' }, { value: 'user', label: '사용자' }]} /></Form.Item></Col>
            <Col span={8}><Form.Item name="grantee_id" label="수혜자 ID" rules={[{ required: true }]}><Input placeholder="역할ID 또는 사용자ID" /></Form.Item></Col>
            <Col span={8}><Form.Item name="source_role_id" label="출처 역할"><Select allowClear options={Object.entries(ROLE_NAMES).map(([k, v]) => ({ value: Number(k), label: `${k}: ${v}` }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="target_table" label="특정 테이블 (데이터세트 없을 때)"><Input /></Form.Item>
          <Form.Item name="target_columns_str" label="특정 컬럼 (쉼표 구분)"><Input /></Form.Item>
          <Form.Item name="row_filter" label="행 필터"><Input placeholder="person_id IN (:assigned_patients)" /></Form.Item>
          <Form.Item name="granted_by" label="부여자"><Input defaultValue="admin01" /></Form.Item>
        </Form>
      </Modal>

      {/* Dataset detail drawer */}
      <Drawer title="데이터세트 상세" open={detailOpen} onClose={() => setDetailOpen(false)} width={550}>
        {selectedDs && (
          <>
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="이름">{selectedDs.name}</Descriptions.Item>
              <Descriptions.Item label="유형"><Tag color={DS_TYPE_COLORS[selectedDs.dataset_type]}>{selectedDs.dataset_type}</Tag></Descriptions.Item>
              <Descriptions.Item label="테이블">{selectedDs.tables?.join(', ')}</Descriptions.Item>
              <Descriptions.Item label="컬럼">{selectedDs.columns ? JSON.stringify(selectedDs.columns) : '전체'}</Descriptions.Item>
              <Descriptions.Item label="행 필터">{selectedDs.row_filter || '없음'}</Descriptions.Item>
              <Descriptions.Item label="등급">{selectedDs.classification}</Descriptions.Item>
              <Descriptions.Item label="소유자">{selectedDs.owner}</Descriptions.Item>
            </Descriptions>
            <h4 style={{ marginTop: 16 }}>부여된 권한 ({selectedDs.grants?.length || 0}건)</h4>
            <Table dataSource={selectedDs.grants || []} rowKey="grant_id" size="small" pagination={false}
              columns={[
                { title: '권한', dataIndex: 'grant_type', render: (v: string) => <Tag color={GRANT_COLORS[v]}>{v}</Tag> },
                { title: '수혜자', render: (_: any, r: any) => `${r.grantee_type}:${r.grantee_id}` },
                { title: '활성', dataIndex: 'active', render: (v: boolean) => v ? <Tag color="green">Y</Tag> : <Tag>N</Tag> },
              ]} />
          </>
        )}
      </Drawer>

      {/* Effective grants drawer */}
      <Drawer title="실효 권한" open={effectiveOpen} onClose={() => setEffectiveOpen(false)} width={650}>
        {effectiveData && (
          <>
            <Alert message={`사용자: ${effectiveData.user_id} | 역할 수: ${effectiveData.role_count} | 총 권한: ${effectiveData.total_grants}`} type="info" showIcon style={{ marginBottom: 16 }} />
            <h4>할당된 역할</h4>
            <Space style={{ marginBottom: 16 }}>
              {effectiveData.roles?.map((r: any) => (
                <Tag key={r.role_id} color={ASSIGN_COLORS[r.type]}>
                  {ROLE_NAMES[r.role_id] || r.role_id} ({r.type}, P{r.priority})
                </Tag>
              ))}
            </Space>
            <h4>실효 권한 (출처 역할별 구분)</h4>
            <Table dataSource={effectiveData.grants || []} rowKey={(r: any) => `${r.grant_id}-${r.source_role_id}`} size="small" pagination={false}
              columns={[
                { title: '권한', dataIndex: 'grant_type', width: 80, render: (v: string) => <Tag color={GRANT_COLORS[v]}>{v}</Tag> },
                { title: '대상', render: (_: any, r: any) => r.dataset_name || r.target_table || '-', width: 150 },
                { title: '출처 역할', dataIndex: 'source_role_name', width: 100, render: (v: string, r: any) => <Tag color={ASSIGN_COLORS[r.assignment_type]}>{v}</Tag> },
                { title: '유형', dataIndex: 'assignment_type', width: 80 },
                { title: '파라미터', dataIndex: 'role_parameters', width: 200, ellipsis: true, render: (v: any) => v ? <Text code style={{ fontSize: 11 }}>{typeof v === 'string' ? v : JSON.stringify(v)}</Text> : '-' },
              ]} />
          </>
        )}
      </Drawer>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// 2. Role Assignments (복수 역할 할당)
// ═══════════════════════════════════════════════════════════
const RoleAssignmentsView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/role-assignments`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const columns = [
    { title: '사용자', dataIndex: 'user_id', width: 80 },
    { title: '이름', dataIndex: 'user_name', width: 80 },
    { title: '부서', dataIndex: 'department', width: 110 },
    { title: '역할', dataIndex: 'role_id', width: 80, render: (v: number) => <Tag color="blue">{ROLE_NAMES[v] || v}</Tag> },
    { title: '유형', dataIndex: 'assignment_type', width: 80, render: (v: string) => <Tag color={ASSIGN_COLORS[v]}>{v}</Tag> },
    { title: '우선순위', dataIndex: 'priority', width: 70 },
    { title: '파라미터', dataIndex: 'parameters', width: 250, ellipsis: true, render: (v: any) => {
      const p = typeof v === 'string' ? JSON.parse(v) : v;
      return Object.entries(p || {}).map(([k, val]) => <Tag key={k}>{k}: {String(val)}</Tag>);
    }},
    { title: '부여자', dataIndex: 'granted_by', width: 70 },
    {
      title: '', width: 60, render: (_: any, r: any) => (
        <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/role-assignments/${r.assignment_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
      ),
    },
  ];

  // Group by user
  const userGroups: Record<string, any[]> = {};
  data.forEach(item => {
    if (!userGroups[item.user_id]) userGroups[item.user_id] = [];
    userGroups[item.user_id].push(item);
  });
  const multiRoleCount = Object.values(userGroups).filter(v => v.length > 1).length;

  return (
    <div>
      <Alert message="복수의 역할 할당이 가능하며, 각 역할에서 취득된 권한은 출처 역할별로 구분 처리됩니다. (primary/secondary/temporary/delegated)" type="info" showIcon style={{ marginBottom: 16 }} />
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}><Card><Statistic title="전체 할당" value={data.length} /></Card></Col>
        <Col span={6}><Card><Statistic title="복수 역할 사용자" value={multiRoleCount} valueStyle={{ color: '#722ed1' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="Primary" value={data.filter(d => d.assignment_type === 'primary').length} valueStyle={{ color: '#1890ff' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="Secondary" value={data.filter(d => d.assignment_type === 'secondary').length} valueStyle={{ color: '#52c41a' }} /></Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>역할 할당</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="assignment_id" size="small" loading={loading} pagination={false} scroll={{ x: 1100 }} />
      <Modal title="역할 할당" open={modalOpen} onOk={async () => {
        const vals = await form.validateFields();
        if (typeof vals.parameters === 'string') { try { vals.parameters = JSON.parse(vals.parameters); } catch { vals.parameters = {}; } }
        await postJson(`${BASE}/role-assignments`, vals);
        setModalOpen(false); load(); message.success('할당 완료');
      }} onCancel={() => setModalOpen(false)} width={500}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="user_id" label="사용자 ID" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="role_id" label="역할" rules={[{ required: true }]}><Select options={Object.entries(ROLE_NAMES).map(([k, v]) => ({ value: Number(k), label: `${k}: ${v}` }))} /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={8}><Form.Item name="assignment_type" label="유형" initialValue="primary"><Select options={Object.keys(ASSIGN_COLORS).map(k => ({ value: k, label: k }))} /></Form.Item></Col>
            <Col span={8}><Form.Item name="priority" label="우선순위" initialValue={100}><InputNumber min={1} max={1000} style={{ width: '100%' }} /></Form.Item></Col>
            <Col span={8}><Form.Item name="granted_by" label="부여자"><Input defaultValue="admin01" /></Form.Item></Col>
          </Row>
          <Form.Item name="parameters" label="파라미터 (JSON)"><TextArea rows={3} placeholder='{"irb_number":"2026-0142","max_patients":200}' /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// 3. Role Parameters (역할 파라미터 정의)
// ═══════════════════════════════════════════════════════════
const RoleParamsView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [validateResult, setValidateResult] = useState<any>(null);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/role-params`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const columns = [
    { title: '역할', dataIndex: 'role_id', width: 80, render: (v: number) => <Tag color="blue">{ROLE_NAMES[v] || v}</Tag> },
    { title: '파라미터', dataIndex: 'param_name', width: 130 },
    { title: '타입', dataIndex: 'param_type', width: 80, render: (v: string) => <Tag>{v}</Tag> },
    { title: '설명', dataIndex: 'description', width: 200, ellipsis: true },
    { title: '기본값', dataIndex: 'default_value', width: 120 },
    { title: '검증 규칙', dataIndex: 'validation_rule', width: 200, render: (v: string) => v ? <Text code style={{ fontSize: 11 }}>{v}</Text> : '-' },
    { title: '필수', dataIndex: 'required', width: 50, render: (v: boolean) => v ? <Tag color="red">필수</Tag> : <Tag>선택</Tag> },
    {
      title: '', width: 60, render: (_: any, r: any) => (
        <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/role-params/${r.param_def_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
      ),
    },
  ];

  const handleValidate = async () => {
    const result = await postJson(`${BASE}/role-params/validate?role_id=2`, {
      irb_number: "2026-0142", max_patients: 200,
    });
    setValidateResult(result);
    message.info(`검증 결과: ${result.valid ? '통과' : `${result.errors?.length}건 오류`}`);
  };

  return (
    <div>
      <Alert message="각 역할에 필요한 파라미터를 정의하고, 역할 할당 시 파라미터 값을 검증합니다. (예: 연구자 역할 → IRB 번호 필수)" type="info" showIcon style={{ marginBottom: 16 }} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between' }}>
        <Button onClick={handleValidate}>연구자 역할 파라미터 검증 테스트</Button>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>파라미터 정의 추가</Button>
        </Space>
      </div>
      {validateResult && (
        <Alert style={{ marginBottom: 12 }} type={validateResult.valid ? 'success' : 'error'}
          message={validateResult.valid ? `검증 통과 (${validateResult.checked_params}개 파라미터)` : `검증 실패: ${validateResult.errors?.map((e: any) => `${e.param}: ${e.error}`).join(', ')}`} closable onClose={() => setValidateResult(null)} />
      )}
      <Table dataSource={data} columns={columns} rowKey="param_def_id" size="small" loading={loading} pagination={false} />
      <Modal title="파라미터 정의 추가" open={modalOpen} onOk={async () => {
        const vals = await form.validateFields();
        await postJson(`${BASE}/role-params`, vals);
        setModalOpen(false); load(); message.success('생성 완료');
      }} onCancel={() => setModalOpen(false)} width={500}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="role_id" label="역할" rules={[{ required: true }]}><Select options={Object.entries(ROLE_NAMES).map(([k, v]) => ({ value: Number(k), label: `${k}: ${v}` }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="param_name" label="파라미터명" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="param_type" label="타입" rules={[{ required: true }]}><Select options={['string', 'integer', 'boolean', 'date', 'list', 'json'].map(v => ({ value: v, label: v }))} /></Form.Item></Col>
            <Col span={12}><Form.Item name="required" label="필수" valuePropName="checked"><Switch /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input /></Form.Item>
          <Form.Item name="default_value" label="기본값"><Input /></Form.Item>
          <Form.Item name="validation_rule" label="검증 규칙"><Input placeholder="RANGE(1,10000) 또는 IN(a,b,c) 또는 REGEX(^...)" /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// 4. EAM Mappings (AMIS 3.0 연계)
// ═══════════════════════════════════════════════════════════
const EamMappingsView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const load = useCallback(async () => {
    setLoading(true);
    try { setData(await fetchJson(`${BASE}/eam-mappings`)); } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleSync = async () => {
    setSyncing(true);
    try {
      const result = await postJson(`${BASE}/eam-mappings/sync-all`);
      message.success(`${result.synced}개 EAM 역할 동기화 완료`);
      load();
    } finally { setSyncing(false); }
  };

  const columns = [
    { title: 'EAM 코드', dataIndex: 'eam_role_code', width: 120 },
    { title: 'EAM 역할명', dataIndex: 'eam_role_name', width: 120 },
    { title: '부서', dataIndex: 'eam_department', width: 100 },
    { title: '플랫폼 역할', dataIndex: 'platform_role_id', width: 100, render: (v: number) => <Tag color="blue">{ROLE_NAMES[v] || v}</Tag> },
    { title: '자동동기화', dataIndex: 'auto_sync', width: 80, render: (v: boolean) => v ? <Tag color="green">ON</Tag> : <Tag>OFF</Tag> },
    { title: '상태', dataIndex: 'sync_status', width: 80, render: (v: string) => <Tag color={SYNC_COLORS[v]}>{v}</Tag> },
    { title: '동기화 수', dataIndex: 'sync_count', width: 80 },
    { title: '최근 동기화', dataIndex: 'last_synced_at', width: 130, render: (v: string) => v ? new Date(v).toLocaleString() : '-' },
    {
      title: '', width: 60, render: (_: any, r: any) => (
        <Popconfirm title="삭제?" onConfirm={async () => { await deleteJson(`${BASE}/eam-mappings/${r.mapping_id}`); load(); }}><Button size="small" danger>삭제</Button></Popconfirm>
      ),
    },
  ];

  return (
    <div>
      <Alert message="AMIS 3.0 EAM(Enterprise Authorization Manager) 역할을 플랫폼 역할에 매핑하여 기본 사용자 역할의 일관성을 유지합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}><Card><Statistic title="전체 매핑" value={data.length} prefix={<LinkOutlined />} /></Card></Col>
        <Col span={6}><Card><Statistic title="동기화 완료" value={data.filter(d => d.sync_status === 'synced').length} valueStyle={{ color: '#52c41a' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="대기중" value={data.filter(d => d.sync_status === 'pending').length} valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={6}><Card><Statistic title="총 동기화 횟수" value={data.reduce((s, d) => s + (d.sync_count || 0), 0)} /></Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
        <Button icon={<SyncOutlined />} loading={syncing} onClick={handleSync} type="primary" ghost>전체 동기화</Button>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setModalOpen(true); }}>매핑 추가</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="mapping_id" size="small" loading={loading} pagination={false} scroll={{ x: 1050 }} />
      <Modal title="EAM 매핑 추가" open={modalOpen} onOk={async () => {
        const vals = await form.validateFields();
        if (vals.mapping_rule_str) { try { vals.mapping_rule = JSON.parse(vals.mapping_rule_str); } catch { vals.mapping_rule = null; } }
        delete vals.mapping_rule_str;
        await postJson(`${BASE}/eam-mappings`, vals);
        setModalOpen(false); load(); message.success('생성 완료');
      }} onCancel={() => setModalOpen(false)} width={500}>
        <Form form={form} layout="vertical">
          <Row gutter={12}>
            <Col span={12}><Form.Item name="eam_role_code" label="EAM 코드" rules={[{ required: true }]}><Input placeholder="EAM-DOC-001" /></Form.Item></Col>
            <Col span={12}><Form.Item name="eam_role_name" label="EAM 역할명" rules={[{ required: true }]}><Input /></Form.Item></Col>
          </Row>
          <Row gutter={12}>
            <Col span={12}><Form.Item name="eam_department" label="부서"><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="platform_role_id" label="플랫폼 역할" rules={[{ required: true }]}><Select options={Object.entries(ROLE_NAMES).map(([k, v]) => ({ value: Number(k), label: `${k}: ${v}` }))} /></Form.Item></Col>
          </Row>
          <Form.Item name="auto_sync" label="자동동기화" valuePropName="checked" initialValue={true}><Switch /></Form.Item>
          <Form.Item name="mapping_rule_str" label="매핑 규칙 (JSON)"><TextArea rows={2} placeholder='{"privilege_level":"clinical","sync_params":{"read_only":true}}' /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// 5. EDW Migration (EDW 권한 이관)
// ═══════════════════════════════════════════════════════════
const EdwMigrationView: React.FC = () => {
  const { message } = App.useApp();
  const [data, setData] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [migs, st] = await Promise.all([
        fetchJson(`${BASE}/edw-migrations`),
        fetchJson(`${BASE}/edw-migrations/stats`),
      ]);
      setData(migs);
      setStats(st);
    } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const columns = [
    { title: 'EDW 소스', dataIndex: 'edw_source', width: 90, render: (v: string) => <Tag color={v === 'EDW_STAT' ? 'blue' : 'magenta'}>{v}</Tag> },
    { title: '권한유형', dataIndex: 'edw_permission_type', width: 90 },
    { title: 'EDW 객체', dataIndex: 'edw_object_name', width: 170, ellipsis: true },
    { title: '수혜자', dataIndex: 'edw_grantee', width: 110 },
    { title: '플랫폼 데이터세트', dataIndex: 'dataset_name', width: 160, render: (v: string) => v || <Text type="secondary">미매핑</Text> },
    { title: '플랫폼 역할', dataIndex: 'platform_role_id', width: 80, render: (v: number) => v ? <Tag color="blue">{ROLE_NAMES[v] || v}</Tag> : '-' },
    { title: '상태', dataIndex: 'status', width: 80, render: (v: string) => <Tag color={MIG_STATUS_COLORS[v]}>{v}</Tag> },
    {
      title: '', width: 150, render: (_: any, r: any) => (
        <Space>
          {r.status === 'pending' && r.platform_dataset_id && (
            <Button size="small" type="primary" onClick={async () => { await putJson(`${BASE}/edw-migrations/${r.migration_id}/migrate`); load(); message.success('이관 완료'); }}>이관</Button>
          )}
          {r.status === 'migrated' && (
            <Button size="small" onClick={async () => { await putJson(`${BASE}/edw-migrations/${r.migration_id}/verify`); load(); message.success('검증 완료'); }}>검증</Button>
          )}
          {r.status === 'pending' && (
            <Popconfirm title="건너뛸까요?" onConfirm={async () => { await putJson(`${BASE}/edw-migrations/${r.migration_id}/skip?reason=수동이관`); load(); }}><Button size="small">건너뜀</Button></Popconfirm>
          )}
        </Space>
      ),
    },
  ];

  const MIG_STEP: Record<string, number> = { pending: 0, mapped: 1, migrated: 2, verified: 3, failed: 2, skipped: 0 };

  return (
    <div>
      <Alert message="기존 EDW(Enterprise Data Warehouse) 통계 조회 권한 및 재식별 권한을 플랫폼으로 이관합니다." type="info" showIcon style={{ marginBottom: 16 }} />
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={5}><Card><Statistic title="전체" value={stats.total || 0} /></Card></Col>
        <Col span={5}><Card><Statistic title="이관 완료" value={stats.by_status?.find((s: any) => s.status === 'migrated')?.cnt || 0} valueStyle={{ color: '#52c41a' }} /></Card></Col>
        <Col span={5}><Card><Statistic title="검증 완료" value={stats.by_status?.find((s: any) => s.status === 'verified')?.cnt || 0} valueStyle={{ color: '#13c2c2' }} /></Card></Col>
        <Col span={5}><Card><Statistic title="대기중" value={stats.by_status?.find((s: any) => s.status === 'pending')?.cnt || 0} valueStyle={{ color: '#fa8c16' }} /></Card></Col>
        <Col span={4}><Card><Statistic title="건너뜀" value={stats.by_status?.find((s: any) => s.status === 'skipped')?.cnt || 0} /></Card></Col>
      </Row>
      <Steps current={2} style={{ marginBottom: 16 }} items={[
        { title: '매핑', description: `${stats.by_status?.find((s: any) => s.status === 'mapped')?.cnt || 0}건` },
        { title: '이관', description: `${stats.by_status?.find((s: any) => s.status === 'migrated')?.cnt || 0}건` },
        { title: '검증', description: `${stats.by_status?.find((s: any) => s.status === 'verified')?.cnt || 0}건` },
      ]} />
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end' }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="migration_id" size="small" loading={loading} pagination={false} scroll={{ x: 1100 }} />
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// 6. Audit (권한 감사)
// ═══════════════════════════════════════════════════════════
const AuditView: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [logs, st] = await Promise.all([
        fetchJson(`${BASE}/audit`),
        fetchJson(`${BASE}/audit/stats`),
      ]);
      setData(logs);
      setStats(st);
    } finally { setLoading(false); }
  }, []);
  useEffect(() => { load(); }, [load]);

  const ACTION_COLORS: Record<string, string> = {
    GRANT: 'green', REVOKE: 'red', ASSIGN_ROLE: 'blue', UNASSIGN_ROLE: 'orange',
    GRANT_REIDENTIFY: 'magenta', EAM_SYNC: 'purple', EDW_MIGRATE: 'cyan',
  };

  const columns = [
    { title: '시간', dataIndex: 'created_at', width: 150, render: (v: string) => new Date(v).toLocaleString() },
    { title: '액션', dataIndex: 'action', width: 130, render: (v: string) => <Tag color={ACTION_COLORS[v] || 'default'}>{v}</Tag> },
    { title: '수행자', dataIndex: 'actor', width: 80 },
    { title: '대상유형', dataIndex: 'target_type', width: 90 },
    { title: '대상ID', dataIndex: 'target_id', width: 80 },
    { title: '상세', dataIndex: 'details', ellipsis: true, render: (v: any) => <Text code style={{ fontSize: 11 }}>{typeof v === 'string' ? v : JSON.stringify(v)}</Text> },
    { title: '출처역할', dataIndex: 'source_roles', width: 100, render: (v: number[]) => v?.map((r: number) => <Tag key={r} color="blue">{ROLE_NAMES[r] || r}</Tag>) },
  ];

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}><Card><Statistic title="전체 감사 로그" value={stats.total || 0} prefix={<AuditOutlined />} /></Card></Col>
        <Col span={8}><Card>
          <div style={{ marginBottom: 4 }}><Text type="secondary">액션별</Text></div>
          {stats.by_action?.slice(0, 4).map((a: any) => <Tag key={a.action} color={ACTION_COLORS[a.action]}>{a.action}: {a.cnt}</Tag>)}
        </Card></Col>
        <Col span={8}><Card>
          <div style={{ marginBottom: 4 }}><Text type="secondary">수행자별</Text></div>
          {stats.by_actor?.slice(0, 4).map((a: any) => <Tag key={a.actor}>{a.actor}: {a.cnt}</Tag>)}
        </Card></Col>
      </Row>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'flex-end' }}>
        <Button icon={<ReloadOutlined />} onClick={load}>새로고침</Button>
      </div>
      <Table dataSource={data} columns={columns} rowKey="audit_id" size="small" loading={loading} pagination={{ pageSize: 20 }} scroll={{ x: 1000 }} />
    </div>
  );
};

// ═══════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════
const PermissionManagement: React.FC = () => {
  const [activeView, setActiveView] = useState('datasets');

  const views: Record<string, React.ReactNode> = {
    datasets: <DatasetGrantsView />,
    roles: <RoleAssignmentsView />,
    params: <RoleParamsView />,
    eam: <EamMappingsView />,
    edw: <EdwMigrationView />,
    audit: <AuditView />,
  };

  return (
    <div>
      <Segmented
        value={activeView}
        onChange={(v) => setActiveView(v as string)}
        options={[
          { value: 'datasets', label: '데이터세트 권한' },
          { value: 'roles', label: '역할 할당' },
          { value: 'params', label: '역할 파라미터' },
          { value: 'eam', label: 'EAM 연계' },
          { value: 'edw', label: 'EDW 이관' },
          { value: 'audit', label: '권한 감사' },
        ]}
        block
        style={{ marginBottom: 16 }}
      />
      {views[activeView]}
    </div>
  );
};

export default PermissionManagement;
