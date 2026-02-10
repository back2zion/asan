import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Statistic, Table, Tag, Space, Button,
  Modal, Input, Select, Popconfirm, Spin, Switch, App, Alert,
} from 'antd';
import {
  SafetyCertificateOutlined, CheckCircleOutlined,
  CloseCircleOutlined,
  PlusOutlined, DeleteOutlined, EditOutlined,
  PushpinOutlined, PlayCircleOutlined,
} from '@ant-design/icons';
import { portalOpsApi } from '../services/portalOpsApi';

const { Text } = Typography;

export const DEPT_COLORS = ['#006241', '#005BAC', '#52A67D', '#FF6F00', '#8B5CF6', '#EC4899', '#14B8A6', '#F59E0B'];

export const ACTION_LABELS: Record<string, string> = {
  login: '로그인',
  page_view: '페이지 조회',
  query_execute: '쿼리 실행',
  data_download: '데이터 다운로드',
  export: '내보내기',
};

export const ANOMALY_TYPE_LABEL: Record<string, string> = {
  excessive_download: '과다 다운로드',
  repeated_query: '반복 쿼리',
  off_hours_access: '비정상 시간대 접속',
};

export const SEVERITY_COLOR: Record<string, string> = { info: 'blue', warning: 'orange', error: 'red', critical: 'magenta' };

export const RULE_TYPE_LABEL: Record<string, string> = { completeness: '완전성', uniqueness: '유일성', validity: '유효성', freshness: '최신성', consistency: '일관성' };

export const RULE_TYPE_COLOR: Record<string, string> = { completeness: 'blue', uniqueness: 'purple', validity: 'green', freshness: 'orange', consistency: 'cyan' };

// ──────────── 공지관리 탭 ────────────

const priorityColor: Record<string, string> = { low: 'default', normal: 'blue', high: 'orange', urgent: 'red' };
const typeLabel: Record<string, string> = { notice: '공지', maintenance: '점검', banner: '배너', popup: '팝업' };
const statusColor: Record<string, string> = { draft: 'default', published: 'green', archived: 'gray' };

export const AnnouncementTab: React.FC = () => {
  const { message } = App.useApp();
  const [announcements, setAnnouncements] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [form, setForm] = useState({ title: '', content: '', ann_type: 'notice', priority: 'normal', is_pinned: false });

  const load = useCallback(async () => {
    setLoading(true);
    try { setAnnouncements(await portalOpsApi.getAnnouncements()); } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const openCreate = () => {
    setEditId(null);
    setForm({ title: '', content: '', ann_type: 'notice', priority: 'normal', is_pinned: false });
    setModalOpen(true);
  };

  const openEdit = (ann: any) => {
    setEditId(ann.ann_id);
    setForm({ title: ann.title, content: ann.content, ann_type: ann.ann_type, priority: ann.priority, is_pinned: ann.is_pinned });
    setModalOpen(true);
  };

  const handleSave = async () => {
    if (!form.title.trim() || !form.content.trim()) { message.warning('제목과 내용을 입력하세요'); return; }
    try {
      if (editId) {
        await portalOpsApi.updateAnnouncement(editId, form);
      } else {
        await portalOpsApi.createAnnouncement(form);
      }
      message.success(editId ? '수정되었습니다' : '생성되었습니다');
      setModalOpen(false);
      load();
    } catch { message.error('저장 실패'); }
  };

  const handlePublish = async (annId: number) => {
    try { await portalOpsApi.updateAnnouncement(annId, { status: 'published' }); message.success('게시되었습니다'); load(); } catch { message.error('실패'); }
  };

  const handleArchive = async (annId: number) => {
    try { await portalOpsApi.updateAnnouncement(annId, { status: 'archived' }); message.success('보관되었습니다'); load(); } catch { message.error('실패'); }
  };

  const handleDelete = async (annId: number) => {
    try { await portalOpsApi.deleteAnnouncement(annId); message.success('삭제되었습니다'); load(); } catch { message.error('실패'); }
  };

  return (
    <div>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between' }}>
        <Text type="secondary">공지사항, 점검 안내, 배너, 팝업을 관리합니다</Text>
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate} size="small">공지 작성</Button>
      </div>
      <Table size="small" loading={loading} dataSource={announcements.map((a: any) => ({ ...a, key: a.ann_id }))}
        pagination={{ pageSize: 10 }}
        columns={[
          { title: '', dataIndex: 'is_pinned', width: 30, render: (v: boolean) => v ? <PushpinOutlined style={{ color: '#f5222d' }} /> : null },
          { title: '유형', dataIndex: 'ann_type', width: 60, render: (v: string) => <Tag>{typeLabel[v] || v}</Tag> },
          { title: '제목', dataIndex: 'title', ellipsis: true },
          { title: '우선순위', dataIndex: 'priority', width: 80, render: (v: string) => <Tag color={priorityColor[v]}>{v}</Tag> },
          { title: '상태', dataIndex: 'status', width: 70, render: (v: string) => <Tag color={statusColor[v]}>{v}</Tag> },
          { title: '조회', dataIndex: 'view_count', width: 50 },
          { title: '작성일', dataIndex: 'created_at', width: 140, render: (v: string) => v ? new Date(v).toLocaleDateString('ko-KR') : '-' },
          {
            title: '관리', width: 180, render: (_: any, r: any) => (
              <Space>
                {r.status === 'draft' && <Button size="small" type="link" onClick={() => handlePublish(r.ann_id)}>게시</Button>}
                {r.status === 'published' && <Button size="small" type="link" onClick={() => handleArchive(r.ann_id)}>보관</Button>}
                <Button size="small" type="link" icon={<EditOutlined />} onClick={() => openEdit(r)} />
                <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(r.ann_id)} okText="삭제" cancelText="취소">
                  <Button size="small" type="link" danger icon={<DeleteOutlined />} />
                </Popconfirm>
              </Space>
            ),
          },
        ]}
      />
      <Modal title={editId ? '공지 수정' : '공지 작성'} open={modalOpen} onOk={handleSave} onCancel={() => setModalOpen(false)} okText="저장" width={600}>
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Input placeholder="제목" value={form.title} onChange={e => setForm(f => ({ ...f, title: e.target.value }))} />
          <Input.TextArea placeholder="내용" value={form.content} onChange={e => setForm(f => ({ ...f, content: e.target.value }))} rows={4} />
          <Row gutter={12}>
            <Col span={8}>
              <Select value={form.ann_type} onChange={v => setForm(f => ({ ...f, ann_type: v }))} style={{ width: '100%' }}
                options={[{ value: 'notice', label: '공지' }, { value: 'maintenance', label: '점검' }, { value: 'banner', label: '배너' }, { value: 'popup', label: '팝업' }]} />
            </Col>
            <Col span={8}>
              <Select value={form.priority} onChange={v => setForm(f => ({ ...f, priority: v }))} style={{ width: '100%' }}
                options={[{ value: 'low', label: '낮음' }, { value: 'normal', label: '보통' }, { value: 'high', label: '높음' }, { value: 'urgent', label: '긴급' }]} />
            </Col>
            <Col span={8}>
              <Switch checked={form.is_pinned} onChange={v => setForm(f => ({ ...f, is_pinned: v }))} checkedChildren="고정" unCheckedChildren="일반" />
            </Col>
          </Row>
        </Space>
      </Modal>
    </div>
  );
};

// ──────────── 메뉴관리 탭 ────────────

export const MenuManagementTab: React.FC = () => {
  const { message } = App.useApp();
  const [menus, setMenus] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [editKey, setEditKey] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<any>({});

  const load = useCallback(async () => {
    setLoading(true);
    try { setMenus(await portalOpsApi.getMenuItems()); } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleToggleVisible = async (menuKey: string, visible: boolean) => {
    try {
      await portalOpsApi.updateMenuItem(menuKey, { visible });
      setMenus(prev => prev.map(m => m.menu_key === menuKey ? { ...m, visible } : m));
    } catch { message.error('변경 실패'); }
  };

  const handleSaveEdit = async () => {
    if (!editKey) return;
    try {
      await portalOpsApi.updateMenuItem(editKey, editForm);
      message.success('수정되었습니다');
      setEditKey(null);
      load();
    } catch { message.error('수정 실패'); }
  };

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <Text type="secondary">GNB 메뉴 구조를 동적으로 관리합니다. 역할별 메뉴 노출을 제어할 수 있습니다.</Text>
      </div>
      <Table size="small" loading={loading} dataSource={menus.map((m: any) => ({ ...m, key: m.menu_key }))}
        pagination={false}
        columns={[
          { title: '순서', dataIndex: 'sort_order', width: 50 },
          { title: '키', dataIndex: 'menu_key', width: 120 },
          { title: '라벨', dataIndex: 'label', width: 150 },
          { title: '아이콘', dataIndex: 'icon', width: 150, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
          { title: '경로', dataIndex: 'path', width: 150 },
          { title: '역할', dataIndex: 'roles', width: 200, render: (v: string[]) => v?.map((r: string) => <Tag key={r} style={{ fontSize: 10 }}>{r}</Tag>) },
          { title: '표시', dataIndex: 'visible', width: 60, render: (v: boolean, r: any) => <Switch size="small" checked={v} onChange={val => handleToggleVisible(r.menu_key, val)} /> },
          {
            title: '', width: 60, render: (_: any, r: any) => (
              <Button size="small" type="link" icon={<EditOutlined />} onClick={() => {
                setEditKey(r.menu_key);
                setEditForm({ label: r.label, icon: r.icon, path: r.path, sort_order: r.sort_order, roles: r.roles });
              }} />
            ),
          },
        ]}
      />
      <Modal title={`메뉴 수정: ${editKey}`} open={!!editKey} onOk={handleSaveEdit} onCancel={() => setEditKey(null)} okText="저장">
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <div><Text type="secondary" style={{ fontSize: 12 }}>라벨</Text><Input value={editForm.label} onChange={e => setEditForm((f: any) => ({ ...f, label: e.target.value }))} /></div>
          <div><Text type="secondary" style={{ fontSize: 12 }}>아이콘</Text><Input value={editForm.icon} onChange={e => setEditForm((f: any) => ({ ...f, icon: e.target.value }))} /></div>
          <div><Text type="secondary" style={{ fontSize: 12 }}>경로</Text><Input value={editForm.path} onChange={e => setEditForm((f: any) => ({ ...f, path: e.target.value }))} /></div>
          <div><Text type="secondary" style={{ fontSize: 12 }}>순서</Text><Input type="number" value={editForm.sort_order} onChange={e => setEditForm((f: any) => ({ ...f, sort_order: parseInt(e.target.value) || 0 }))} /></div>
          <div><Text type="secondary" style={{ fontSize: 12 }}>역할</Text><Select mode="multiple" placeholder="역할" value={editForm.roles} onChange={v => setEditForm((f: any) => ({ ...f, roles: v }))} style={{ width: '100%' }}
            options={['admin', 'researcher', 'staff', 'developer'].map(r => ({ label: r, value: r }))} /></div>
        </Space>
      </Modal>
    </div>
  );
};

// ──────────── 데이터 품질 탭 ────────────

export const DataQualityTab: React.FC = () => {
  const { message } = App.useApp();
  const [rules, setRules] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [checking, setChecking] = useState(false);
  const [checkResult, setCheckResult] = useState<any>(null);
  const [summary, setSummary] = useState<any>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [r, s] = await Promise.all([portalOpsApi.getQualityRules(), portalOpsApi.getQualitySummary()]);
      setRules(r);
      setSummary(s);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const runCheck = async () => {
    setChecking(true);
    try {
      const result = await portalOpsApi.runQualityCheck();
      setCheckResult(result);
      message.success(`품질 검사 완료: ${result.passed}/${result.total_rules} 통과`);
      load();
    } catch { message.error('품질 검사 실패'); }
    setChecking(false);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Row gutter={12}>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="전체 규칙" value={summary?.total_rules || 0} prefix={<SafetyCertificateOutlined />} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="통과" value={summary?.passed || 0} valueStyle={{ color: '#52c41a' }} prefix={<CheckCircleOutlined />} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="실패" value={summary?.failed || 0} valueStyle={{ color: '#f5222d' }} prefix={<CloseCircleOutlined />} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="전체 점수" value={summary?.overall_score ?? '-'} suffix="점" valueStyle={{ color: '#006241' }} />
          </Card>
        </Col>
      </Row>

      {checkResult && (
        <Alert
          type={checkResult.failed === 0 ? 'success' : 'warning'}
          message={`품질 검사 결과: ${checkResult.passed}/${checkResult.total_rules} 통과 (${checkResult.overall_score}점)`}
          closable onClose={() => setCheckResult(null)}
        />
      )}

      <Card size="small"
        title={<><SafetyCertificateOutlined /> 품질 규칙</>}
        extra={<Button type="primary" icon={<PlayCircleOutlined />} size="small" onClick={runCheck} loading={checking}>전체 검사 실행</Button>}
      >
        <Table size="small" loading={loading} dataSource={rules.map((r: any) => ({ ...r, key: r.rule_id }))}
          pagination={false}
          columns={[
            { title: '테이블', dataIndex: 'table_name', width: 140 },
            { title: '컬럼', dataIndex: 'column_name', width: 140 },
            { title: '유형', dataIndex: 'rule_type', width: 80, render: (v: string) => <Tag color={RULE_TYPE_COLOR[v]}>{RULE_TYPE_LABEL[v] || v}</Tag> },
            { title: '설명', dataIndex: 'description', ellipsis: true },
            { title: '기준', dataIndex: 'threshold', width: 60, render: (v: number) => `${v}` },
            {
              title: '점수', dataIndex: 'last_score', width: 80, render: (v: number | null, r: any) => {
                if (v == null) return <Text type="secondary">미검사</Text>;
                const passed = r.rule_type === 'freshness' ? v <= r.threshold : v >= r.threshold;
                return <Text style={{ color: passed ? '#52c41a' : '#f5222d', fontWeight: 600 }}>{v.toFixed(1)}</Text>;
              },
            },
            {
              title: '결과', width: 60, render: (_: any, r: any) => {
                if (r.last_score == null) return '-';
                const passed = r.rule_type === 'freshness' ? r.last_score <= r.threshold : r.last_score >= r.threshold;
                return passed ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#f5222d' }} />;
              },
            },
          ]}
        />
      </Card>
    </div>
  );
};

// ──────────── 시스템 설정 탭 ────────────

export const SystemSettingsTab: React.FC = () => {
  const { message } = App.useApp();
  const [settings, setSettings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [editKey, setEditKey] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');

  const load = useCallback(async () => {
    setLoading(true);
    try { setSettings(await portalOpsApi.getSettings()); } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleSave = async () => {
    if (!editKey) return;
    try {
      let parsed: any;
      try { parsed = JSON.parse(editValue); } catch { parsed = editValue; }
      await portalOpsApi.updateSetting(editKey, parsed);
      message.success('설정이 변경되었습니다');
      setEditKey(null);
      load();
    } catch { message.error('저장 실패'); }
  };

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <Text type="secondary">포털 시스템 설정을 관리합니다</Text>
      </div>
      <Table size="small" loading={loading} dataSource={settings.map((s: any) => ({ ...s, rowKey: s.key }))}
        rowKey="key" pagination={false}
        columns={[
          { title: '설정 키', dataIndex: 'key', width: 200, render: (v: string) => <Text code>{v}</Text> },
          { title: '값', dataIndex: 'value', width: 200, render: (v: any) => <Text>{JSON.stringify(v)}</Text> },
          { title: '설명', dataIndex: 'description', ellipsis: true },
          { title: '수정자', dataIndex: 'updated_by', width: 80 },
          { title: '수정일', dataIndex: 'updated_at', width: 140, render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
          {
            title: '', width: 60, render: (_: any, r: any) => (
              <Button size="small" type="link" icon={<EditOutlined />} onClick={() => {
                setEditKey(r.key);
                setEditValue(JSON.stringify(r.value));
              }} />
            ),
          },
        ]}
      />
      <Modal title={`설정 수정: ${editKey}`} open={!!editKey} onOk={handleSave} onCancel={() => setEditKey(null)} okText="저장">
        <Input.TextArea value={editValue} onChange={e => setEditValue(e.target.value)} rows={3} style={{ fontFamily: 'monospace' }} />
      </Modal>
    </div>
  );
};
