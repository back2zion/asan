import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Statistic, Tabs, Table, Tag, Space, Button,
  Progress, Badge, List, Modal, Input, Select, Popconfirm, Spin,
  Switch, Tooltip, Alert, Descriptions, App,
} from 'antd';
import {
  SettingOutlined, MonitorOutlined, NotificationOutlined, MenuOutlined,
  SafetyCertificateOutlined, AlertOutlined, CheckCircleOutlined,
  CloseCircleOutlined, ExclamationCircleOutlined, ReloadOutlined,
  PlusOutlined, DeleteOutlined, EditOutlined, EyeOutlined,
  UserOutlined, CloudServerOutlined, DatabaseOutlined, PushpinOutlined,
  DesktopOutlined, PlayCircleOutlined,
} from '@ant-design/icons';
import { portalOpsApi } from '../services/portalOpsApi';

const { Title, Paragraph, Text } = Typography;

// ──────────── 모니터링 탭 ────────────

const MonitoringTab: React.FC = () => {
  const { message } = App.useApp();
  const [resources, setResources] = useState<any>(null);
  const [services, setServices] = useState<any>(null);
  const [logStats, setLogStats] = useState<any>(null);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [logs, setLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [res, svc, stats, al, lg] = await Promise.all([
        portalOpsApi.getSystemResources().catch(() => null),
        portalOpsApi.getServiceStatus().catch(() => null),
        portalOpsApi.getAccessLogStats().catch(() => null),
        portalOpsApi.getAlerts({ status: 'active' }).catch(() => []),
        portalOpsApi.getAccessLogs({ limit: 20 }).catch(() => []),
      ]);
      setResources(res);
      setServices(svc);
      setLogStats(stats);
      setAlerts(al);
      setLogs(lg);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const handleResolveAlert = async (alertId: number) => {
    try {
      await portalOpsApi.updateAlert(alertId, { status: 'resolved', resolved_by: 'admin' });
      setAlerts(prev => prev.filter(a => a.alert_id !== alertId));
      message.success('알림이 해결되었습니다');
    } catch { message.error('처리 실패'); }
  };

  const severityColor: Record<string, string> = { info: 'blue', warning: 'orange', error: 'red', critical: 'magenta' };
  const statusIcon = (s: string) => s === 'healthy' ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : s === 'degraded' ? <ExclamationCircleOutlined style={{ color: '#faad14' }} /> : <CloseCircleOutlined style={{ color: '#f5222d' }} />;

  if (loading) return <div style={{ textAlign: 'center', padding: 40 }}><Spin size="large" /></div>;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* System Resources */}
      <Row gutter={12}>
        <Col xs={24} md={8}>
          <Card size="small" title={<><DesktopOutlined /> CPU</>}>
            <Progress type="dashboard" percent={resources?.cpu?.percent || 0} size={80}
              strokeColor={resources?.cpu?.percent > 80 ? '#f5222d' : '#006241'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>{resources?.cpu?.cores}코어</Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title={<><CloudServerOutlined /> 메모리</>}>
            <Progress type="dashboard" percent={resources?.memory?.percent || 0} size={80}
              strokeColor={resources?.memory?.percent > 85 ? '#f5222d' : '#005BAC'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>
              {resources?.memory?.used_gb}GB / {resources?.memory?.total_gb}GB
            </Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title={<><DatabaseOutlined /> 디스크</>}>
            <Progress type="dashboard" percent={resources?.disk?.percent || 0} size={80}
              strokeColor={resources?.disk?.percent > 90 ? '#f5222d' : '#52A67D'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>
              {resources?.disk?.used_gb}GB / {resources?.disk?.total_gb}GB
            </Text>
          </Card>
        </Col>
      </Row>

      {/* Services + Alerts */}
      <Row gutter={12}>
        <Col xs={24} md={12}>
          <Card size="small" title={<><MonitorOutlined /> 서비스 상태</>}
            extra={<Button size="small" icon={<ReloadOutlined />} onClick={refresh}>새로고침</Button>}
          >
            <List size="small" dataSource={services?.services || []} renderItem={(svc: any) => (
              <List.Item>
                <Space>
                  {statusIcon(svc.status)}
                  <Text strong>{svc.name}</Text>
                  <Text type="secondary">:{svc.port}</Text>
                </Space>
                <Space>
                  {svc.latency_ms != null && <Tag>{svc.latency_ms}ms</Tag>}
                  <Tag color={svc.status === 'healthy' ? 'green' : svc.status === 'degraded' ? 'orange' : 'red'}>{svc.status}</Tag>
                </Space>
              </List.Item>
            )} />
            {services && (
              <div style={{ textAlign: 'center', marginTop: 8 }}>
                <Text type="secondary">{services.healthy_count}/{services.total_count} 정상</Text>
              </div>
            )}
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card size="small" title={<><AlertOutlined /> 활성 알림 ({alerts.length})</>}>
            {alerts.length === 0 ? <Text type="secondary">활성 알림 없음</Text> : (
              <List size="small" dataSource={alerts} renderItem={(alert: any) => (
                <List.Item actions={[
                  <Button size="small" type="link" onClick={() => handleResolveAlert(alert.alert_id)}>해결</Button>
                ]}>
                  <List.Item.Meta
                    title={<Space><Tag color={severityColor[alert.severity]}>{alert.severity}</Tag><Text>{alert.source}</Text></Space>}
                    description={<Text style={{ fontSize: 12 }}>{alert.message}</Text>}
                  />
                </List.Item>
              )} />
            )}
          </Card>
        </Col>
      </Row>

      {/* Access Logs */}
      <Row gutter={12}>
        <Col xs={24} md={8}>
          <Card size="small" title="접속 통계">
            <Statistic title="전체 로그" value={logStats?.total_logs || 0} />
            <Statistic title="오늘 접속" value={logStats?.today_logs || 0} style={{ marginTop: 8 }} />
          </Card>
        </Col>
        <Col xs={24} md={16}>
          <Card size="small" title="최근 접속 로그">
            <Table size="small" dataSource={logs.map((l: any, i: number) => ({ ...l, key: i }))} pagination={false}
              scroll={{ y: 200 }}
              columns={[
                { title: '사용자', dataIndex: 'user_name', width: 80, render: (v: string, r: any) => v || r.user_id },
                { title: '행위', dataIndex: 'action', width: 100, render: (v: string) => <Tag>{v}</Tag> },
                { title: '리소스', dataIndex: 'resource', width: 150, ellipsis: true },
                { title: 'IP', dataIndex: 'ip_address', width: 120 },
                { title: '시간', dataIndex: 'created_at', width: 150, render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
              ]}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

// ──────────── 공지관리 탭 ────────────

const AnnouncementTab: React.FC = () => {
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

  const priorityColor: Record<string, string> = { low: 'default', normal: 'blue', high: 'orange', urgent: 'red' };
  const typeLabel: Record<string, string> = { notice: '공지', maintenance: '점검', banner: '배너', popup: '팝업' };
  const statusColor: Record<string, string> = { draft: 'default', published: 'green', archived: 'gray' };

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

const MenuManagementTab: React.FC = () => {
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
          <Input addonBefore="라벨" value={editForm.label} onChange={e => setEditForm((f: any) => ({ ...f, label: e.target.value }))} />
          <Input addonBefore="아이콘" value={editForm.icon} onChange={e => setEditForm((f: any) => ({ ...f, icon: e.target.value }))} />
          <Input addonBefore="경로" value={editForm.path} onChange={e => setEditForm((f: any) => ({ ...f, path: e.target.value }))} />
          <Input addonBefore="순서" type="number" value={editForm.sort_order} onChange={e => setEditForm((f: any) => ({ ...f, sort_order: parseInt(e.target.value) || 0 }))} />
          <Select mode="multiple" placeholder="역할" value={editForm.roles} onChange={v => setEditForm((f: any) => ({ ...f, roles: v }))} style={{ width: '100%' }}
            options={['admin', 'researcher', 'staff', 'developer'].map(r => ({ label: r, value: r }))} />
        </Space>
      </Modal>
    </div>
  );
};

// ──────────── 데이터 품질 탭 ────────────

const DataQualityTab: React.FC = () => {
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

  const ruleTypeLabel: Record<string, string> = { completeness: '완전성', uniqueness: '유일성', validity: '유효성', freshness: '최신성', consistency: '일관성' };
  const ruleTypeColor: Record<string, string> = { completeness: 'blue', uniqueness: 'purple', validity: 'green', freshness: 'orange', consistency: 'cyan' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* Summary */}
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

      {/* Check result alert */}
      {checkResult && (
        <Alert
          type={checkResult.failed === 0 ? 'success' : 'warning'}
          message={`품질 검사 결과: ${checkResult.passed}/${checkResult.total_rules} 통과 (${checkResult.overall_score}점)`}
          closable onClose={() => setCheckResult(null)}
        />
      )}

      {/* Rules table */}
      <Card size="small"
        title={<><SafetyCertificateOutlined /> 품질 규칙</>}
        extra={<Button type="primary" icon={<PlayCircleOutlined />} size="small" onClick={runCheck} loading={checking}>전체 검사 실행</Button>}
      >
        <Table size="small" loading={loading} dataSource={rules.map((r: any) => ({ ...r, key: r.rule_id }))}
          pagination={false}
          columns={[
            { title: '테이블', dataIndex: 'table_name', width: 140 },
            { title: '컬럼', dataIndex: 'column_name', width: 140 },
            { title: '유형', dataIndex: 'rule_type', width: 80, render: (v: string) => <Tag color={ruleTypeColor[v]}>{ruleTypeLabel[v] || v}</Tag> },
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

const SystemSettingsTab: React.FC = () => {
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

// ──────────── 메인 페이지 ────────────

const PortalOps: React.FC = () => {
  const [activeTab, setActiveTab] = useState('monitoring');
  const [overview, setOverview] = useState<any>(null);

  useEffect(() => {
    portalOpsApi.getOverview().then(setOverview).catch(() => {});
  }, []);

  const statCards = [
    { title: '접속 로그', value: overview?.access_logs?.total, sub: `오늘 ${overview?.access_logs?.today || 0}건`, icon: <UserOutlined />, color: '#006241' },
    { title: '활성 알림', value: overview?.alerts?.active, sub: `위험 ${overview?.alerts?.critical || 0}건`, icon: <AlertOutlined />, color: overview?.alerts?.critical > 0 ? '#f5222d' : '#005BAC' },
    { title: '게시 공지', value: overview?.announcements?.published, icon: <NotificationOutlined />, color: '#52A67D' },
    { title: '품질 점수', value: overview?.quality?.total_rules ? `${overview.quality.passed}/${overview.quality.total_rules}` : '-', icon: <SafetyCertificateOutlined />, color: '#FF6F00' },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 120px)' }}>
      <Card style={{ marginBottom: 12 }}>
        <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
          <SettingOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
          포털 운영 관리
        </Title>
        <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 15, color: '#6c757d' }}>
          모니터링, 공지관리, 메뉴관리, 데이터 품질, 시스템 설정
        </Paragraph>
      </Card>

      <Row gutter={12} style={{ marginBottom: 12 }}>
        {statCards.map((s, i) => (
          <Col xs={12} md={6} key={i}>
            <Card styles={{ body: { padding: '12px 16px' } }}>
              <Statistic
                title={s.title}
                value={s.value ?? '-'}
                prefix={<span style={{ color: s.color }}>{s.icon}</span>}
                valueStyle={{ fontSize: 20 }}
                loading={overview === null}
              />
              {(s as any).sub && <Text type="secondary" style={{ fontSize: 11 }}>{(s as any).sub}</Text>}
            </Card>
          </Col>
        ))}
      </Row>

      <Card style={{ flex: 1, overflow: 'hidden' }} styles={{ body: { height: '100%', display: 'flex', flexDirection: 'column', overflow: 'auto' } }}>
        <Tabs activeKey={activeTab} onChange={setActiveTab} tabBarStyle={{ marginBottom: 12 }}
          items={[
            { key: 'monitoring', label: <><MonitorOutlined /> 모니터링</>, children: <MonitoringTab /> },
            { key: 'announcements', label: <><NotificationOutlined /> 공지관리</>, children: <AnnouncementTab /> },
            { key: 'menus', label: <><MenuOutlined /> 메뉴관리</>, children: <MenuManagementTab /> },
            { key: 'quality', label: <><SafetyCertificateOutlined /> 데이터 품질</>, children: <DataQualityTab /> },
            { key: 'settings', label: <><SettingOutlined /> 시스템 설정</>, children: <SystemSettingsTab /> },
          ]}
        />
      </Card>
    </div>
  );
};

export default PortalOps;
