import React, { useState, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Statistic, Tabs, Table, Tag, Space, Button,
  Progress, Badge, List, Popconfirm, Spin,
  Switch, Alert, App, Segmented,
} from 'antd';
import {
  SettingOutlined, MonitorOutlined, NotificationOutlined, MenuOutlined,
  SafetyCertificateOutlined, AlertOutlined, CheckCircleOutlined,
  CloseCircleOutlined, ExclamationCircleOutlined, ReloadOutlined,
  DeleteOutlined,
  UserOutlined, CloudServerOutlined, DatabaseOutlined,
  DesktopOutlined, WarningOutlined,
  TeamOutlined, LineChartOutlined,
} from '@ant-design/icons';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RTooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell, LineChart, Line } from 'recharts';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { portalOpsApi } from '../services/portalOpsApi';
import {
  DEPT_COLORS, ACTION_LABELS, ANOMALY_TYPE_LABEL, SEVERITY_COLOR,
  AnnouncementTab, MenuManagementTab, DataQualityTab, SystemSettingsTab,
} from './PortalOpsHelpers';

const { Title, Paragraph, Text } = Typography;

// ──────────── 모니터링 탭 ────────────

const MonitoringTab: React.FC = () => {
  const { message } = App.useApp();
  const queryClient = useQueryClient();
  const [section, setSection] = useState<string>('overview');

  const { data: resources, isLoading: loadingResources } = useQuery({ queryKey: ['po-resources'], queryFn: () => portalOpsApi.getSystemResources().catch(() => null) });
  const { data: services, isLoading: loadingServices } = useQuery({ queryKey: ['po-services'], queryFn: () => portalOpsApi.getServiceStatus().catch(() => null) });
  const { data: logStats, isLoading: loadingLogStats } = useQuery({ queryKey: ['po-log-stats'], queryFn: () => portalOpsApi.getAccessLogStats().catch(() => null) });
  const { data: alertsData, isLoading: loadingAlerts } = useQuery({ queryKey: ['po-alerts'], queryFn: () => portalOpsApi.getAlerts({ status: 'active' }).catch(() => []) });
  const { data: logsData, isLoading: loadingLogs } = useQuery({ queryKey: ['po-logs'], queryFn: () => portalOpsApi.getAccessLogs({ limit: 30 }).catch(() => []) });
  const { data: anomalies, isLoading: loadingAnomalies } = useQuery({ queryKey: ['po-anomalies'], queryFn: () => portalOpsApi.getAccessAnomalies(7).catch(() => null) });
  const { data: trend, isLoading: loadingTrend } = useQuery({ queryKey: ['po-trend'], queryFn: () => portalOpsApi.getAccessTrend(7).catch(() => null) });
  const { data: retentionData, isLoading: loadingRetention } = useQuery({ queryKey: ['po-retention'], queryFn: () => portalOpsApi.getRetentionPolicies().catch(() => ({ policies: [] })) });

  const alerts = alertsData ?? [];
  const logs = logsData ?? [];
  const retention = retentionData?.policies ?? [];
  const loading = loadingResources || loadingServices || loadingLogStats || loadingAlerts || loadingLogs || loadingAnomalies || loadingTrend || loadingRetention;

  const refresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ['po-resources'] });
    queryClient.invalidateQueries({ queryKey: ['po-services'] });
    queryClient.invalidateQueries({ queryKey: ['po-log-stats'] });
    queryClient.invalidateQueries({ queryKey: ['po-alerts'] });
    queryClient.invalidateQueries({ queryKey: ['po-logs'] });
    queryClient.invalidateQueries({ queryKey: ['po-anomalies'] });
    queryClient.invalidateQueries({ queryKey: ['po-trend'] });
    queryClient.invalidateQueries({ queryKey: ['po-retention'] });
  }, [queryClient]);

  const handleResolveAlert = async (alertId: number) => {
    try {
      await portalOpsApi.updateAlert(alertId, { status: 'resolved', resolved_by: 'admin' });
      queryClient.invalidateQueries({ queryKey: ['po-alerts'] });
      message.success('알림이 해결되었습니다');
    } catch { message.error('처리 실패'); }
  };

  const statusIcon = (s: string) => s === 'healthy' ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : s === 'degraded' ? <ExclamationCircleOutlined style={{ color: '#faad14' }} /> : <CloseCircleOutlined style={{ color: '#f5222d' }} />;

  if (loading) return <div style={{ textAlign: 'center', padding: 40 }}><Spin size="large" /></div>;

  const renderOverview = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Alert
        type="info"
        showIcon
        icon={<LineChartOutlined />}
        message={
          <Space>
            <span>시계열 추이 분석이 필요하면 Grafana 대시보드를 확인하세요.</span>
            <Button type="primary" size="small" icon={<LineChartOutlined />}
              onClick={() => window.open('http://localhost:13000/d/idp-main', '_blank')}>
              상세 모니터링 (Grafana)
            </Button>
          </Space>
        }
        style={{ marginBottom: 4 }}
      />
      <Row gutter={12}>
        <Col xs={24} md={8}>
          <Card size="small" title={<><DesktopOutlined /> CPU</>}>
            <Progress type="dashboard" percent={resources?.cpu?.percent || 0} size={80}
              strokeColor={resources?.cpu?.percent > 80 ? '#f5222d' : '#006241'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>{resources?.cpu?.cores ?? '-'}코어</Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title={<><CloudServerOutlined /> 메모리</>}>
            <Progress type="dashboard" percent={resources?.memory?.percent || 0} size={80}
              strokeColor={resources?.memory?.percent > 85 ? '#f5222d' : '#005BAC'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>
              {resources?.memory?.used_gb ?? 0}GB / {resources?.memory?.total_gb ?? 0}GB
            </Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title={<><DatabaseOutlined /> 디스크</>}>
            <Progress type="dashboard" percent={resources?.disk?.percent || 0} size={80}
              strokeColor={resources?.disk?.percent > 90 ? '#f5222d' : '#52A67D'} />
            <Text type="secondary" style={{ display: 'block', textAlign: 'center', marginTop: 4 }}>
              {resources?.disk?.used_gb ?? 0}GB / {resources?.disk?.total_gb ?? 0}GB
            </Text>
          </Card>
        </Col>
      </Row>

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
                    title={<Space><Tag color={SEVERITY_COLOR[alert.severity]}>{alert.severity}</Tag><Text>{alert.source}</Text></Space>}
                    description={<Text style={{ fontSize: 12 }}>{alert.message}</Text>}
                  />
                </List.Item>
              )} />
            )}
          </Card>
        </Col>
      </Row>

      {anomalies && anomalies.total_anomalies > 0 && (
        <Alert
          type="warning"
          showIcon
          icon={<WarningOutlined />}
          message={`이상 접속 탐지: ${anomalies.total_anomalies}건 (최근 ${anomalies.period_days}일)`}
          description={
            <ul style={{ margin: '4px 0 0', paddingLeft: 20 }}>
              {anomalies.anomalies.slice(0, 5).map((a: any, i: number) => (
                <li key={i}><Tag color={a.severity === 'warning' ? 'orange' : 'blue'}>{ANOMALY_TYPE_LABEL[a.type] || a.type}</Tag> {a.user_name} ({a.department}) — {a.detail}</li>
              ))}
              {anomalies.total_anomalies > 5 && <li>외 {anomalies.total_anomalies - 5}건...</li>}
            </ul>
          }
        />
      )}
    </div>
  );

  const renderAccessStats = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Row gutter={12}>
        <Col xs={12} md={6}><Card size="small"><Statistic title="전체 로그" value={logStats?.total_logs || 0} prefix={<UserOutlined />} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="오늘 접속" value={logStats?.today_logs || 0} valueStyle={{ color: '#006241' }} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="부서 수" value={logStats?.by_department?.length || 0} prefix={<TeamOutlined />} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="사용자 수" value={logStats?.top_users?.length || 0} prefix={<UserOutlined />} /></Card></Col>
      </Row>

      <Row gutter={12}>
        <Col xs={24} md={12}>
          <Card size="small" title={<><TeamOutlined /> 부서별 접속 현황</>}>
            {logStats?.by_department?.length > 0 ? (
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie data={logStats.by_department.map((d: any) => ({ name: d.department, value: d.count }))}
                    cx="50%" cy="50%" outerRadius={80} dataKey="value" label={({ name, percent }: any) => `${name} ${(percent * 100).toFixed(0)}%`}>
                    {logStats.by_department.map((_: any, i: number) => <Cell key={i} fill={DEPT_COLORS[i % DEPT_COLORS.length]} />)}
                  </Pie>
                  <RTooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : <Text type="secondary">데이터 없음</Text>}
          </Card>
        </Col>

        <Col xs={24} md={12}>
          <Card size="small" title="행위별 접속 현황">
            {logStats?.by_action?.length > 0 ? (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={logStats.by_action.map((a: any) => ({ name: ACTION_LABELS[a.action] || a.action, count: a.count }))}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis />
                  <RTooltip />
                  <Bar dataKey="count" name="건수" fill="#006241" />
                </BarChart>
              </ResponsiveContainer>
            ) : <Text type="secondary">데이터 없음</Text>}
          </Card>
        </Col>
      </Row>

      {trend?.trend?.length > 0 && (
        <Card size="small" title={<><LineChartOutlined /> 일별 접속 추이 (최근 {trend.days}일)</>}>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={trend.trend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 10 }} />
              <YAxis />
              <RTooltip />
              <Legend />
              <Line type="monotone" dataKey="total" name="전체" stroke="#006241" strokeWidth={2} />
              <Line type="monotone" dataKey="login" name="로그인" stroke="#005BAC" strokeDasharray="4 2" />
              <Line type="monotone" dataKey="query_execute" name="쿼리" stroke="#FF6F00" strokeDasharray="4 2" />
              <Line type="monotone" dataKey="data_download" name="다운로드" stroke="#f5222d" strokeDasharray="4 2" />
            </LineChart>
          </ResponsiveContainer>
        </Card>
      )}

      {logStats?.user_action_matrix?.length > 0 && (
        <Card size="small" title="사용자-행위 매트릭스">
          <Table size="small" dataSource={logStats.user_action_matrix.map((r: any, i: number) => ({ ...r, key: i }))} pagination={false}
            scroll={{ x: 700 }}
            columns={[
              { title: '사용자', dataIndex: 'user_name', width: 80, fixed: 'left' as const },
              { title: '로그인', dataIndex: 'login', width: 70, render: (v: number) => v || '-' },
              { title: '페이지 조회', dataIndex: 'page_view', width: 90, render: (v: number) => v || '-' },
              { title: '쿼리 실행', dataIndex: 'query_execute', width: 80, render: (v: number) => v ? <Text strong style={{ color: '#005BAC' }}>{v}</Text> : '-' },
              { title: '다운로드', dataIndex: 'data_download', width: 80, render: (v: number) => v ? <Text strong style={{ color: v >= 3 ? '#f5222d' : '#006241' }}>{v}</Text> : '-' },
              { title: '내보내기', dataIndex: 'export', width: 70, render: (v: number) => v || '-' },
            ]}
          />
        </Card>
      )}
    </div>
  );

  const renderAnomalies = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Row gutter={12}>
        <Col xs={12} md={8}><Card size="small"><Statistic title="이상 탐지" value={anomalies?.total_anomalies || 0} prefix={<WarningOutlined />} valueStyle={{ color: (anomalies?.total_anomalies || 0) > 0 ? '#f5222d' : '#52c41a' }} /></Card></Col>
        <Col xs={12} md={8}><Card size="small"><Statistic title="분석 기간" value={`${anomalies?.period_days || 7}일`} /></Card></Col>
        <Col xs={12} md={8}><Card size="small"><Statistic title="유형 수" value={new Set(anomalies?.anomalies?.map((a: any) => a.type) || []).size} /></Card></Col>
      </Row>
      {anomalies?.anomalies?.length > 0 ? (
        <Table size="small" dataSource={anomalies.anomalies.map((a: any, i: number) => ({ ...a, key: i }))} pagination={false}
          columns={[
            { title: '유형', dataIndex: 'type', width: 140, render: (v: string) => <Tag color={v === 'excessive_download' ? 'red' : v === 'off_hours_access' ? 'orange' : 'blue'}>{ANOMALY_TYPE_LABEL[v] || v}</Tag> },
            { title: '심각도', dataIndex: 'severity', width: 80, render: (v: string) => <Tag color={SEVERITY_COLOR[v]}>{v}</Tag> },
            { title: '사용자', dataIndex: 'user_name', width: 80 },
            { title: '부서', dataIndex: 'department', width: 100 },
            { title: '상세', dataIndex: 'detail', ellipsis: true },
            { title: '건수', dataIndex: 'count', width: 60 },
            { title: '날짜', dataIndex: 'date', width: 100, render: (v: string | null) => v || '-' },
          ]}
        />
      ) : (
        <Alert type="success" showIcon message="이상 접속이 감지되지 않았습니다" />
      )}
    </div>
  );

  const renderLogs = () => (
    <Card size="small" title="최근 접속 로그" extra={<Button size="small" icon={<ReloadOutlined />} onClick={refresh}>새로고침</Button>}>
      <Table size="small" dataSource={logs.map((l: any, i: number) => ({ ...l, key: i }))} pagination={{ pageSize: 15, size: 'small' }}
        scroll={{ x: 900 }}
        columns={[
          { title: '사용자', dataIndex: 'user_name', width: 80, render: (v: string, r: any) => v || r.user_id },
          { title: '부서', dataIndex: 'department', width: 100, render: (v: string) => v || <Text type="secondary">-</Text> },
          { title: '행위', dataIndex: 'action', width: 110, render: (v: string) => <Tag>{ACTION_LABELS[v] || v}</Tag> },
          { title: '리소스', dataIndex: 'resource', width: 150, ellipsis: true },
          { title: 'IP', dataIndex: 'ip_address', width: 120 },
          { title: '소요(ms)', dataIndex: 'duration_ms', width: 80 },
          { title: '시간', dataIndex: 'created_at', width: 150, render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
        ]}
      />
    </Card>
  );

  const handleRetentionToggle = async (policyId: number, days: number, enabled: boolean) => {
    try {
      await portalOpsApi.updateRetentionPolicy(policyId, days, enabled);
      message.success('보존 정책이 업데이트되었습니다');
      refresh();
    } catch { message.error('정책 업데이트 실패'); }
  };

  const handleCleanup = async () => {
    try {
      const result = await portalOpsApi.runRetentionCleanup();
      message.success(`정리 완료: ${result.total_deleted}건 삭제`);
      refresh();
    } catch { message.error('정리 실행 실패'); }
  };

  const renderRetention = () => (
    <Card size="small" title="로그 보존 정책 (SER-007)" extra={
      <Popconfirm title="보존 기간 초과 로그를 정리하시겠습니까?" onConfirm={handleCleanup} okText="실행" cancelText="취소">
        <Button size="small" icon={<DeleteOutlined />} danger>정리 실행</Button>
      </Popconfirm>
    }>
      <Table size="small" dataSource={retention.map((r: any) => ({ ...r, key: r.policy_id }))} pagination={false}
        columns={[
          { title: '로그 유형', dataIndex: 'display_name', width: 150 },
          { title: '테이블', dataIndex: 'log_table', width: 150, render: (v: string) => <Text code>{v}</Text> },
          { title: '보존 기간', dataIndex: 'retention_days', width: 100, render: (v: number) => v >= 365 ? `${Math.round(v / 365)}년` : `${v}일` },
          { title: '현재 건수', dataIndex: 'current_rows', width: 100, render: (v: number) => v?.toLocaleString() || '0' },
          { title: '최초 기록', dataIndex: 'oldest_record', width: 150, render: (v: string) => v ? new Date(v).toLocaleDateString('ko-KR') : '-' },
          { title: '최근 정리', dataIndex: 'last_cleanup_at', width: 150, render: (v: string) => v ? new Date(v).toLocaleString('ko-KR') : <Text type="secondary">미실행</Text> },
          { title: '삭제 건수', dataIndex: 'rows_deleted_last', width: 90 },
          { title: '활성', dataIndex: 'enabled', width: 70, render: (v: boolean, r: any) => (
            <Switch size="small" checked={v} onChange={(checked) => handleRetentionToggle(r.policy_id, r.retention_days, checked)} />
          )},
        ]}
      />
    </Card>
  );

  return (
    <div>
      <Segmented
        block
        options={[
          { label: '시스템 현황', value: 'overview', icon: <MonitorOutlined /> },
          { label: '접속 통계', value: 'stats', icon: <LineChartOutlined /> },
          { label: '이상 탐지', value: 'anomalies', icon: <WarningOutlined /> },
          { label: '접속 로그', value: 'logs', icon: <UserOutlined /> },
          { label: '보존 정책', value: 'retention', icon: <DatabaseOutlined /> },
        ]}
        value={section}
        onChange={(v) => setSection(v as string)}
        style={{ marginBottom: 12 }}
      />
      {section === 'overview' && renderOverview()}
      {section === 'stats' && renderAccessStats()}
      {section === 'anomalies' && renderAnomalies()}
      {section === 'logs' && renderLogs()}
      {section === 'retention' && renderRetention()}
    </div>
  );
};

// ──────────── 메인 페이지 ────────────

const PortalOps: React.FC = () => {
  const [activeTab, setActiveTab] = useState('monitoring');

  const { data: overview } = useQuery({
    queryKey: ['po-overview'],
    queryFn: () => portalOpsApi.getOverview().catch(() => null),
  });

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
                loading={overview == null}
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
