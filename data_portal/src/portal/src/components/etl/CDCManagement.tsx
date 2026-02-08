/**
 * DIR-005: CDC 변경 데이터 캡처 관리
 * 커넥터 설정, 서비스 제어, 오프셋 추적, 이벤트 모니터링, Iceberg 싱크
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Switch, Segmented, Badge,
  Tooltip, Descriptions, Drawer, Popconfirm, message,
} from 'antd';
import {
  DatabaseOutlined, PlayCircleOutlined, PauseCircleOutlined,
  StopOutlined, ReloadOutlined, PlusOutlined, ThunderboltOutlined,
  CloudServerOutlined, CheckCircleOutlined, CloseCircleOutlined,
  SyncOutlined, WarningOutlined, ApiOutlined, SettingOutlined,
  DeleteOutlined, EyeOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/cdc';

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

const DB_TYPE_CONFIG: Record<string, { color: string; label: string }> = {
  postgresql: { color: '#336791', label: 'PostgreSQL' },
  mysql: { color: '#4479A1', label: 'MySQL' },
  oracle: { color: '#F80000', label: 'Oracle' },
  sqlserver: { color: '#CC2927', label: 'SQL Server' },
  mongodb: { color: '#47A248', label: 'MongoDB' },
};

const STATUS_CONFIG: Record<string, { color: string; icon: React.ReactNode }> = {
  running: { color: 'green', icon: <SyncOutlined spin /> },
  stopped: { color: 'default', icon: <StopOutlined /> },
  error: { color: 'red', icon: <CloseCircleOutlined /> },
  paused: { color: 'orange', icon: <PauseCircleOutlined /> },
};

const CDCManagement: React.FC = () => {
  const [section, setSection] = useState<string>('overview');
  const [services, setServices] = useState<any[]>([]);
  const [summary, setSummary] = useState<any>(null);
  const [events, setEvents] = useState<any[]>([]);
  const [eventStats, setEventStats] = useState<any>(null);
  const [offsets, setOffsets] = useState<any[]>([]);
  const [sinks, setSinks] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [detailDrawer, setDetailDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });
  const [connectorModal, setConnectorModal] = useState(false);
  const [connectorForm] = Form.useForm();

  const loadOverview = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/service-status`);
      setServices(data.services || []);
      setSummary(data.summary || null);
    } catch { message.error('CDC 현황 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadEvents = useCallback(async () => {
    setLoading(true);
    try {
      const [evtData, statsData] = await Promise.all([
        fetchJSON(`${API_BASE}/events?limit=50`),
        fetchJSON(`${API_BASE}/events/stats`),
      ]);
      setEvents(evtData.events || []);
      setEventStats(statsData);
    } catch { message.error('이벤트 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadOffsets = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/offsets`);
      setOffsets(data.offsets || []);
    } catch { message.error('오프셋 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadSinks = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/iceberg-sinks`);
      setSinks(data.sinks || []);
    } catch { message.error('Iceberg 싱크 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    if (section === 'overview') loadOverview();
    else if (section === 'events') loadEvents();
    else if (section === 'offsets') loadOffsets();
    else if (section === 'iceberg') loadSinks();
  }, [section, loadOverview, loadEvents, loadOffsets, loadSinks]);

  const handleServiceAction = async (connectorId: number, action: string) => {
    try {
      const data = await postJSON(`${API_BASE}/connectors/${connectorId}/service`, { action });
      message.success(data.message);
      loadOverview();
    } catch { message.error('서비스 제어 실패'); }
  };

  const handleTestConnection = async (connectorId: number) => {
    try {
      const data = await postJSON(`${API_BASE}/connectors/${connectorId}/test`, {});
      if (data.success) message.success(data.message);
      else message.warning(data.message);
    } catch { message.error('연결 테스트 실패'); }
  };

  const handleViewDetail = async (connectorId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/connectors/${connectorId}`);
      setDetailDrawer({ open: true, data });
    } catch { message.error('상세 조회 실패'); }
  };

  const handleCreateConnector = async () => {
    const values = await connectorForm.validateFields();
    try {
      await postJSON(`${API_BASE}/connectors`, values);
      message.success('커넥터 생성 완료');
      setConnectorModal(false);
      connectorForm.resetFields();
      loadOverview();
    } catch { message.error('생성 실패'); }
  };

  const handleResetOffset = async (connectorId: number, topicName: string) => {
    try {
      const data = await postJSON(`${API_BASE}/offsets/${connectorId}/reset?topic_name=${encodeURIComponent(topicName)}`, {});
      message.success(data.message);
      loadOffsets();
    } catch { message.error('오프셋 리셋 실패'); }
  };

  // ── Overview ──
  const renderOverview = () => (
    <>
      {summary && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={12} md={6}><Card size="small"><Statistic title="총 커넥터" value={summary.total_connectors} prefix={<DatabaseOutlined />} /></Card></Col>
          <Col xs={12} md={6}><Card size="small"><Statistic title="Running" value={summary.running} valueStyle={{ color: '#3f8600' }} prefix={<SyncOutlined spin={summary.running > 0} />} /></Card></Col>
          <Col xs={12} md={6}><Card size="small"><Statistic title="Stopped" value={summary.stopped} prefix={<StopOutlined />} /></Card></Col>
          <Col xs={12} md={6}><Card size="small"><Statistic title="Error" value={summary.error} valueStyle={{ color: summary.error > 0 ? '#cf1322' : undefined }} prefix={<WarningOutlined />} /></Card></Col>
        </Row>
      )}

      <Card size="small" title={<><CloudServerOutlined /> CDC 커넥터</>}
        extra={
          <Space>
            <Button icon={<PlusOutlined />} type="primary" onClick={() => setConnectorModal(true)}>커넥터 추가</Button>
            <Button icon={<ReloadOutlined />} onClick={loadOverview}>새로고침</Button>
          </Space>
        }
      >
        <Table
          dataSource={services.map(s => ({ ...s, key: s.connector_id }))}
          size="small"
          pagination={false}
          columns={[
            {
              title: '커넥터', key: 'name', width: 250,
              render: (_: any, r: any) => (
                <Space>
                  <Tag color={DB_TYPE_CONFIG[r.db_type]?.color}>{DB_TYPE_CONFIG[r.db_type]?.label}</Tag>
                  <Text strong>{r.name}</Text>
                </Space>
              ),
            },
            {
              title: '상태', key: 'status', width: 100,
              render: (_: any, r: any) => {
                const cfg = STATUS_CONFIG[r.status] || STATUS_CONFIG.stopped;
                return <Tag icon={cfg.icon} color={cfg.color}>{r.status}</Tag>;
              },
            },
            { title: '토픽', key: 'topics', width: 100, render: (_: any, r: any) => <><Text strong>{r.topics_active}</Text><Text type="secondary">/{r.topics_total}</Text></> },
            { title: '이벤트/1h', dataIndex: 'events_1h', key: 'events', width: 100, render: (v: number) => v > 0 ? v.toLocaleString() : '-' },
            { title: 'Rows/1h', dataIndex: 'total_rows_1h', key: 'rows', width: 100, render: (v: number) => v > 0 ? v.toLocaleString() : '-' },
            { title: '지연(ms)', dataIndex: 'avg_latency_ms', key: 'lat', width: 90, render: (v: number) => v > 0 ? `${v}ms` : '-' },
            { title: '에러', dataIndex: 'errors_1h', key: 'err', width: 70, render: (v: number) => v > 0 ? <Text type="danger">{v}</Text> : '-' },
            {
              title: '제어', key: 'actions', width: 220,
              render: (_: any, r: any) => (
                <Space size="small">
                  {r.status === 'stopped' || r.status === 'error' ? (
                    <Button size="small" type="primary" icon={<PlayCircleOutlined />} onClick={() => handleServiceAction(r.connector_id, 'start')}>기동</Button>
                  ) : r.status === 'running' ? (
                    <>
                      <Button size="small" icon={<PauseCircleOutlined />} onClick={() => handleServiceAction(r.connector_id, 'pause')}>일시정지</Button>
                      <Button size="small" danger icon={<StopOutlined />} onClick={() => handleServiceAction(r.connector_id, 'stop')}>중지</Button>
                    </>
                  ) : r.status === 'paused' ? (
                    <Button size="small" type="primary" icon={<PlayCircleOutlined />} onClick={() => handleServiceAction(r.connector_id, 'resume')}>재개</Button>
                  ) : null}
                  <Tooltip title="연결 테스트"><Button size="small" icon={<ThunderboltOutlined />} onClick={() => handleTestConnection(r.connector_id)} /></Tooltip>
                  <Tooltip title="상세"><Button size="small" icon={<EyeOutlined />} onClick={() => handleViewDetail(r.connector_id)} /></Tooltip>
                </Space>
              ),
            },
          ]}
        />
      </Card>
    </>
  );

  // ── Events ──
  const renderEvents = () => (
    <>
      {eventStats?.last_1h && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={12} md={4}><Card size="small"><Statistic title="이벤트/1h" value={eventStats.last_1h.total_events} /></Card></Col>
          <Col xs={12} md={4}><Card size="small"><Statistic title="INSERT" value={eventStats.last_1h.inserts} valueStyle={{ color: '#52c41a' }} /></Card></Col>
          <Col xs={12} md={4}><Card size="small"><Statistic title="UPDATE" value={eventStats.last_1h.updates} valueStyle={{ color: '#1890ff' }} /></Card></Col>
          <Col xs={12} md={4}><Card size="small"><Statistic title="DELETE" value={eventStats.last_1h.deletes} valueStyle={{ color: '#faad14' }} /></Card></Col>
          <Col xs={12} md={4}><Card size="small"><Statistic title="평균 지연" value={eventStats.last_1h.avg_latency_ms} suffix="ms" /></Card></Col>
          <Col xs={12} md={4}><Card size="small"><Statistic title="에러" value={eventStats.last_1h.errors} valueStyle={{ color: eventStats.last_1h.errors > 0 ? '#cf1322' : undefined }} /></Card></Col>
        </Row>
      )}
      <Card size="small" title="CDC 이벤트 로그" extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadEvents}>새로고침</Button>}>
        <Table
          dataSource={events.map((e, i) => ({ ...e, key: i }))}
          size="small"
          pagination={{ pageSize: 20 }}
          columns={[
            { title: '시간', dataIndex: 'created_at', key: 'time', width: 140, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm:ss') : '-' },
            {
              title: '유형', dataIndex: 'event_type', key: 'type', width: 90,
              render: (v: string) => {
                const colors: Record<string, string> = { INSERT: 'green', UPDATE: 'blue', DELETE: 'orange', ERROR: 'red', DDL: 'purple', SNAPSHOT: 'cyan' };
                return <Tag color={colors[v] || 'default'}>{v}</Tag>;
              },
            },
            { title: '토픽', dataIndex: 'topic_name', key: 'topic', width: 200, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
            { title: '테이블', dataIndex: 'table_name', key: 'table', width: 150 },
            { title: 'Rows', dataIndex: 'row_count', key: 'rows', width: 70, render: (v: number) => v || '-' },
            { title: '지연', dataIndex: 'latency_ms', key: 'lat', width: 80, render: (v: number) => v ? `${v}ms` : '-' },
            { title: '오류', dataIndex: 'error_message', key: 'err', ellipsis: true, render: (v: string) => v ? <Text type="danger" style={{ fontSize: 11 }}>{v}</Text> : '-' },
          ]}
        />
      </Card>
    </>
  );

  // ── Offsets ──
  const renderOffsets = () => (
    <Card size="small" title="오프셋 추적" extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadOffsets}>새로고침</Button>}>
      <Table
        dataSource={offsets.map((o, i) => ({ ...o, key: i }))}
        size="small"
        pagination={false}
        columns={[
          { title: '커넥터', dataIndex: 'connector_name', key: 'conn', width: 200 },
          { title: '토픽', dataIndex: 'topic_name', key: 'topic', width: 220, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
          { title: 'LSN', dataIndex: 'lsn', key: 'lsn', width: 130, render: (v: string) => v ? <Text code>{v}</Text> : '-' },
          { title: 'Binlog', key: 'binlog', width: 150, render: (_: any, r: any) => r.binlog_file ? `${r.binlog_file}:${r.binlog_pos}` : '-' },
          { title: 'SCN', dataIndex: 'scn', key: 'scn', width: 100, render: (v: string) => v || '-' },
          { title: '마지막 이벤트', dataIndex: 'last_event_time', key: 'last', width: 150, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm:ss') : '-' },
          {
            title: '', key: 'actions', width: 100,
            render: (_: any, r: any) => (
              <Popconfirm title="오프셋을 초기화하시겠습니까? 처음부터 재캡처됩니다." onConfirm={() => handleResetOffset(r.connector_id, r.topic_name)} okText="초기화" cancelText="취소">
                <Button size="small" danger>리셋</Button>
              </Popconfirm>
            ),
          },
        ]}
      />
    </Card>
  );

  // ── Iceberg ──
  const renderIceberg = () => (
    <Card size="small" title="Iceberg 테이블 싱크" extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadSinks}>새로고침</Button>}>
      {sinks.length > 0 ? (
        <Table
          dataSource={sinks.map(s => ({ ...s, key: s.sink_id }))}
          size="small"
          pagination={false}
          columns={[
            { title: '커넥터', dataIndex: 'connector_name', key: 'conn', width: 200 },
            { title: '카탈로그', dataIndex: 'catalog_name', key: 'cat', width: 130, render: (v: string) => <Text code>{v}</Text> },
            { title: '네임스페이스', dataIndex: 'namespace', key: 'ns', width: 120 },
            { title: '경로', dataIndex: 'warehouse_path', key: 'path', width: 250, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
            { title: '포맷', dataIndex: 'file_format', key: 'fmt', width: 90, render: (v: string) => <Tag>{v.toUpperCase()}</Tag> },
            { title: '쓰기 모드', dataIndex: 'write_mode', key: 'mode', width: 100, render: (v: string) => <Tag color="blue">{v}</Tag> },
            { title: '활성', dataIndex: 'enabled', key: 'enabled', width: 70, render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? 'ON' : 'OFF'}</Tag> },
          ]}
          expandable={{
            expandedRowRender: (r: any) => (
              <Descriptions size="small" column={2}>
                <Descriptions.Item label="파티션 스펙">{JSON.stringify(r.partition_spec, null, 2)}</Descriptions.Item>
              </Descriptions>
            ),
          }}
        />
      ) : <Empty description="Iceberg 싱크가 설정되지 않았습니다" />}
    </Card>
  );

  return (
    <Spin spinning={loading}>
      <Segmented
        block
        options={[
          { label: '서비스 현황', value: 'overview', icon: <CloudServerOutlined /> },
          { label: '이벤트 모니터링', value: 'events', icon: <ThunderboltOutlined /> },
          { label: '오프셋 추적', value: 'offsets', icon: <DatabaseOutlined /> },
          { label: 'Iceberg 싱크', value: 'iceberg', icon: <ApiOutlined /> },
        ]}
        value={section}
        onChange={(v) => setSection(v as string)}
        style={{ marginBottom: 16 }}
      />

      {section === 'overview' && renderOverview()}
      {section === 'events' && renderEvents()}
      {section === 'offsets' && renderOffsets()}
      {section === 'iceberg' && renderIceberg()}

      {/* Connector Detail Drawer */}
      <Drawer
        title={detailDrawer.data?.name || 'CDC 커넥터 상세'}
        open={detailDrawer.open}
        onClose={() => setDetailDrawer({ open: false, data: null })}
        width={600}
      >
        {detailDrawer.data && (
          <>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="DB 유형"><Tag color={DB_TYPE_CONFIG[detailDrawer.data.db_type]?.color}>{detailDrawer.data.db_type}</Tag></Descriptions.Item>
              <Descriptions.Item label="상태"><Tag color={STATUS_CONFIG[detailDrawer.data.status]?.color}>{detailDrawer.data.status}</Tag></Descriptions.Item>
              <Descriptions.Item label="호스트">{detailDrawer.data.host}:{detailDrawer.data.port}</Descriptions.Item>
              <Descriptions.Item label="데이터베이스">{detailDrawer.data.database_name}</Descriptions.Item>
              <Descriptions.Item label="스키마">{detailDrawer.data.schema_name || '-'}</Descriptions.Item>
              <Descriptions.Item label="사용자">{detailDrawer.data.username}</Descriptions.Item>
            </Descriptions>
            <Card size="small" title={`토픽 (${detailDrawer.data.topics?.length || 0})`} style={{ marginTop: 16 }}>
              <Table
                dataSource={(detailDrawer.data.topics || []).map((t: any) => ({ ...t, key: t.topic_id }))}
                size="small"
                pagination={false}
                columns={[
                  { title: '테이블', dataIndex: 'table_name', key: 'tbl' },
                  { title: '캡처 모드', dataIndex: 'capture_mode', key: 'mode', render: (v: string) => <Tag>{v}</Tag> },
                  { title: '상태', dataIndex: 'status', key: 'st', render: (v: string) => <Tag color={v === 'active' ? 'green' : v === 'error' ? 'red' : 'default'}>{v}</Tag> },
                ]}
              />
            </Card>
            {detailDrawer.data.iceberg_sink && (
              <Card size="small" title="Iceberg 싱크 설정" style={{ marginTop: 16 }}>
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="카탈로그">{detailDrawer.data.iceberg_sink.catalog_name}</Descriptions.Item>
                  <Descriptions.Item label="경로">{detailDrawer.data.iceberg_sink.warehouse_path}</Descriptions.Item>
                  <Descriptions.Item label="포맷">{detailDrawer.data.iceberg_sink.file_format}</Descriptions.Item>
                  <Descriptions.Item label="쓰기 모드">{detailDrawer.data.iceberg_sink.write_mode}</Descriptions.Item>
                </Descriptions>
              </Card>
            )}
          </>
        )}
      </Drawer>

      {/* Add Connector Modal */}
      <Modal
        title="CDC 커넥터 추가"
        open={connectorModal}
        onOk={handleCreateConnector}
        onCancel={() => setConnectorModal(false)}
        okText="생성"
        cancelText="취소"
        width={600}
      >
        <Form form={connectorForm} layout="vertical">
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="name" label="커넥터명" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="db_type" label="DB 유형" rules={[{ required: true }]}>
                <Select options={Object.entries(DB_TYPE_CONFIG).map(([k, v]) => ({ value: k, label: v.label }))} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="host" label="호스트" rules={[{ required: true }]}>
                <Input placeholder="10.10.1.50" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item name="port" label="포트" rules={[{ required: true }]}>
                <Input type="number" placeholder="5432" />
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item name="database_name" label="DB명" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={8}><Form.Item name="username" label="사용자" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="password" label="비밀번호"><Input.Password /></Form.Item></Col>
            <Col span={8}><Form.Item name="schema_name" label="스키마"><Input placeholder="public" /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Modal>
    </Spin>
  );
};

export default CDCManagement;
