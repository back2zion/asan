/**
 * DIR-001: 실행 로그 탭
 * 필터, 통계, 로그 테이블, SSE 라이브 로그
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Select, DatePicker, Spin, Empty, Segmented, message,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, SyncOutlined,
  ReloadOutlined, FieldTimeOutlined, DatabaseOutlined,
  FileTextOutlined, WifiOutlined, DisconnectOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

interface LogEntry {
  log_id: number;
  job_id: number | null;
  job_name: string | null;
  job_type: string | null;
  run_id: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  duration_sec: number | null;
  rows_processed: number;
  rows_failed: number;
  error_message: string | null;
}

interface LogStats {
  total: number;
  success_count: number;
  failed_count: number;
  success_rate: number;
  avg_duration_sec: number;
  total_rows: number;
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

const STATUS_TAG: Record<string, React.ReactNode> = {
  success: <Tag icon={<CheckCircleOutlined />} color="success">성공</Tag>,
  failed: <Tag icon={<CloseCircleOutlined />} color="error">실패</Tag>,
  running: <Tag icon={<SyncOutlined spin />} color="processing">실행중</Tag>,
  warning: <Tag color="warning">경고</Tag>,
};

const TYPE_TAG: Record<string, React.ReactNode> = {
  batch: <Tag color="blue">Batch</Tag>,
  stream: <Tag color="green">Stream</Tag>,
  cdc: <Tag color="orange">CDC</Tag>,
};

const ExecutionLogs: React.FC = () => {
  const [view, setView] = useState<string>('table');
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [stats, setStats] = useState<LogStats | null>(null);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterJobId, setFilterJobId] = useState<number | undefined>();
  const [filterStatus, setFilterStatus] = useState<string | undefined>();

  // SSE
  const [liveConnected, setLiveConnected] = useState(false);
  const [liveEvents, setLiveEvents] = useState<any[]>([]);
  const eventSourceRef = useRef<EventSource | null>(null);
  const liveLogRef = useRef<HTMLDivElement>(null);

  const loadLogs = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (filterJobId) params.append('job_id', String(filterJobId));
      if (filterStatus) params.append('status', filterStatus);
      params.append('limit', '100');

      const data = await fetchJSON(`${API_BASE}/logs?${params}`);
      setLogs(data.logs || []);
      setStats(data.stats || null);
    } catch { message.error('로그 로드 실패'); }
    finally { setLoading(false); }
  }, [filterJobId, filterStatus]);

  const loadJobs = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/jobs`);
      setJobs((data.jobs || []).map((j: any) => ({ job_id: j.job_id, name: j.name })));
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadJobs(); }, [loadJobs]);
  useEffect(() => { loadLogs(); }, [loadLogs]);

  // SSE connection
  const connectSSE = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }
    const es = new EventSource(`${API_BASE}/logs/stream`);
    es.onopen = () => setLiveConnected(true);
    es.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        setLiveEvents(prev => [...prev.slice(-99), { ...data, _ts: new Date().toISOString() }]);
      } catch { /* ignore */ }
    };
    es.onerror = () => {
      setLiveConnected(false);
      es.close();
    };
    eventSourceRef.current = es;
  }, []);

  const disconnectSSE = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setLiveConnected(false);
  }, []);

  useEffect(() => {
    return () => { disconnectSSE(); };
  }, [disconnectSSE]);

  // Auto-scroll live log
  useEffect(() => {
    if (liveLogRef.current) {
      liveLogRef.current.scrollTop = liveLogRef.current.scrollHeight;
    }
  }, [liveEvents]);

  const formatDuration = (sec: number | null) => {
    if (sec === null || sec === undefined) return '-';
    if (sec < 60) return `${sec}초`;
    return `${Math.floor(sec / 60)}분 ${sec % 60}초`;
  };

  const columns = [
    { title: 'Job', key: 'job', width: 180, render: (_: any, r: LogEntry) => <Text strong>{r.job_name || '-'}</Text> },
    { title: '유형', key: 'type', width: 80, render: (_: any, r: LogEntry) => TYPE_TAG[r.job_type || ''] || '-' },
    { title: 'Run ID', dataIndex: 'run_id', key: 'run_id', width: 200, ellipsis: true, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
    { title: '상태', key: 'status', width: 80, render: (_: any, r: LogEntry) => STATUS_TAG[r.status] || <Tag>{r.status}</Tag> },
    { title: '시작', dataIndex: 'started_at', key: 'started_at', width: 150, render: (v: string | null) => v ? dayjs(v).format('MM-DD HH:mm:ss') : '-' },
    { title: '소요', key: 'dur', width: 90, render: (_: any, r: LogEntry) => formatDuration(r.duration_sec) },
    { title: '처리 건수', dataIndex: 'rows_processed', key: 'rows', width: 110, render: (v: number) => v > 0 ? v.toLocaleString() : '-' },
    { title: '실패', dataIndex: 'rows_failed', key: 'failed', width: 80, render: (v: number) => v > 0 ? <Text type="danger">{v.toLocaleString()}</Text> : '-' },
    {
      title: '오류', dataIndex: 'error_message', key: 'error', ellipsis: true,
      render: (v: string | null) => v ? <Text type="danger" style={{ fontSize: 11 }}>{v}</Text> : '-',
    },
  ];

  const statusColor = (status: string) => {
    switch (status) {
      case 'success': return '#52c41a';
      case 'failed': return '#ff4d4f';
      case 'running': return '#1890ff';
      default: return '#faad14';
    }
  };

  return (
    <div>
      <Segmented
        options={[
          { label: '로그 테이블', value: 'table', icon: <FileTextOutlined /> },
          { label: '라이브 로그', value: 'live', icon: <WifiOutlined /> },
        ]}
        value={view}
        onChange={(v) => setView(v as string)}
        style={{ marginBottom: 16 }}
      />

      {view === 'table' ? (
        <>
          {stats && (
            <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
              <Col xs={12} md={4}><Card size="small"><Statistic title="총 실행" value={stats.total} prefix={<DatabaseOutlined />} /></Card></Col>
              <Col xs={12} md={4}><Card size="small"><Statistic title="성공률" value={stats.success_rate} suffix="%" valueStyle={{ color: stats.success_rate >= 90 ? '#3f8600' : '#cf1322' }} prefix={<CheckCircleOutlined />} /></Card></Col>
              <Col xs={12} md={4}><Card size="small"><Statistic title="평균 시간" value={formatDuration(stats.avg_duration_sec)} prefix={<FieldTimeOutlined />} /></Card></Col>
              <Col xs={12} md={4}><Card size="small"><Statistic title="총 처리 건수" value={stats.total_rows} prefix={<DatabaseOutlined />} /></Card></Col>
              <Col xs={12} md={4}><Card size="small"><Statistic title="성공" value={stats.success_count} valueStyle={{ color: '#3f8600' }} /></Card></Col>
              <Col xs={12} md={4}><Card size="small"><Statistic title="실패" value={stats.failed_count} valueStyle={{ color: '#cf1322' }} /></Card></Col>
            </Row>
          )}

          <Card
            size="small"
            title="실행 로그"
            extra={
              <Space>
                <Select
                  allowClear
                  placeholder="Job 필터"
                  style={{ width: 180 }}
                  size="small"
                  value={filterJobId}
                  onChange={setFilterJobId}
                  options={jobs.map(j => ({ value: j.job_id, label: j.name }))}
                />
                <Select
                  allowClear
                  placeholder="상태"
                  style={{ width: 100 }}
                  size="small"
                  value={filterStatus}
                  onChange={setFilterStatus}
                  options={[
                    { value: 'success', label: '성공' },
                    { value: 'failed', label: '실패' },
                    { value: 'running', label: '실행중' },
                    { value: 'warning', label: '경고' },
                  ]}
                />
                <Button icon={<ReloadOutlined />} size="small" onClick={loadLogs}>새로고침</Button>
              </Space>
            }
          >
            <Spin spinning={loading}>
              {logs.length > 0 ? (
                <Table
                  dataSource={logs.map(l => ({ ...l, key: l.log_id }))}
                  columns={columns}
                  size="small"
                  pagination={{ pageSize: 20 }}
                  scroll={{ x: 1100 }}
                />
              ) : !loading ? <Empty description="실행 로그가 없습니다" /> : null}
            </Spin>
          </Card>
        </>
      ) : (
        <Card
          size="small"
          title={
            <Space>
              {liveConnected ? <WifiOutlined style={{ color: '#52c41a' }} /> : <DisconnectOutlined style={{ color: '#ff4d4f' }} />}
              <span>라이브 로그 스트리밍</span>
              {liveConnected && <Tag color="green">연결됨</Tag>}
            </Space>
          }
          extra={
            <Space>
              {!liveConnected ? (
                <Button type="primary" size="small" icon={<WifiOutlined />} onClick={connectSSE}>연결</Button>
              ) : (
                <Button danger size="small" icon={<DisconnectOutlined />} onClick={disconnectSSE}>연결 해제</Button>
              )}
              <Button size="small" onClick={() => setLiveEvents([])}>초기화</Button>
            </Space>
          }
        >
          <div
            ref={liveLogRef}
            style={{
              background: '#1e1e1e',
              color: '#d4d4d4',
              fontFamily: "'Fira Code', 'Consolas', monospace",
              fontSize: 12,
              padding: 16,
              borderRadius: 8,
              height: 500,
              overflow: 'auto',
              lineHeight: 1.8,
            }}
          >
            {liveEvents.length === 0 ? (
              <div style={{ color: '#888', textAlign: 'center', paddingTop: 200 }}>
                {liveConnected ? 'Waiting for events...' : "'연결' 버튼을 클릭하세요"}
              </div>
            ) : (
              liveEvents.map((evt, i) => (
                <div key={i}>
                  <span style={{ color: '#888' }}>[{dayjs(evt._ts).format('HH:mm:ss')}]</span>
                  {' '}
                  <span style={{ color: statusColor(evt.status) }}>[{evt.status?.toUpperCase()}]</span>
                  {' '}
                  <span style={{ color: '#569CD6' }}>{evt.job_name || `job_${evt.job_id}`}</span>
                  {' '}
                  {evt.rows_processed > 0 && <span style={{ color: '#4EC9B0' }}>({evt.rows_processed?.toLocaleString()} rows)</span>}
                  {evt.error && <span style={{ color: '#f44747' }}> ERR: {evt.error}</span>}
                </div>
              ))
            )}
          </div>
        </Card>
      )}
    </div>
  );
};

export default ExecutionLogs;
