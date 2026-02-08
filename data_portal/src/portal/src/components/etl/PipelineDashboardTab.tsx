import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Tooltip, Alert, Drawer, Segmented, Spin, Empty, App,
} from 'antd';
import {
  SyncOutlined, CheckCircleOutlined, CloseCircleOutlined,
  ClockCircleOutlined, PlayCircleOutlined,
  BarChartOutlined, SettingOutlined, UnorderedListOutlined,
  ExpandOutlined, AppstoreOutlined, ReloadOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';
const AIRFLOW_UI_URL = 'http://localhost:18080';
const AIRFLOW_EMBED_URL = 'http://localhost:18081';

interface DagInfo {
  dag_id: string;
  description: string;
  owners: string[];
  schedule_interval: string;
  is_paused: boolean;
  is_active: boolean;
  tags: string[];
  next_dagrun: string | null;
  status: string;
  last_run: string | null;
  recent_runs: string[];
}

interface DagRun {
  run_id: string;
  state: string;
  start_date: string | null;
  end_date: string | null;
  execution_date: string | null;
  run_type: string;
  note: string | null;
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

const PipelineDashboardTab: React.FC = () => {
  const { message: msg, modal } = App.useApp();
  const [dags, setDags] = useState<DagInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [airflowHealthy, setAirflowHealthy] = useState<boolean | null>(null);
  const [historyDrawer, setHistoryDrawer] = useState<{ open: boolean; dagId: string | null }>({ open: false, dagId: null });
  const [historyRuns, setHistoryRuns] = useState<DagRun[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [viewMode, setViewMode] = useState<'list' | 'airflow'>('list');
  const [airflowIframeLoading, setAirflowIframeLoading] = useState(true);
  const [airflowIframeError, setAirflowIframeError] = useState(false);

  const loadDags = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/dags`);
      setDags(data.dags || []);
      setAirflowHealthy(true);
    } catch {
      setAirflowHealthy(false);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadDags(); }, [loadDags]);

  const handleTriggerDag = (dagId: string) => {
    modal.confirm({
      title: `${dagId} 파이프라인을 수동으로 실행하시겠습니까?`,
      content: '스케줄과 별개로 새로운 DAG Run이 생성됩니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: async () => {
        try {
          const data = await postJSON(`${API_BASE}/dags/${dagId}/trigger`, {});
          msg.success(data.message || '실행 요청됨');
          setTimeout(loadDags, 2000);
        } catch (e: any) {
          msg.error(e.message || 'DAG 실행 실패');
        }
      },
    });
  };

  const handleOpenHistory = async (dagId: string) => {
    setHistoryDrawer({ open: true, dagId });
    setHistoryLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/dags/${dagId}/runs?limit=10`);
      setHistoryRuns(data.runs || []);
    } catch {
      setHistoryRuns([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  const statusToComponent = (status: string) => {
    switch (status) {
      case 'success': return <Tag icon={<CheckCircleOutlined />} color="success">성공</Tag>;
      case 'running': return <Tag icon={<SyncOutlined spin />} color="processing">실행중</Tag>;
      case 'failed': return <Tag icon={<CloseCircleOutlined />} color="error">실패</Tag>;
      case 'queued': return <Tag icon={<ClockCircleOutlined />} color="default">대기중</Tag>;
      case 'no_runs': return <Tag color="default">실행 없음</Tag>;
      default: return <Tag color="default">{status}</Tag>;
    }
  };

  const getStateIcon = (state: string) => {
    switch (state) {
      case 'success': return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'running': return <SyncOutlined spin style={{ color: '#1890ff' }} />;
      case 'failed': return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      default: return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
    }
  };

  const pipelineColumns = [
    { title: '파이프라인 ID', dataIndex: 'dag_id', key: 'dag_id', width: 240, render: (id: string) => <Text strong>{id}</Text> },
    { title: '상태', dataIndex: 'status', key: 'status', width: 100, render: statusToComponent },
    { title: '스케줄', dataIndex: 'schedule_interval', key: 'schedule_interval', width: 130, render: (s: string) => <Text code>{s || '-'}</Text> },
    {
      title: '최근 실행', dataIndex: 'recent_runs', key: 'recent_runs', width: 140,
      render: (runs: string[]) => <Space>{runs.length > 0 ? runs.map((r, i) => <Tooltip key={i} title={r}>{getStateIcon(r)}</Tooltip>) : '-'}</Space>,
    },
    { title: '마지막 실행', dataIndex: 'last_run', key: 'last_run', width: 120, render: (v: string | null) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
    { title: '태그', dataIndex: 'tags', key: 'tags', width: 180, render: (tags: string[]) => <Space wrap size={4}>{tags.map(t => <Tag key={t}>{t}</Tag>)}</Space> },
    {
      title: '작업', key: 'action', width: 200,
      render: (_: any, r: DagInfo) => (
        <Space size="small">
          <Button type="primary" size="small" icon={<PlayCircleOutlined />} onClick={() => handleTriggerDag(r.dag_id)} disabled={r.is_paused}>수동 실행</Button>
          <Button size="small" icon={<BarChartOutlined />} onClick={() => handleOpenHistory(r.dag_id)}>이력</Button>
        </Space>
      ),
    },
  ];

  const historyColumns = [
    { title: 'Run ID', dataIndex: 'run_id', key: 'run_id', width: 280, ellipsis: true },
    { title: '상태', dataIndex: 'state', key: 'state', width: 100, render: statusToComponent },
    { title: '시작', dataIndex: 'start_date', key: 'start_date', width: 170, render: (v: string | null) => v ? dayjs(v).format('YYYY-MM-DD HH:mm:ss') : '-' },
    { title: '종료', dataIndex: 'end_date', key: 'end_date', width: 170, render: (v: string | null) => v ? dayjs(v).format('YYYY-MM-DD HH:mm:ss') : '-' },
    {
      title: '소요', key: 'dur', width: 100,
      render: (_: any, r: DagRun) => {
        if (!r.start_date || !r.end_date) return '-';
        const sec = dayjs(r.end_date).diff(dayjs(r.start_date), 'second');
        return sec < 60 ? `${sec}초` : `${Math.floor(sec / 60)}분 ${sec % 60}초`;
      },
    },
    { title: '유형', dataIndex: 'run_type', key: 'run_type', width: 100, render: (v: string) => <Tag>{v}</Tag> },
  ];

  const summary = {
    total: dags.length,
    running: dags.filter(p => p.status === 'running').length,
    success: dags.filter(p => p.status === 'success').length,
    failed: dags.filter(p => p.status === 'failed').length,
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={loadDags} loading={loading} size="small">새로고침</Button>
          <Segmented
            options={[
              { label: '요약 뷰', value: 'list', icon: <UnorderedListOutlined /> },
              { label: 'Airflow UI', value: 'airflow', icon: <AppstoreOutlined /> },
            ]}
            value={viewMode}
            onChange={(v) => { setViewMode(v as any); if (v === 'airflow') { setAirflowIframeLoading(true); setAirflowIframeError(false); } }}
          />
          <Button icon={<ExpandOutlined />} href={AIRFLOW_UI_URL} target="_blank">새 창</Button>
        </Space>
      </div>

      {viewMode === 'list' ? (
        <>
          {airflowHealthy === false && <Alert message="Airflow 연결 실패" type="error" showIcon style={{ marginBottom: 16 }} />}
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={12} md={6}><Card size="small"><Statistic title="총 파이프라인" value={summary.total} prefix={<SettingOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="성공" value={summary.success} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="실패" value={summary.failed} valueStyle={{ color: '#cf1322' }} prefix={<CloseCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="실행중" value={summary.running} valueStyle={{ color: '#1890ff' }} prefix={<SyncOutlined spin={summary.running > 0} />} /></Card></Col>
          </Row>
          <Card size="small" title="파이프라인 목록">
            <Spin spinning={loading}>
              {dags.length > 0 ? (
                <Table columns={pipelineColumns} dataSource={dags.map(d => ({ ...d, key: d.dag_id }))} pagination={{ pageSize: 10 }} scroll={{ x: 1100 }}
                  expandable={{ expandedRowRender: r => <p style={{ margin: 0 }}>{r.description}</p> }} />
              ) : !loading ? <Empty description="등록된 파이프라인이 없습니다" /> : null}
            </Spin>
          </Card>
        </>
      ) : (
        <Card styles={{ body: { padding: 0, height: '100%' } }} style={{ minHeight: 600 }}>
          {airflowIframeError ? (
            <div style={{ padding: 24, textAlign: 'center' }}>
              <Alert type="warning" message="Airflow 임베딩 차단" description={<Button type="primary" icon={<ExpandOutlined />} href={`${AIRFLOW_UI_URL}/home`} target="_blank" style={{ marginTop: 16 }}>새 창에서 열기</Button>} />
            </div>
          ) : (
            <iframe src={`${AIRFLOW_EMBED_URL}/home`} style={{ width: '100%', height: '100%', border: 'none', minHeight: 600 }} title="Apache Airflow"
              onLoad={() => setAirflowIframeLoading(false)} onError={() => setAirflowIframeError(true)} />
          )}
        </Card>
      )}

      <Drawer title={<><BarChartOutlined /> 실행 기록: {historyDrawer.dagId}</>} placement="right" width={750}
        onClose={() => setHistoryDrawer({ open: false, dagId: null })} open={historyDrawer.open}>
        <Spin spinning={historyLoading}>
          {historyRuns.length > 0 ? (
            <Table columns={historyColumns} dataSource={historyRuns.map((r, i) => ({ ...r, key: i }))} pagination={false} size="small" scroll={{ x: 800 }} />
          ) : !historyLoading ? <Empty description="실행 기록이 없습니다" /> : null}
        </Spin>
      </Drawer>
    </div>
  );
};

export default PipelineDashboardTab;
