import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Table,
  Tag,
  Space,
  Button,
  Typography,
  Row,
  Col,
  Statistic,
  Tooltip,
  Alert,
  Modal,
  App,
  Drawer,
  Segmented,
  Spin,
  Empty,
} from 'antd';
import {
  SyncOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  PlayCircleOutlined,
  LinkOutlined,
  BarChartOutlined,
  SettingOutlined,
  UnorderedListOutlined,
  ExpandOutlined,
  AppstoreOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/ko';

dayjs.extend(relativeTime);
dayjs.locale('ko');

const { Title, Paragraph, Text } = Typography;

const API_BASE = '/api/v1/etl';
const AIRFLOW_UI_URL = 'http://localhost:18080';
const AIRFLOW_EMBED_URL = 'http://localhost:18081';

// --- Types ---
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

// --- API Helpers ---
async function fetchDags(): Promise<DagInfo[]> {
  const res = await fetch(`${API_BASE}/dags`);
  if (!res.ok) throw new Error('DAG 목록 로드 실패');
  const data = await res.json();
  return data.dags || [];
}

async function fetchDagRuns(dagId: string, limit = 10): Promise<DagRun[]> {
  const res = await fetch(`${API_BASE}/dags/${dagId}/runs?limit=${limit}`);
  if (!res.ok) throw new Error('실행 기록 로드 실패');
  const data = await res.json();
  return data.runs || [];
}

async function triggerDag(dagId: string): Promise<string> {
  const res = await fetch(`${API_BASE}/dags/${dagId}/trigger`, { method: 'POST' });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || 'DAG 실행 요청 실패');
  }
  const data = await res.json();
  return data.message || '실행 요청됨';
}

async function checkHealth(): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}/health`);
    if (!res.ok) return false;
    const data = await res.json();
    return data.status === 'healthy';
  } catch {
    return false;
  }
}

// --- Component ---
const ETL: React.FC = () => {
  const { message } = App.useApp();
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
      const result = await fetchDags();
      setDags(result);
      setAirflowHealthy(true);
    } catch {
      setAirflowHealthy(false);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadDags();
    checkHealth().then(setAirflowHealthy);
  }, [loadDags]);

  const handleTriggerDag = (dagId: string) => {
    Modal.confirm({
      title: `${dagId} 파이프라인을 수동으로 실행하시겠습니까?`,
      content: '스케줄과 별개로 새로운 DAG Run이 생성됩니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: async () => {
        try {
          const msg = await triggerDag(dagId);
          message.success(msg);
          // 잠시 후 새로고침
          setTimeout(loadDags, 2000);
        } catch (e: any) {
          message.error(e.message || 'DAG 실행 실패');
        }
      },
    });
  };

  const handleOpenHistory = async (dagId: string) => {
    setHistoryDrawer({ open: true, dagId });
    setHistoryLoading(true);
    try {
      const runs = await fetchDagRuns(dagId);
      setHistoryRuns(runs);
    } catch {
      setHistoryRuns([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  const statusToComponent = (status: string) => {
    switch (status) {
      case 'success':
        return <Tag icon={<CheckCircleOutlined />} color="success">성공</Tag>;
      case 'running':
        return <Tag icon={<SyncOutlined spin />} color="processing">실행중</Tag>;
      case 'failed':
        return <Tag icon={<CloseCircleOutlined />} color="error">실패</Tag>;
      case 'queued':
        return <Tag icon={<ClockCircleOutlined />} color="default">대기중</Tag>;
      case 'no_runs':
        return <Tag color="default">실행 없음</Tag>;
      default:
        return <Tag color="default">{status}</Tag>;
    }
  };

  const getStateIcon = (state: string) => {
    switch (state) {
      case 'success': return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'running': return <SyncOutlined spin style={{ color: '#1890ff' }} />;
      case 'failed': return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      case 'queued': return <ClockCircleOutlined style={{ color: '#faad14' }} />;
      default: return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
    }
  };

  const pipelineColumns = [
    {
      title: '파이프라인 ID',
      dataIndex: 'dag_id',
      key: 'dag_id',
      width: 240,
      fixed: 'left' as const,
      render: (id: string) => <Text strong style={{ whiteSpace: 'nowrap' }}>{id}</Text>,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: statusToComponent,
    },
    {
      title: '스케줄',
      dataIndex: 'schedule_interval',
      key: 'schedule_interval',
      width: 130,
      render: (schedule: string) => <Text code style={{ whiteSpace: 'nowrap' }}>{schedule || '-'}</Text>,
    },
    {
      title: '최근 실행 기록',
      dataIndex: 'recent_runs',
      key: 'recent_runs',
      width: 140,
      render: (runs: string[]) => (
        <Space>
          {runs.length > 0 ? runs.map((runState, index) => (
            <Tooltip key={index} title={`${index + 1}번째 최근 실행: ${runState}`}>
              {getStateIcon(runState)}
            </Tooltip>
          )) : <Text type="secondary">-</Text>}
        </Space>
      ),
    },
    {
      title: '마지막 실행',
      dataIndex: 'last_run',
      key: 'last_run',
      width: 120,
      render: (isoString: string | null) =>
        isoString ? <span style={{ whiteSpace: 'nowrap' }}>{dayjs(isoString).fromNow()}</span> : <Text type="secondary">-</Text>,
    },
    {
      title: '소유자',
      dataIndex: 'owners',
      key: 'owners',
      width: 150,
      render: (owners: string[]) => <span style={{ whiteSpace: 'nowrap' }}>{owners.join(', ')}</span>,
    },
    {
      title: '태그',
      dataIndex: 'tags',
      key: 'tags',
      width: 180,
      render: (tags: string[]) => (
        <Space wrap size={4}>
          {tags.map(tag => <Tag key={tag} style={{ margin: 0 }}>{tag}</Tag>)}
        </Space>
      ),
    },
    {
      title: '작업',
      key: 'action',
      width: 240,
      fixed: 'right' as const,
      render: (_: any, record: DagInfo) => (
        <Space size="small">
          <Button
            type="primary"
            size="small"
            icon={<PlayCircleOutlined />}
            onClick={() => handleTriggerDag(record.dag_id)}
            disabled={record.is_paused}
          >
            수동 실행
          </Button>
          <Button
            size="small"
            icon={<BarChartOutlined />}
            onClick={() => handleOpenHistory(record.dag_id)}
          >
            실행기록
          </Button>
        </Space>
      ),
    },
  ];

  const historyColumns = [
    {
      title: 'Run ID',
      dataIndex: 'run_id',
      key: 'run_id',
      width: 280,
      ellipsis: true,
      render: (v: string) => <Text style={{ fontSize: 12 }}>{v}</Text>,
    },
    {
      title: '상태',
      dataIndex: 'state',
      key: 'state',
      width: 100,
      render: statusToComponent,
    },
    {
      title: '시작 시간',
      dataIndex: 'start_date',
      key: 'start_date',
      width: 170,
      render: (v: string | null) => v ? dayjs(v).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '종료 시간',
      dataIndex: 'end_date',
      key: 'end_date',
      width: 170,
      render: (v: string | null) => v ? dayjs(v).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '소요 시간',
      key: 'duration',
      width: 120,
      render: (_: any, record: DagRun) => {
        if (!record.start_date || !record.end_date) return '-';
        const sec = dayjs(record.end_date).diff(dayjs(record.start_date), 'second');
        if (sec < 60) return `${sec}초`;
        const min = Math.floor(sec / 60);
        const remainSec = sec % 60;
        if (min < 60) return `${min}분 ${remainSec}초`;
        const hr = Math.floor(min / 60);
        const remainMin = min % 60;
        return `${hr}시간 ${remainMin}분`;
      },
    },
    {
      title: '유형',
      dataIndex: 'run_type',
      key: 'run_type',
      width: 100,
      render: (v: string) => <Tag>{v}</Tag>,
    },
  ];

  const summary = {
    total: dags.length,
    running: dags.filter(p => p.status === 'running').length,
    success: dags.filter(p => p.status === 'success').length,
    failed: dags.filter(p => p.status === 'failed').length,
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%', height: viewMode === 'airflow' ? 'calc(100vh - 120px)' : 'auto' }}>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              ETL 파이프라인 대시보드
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              Apache Airflow 기반 데이터 파이프라인 관리
              {airflowHealthy === false && ' (Airflow 미연결)'}
            </Paragraph>
          </Col>
          <Col>
            <Space>
              <Button icon={<ReloadOutlined />} onClick={loadDags} loading={loading} size="small">
                새로고침
              </Button>
              <Segmented
                options={[
                  { label: '요약 뷰', value: 'list', icon: <UnorderedListOutlined /> },
                  { label: 'Airflow UI', value: 'airflow', icon: <AppstoreOutlined /> },
                ]}
                value={viewMode}
                onChange={(value) => {
                  setViewMode(value as 'list' | 'airflow');
                  if (value === 'airflow') {
                    setAirflowIframeLoading(true);
                    setAirflowIframeError(false);
                  }
                }}
              />
              <Button
                icon={<ExpandOutlined />}
                href={AIRFLOW_UI_URL}
                target="_blank"
              >
                새 창
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {viewMode === 'list' ? (
        <>
          {airflowHealthy === false && (
            <Alert
              message="Airflow 연결 실패"
              description="Airflow 서버에 연결할 수 없습니다. 서버 상태를 확인해주세요."
              type="error"
              showIcon
            />
          )}

          <Row gutter={[16, 16]}>
            <Col xs={12} sm={12} md={6}>
              <Card>
                <Statistic title="총 파이프라인" value={summary.total} prefix={<SettingOutlined />} />
              </Card>
            </Col>
            <Col xs={12} sm={12} md={6}>
              <Card>
                <Statistic title="성공" value={summary.success} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} />
              </Card>
            </Col>
            <Col xs={12} sm={12} md={6}>
              <Card>
                <Statistic title="실패" value={summary.failed} valueStyle={{ color: '#cf1322' }} prefix={<CloseCircleOutlined />} />
              </Card>
            </Col>
            <Col xs={12} sm={12} md={6}>
              <Card>
                <Statistic title="실행중" value={summary.running} valueStyle={{ color: '#1890ff' }} prefix={<SyncOutlined spin={summary.running > 0} />} />
              </Card>
            </Col>
          </Row>

          <Card title="파이프라인 목록">
            <Spin spinning={loading}>
              {dags.length > 0 ? (
                <Table
                  columns={pipelineColumns}
                  dataSource={dags.map(d => ({ ...d, key: d.dag_id }))}
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 1300 }}
                  expandable={{
                    expandedRowRender: record => <p style={{ margin: 0 }}>{record.description}</p>,
                  }}
                />
              ) : !loading ? (
                <Empty description="등록된 파이프라인이 없습니다" />
              ) : null}
            </Spin>
          </Card>
        </>
      ) : (
        <Card
          styles={{ body: { padding: 0, height: '100%', position: 'relative' } }}
          style={{ flex: 1, overflow: 'hidden', minHeight: 600 }}
        >
          {airflowIframeLoading && !airflowIframeError && (
            <div style={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              zIndex: 10,
              textAlign: 'center'
            }}>
              <Spin size="large" />
              <div style={{ marginTop: 16, color: '#666' }}>Airflow 로딩 중...</div>
            </div>
          )}
          {airflowIframeError ? (
            <div style={{ padding: 24, textAlign: 'center' }}>
              <Alert
                type="warning"
                message="Airflow 임베딩이 차단되었습니다"
                description={
                  <div>
                    <p>Airflow의 X-Frame-Options 설정으로 인해 임베딩이 불가합니다.</p>
                    <Button
                      type="primary"
                      icon={<ExpandOutlined />}
                      href={`${AIRFLOW_UI_URL}/home`}
                      target="_blank"
                      style={{ marginTop: 16 }}
                    >
                      새 창에서 Airflow 열기
                    </Button>
                  </div>
                }
                style={{ maxWidth: 500, margin: '0 auto' }}
              />
            </div>
          ) : (
            <iframe
              src={`${AIRFLOW_EMBED_URL}/home`}
              style={{
                width: '100%',
                height: '100%',
                border: 'none',
                minHeight: 600
              }}
              title="Apache Airflow"
              onLoad={() => setAirflowIframeLoading(false)}
              onError={() => setAirflowIframeError(true)}
            />
          )}
        </Card>
      )}

      {/* 실행기록 Drawer */}
      <Drawer
        title={
          <Space>
            <BarChartOutlined />
            <span>실행 기록: {historyDrawer.dagId}</span>
          </Space>
        }
        placement="right"
        width={750}
        onClose={() => setHistoryDrawer({ open: false, dagId: null })}
        open={historyDrawer.open}
      >
        <Spin spinning={historyLoading}>
          {historyRuns.length > 0 ? (
            <>
              <Alert
                message="최근 실행 내역"
                description={`${historyDrawer.dagId} 파이프라인의 최근 실행 기록입니다.`}
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
              <Table
                columns={historyColumns}
                dataSource={historyRuns.map((r, i) => ({ ...r, key: i }))}
                pagination={false}
                size="small"
                scroll={{ x: 800 }}
              />
              <div style={{ marginTop: 16, textAlign: 'center' }}>
                <Button
                  type="link"
                  icon={<LinkOutlined />}
                  href={`${AIRFLOW_UI_URL}/dags/${historyDrawer.dagId}/grid`}
                  target="_blank"
                >
                  Airflow UI에서 상세 보기
                </Button>
              </div>
            </>
          ) : !historyLoading ? (
            <Empty description="실행 기록이 없습니다" />
          ) : null}
        </Spin>
      </Drawer>
    </Space>
  );
};

export default ETL;
