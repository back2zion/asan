import React, { useState } from 'react';
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
  Timeline,
  Drawer,
  Segmented,
  Spin
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
  AppstoreOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/ko';

dayjs.extend(relativeTime);
dayjs.locale('ko');

const { Title, Paragraph, Text } = Typography;

// --- Mock Data (simulating Airflow API response) ---

const mockPipelines = [
  {
    key: '1',
    dag_id: 'cdw_patient_master_sync',
    owner: 'Data Engineering',
    schedule_interval: '0 1 * * *', // 매일 01:00
    status: 'success',
    last_run: dayjs().subtract(1, 'day').add(1, 'hour').toISOString(),
    next_run: dayjs().add(1, 'hour').toISOString(),
    recent_runs: ['success', 'success', 'success', 'failed', 'success'],
    description: 'CDW 환자 마스터 정보를 Bronze에서 Silver 단계로 정제하고 동기화합니다.',
  },
  {
    key: '2',
    dag_id: 'edw_prescription_daily_batch',
    owner: 'Data Engineering',
    schedule_interval: '0 3 * * *', // 매일 03:00
    status: 'running',
    last_run: dayjs().subtract(3, 'minutes').toISOString(),
    next_run: dayjs().add(1, 'day').add(3, 'hour').toISOString(),
    recent_runs: ['success', 'success', 'running', 'success', 'success'],
    description: 'OCS 처방 데이터를 취합하여 일별 EDW 배치 작업을 수행합니다.',
  },
  {
    key: '3',
    dag_id: 'datamart_ml_features_build',
    owner: 'ML Engineering',
    schedule_interval: '0 6 * * 1', // 매주 월요일 06:00
    status: 'failed',
    last_run: dayjs().subtract(6, 'day').toISOString(),
    next_run: dayjs().add(1, 'day').add(6, 'hour').toISOString(),
    recent_runs: ['success', 'success', 'success', 'success', 'failed'],
    description: '주요 마트 데이터를 기반으로 ML 모델 학습용 피처를 생성합니다.',
  },
  {
    key: '4',
    dag_id: 'realtime_vitals_stream_to_lake',
    owner: 'Platform Team',
    schedule_interval: '@continuous',
    status: 'success',
    last_run: dayjs().subtract(30, 'seconds').toISOString(),
    next_run: null, // 스트리밍 작업은 다음 실행 시간이 없음
    recent_runs: ['success', 'success', 'success', 'success', 'success'],
    description: 'EMR에서 발생하는 환자 바이탈 데이터를 실시간으로 수집하여 Landing Zone에 적재합니다.',
  },
];

const AIRFLOW_UI_URL = 'http://localhost:18080';  // Direct URL for external links
const AIRFLOW_EMBED_URL = 'http://localhost:18081';  // Proxied URL for iframe embedding

// Mock 실행 기록 데이터
const mockRunHistory: Record<string, any[]> = {
  'cdw_patient_master_sync': [
    { run_id: 'run_20260206_010000', status: 'success', start: '2026-02-06 01:00:00', end: '2026-02-06 01:15:32', duration: '15분 32초' },
    { run_id: 'run_20260205_010000', status: 'success', start: '2026-02-05 01:00:00', end: '2026-02-05 01:14:58', duration: '14분 58초' },
    { run_id: 'run_20260204_010000', status: 'success', start: '2026-02-04 01:00:00', end: '2026-02-04 01:16:12', duration: '16분 12초' },
    { run_id: 'run_20260203_010000', status: 'failed', start: '2026-02-03 01:00:00', end: '2026-02-03 01:05:22', duration: '5분 22초', error: 'Connection timeout to source DB' },
    { run_id: 'run_20260202_010000', status: 'success', start: '2026-02-02 01:00:00', end: '2026-02-02 01:13:45', duration: '13분 45초' },
  ],
  'edw_prescription_daily_batch': [
    { run_id: 'run_20260206_030000', status: 'running', start: '2026-02-06 03:00:00', end: '-', duration: '진행중...' },
    { run_id: 'run_20260205_030000', status: 'success', start: '2026-02-05 03:00:00', end: '2026-02-05 03:45:12', duration: '45분 12초' },
    { run_id: 'run_20260204_030000', status: 'success', start: '2026-02-04 03:00:00', end: '2026-02-04 03:42:33', duration: '42분 33초' },
  ],
  'datamart_ml_features_build': [
    { run_id: 'run_20260203_060000', status: 'failed', start: '2026-02-03 06:00:00', end: '2026-02-03 06:12:45', duration: '12분 45초', error: 'OutOfMemoryError: heap space' },
    { run_id: 'run_20260127_060000', status: 'success', start: '2026-01-27 06:00:00', end: '2026-01-27 07:23:11', duration: '1시간 23분' },
    { run_id: 'run_20260120_060000', status: 'success', start: '2026-01-20 06:00:00', end: '2026-01-20 07:18:44', duration: '1시간 18분' },
  ],
  'realtime_vitals_stream_to_lake': [
    { run_id: 'stream_continuous', status: 'success', start: '2026-02-01 00:00:00', end: '-', duration: '연속 실행중', records: '1,234,567건 처리' },
  ],
};

const ETL: React.FC = () => {
  const { message } = App.useApp();
  const [historyDrawer, setHistoryDrawer] = useState<{ open: boolean; dagId: string | null }>({ open: false, dagId: null });
  const [viewMode, setViewMode] = useState<'list' | 'airflow'>('list');
  const [airflowLoading, setAirflowLoading] = useState(true);
  const [airflowError, setAirflowError] = useState(false);

  const handleTriggerDag = (dagId: string) => {
    Modal.confirm({
      title: `${dagId} 파이프라인을 수동으로 실행하시겠습니까?`,
      content: '스케줄과 별개로 새로운 DAG Run이 생성됩니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: () => {
        // In a real app, this would trigger an API call to Airflow
        message.success(`'${dagId}' 파이프라인 실행을 요청했습니다.`);
      },
    });
  };

  const statusToComponent = (status: string) => {
    switch (status) {
      case 'success':
        return <Tag icon={<CheckCircleOutlined />} color="success">성공</Tag>;
      case 'running':
        return <Tag icon={<SyncOutlined spin />} color="processing">실행중</Tag>;
      case 'failed':
        return <Tag icon={<CloseCircleOutlined />} color="error">실패</Tag>;
      default:
        return <Tag color="default">{status}</Tag>;
    }
  };

  const pipelineColumns = [
    {
      title: '파이프라인 ID',
      dataIndex: 'dag_id',
      key: 'dag_id',
      width: 220,
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
      render: (schedule: string) => <Text code style={{ whiteSpace: 'nowrap' }}>{schedule}</Text>,
    },
    {
      title: '최근 실행 기록',
      dataIndex: 'recent_runs',
      key: 'recent_runs',
      width: 140,
      render: (runs: string[]) => (
        <Space>
          {runs.map((runStatus, index) => (
            <Tooltip key={index} title={`${index + 1}번째 전 실행: ${runStatus}`}>
              {runStatus === 'success' ? <CheckCircleOutlined style={{ color: 'green' }} />
               : runStatus === 'failed' ? <CloseCircleOutlined style={{ color: 'red' }} />
               : <SyncOutlined spin style={{ color: 'blue' }} />}
            </Tooltip>
          ))}
        </Space>
      ),
    },
    {
      title: '마지막 실행',
      dataIndex: 'last_run',
      key: 'last_run',
      width: 120,
      render: (isoString: string) => <span style={{ whiteSpace: 'nowrap' }}>{dayjs(isoString).fromNow()}</span>,
    },
    {
      title: '소유자',
      dataIndex: 'owner',
      key: 'owner',
      width: 140,
      render: (owner: string) => <span style={{ whiteSpace: 'nowrap' }}>{owner}</span>,
    },
    {
      title: '작업',
      key: 'action',
      width: 240,
      fixed: 'right' as const,
      render: (_: any, record: any) => (
        <Space size="small">
          <Button
            type="primary"
            size="small"
            icon={<PlayCircleOutlined />}
            onClick={() => handleTriggerDag(record.dag_id)}
          >
            수동 실행
          </Button>
          <Button
            size="small"
            icon={<BarChartOutlined />}
            onClick={() => setHistoryDrawer({ open: true, dagId: record.dag_id })}
          >
            실행기록
          </Button>
        </Space>
      ),
    },
  ];

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success': return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'running': return <SyncOutlined spin style={{ color: '#1890ff' }} />;
      case 'failed': return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      default: return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
    }
  };

  const historyColumns = [
    { title: 'Run ID', dataIndex: 'run_id', key: 'run_id', width: 180 },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => statusToComponent(status),
    },
    { title: '시작 시간', dataIndex: 'start', key: 'start', width: 160 },
    { title: '종료 시간', dataIndex: 'end', key: 'end', width: 160 },
    { title: '소요 시간', dataIndex: 'duration', key: 'duration', width: 120 },
  ];

  const summary = {
    total: mockPipelines.length,
    running: mockPipelines.filter(p => p.status === 'running').length,
    success: mockPipelines.filter(p => p.status === 'success').length,
    failed: mockPipelines.filter(p => p.status === 'failed').length,
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
            </Paragraph>
          </Col>
          <Col>
            <Space>
              <Segmented
                options={[
                  { label: '요약 뷰', value: 'list', icon: <UnorderedListOutlined /> },
                  { label: 'Airflow UI', value: 'airflow', icon: <AppstoreOutlined /> },
                ]}
                value={viewMode}
                onChange={(value) => {
                  setViewMode(value as 'list' | 'airflow');
                  if (value === 'airflow') {
                    setAirflowLoading(true);
                    setAirflowError(false);
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
        {viewMode === 'list' && (
          <Alert
            message="GUI 기반 ETL 관리"
            description="통합 데이터 플랫폼의 모든 데이터 흐름은 Airflow 파이프라인을 통해 코드로 관리(Pipeline-as-Code)됩니다. 이 대시보드에서 핵심 파이프라인의 상태를 확인하고, 상세 분석 및 관리는 Airflow UI에서 수행할 수 있습니다."
            type="info"
            showIcon
            style={{ marginTop: 16 }}
          />
        )}
      </Card>
      
      {viewMode === 'list' ? (
        <>
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
                <Statistic title="실행중" value={summary.running} valueStyle={{ color: '#1890ff' }} prefix={<SyncOutlined spin />} />
              </Card>
            </Col>
          </Row>

          <Card title="파이프라인 목록">
            <Table
              columns={pipelineColumns}
              dataSource={mockPipelines}
              pagination={{ pageSize: 10 }}
              scroll={{ x: 1100 }}
              expandable={{
                expandedRowRender: record => <p style={{ margin: 0 }}>{record.description}</p>,
              }}
            />
          </Card>
        </>
      ) : (
        <Card
          styles={{ body: { padding: 0, height: '100%', position: 'relative' } }}
          style={{ flex: 1, overflow: 'hidden', minHeight: 600 }}
        >
          {airflowLoading && !airflowError && (
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
              <div style={{ marginTop: 8, color: '#999', fontSize: 12 }}>
                로그인: admin / admin
              </div>
            </div>
          )}
          {airflowError ? (
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
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary">로그인: admin / admin</Text>
                    </div>
                  </div>
                }
                style={{ maxWidth: 500, margin: '0 auto' }}
              />
            </div>
          ) : (
            <iframe
              src={`${AIRFLOW_EMBED_URL}/dags/cdw_patient_master_sync/grid`}
              style={{
                width: '100%',
                height: '100%',
                border: 'none',
                minHeight: 600
              }}
              title="Apache Airflow"
              onLoad={() => setAirflowLoading(false)}
              onError={() => setAirflowError(true)}
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
        width={700}
        onClose={() => setHistoryDrawer({ open: false, dagId: null })}
        open={historyDrawer.open}
      >
        {historyDrawer.dagId && mockRunHistory[historyDrawer.dagId] ? (
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
              dataSource={mockRunHistory[historyDrawer.dagId].map((r, i) => ({ ...r, key: i }))}
              pagination={false}
              size="small"
              scroll={{ x: 600 }}
              expandable={{
                expandedRowRender: record => record.error ? (
                  <Alert message="에러 메시지" description={record.error} type="error" />
                ) : record.records ? (
                  <Alert message="처리 현황" description={record.records} type="success" />
                ) : null,
                rowExpandable: record => !!record.error || !!record.records,
              }}
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
        ) : (
          <Alert message="실행 기록이 없습니다." type="warning" />
        )}
      </Drawer>
    </Space>
  );
};

export default ETL;