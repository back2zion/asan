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
  App
} from 'antd';
import { 
  SyncOutlined, 
  CheckCircleOutlined, 
  CloseCircleOutlined,
  ClockCircleOutlined,
  PlayCircleOutlined,
  LinkOutlined,
  BarChartOutlined,
  SettingOutlined
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

const AIRFLOW_UI_URL = 'http://localhost:18080';

const ETL: React.FC = () => {
  const { message } = App.useApp();

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
      render: (id: string, record: any) => <Text strong>{id}</Text>,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: statusToComponent,
    },
    {
      title: '스케줄',
      dataIndex: 'schedule_interval',
      key: 'schedule_interval',
      render: (schedule: string) => <Text code>{schedule}</Text>,
    },
    {
      title: '최근 실행 기록',
      dataIndex: 'recent_runs',
      key: 'recent_runs',
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
      render: (isoString: string) => dayjs(isoString).fromNow(),
    },
    {
      title: '소유자',
      dataIndex: 'owner',
      key: 'owner',
    },
    {
      title: '작업',
      key: 'action',
      render: (_: any, record: any) => (
        <Space size="middle">
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
            href={`${AIRFLOW_UI_URL}/dags/${record.dag_id}/grid`}
            target="_blank"
          >
            실행기록 보기
          </Button>
        </Space>
      ),
    },
  ];

  const summary = {
    total: mockPipelines.length,
    running: mockPipelines.filter(p => p.status === 'running').length,
    success: mockPipelines.filter(p => p.status === 'success').length,
    failed: mockPipelines.filter(p => p.status === 'failed').length,
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3}>ETL 파이프라인 대시보드</Title>
            <Paragraph type="secondary">
              Apache Airflow 기반으로 운영되는 데이터 파이프라인의 현황을 모니터링하고 관리합니다.
            </Paragraph>
          </Col>
          <Col>
            <Button 
              type="primary" 
              icon={<LinkOutlined />}
              href={AIRFLOW_UI_URL}
              target="_blank"
            >
              Airflow UI 열기
            </Button>
          </Col>
        </Row>
        <Alert
          message="GUI 기반 ETL 관리"
          description="통합 데이터 플랫폼의 모든 데이터 흐름은 Airflow 파이프라인을 통해 코드로 관리(Pipeline-as-Code)됩니다. 이 대시보드에서 핵심 파이프라인의 상태를 확인하고, 상세 분석 및 관리는 Airflow UI에서 수행할 수 있습니다."
          type="info"
          showIcon
          style={{ marginTop: 16 }}
        />
      </Card>
      
      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic title="총 파이프라인" value={summary.total} prefix={<SettingOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="성공" value={summary.success} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="실패" value={summary.failed} valueStyle={{ color: '#cf1322' }} prefix={<CloseCircleOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
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
          expandable={{
            expandedRowRender: record => <p style={{ margin: 0 }}>{record.description}</p>,
          }}
        />
      </Card>
    </Space>
  );
};

export default ETL;