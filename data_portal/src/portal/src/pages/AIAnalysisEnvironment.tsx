import React, { useState, useEffect } from 'react';
import {
  Card, Button, Typography, Space, Row, Col, Badge, Select, Switch,
  Table, Alert, Tabs, App,
} from 'antd';
import {
  RocketOutlined, RobotOutlined, MonitorOutlined, ShareAltOutlined,
  PlayCircleOutlined, StopOutlined, SettingOutlined, CloudServerOutlined,
  FileTextOutlined, DeleteOutlined,
} from '@ant-design/icons';
import {
  aiEnvironmentService,
  type ContainerResponse,
  type SystemResourceMetrics,
  type AnalysisTemplate,
  type ContainerCreateRequest,
} from '../services/aiEnvironmentService.ts';
import ResourceOverview from '../components/aienv/ResourceOverview';
import CreateEnvironmentModal from '../components/aienv/CreateEnvironmentModal';
import { TemplatesPanel, AIAnalysisPanel, MonitoringPanel, ProjectsPanel } from '../components/aienv/TabPanels';

const { Title, Paragraph, Text } = Typography;

const AIAnalysisEnvironment: React.FC = () => {
  const { message } = App.useApp();
  const [activeTab, setActiveTab] = useState('environment');
  const [userRole, setUserRole] = useState('researcher');
  const [autoScaling, setAutoScaling] = useState(true);
  const [gpuOptimization, setGpuOptimization] = useState(true);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [loading, setLoading] = useState(false);

  const [containers, setContainers] = useState<ContainerResponse[]>([]);
  const [resourceMetrics, setResourceMetrics] = useState<SystemResourceMetrics | null>(null);
  const [realSystemInfo, setRealSystemInfo] = useState<any>(null);
  const [templates, setTemplates] = useState<AnalysisTemplate[]>([]);

  useEffect(() => { loadData(); }, []);

  const loadData = async () => {
    setLoading(true);
    try {
      const [containersData, resourcesData, realSystemData, templatesData] = await Promise.all([
        aiEnvironmentService.getContainers(),
        aiEnvironmentService.getSystemResources(),
        aiEnvironmentService.getRealSystemInfo(),
        aiEnvironmentService.getTemplates(),
      ]);
      setContainers(containersData);
      setResourceMetrics(resourcesData);
      setRealSystemInfo(realSystemData);
      setTemplates(templatesData);
    } catch (error) {
      message.error('데이터를 불러오는데 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateEnvironment = async (values: any) => {
    try {
      setLoading(true);
      const request: ContainerCreateRequest = {
        name: values.name,
        description: values.description,
        resources: {
          cpu_cores: values.cpu,
          memory_gb: parseInt(values.memory.replace('GB', '')),
          gpu_count: values.gpu || 0,
          disk_gb: values.disk || 50,
        },
        template_id: values.template,
      };
      const newContainer = await aiEnvironmentService.createContainer(request);
      setContainers([...containers, newContainer]);
      setCreateModalVisible(false);
      message.success('분석 환경이 생성되었습니다. 잠시 후 사용 가능합니다.');
    } catch (error: any) {
      message.error(error.message || '환경 생성에 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleStartContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.startContainer(containerId);
      message.success(result.message);
      setContainers(prev =>
        prev.map(c => c.id === containerId ? { ...c, status: 'running', access_url: result.access_url } : c)
      );
    } catch (error: any) {
      message.error(error.message || '컨테이너 시작에 실패했습니다.');
    }
  };

  const handleStopContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.stopContainer(containerId);
      message.success(result.message);
      const updated = await aiEnvironmentService.getContainers();
      setContainers(updated);
    } catch (error: any) {
      message.error(error.message || '컨테이너 중지에 실패했습니다.');
    }
  };

  const handleDeleteContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.deleteContainer(containerId);
      message.success(result.message);
      const updated = await aiEnvironmentService.getContainers();
      setContainers(updated);
    } catch (error: any) {
      message.error(error.message || '컨테이너 삭제에 실패했습니다.');
    }
  };

  const containerColumns = [
    {
      title: '환경명',
      dataIndex: 'name',
      key: 'name',
      render: (name: string, record: ContainerResponse) => (
        <Space direction="vertical" size="small">
          <Space>
            <CloudServerOutlined style={{ color: '#006241' }} />
            <Text strong>{name}</Text>
          </Space>
          {record.description && <Text type="secondary" style={{ fontSize: '12px' }}>{record.description}</Text>}
        </Space>
      ),
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const cfg: Record<string, { color: string; text: string }> = {
          running: { color: 'success', text: '실행중' },
          stopped: { color: 'default', text: '중지됨' },
          creating: { color: 'processing', text: '생성중' },
          error: { color: 'error', text: '오류' },
          terminating: { color: 'warning', text: '종료중' },
        };
        const c = cfg[status] || { color: 'default', text: status };
        return <Badge status={c.color as any} text={c.text} />;
      },
    },
    {
      title: '리소스',
      key: 'resources',
      render: (record: ContainerResponse) => (
        <Space direction="vertical" size="small">
          <Text style={{ fontSize: '12px' }}>CPU: {record.resources.cpu_cores}코어 | MEM: {record.resources.memory_gb}GB</Text>
          {record.resources.gpu_count > 0 && (
            <Text style={{ fontSize: '12px', color: '#FF6F00' }}>GPU: {record.resources.gpu_count}개</Text>
          )}
        </Space>
      ),
    },
    {
      title: '가동시간',
      dataIndex: 'uptime_seconds',
      key: 'uptime_seconds',
      render: (v: number) => <Text>{aiEnvironmentService.formatUptime(v)}</Text>,
    },
    {
      title: '작업',
      key: 'actions',
      render: (record: ContainerResponse) => (
        <Space>
          {record.status === 'running' && record.access_url && (
            <Button type="primary" size="small" icon={<RocketOutlined />} onClick={() => window.open(record.access_url, '_blank')}>
              접속
            </Button>
          )}
          <Button
            size="small"
            icon={record.status === 'running' ? <StopOutlined /> : <PlayCircleOutlined />}
            onClick={() => record.status === 'running' ? handleStopContainer(record.id) : handleStartContainer(record.id)}
            disabled={record.status === 'creating' || record.status === 'terminating'}
          >
            {record.status === 'running' ? '중지' : '시작'}
          </Button>
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDeleteContainer(record.id)}
            disabled={record.status === 'creating' || record.status === 'terminating'}
          >
            삭제
          </Button>
        </Space>
      ),
    },
  ];

  const tabItems = [
    { key: 'environment', label: <span style={{ fontWeight: '500', fontSize: '15px' }}><CloudServerOutlined style={{ color: '#006241', marginRight: '8px' }} />분석 환경</span> },
    { key: 'templates', label: <span style={{ fontWeight: '500', fontSize: '15px' }}><FileTextOutlined style={{ color: '#006241', marginRight: '8px' }} />템플릿</span> },
    { key: 'ai_analysis', label: <span style={{ fontWeight: '500', fontSize: '15px' }}><RobotOutlined style={{ color: '#006241', marginRight: '8px' }} />생성형 AI</span> },
    { key: 'monitoring', label: <span style={{ fontWeight: '500', fontSize: '15px' }}><MonitorOutlined style={{ color: '#006241', marginRight: '8px' }} />리소스 모니터링</span> },
    { key: 'projects', label: <span style={{ fontWeight: '500', fontSize: '15px' }}><ShareAltOutlined style={{ color: '#006241', marginRight: '8px' }} />프로젝트 관리</span> },
  ];

  return (
    <>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* Header */}
        <Card>
          <Row align="middle" justify="space-between">
            <Col>
              <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
                <RobotOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
                AI 데이터 분석환경 제공
              </Title>
              <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
                컨테이너 기반의 AI 데이터 분석환경 플랫폼
              </Paragraph>
            </Col>
            <Col>
              <Space direction="vertical" size="small" style={{ textAlign: 'right' }}>
                <Badge status="processing" text={<span style={{ color: '#006241', fontWeight: '500' }}>시스템 정상</span>} />
                <Badge status="success" text={<span style={{ color: '#52A67D', fontWeight: '500' }}>GPU 가용</span>} />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Settings */}
        <Card
          title={<span style={{ color: '#333', fontWeight: '600' }}><SettingOutlined style={{ color: '#006241', marginRight: '8px' }} /> 환경 설정</span>}
          style={{ borderRadius: '8px', border: '1px solid #e9ecef', boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)' }}
        >
          <Row gutter={[24, 16]}>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <Text strong style={{ color: '#333', fontSize: '14px' }}>사용자 역할:</Text>
                <Select
                  value={userRole}
                  onChange={setUserRole}
                  style={{ width: '100%' }}
                  size="large"
                  options={[
                    { value: 'researcher', label: '연구자' },
                    { value: 'data_scientist', label: '데이터 사이언티스트' },
                    { value: 'ml_engineer', label: 'ML 엔지니어' },
                    { value: 'admin', label: '시스템 관리자' },
                  ]}
                />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>자동 스케일링:</Text>
                <Switch checked={autoScaling} onChange={setAutoScaling} checkedChildren="활성화" unCheckedChildren="비활성화" />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>GPU 최적화:</Text>
                <Switch checked={gpuOptimization} onChange={setGpuOptimization} checkedChildren="활성화" unCheckedChildren="비활성화" />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Resource Overview */}
        <ResourceOverview realSystemInfo={realSystemInfo} />

        {/* Main Tabs */}
        <Card
          styles={{ body: { padding: 0 } }}
          style={{ borderRadius: '8px', border: '1px solid #e9ecef', boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)' }}
        >
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            size="large"
            style={{ padding: '0 24px', background: '#ffffff' }}
            items={tabItems}
          />
          <div style={{ padding: '24px' }}>
            {activeTab === 'environment' && (
              <Card
                title="JupyterLab 분석 환경"
                extra={<Button type="primary" icon={<RocketOutlined />} onClick={() => setCreateModalVisible(true)}>새 환경 생성</Button>}
              >
                <Table columns={containerColumns} dataSource={containers} rowKey="id" pagination={false} size="middle" />
              </Card>
            )}
            {activeTab === 'templates' && <TemplatesPanel templates={templates} />}
            {activeTab === 'ai_analysis' && <AIAnalysisPanel />}
            {activeTab === 'monitoring' && <MonitoringPanel />}
            {activeTab === 'projects' && <ProjectsPanel />}
          </div>
        </Card>
      </Space>

      <CreateEnvironmentModal
        visible={createModalVisible}
        loading={loading}
        templates={templates}
        resourceMetrics={resourceMetrics}
        onSubmit={handleCreateEnvironment}
        onCancel={() => setCreateModalVisible(false)}
      />
    </>
  );
};

export default AIAnalysisEnvironment;
