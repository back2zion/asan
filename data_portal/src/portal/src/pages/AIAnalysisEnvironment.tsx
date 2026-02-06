import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Button, 
  Typography, 
  Space, 
  Row, 
  Col, 
  Alert, 
  Tabs, 
  Badge, 
  Select, 
  Switch,
  Progress,
  Statistic,
  Table,
  Modal,
  Form,
  Input,
  Tag,
  App,
  Spin
} from 'antd';
import { 
  RocketOutlined, 
  RobotOutlined, 
  MonitorOutlined,
  ShareAltOutlined,
  PlayCircleOutlined,
  StopOutlined,
  SettingOutlined,
  CloudServerOutlined,
  ThunderboltOutlined,
  FileTextOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import { 
  aiEnvironmentService, 
  type ContainerResponse, 
  type SystemResourceMetrics, 
  type AnalysisTemplate,
  type ContainerCreateRequest 
} from '../services/aiEnvironmentService.ts';

const { Title, Paragraph, Text } = Typography;
const { Option } = Select;

const AIAnalysisEnvironment: React.FC = () => {
  const { message } = App.useApp();
  const [activeTab, setActiveTab] = useState('environment');
  const [userRole, setUserRole] = useState('researcher');
  const [autoScaling, setAutoScaling] = useState(true);
  const [gpuOptimization, setGpuOptimization] = useState(true);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [loading, setLoading] = useState(false);
  const [form] = Form.useForm();

  // 실제 API에서 가져올 데이터
  const [containers, setContainers] = useState<ContainerResponse[]>([]);
  const [resourceMetrics, setResourceMetrics] = useState<SystemResourceMetrics | null>(null);
  const [realSystemInfo, setRealSystemInfo] = useState<any>(null);
  const [templates, setTemplates] = useState<AnalysisTemplate[]>([]);

  // 데이터 로딩
  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setLoading(true);
    try {
      const [containersData, resourcesData, realSystemData, templatesData] = await Promise.all([
        aiEnvironmentService.getContainers(),
        aiEnvironmentService.getSystemResources(),
        aiEnvironmentService.getRealSystemInfo(),
        aiEnvironmentService.getTemplates()
      ]);

      setContainers(containersData);
      setResourceMetrics(resourcesData);
      setRealSystemInfo(realSystemData);
      setTemplates(templatesData);
    } catch (error) {
      console.error('Failed to load data:', error);
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
          disk_gb: values.disk || 50
        },
        template_id: values.template
      };
      
      const newContainer = await aiEnvironmentService.createContainer(request);
      setContainers([...containers, newContainer]);
      setCreateModalVisible(false);
      form.resetFields();
      
      message.success('분석 환경이 생성되었습니다. 잠시 후 사용 가능합니다.');
    } catch (error: any) {
      console.error('Environment creation failed:', error);
      message.error(error.message || '환경 생성에 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleStartContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.startContainer(containerId);
      message.success(result.message);
      
      // 컨테이너 목록에서 해당 컨테이너를 찾아 상태와 access_url을 업데이트합니다.
      setContainers(prevContainers => 
        prevContainers.map(container =>
          container.id === containerId 
            ? { ...container, status: 'running', access_url: result.access_url }
            : container
        )
      );
    } catch (error: any) {
      message.error(error.message || '컨테이너 시작에 실패했습니다.');
    }
  };

  const handleStopContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.stopContainer(containerId);
      message.success(result.message);
      
      // 컨테이너 목록 새로고침
      const updatedContainers = await aiEnvironmentService.getContainers();
      setContainers(updatedContainers);
    } catch (error: any) {
      message.error(error.message || '컨테이너 중지에 실패했습니다.');
    }
  };

  const handleDeleteContainer = async (containerId: string) => {
    try {
      const result = await aiEnvironmentService.deleteContainer(containerId);
      message.success(result.message);
      
      // 컨테이너 목록 새로고침
      const updatedContainers = await aiEnvironmentService.getContainers();
      setContainers(updatedContainers);
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
          {record.description && (
            <Text type="secondary" style={{ fontSize: '12px' }}>
              {record.description}
            </Text>
          )}
        </Space>
      )
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const statusConfig = {
          running: { color: 'success', text: '실행중' },
          stopped: { color: 'default', text: '중지됨' },
          creating: { color: 'processing', text: '생성중' },
          error: { color: 'error', text: '오류' },
          terminating: { color: 'warning', text: '종료중' }
        };
        const config = statusConfig[status as keyof typeof statusConfig] || 
                      { color: 'default', text: status };
        return <Badge status={config.color as any} text={config.text} />;
      }
    },
    {
      title: '리소스',
      key: 'resources',
      render: (record: ContainerResponse) => (
        <Space direction="vertical" size="small">
          <Text style={{ fontSize: '12px' }}>
            CPU: {record.resources.cpu_cores}코어 | MEM: {record.resources.memory_gb}GB
          </Text>
          {record.resources.gpu_count > 0 && (
            <Text style={{ fontSize: '12px', color: '#FF6F00' }}>
              GPU: {record.resources.gpu_count}개
            </Text>
          )}
        </Space>
      )
    },
    {
      title: '가동시간',
      dataIndex: 'uptime_seconds',
      key: 'uptime_seconds',
      render: (uptimeSeconds: number) => (
        <Text>{aiEnvironmentService.formatUptime(uptimeSeconds)}</Text>
      )
    },
    {
      title: '작업',
      key: 'actions',
      render: (record: ContainerResponse) => (
        <Space>
          {record.status === 'running' && record.access_url && (
            <Button 
              type="primary" 
              size="small"
              icon={<RocketOutlined />}
              onClick={() => window.open(record.access_url, '_blank')}
            >
              접속
            </Button>
          )}
          <Button 
            size="small"
            icon={record.status === 'running' ? <StopOutlined /> : <PlayCircleOutlined />}
            onClick={() => {
              if (record.status === 'running') {
                handleStopContainer(record.id);
              } else {
                handleStartContainer(record.id);
              }
            }}
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
      )
    }
  ];

  return (
    <div style={{ padding: '0' }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* Header */}
        <Card style={{
          borderRadius: '12px',
          border: '1px solid #e9ecef',
          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.08)',
          background: '#ffffff'
        }}>
          <Row align="middle" justify="space-between">
            <Col>
              <Title level={3} style={{ 
                margin: 0, 
                color: '#333',
                fontWeight: '600'
              }}>
                <RobotOutlined style={{ 
                  color: '#006241', 
                  marginRight: '12px',
                  fontSize: '28px'
                }} /> 
                AI 데이터 분석환경 제공
              </Title>
              <Paragraph type="secondary" style={{ 
                margin: '8px 0 0 40px',
                fontSize: '15px',
                color: '#6c757d'
              }}>
                컨테이너 기반의 AI 데이터 분석환경 플랫폼
              </Paragraph>
            </Col>
            <Col>
              <Space direction="vertical" size="small" style={{ textAlign: 'right' }}>
                <Badge 
                  status="processing" 
                  text={<span style={{ color: '#006241', fontWeight: '500' }}>시스템 정상</span>} 
                />
                <Badge 
                  status="success" 
                  text={<span style={{ color: '#52A67D', fontWeight: '500' }}>GPU 가용</span>} 
                />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* User Settings */}
        <Card 
          title={
            <span style={{ color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#006241', marginRight: '8px' }} /> 
              환경 설정
            </span>
          } 
          style={{
            borderRadius: '8px',
            border: '1px solid #e9ecef',
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)'
          }}
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
                    { value: 'admin', label: '시스템 관리자' }
                  ]}
                />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>자동 스케일링:</Text>
                <Switch 
                  checked={autoScaling} 
                  onChange={setAutoScaling}
                  checkedChildren="활성화" 
                  unCheckedChildren="비활성화"
                  size="default"
                />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>GPU 최적화:</Text>
                <Switch 
                  checked={gpuOptimization} 
                  onChange={setGpuOptimization}
                  checkedChildren="활성화" 
                  unCheckedChildren="비활성화"
                  size="default"
                />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Resource Overview */}
        {realSystemInfo ? (
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={8}>
              <Card style={{ textAlign: 'center' }}>
                <Statistic
                  title="GPU 사용률"
                  value={realSystemInfo.gpu.usage_percent}
                  precision={1}
                  suffix="%"
                  valueStyle={{ color: '#FF6F00', fontSize: '24px', fontWeight: '700' }}
                />
                <Progress 
                  percent={realSystemInfo.gpu.usage_percent} 
                  strokeColor="#FF6F00"
                  size="small" 
                />
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {realSystemInfo.gpu.description}
                </Text>
              </Card>
            </Col>
            <Col xs={24} sm={8}>
              <Card style={{ textAlign: 'center' }}>
                <Statistic
                  title="CPU 사용률"
                  value={realSystemInfo.cpu.usage_percent}
                  precision={1}
                  suffix="%"
                  valueStyle={{ color: '#006241', fontSize: '24px', fontWeight: '700' }}
                />
                <Progress 
                  percent={realSystemInfo.cpu.usage_percent} 
                  strokeColor="#006241"
                  size="small" 
                />
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {realSystemInfo.cpu.description}
                </Text>
              </Card>
            </Col>
            <Col xs={24} sm={8}>
              <Card style={{ textAlign: 'center' }}>
                <Statistic
                  title="메모리 사용률"
                  value={realSystemInfo.memory.usage_percent}
                  precision={1}
                  suffix="%"
                  valueStyle={{ color: '#52A67D', fontSize: '24px', fontWeight: '700' }}
                />
                <Progress 
                  percent={realSystemInfo.memory.usage_percent} 
                  strokeColor="#52A67D"
                  size="small" 
                />
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {realSystemInfo.memory.description}
                </Text>
              </Card>
            </Col>
          </Row>
        ) : (
          <Row gutter={[16, 16]}>
            <Col span={24}>
              <Card style={{ textAlign: 'center', padding: '40px 0' }}>
                <Spin size="large" />
                <div style={{ marginTop: 16 }}>
                  <Text>리소스 정보를 불러오고 있습니다...</Text>
                </div>
              </Card>
            </Col>
          </Row>
        )}

        {/* Main Tabs */}
        <Card 
          styles={{ body: { padding: 0 } }}
          style={{
            borderRadius: '8px',
            border: '1px solid #e9ecef',
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)'
          }}
        >
          <Tabs 
            activeKey={activeTab} 
            onChange={setActiveTab}
            size="large"
            style={{ 
              padding: '0 24px',
              background: '#ffffff'
            }}
            items={[
              {
                key: 'environment',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <CloudServerOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    분석 환경
                  </span>
                )
              },
              {
                key: 'templates',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <FileTextOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    템플릿
                  </span>
                )
              },
              {
                key: 'ai_analysis',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <RobotOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    생성형 AI
                  </span>
                )
              },
              {
                key: 'monitoring',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <MonitorOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    리소스 모니터링
                  </span>
                )
              },
              {
                key: 'projects',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <ShareAltOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    프로젝트 관리
                  </span>
                )
              }
            ]}
          />
          
          <div style={{ padding: '24px' }}>
            {activeTab === 'environment' && (
              <div>
                <Card 
                  title="JupyterLab 분석 환경" 
                  extra={
                    <Button 
                      type="primary" 
                      icon={<RocketOutlined />}
                      onClick={() => setCreateModalVisible(true)}
                    >
                      새 환경 생성
                    </Button>
                  }
                >
                  <Alert
                    message="사용자별 독립 분석 환경"
                    description="GPU 연산 처리가 가능한 JupyterLab 환경을 제공합니다. 딥러닝 및 기계학습 분석을 위한 최적화된 라이브러리가 사전 설치되어 있습니다."
                    type="info"
                    showIcon
                    style={{
                      borderRadius: '6px',
                      border: '1px solid #b3d8ff',
                      backgroundColor: '#f0f8f3',
                      marginBottom: '16px'
                    }}
                  />
                  
                  <Table
                    columns={containerColumns}
                    dataSource={containers}
                    rowKey="id"
                    pagination={false}
                    size="middle"
                  />
                </Card>
              </div>
            )}

            {activeTab === 'templates' && (
              <div>
                <Card title="분석 템플릿 라이브러리">
                  <Alert
                    message="템플릿 기반 분석 환경"
                    description="탐색적 데이터 분석, 자연어 분석, 공간 분석을 위한 사전 정의된 분석 절차를 제공합니다."
                    type="info"
                    showIcon
                    style={{
                      borderRadius: '6px',
                      backgroundColor: '#f0f8f3',
                      marginBottom: '16px'
                    }}
                  />
                  
                  <Row gutter={[16, 16]}>
                    {templates.map(template => (
                      <Col xs={24} md={8} key={template.id}>
                        <Card
                          hoverable
                          size="small"
                          style={{
                            borderRadius: '8px',
                            border: '1px solid #e9ecef'
                          }}
                          actions={[
                            <Button type="link" icon={<PlayCircleOutlined />}>
                              실행
                            </Button>
                          ]}
                        >
                          <Card.Meta
                            avatar={<FileTextOutlined style={{ color: '#006241', fontSize: '24px' }} />}
                            title={template.name}
                            description={
                              <Space direction="vertical" size="small">
                                <Text>{template.description}</Text>
                                <Space>
                                  <Tag color={template.category === 'basic' ? 'green' : 'orange'}>
                                    {template.category === 'basic' ? '기본' : '고급'}
                                  </Tag>
                                  <Text type="secondary" style={{ fontSize: '12px' }}>
                                    예상 시간: {template.estimated_runtime_minutes}분
                                  </Text>
                                </Space>
                              </Space>
                            }
                          />
                        </Card>
                      </Col>
                    ))}
                  </Row>
                </Card>
              </div>
            )}

            {activeTab === 'ai_analysis' && (
              <div>
                <Card title="생성형 AI 분석 기능">
                  <Alert
                    message="SOTA LLM 기반 지능형 분석"
                    description="GraphRAG와 LangGraph를 활용한 고도화된 AI 분석 기능을 제공합니다. Qwen3-Next, MiniMax M1 등 최신 모델을 지원합니다."
                    type="success"
                    showIcon
                    style={{
                      borderRadius: '6px',
                      backgroundColor: '#f0f8f3',
                      marginBottom: '16px'
                    }}
                  />
                  
                  <Row gutter={[16, 16]}>
                    <Col xs={24} md={12}>
                      <Card type="inner" title="GraphRAG 시스템" size="small">
                        <Space direction="vertical" style={{ width: '100%' }}>
                          <div style={{ padding: '8px', background: '#f6ffed', borderRadius: '4px' }}>
                            <Text strong>지식 그래프 기반 검색</Text>
                            <div><Text type="secondary" style={{ fontSize: '12px' }}>의료 지식베이스 연동</Text></div>
                          </div>
                          <div style={{ padding: '8px', background: '#fff7e6', borderRadius: '4px' }}>
                            <Text strong>벡터 검색 증강</Text>
                            <div><Text type="secondary" style={{ fontSize: '12px' }}>컨텍스트 기반 응답 생성</Text></div>
                          </div>
                        </Space>
                      </Card>
                    </Col>
                    <Col xs={24} md={12}>
                      <Card type="inner" title="LangGraph 에이전트" size="small">
                        <Space direction="vertical" style={{ width: '100%' }}>
                          <div style={{ padding: '8px', background: '#e6f7ff', borderRadius: '4px' }}>
                            <Text strong>다중 LLM 통합</Text>
                            <div><Text type="secondary" style={{ fontSize: '12px' }}>에이전트 기반 워크플로우</Text></div>
                          </div>
                          <div style={{ padding: '8px', background: '#f9f0ff', borderRadius: '4px' }}>
                            <Text strong>도구 연동 시스템</Text>
                            <div><Text type="secondary" style={{ fontSize: '12px' }}>데이터베이스 능동 상호작용</Text></div>
                          </div>
                        </Space>
                      </Card>
                    </Col>
                  </Row>
                </Card>
              </div>
            )}

            {activeTab === 'monitoring' && (
              <div>
                <Card title="실시간 리소스 모니터링">
                  <Alert
                    message="프로젝트별 리소스 관리"
                    description="CPU/Memory/GPU 사용량을 실시간으로 모니터링하고 프로젝트별 리소스 할당을 관리합니다."
                    type="warning"
                    showIcon
                    style={{
                      borderRadius: '6px',
                      backgroundColor: '#fff8f0',
                      marginBottom: '16px'
                    }}
                  />
                  
                  <Text>실시간 모니터링 대시보드가 여기에 표시됩니다.</Text>
                </Card>
              </div>
            )}

            {activeTab === 'projects' && (
              <div>
                <Card title="프로젝트 및 공유 관리">
                  <Alert
                    message="분석 작업 공유 및 결과 활용"
                    description="분석 프로젝트를 공유하고 권한을 관리할 수 있습니다. 분석 결과를 다양한 포맷으로 다운로드할 수 있습니다."
                    type="info"
                    showIcon
                    style={{
                      borderRadius: '6px',
                      backgroundColor: '#f0f8f3',
                      marginBottom: '16px'
                    }}
                  />
                  
                  <Text>프로젝트 관리 기능이 여기에 표시됩니다.</Text>
                </Card>
              </div>
            )}
          </div>
        </Card>

      </Space>

      {/* Create Environment Modal */}
      <Modal
        title="새 분석 환경 생성"
        open={createModalVisible}
        onCancel={() => setCreateModalVisible(false)}
        footer={null}
        width={600}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleCreateEnvironment}
        >
          <Form.Item
            name="name"
            label="환경명"
            rules={[{ required: true, message: '환경명을 입력하세요!' }]}
          >
            <Input placeholder="예: Medical Deep Learning Lab" />
          </Form.Item>

          <Form.Item
            name="description"
            label="설명"
          >
            <Input.TextArea 
              placeholder="이 분석 환경에 대한 간단한 설명을 입력하세요" 
              rows={2}
            />
          </Form.Item>

          <Form.Item
            name="template"
            label="분석 템플릿"
          >
            <Select placeholder="템플릿을 선택하세요 (선택사항)" allowClear>
              {templates.map(template => (
                <Option key={template.id} value={template.id}>
                  {template.name} - {template.description}
                </Option>
              ))}
            </Select>
          </Form.Item>
          
          <Row gutter={16}>
            <Col span={6}>
              <Form.Item
                name="cpu"
                label="CPU 코어"
                rules={[{ required: true, message: 'CPU 코어 수를 선택하세요!' }]}
              >
                <Select>
                  <Option value={2}>2 코어</Option>
                  <Option value={4}>4 코어</Option>
                  <Option value={8}>8 코어</Option>
                  <Option value={16}>16 코어</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="memory"
                label="메모리"
                rules={[{ required: true, message: '메모리 크기를 선택하세요!' }]}
              >
                <Select>
                  <Option value="4GB">4GB</Option>
                  <Option value="8GB">8GB</Option>
                  <Option value="16GB">16GB</Option>
                  <Option value="32GB">32GB</Option>
                  <Option value="64GB">64GB</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="gpu"
                label="GPU"
                initialValue={0}
              >
                <Select>
                  <Option value={0}>없음</Option>
                  <Option value={1}>1개</Option>
                  <Option value={2}>2개</Option>
                  <Option value={4}>4개</Option>
                </Select>
              </Form.Item>
            </Col>
            <Col span={6}>
              <Form.Item
                name="disk"
                label="디스크 용량"
                rules={[{ required: true, message: '디스크 용량을 선택하세요!' }]}
                initialValue={50}
              >
                <Select>
                  <Option value={20}>20GB</Option>
                  <Option value={50}>50GB</Option>
                  <Option value={100}>100GB</Option>
                  <Option value={200}>200GB</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>

          {resourceMetrics && (
            <Alert
              message="현재 리소스 상황"
              description={
                <Space direction="vertical" size="small" style={{ width: '100%' }}>
                  <div>
                    CPU: {resourceMetrics.cpu.used_cores}/{resourceMetrics.cpu.total_cores} 코어 사용중 
                    ({resourceMetrics.cpu.utilization_percent.toFixed(1)}%)
                  </div>
                  <div>
                    메모리: {resourceMetrics.memory.used_gb}GB/{resourceMetrics.memory.total_gb}GB 사용중 
                    ({resourceMetrics.memory.utilization_percent.toFixed(1)}%)
                  </div>
                  <div>
                    GPU: {resourceMetrics.gpu.used_count}/{resourceMetrics.gpu.total_count}개 사용중 
                    ({resourceMetrics.gpu.utilization_percent.toFixed(1)}%)
                  </div>
                </Space>
              }
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
            />
          )}
          
          <Form.Item>
            <Space>
              <Button 
                type="primary" 
                htmlType="submit"
                loading={loading}
              >
                생성
              </Button>
              <Button onClick={() => setCreateModalVisible(false)}>
                취소
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default AIAnalysisEnvironment;