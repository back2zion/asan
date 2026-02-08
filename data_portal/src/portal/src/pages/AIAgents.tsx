import React, { useState } from 'react';
import { Card, Tabs, Form, Input, Select, Button, Space, Table, Tag, Alert, Badge, List, Modal, App, Row, Col, Statistic } from 'antd';
import { 
  RobotOutlined, 
  CheckCircleOutlined, 
  ClockCircleOutlined,
  ExclamationCircleOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined
} from '@ant-design/icons';
import { useQuery, useMutation } from '@tanstack/react-query';
import axios from 'axios';

const { TextArea } = Input;

interface AgentTask {
  task_id: string;
  task_type: string;
  description: string;
  status: string;
  priority: string;
  created_at: string;
}

const AIAgents: React.FC = () => {
  const { message } = App.useApp();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [form] = Form.useForm();

  // Fetch pending tasks
  const { data: pendingTasks, refetch } = useQuery({
    queryKey: ['pending-tasks'],
    queryFn: async () => {
      const response = await axios.get('/api/v1/agents/pending');
      return response.data.pending_tasks;
    }
  });

  // Create agent task
  const createTask = useMutation({
    mutationFn: async (values: any) => {
      const response = await axios.post('/api/v1/agents/execute', values);
      return response.data;
    },
    onSuccess: (data) => {
      message.success(`작업이 생성되었습니다. ID: ${data.task_id}`);
      setIsModalOpen(false);
      form.resetFields();
      refetch();
    },
    onError: () => {
      message.error('작업 생성 중 오류가 발생했습니다.');
    }
  });

  // Approve task
  const approveTask = useMutation({
    mutationFn: async ({ task_id, approved, feedback }: { task_id: string; approved: boolean; feedback: string }) => {
      const response = await axios.post('/api/v1/agents/approve', {
        task_id,
        approved,
        feedback
      });
      return response.data;
    },
    onSuccess: (_data, variables) => {
      message.success(variables.approved ? '작업이 승인되었습니다.' : '작업이 거부되었습니다.');
      refetch();
    }
  });

  // Launch Claude Code session
  const launchClaude = useMutation({
    mutationFn: async (values: any) => {
      const response = await axios.post('/api/v1/agents/claude-code/launch', values);
      return response.data;
    },
    onSuccess: (data) => {
      message.success(`Claude Code 세션이 시작되었습니다. ID: ${data.session_id}`);
    }
  });

  const taskColumns = [
    {
      title: 'ID',
      dataIndex: 'task_id',
      key: 'task_id',
      width: 100,
    },
    {
      title: '유형',
      dataIndex: 'task_type',
      key: 'task_type',
      render: (type: string) => <Tag color="blue">{type}</Tag>,
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '우선순위',
      dataIndex: 'priority',
      key: 'priority',
      render: (priority: string) => {
        const color = priority === 'critical' ? 'red' : priority === 'high' ? 'orange' : 'default';
        return <Tag color={color}>{priority.toUpperCase()}</Tag>;
      },
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        let icon;
        let color;
        
        switch(status) {
          case 'pending_approval':
            icon = <ClockCircleOutlined />;
            color = 'orange';
            break;
          case 'approved':
            icon = <CheckCircleOutlined />;
            color = 'green';
            break;
          case 'rejected':
            icon = <ExclamationCircleOutlined />;
            color = 'red';
            break;
          default:
            icon = <ClockCircleOutlined />;
            color = 'default';
        }
        
        return <Tag color={color} icon={icon}>{status}</Tag>;
      },
    },
    {
      title: '작업',
      key: 'action',
      render: (_: any, record: any) => (
        <Space size="middle">
          <Button 
            size="small" 
            type="primary"
            onClick={() => approveTask.mutate({ 
              task_id: record.task_id, 
              approved: true,
              feedback: '' 
            })}
          >
            승인
          </Button>
          <Button 
            size="small" 
            danger
            onClick={() => approveTask.mutate({ 
              task_id: record.task_id, 
              approved: false,
              feedback: '작업이 적절하지 않습니다.' 
            })}
          >
            거부
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="대기 중인 작업"
              value={pendingTasks?.length || 0}
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: '#ff6600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="실행 중인 에이전트"
              value={3}
              prefix={<RobotOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="완료된 작업"
              value={45}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Claude 세션"
              value={2}
              prefix={<PlayCircleOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Card 
        title={
          <Space>
            <RobotOutlined />
            <span>AI 에이전트 관리 - HumanLayer 통합</span>
          </Space>
        }
        extra={
          <Badge status="processing" text="HumanLayer 연결됨" />
        }
      >
        <Tabs
          defaultActiveKey="tasks"
          items={[
            {
              key: 'create',
              label: '작업 생성',
              children: (
                <Card>
                  <Form
                    form={form}
                    layout="vertical"
                    onFinish={createTask.mutate}
                  >
                    <Form.Item
                      name="task_type"
                      label="작업 유형"
                      rules={[{ required: true, message: '작업 유형을 선택해주세요' }]}
                    >
                      <Select placeholder="작업 유형 선택">
                        <Select.Option value="data_analysis">데이터 분석</Select.Option>
                        <Select.Option value="etl_pipeline">ETL 파이프라인</Select.Option>
                        <Select.Option value="query_generation">쿼리 생성</Select.Option>
                        <Select.Option value="report_generation">보고서 생성</Select.Option>
                      </Select>
                    </Form.Item>

                    <Form.Item
                      name="description"
                      label="작업 설명"
                      rules={[{ required: true, message: '작업 설명을 입력해주세요' }]}
                    >
                      <TextArea
                        rows={4}
                        placeholder="수행할 작업을 상세히 설명해주세요..."
                      />
                    </Form.Item>

                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="require_approval"
                          label="승인 필요"
                          initialValue={true}
                        >
                          <Select>
                            <Select.Option value={true}>예</Select.Option>
                            <Select.Option value={false}>아니오</Select.Option>
                          </Select>
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="priority"
                          label="우선순위"
                          initialValue="medium"
                        >
                          <Select>
                            <Select.Option value="low">낮음</Select.Option>
                            <Select.Option value="medium">중간</Select.Option>
                            <Select.Option value="high">높음</Select.Option>
                            <Select.Option value="critical">긴급</Select.Option>
                          </Select>
                        </Form.Item>
                      </Col>
                    </Row>

                    <Form.Item>
                      <Button type="primary" htmlType="submit" loading={createTask.isPending}>
                        작업 생성
                      </Button>
                    </Form.Item>
                  </Form>
                </Card>
              )
            },
            {
              key: 'pending',
              label: (
                <Badge count={pendingTasks?.length || 0} offset={[10, 0]}>
                  <span>승인 대기</span>
                </Badge>
              ),
              children: (
                <>
                  <Table
                    columns={taskColumns}
                    dataSource={pendingTasks || []}
                    rowKey="task_id"
                    pagination={{ pageSize: 10 }}
                  />
                </>
              )
            },
            {
              key: 'claude',
              label: 'Claude Code 세션',
              children: (
                <Card>
                  <Form
                    layout="vertical"
                    onFinish={(values) => launchClaude.mutate(values)}
                  >
                    <Form.Item
                      name="project_path"
                      label="프로젝트 경로"
                      rules={[{ required: true }]}
                      initialValue="/home/babelai/projects/datastreams/amc"
                    >
                      <Input />
                    </Form.Item>

                    <Form.Item
                      name="instructions"
                      label="Claude Code 지시사항"
                    >
                      <TextArea
                        rows={6}
                        placeholder="Claude Code에게 수행할 작업을 설명하세요..."
                      />
                    </Form.Item>

                    <Form.Item>
                      <Button
                        type="primary"
                        htmlType="submit"
                        icon={<PlayCircleOutlined />}
                        loading={launchClaude.isPending}
                      >
                        Claude Code 세션 시작
                      </Button>
                    </Form.Item>
                  </Form>

                  <List
                    header={<div>활성 Claude Code 세션</div>}
                    bordered
                    style={{ marginTop: 24 }}
                    dataSource={[
                      { id: 'session_001', status: 'running', created: '10:30 AM' },
                      { id: 'session_002', status: 'running', created: '09:15 AM' },
                    ]}
                    renderItem={(item: any) => (
                      <List.Item
                        key={item.id}
                        actions={[
                          <Button key="view" size="small">보기</Button>,
                          <Button key="stop" size="small" danger>종료</Button>
                        ]}
                      >
                        <List.Item.Meta
                          title={item.id}
                          description={`생성 시간: ${item.created}`}
                        />
                        <Badge status="processing" text="실행 중" />
                      </List.Item>
                    )}
                  />
                </Card>
              )
            }
          ]}
        />
      </Card>
    </div>
  );
};

export default AIAgents;