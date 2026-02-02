import React, { useState } from 'react';
import { Card, Tabs, Button, Space, Table, Tag, Progress, Alert, Row, Col, Statistic, List, Badge } from 'antd';
import { 
  CodeOutlined, 
  CloudServerOutlined, 
  ThunderboltOutlined,
  RobotOutlined,
  ExperimentOutlined,
  FundProjectionScreenOutlined
} from '@ant-design/icons';

const AIEnvironment: React.FC = () => {
  const [selectedEnvironment, setSelectedEnvironment] = useState('jupyter');

  // Sample data for containers
  const containers = [
    {
      id: 'cont-001',
      name: 'research-env-01',
      user: 'dr.kim',
      status: 'running',
      cpu: '4 cores',
      memory: '16GB',
      gpu: '1x RTX 3090',
      created: '2024-11-17 10:00',
    },
    {
      id: 'cont-002',
      name: 'ml-training-02',
      user: 'dr.lee',
      status: 'running',
      cpu: '8 cores',
      memory: '32GB',
      gpu: '1x RTX 3090',
      created: '2024-11-17 09:30',
    },
    {
      id: 'cont-003',
      name: 'deep-learning-03',
      user: 'dr.park',
      status: 'stopped',
      cpu: '4 cores',
      memory: '16GB',
      gpu: 'None',
      created: '2024-11-16 15:00',
    },
  ];

  // Available templates
  const templates = [
    {
      name: '탐색적 데이터 분석',
      description: 'EDA를 위한 기본 환경',
      libraries: ['pandas', 'numpy', 'matplotlib', 'seaborn'],
      icon: <FundProjectionScreenOutlined />,
    },
    {
      name: '딥러닝 연구',
      description: 'GPU 가속 딥러닝 환경',
      libraries: ['tensorflow', 'pytorch', 'keras', 'cuda'],
      icon: <ThunderboltOutlined />,
    },
    {
      name: '자연어 처리',
      description: 'NLP 연구를 위한 환경',
      libraries: ['transformers', 'spacy', 'nltk', 'gensim'],
      icon: <RobotOutlined />,
    },
    {
      name: '의료 영상 분석',
      description: '의료 이미지 처리 환경',
      libraries: ['pydicom', 'nibabel', 'opencv', 'scikit-image'],
      icon: <ExperimentOutlined />,
    },
  ];

  const containerColumns = [
    {
      title: '컨테이너명',
      dataIndex: 'name',
      key: 'name',
      render: (text: string) => <strong>{text}</strong>,
    },
    {
      title: '사용자',
      dataIndex: 'user',
      key: 'user',
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const color = status === 'running' ? 'green' : 'red';
        return <Tag color={color}>{status.toUpperCase()}</Tag>;
      },
    },
    {
      title: 'CPU',
      dataIndex: 'cpu',
      key: 'cpu',
    },
    {
      title: 'Memory',
      dataIndex: 'memory',
      key: 'memory',
    },
    {
      title: 'GPU',
      dataIndex: 'gpu',
      key: 'gpu',
      render: (gpu: string) => (
        gpu === 'None' ? <Tag>없음</Tag> : <Tag color="blue">{gpu}</Tag>
      ),
    },
    {
      title: '작업',
      key: 'action',
      render: (_: any, record: any) => (
        <Space size="middle">
          <Button size="small" type="primary">
            JupyterLab 열기
          </Button>
          <Button size="small">중지</Button>
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
              title="활성 컨테이너"
              value={2}
              prefix={<CloudServerOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="GPU 사용률"
              value={65}
              suffix="%"
              prefix={<ThunderboltOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="총 메모리"
              value="64 GB"
              prefix={<CloudServerOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="실행 중인 노트북"
              value={8}
              prefix={<CodeOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Card title="SFR-006: AI 데이터 분석환경">
        <Tabs 
          defaultActiveKey="containers"
          items={[
            {
              key: 'containers',
              label: '컨테이너 관리',
              children: (
                <div>
                  <Space style={{ marginBottom: 16 }}>
                    <Button type="primary" icon={<CloudServerOutlined />}>
                      새 컨테이너 생성
                    </Button>
                    <Button icon={<ExperimentOutlined />}>
                      템플릿에서 생성
                    </Button>
                  </Space>
                  
                  <Table 
                    columns={containerColumns} 
                    dataSource={containers}
                    pagination={false}
                    rowKey="id"
                  />
                  
                  <Alert
                    message="GPU 리소스 현황"
                    description="RTX 3090 2대 중 2대 사용 중. GPU 가상화를 통해 최대 4개의 컨테이너에서 동시 사용 가능합니다."
                    type="info"
                    showIcon
                    style={{ marginTop: 16 }}
                  />
                </div>
              )
            },
            {
              key: 'templates',
              label: '분석 템플릿',
              children: (
                <Row gutter={[16, 16]}>
                  {templates.map((template, idx) => (
                    <Col span={12} key={idx}>
                      <Card>
                        <Space direction="vertical" style={{ width: '100%' }}>
                          <Space>
                            {template.icon}
                            <strong>{template.name}</strong>
                          </Space>
                          <p>{template.description}</p>
                          <Space wrap>
                            {template.libraries.map((lib, libIdx) => (
                              <Tag key={libIdx}>{lib}</Tag>
                            ))}
                          </Space>
                          <Button type="primary" block>
                            이 템플릿으로 시작
                          </Button>
                        </Space>
                      </Card>
                    </Col>
                  ))}
                </Row>
              )
            },
            {
              key: 'monitoring',
              label: '리소스 모니터링',
              children: (
                <Row gutter={[16, 16]}>
                  <Col span={12}>
                    <Card title="CPU 사용률">
                      <Progress percent={45} status="active" />
                      <p>12 cores / 32 cores 사용 중</p>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card title="메모리 사용률">
                      <Progress percent={72} status="active" />
                      <p>46 GB / 64 GB 사용 중</p>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card title="GPU 사용률 (RTX 3090 #1)">
                      <Progress percent={85} strokeColor="#52c41a" />
                      <p>VRAM: 20 GB / 24 GB</p>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card title="GPU 사용률 (RTX 3090 #2)">
                      <Progress percent={45} strokeColor="#52c41a" />
                      <p>VRAM: 11 GB / 24 GB</p>
                    </Card>
                  </Col>
                </Row>
              )
            },
            {
              key: 'libraries',
              label: '라이브러리 관리',
              children: (
                <Card>
                  <List
                    header={<div>설치된 주요 라이브러리</div>}
                    bordered
                    dataSource={[
                      'Python 3.11.5',
                      'TensorFlow 2.14.0 (GPU)',
                      'PyTorch 2.1.0 (CUDA 11.8)',
                      'scikit-learn 1.3.2',
                      'pandas 2.1.3',
                      'numpy 1.24.3',
                      'LangChain 0.0.350',
                      'transformers 4.36.0',
                    ]}
                    renderItem={item => (
                      <List.Item>
                        <Badge status="success" text={item} />
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

export default AIEnvironment;