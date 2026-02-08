import React, { useState, useMemo } from 'react';
import {
  Card, Typography, Space, Row, Col, Tag, Badge, Statistic, Tabs, Table, Spin, Alert, Descriptions, List, Button, Tooltip,
} from 'antd';
import {
  ApiOutlined, CloudServerOutlined, DatabaseOutlined, RobotOutlined,
  CheckCircleOutlined, CloseCircleOutlined, ExclamationCircleOutlined,
  ReloadOutlined, ThunderboltOutlined, ClusterOutlined, HddOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import ReactFlow, { Background, Controls, Node, Edge } from 'reactflow';
import 'reactflow/dist/style.css';
import {
  aiArchitectureApi,
  type ArchitectureOverview,
  type HealthCheck,
  type ContainerInfo,
  type McpTool,
} from '../services/aiArchitectureApi';

const { Title, Paragraph, Text } = Typography;

const STATUS_MAP: Record<string, { color: string; icon: React.ReactNode }> = {
  healthy: { color: 'green', icon: <CheckCircleOutlined /> },
  connected: { color: 'green', icon: <CheckCircleOutlined /> },
  initializing: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  degraded: { color: 'orange', icon: <ExclamationCircleOutlined /> },
  not_available: { color: 'default', icon: <ExclamationCircleOutlined /> },
  disconnected: { color: 'red', icon: <CloseCircleOutlined /> },
  error: { color: 'red', icon: <CloseCircleOutlined /> },
  unavailable: { color: 'red', icon: <CloseCircleOutlined /> },
};

const TYPE_COLORS: Record<string, string> = {
  protocol: '#722ed1',
  pipeline: '#1890ff',
  agent: '#52c41a',
  model: '#fa8c16',
};

// ── MCP Topology ReactFlow ──────────────────────────────

const buildMcpNodes = (tools: McpTool[]): { nodes: Node[]; edges: Edge[] } => {
  const nodes: Node[] = [];
  const edges: Edge[] = [];

  // Center: MCP Server
  nodes.push({
    id: 'mcp-server',
    position: { x: 250, y: 0 },
    data: { label: 'IDP MCP Server (v2.0)' },
    style: { padding: '12px 24px', borderRadius: 8, border: '2px solid #722ed1', background: '#f9f0ff', fontWeight: 'bold', fontSize: 14, minWidth: 220, textAlign: 'center' },
  });

  // Left column: Data sources
  const sources = [
    { id: 'qdrant', label: 'Qdrant (RAG)', y: 0 },
    { id: 'omop', label: 'OMOP CDM (92M rows)', y: 100 },
    { id: 'llm', label: 'LLM (XiYan/Qwen3)', y: 200 },
  ];
  sources.forEach(s => {
    nodes.push({
      id: s.id,
      position: { x: 0, y: 120 + s.y },
      data: { label: s.label },
      style: { padding: '8px 16px', borderRadius: 6, border: '1px solid #1890ff', background: '#e6f7ff', fontSize: 12, minWidth: 170, textAlign: 'center' },
    });
    edges.push({
      id: `e-${s.id}-mcp`,
      source: s.id,
      target: 'mcp-server',
      animated: true,
      style: { stroke: '#1890ff', strokeWidth: 1.5 },
    });
  });

  // Right column: Tools
  tools.forEach((tool, idx) => {
    const catColors: Record<string, string> = { data: '#52c41a', sql: '#fa8c16', search: '#1890ff', governance: '#722ed1' };
    const color = catColors[tool.category] || '#999';
    nodes.push({
      id: `tool-${tool.name}`,
      position: { x: 540, y: 60 + idx * 60 },
      data: { label: `${tool.name}` },
      style: { padding: '6px 14px', borderRadius: 6, border: `1px solid ${color}`, background: '#fff', fontSize: 11, minWidth: 150, textAlign: 'center' },
    });
    edges.push({
      id: `e-mcp-${tool.name}`,
      source: 'mcp-server',
      target: `tool-${tool.name}`,
      style: { stroke: color, strokeWidth: 1 },
    });
  });

  return { nodes, edges };
};

// ── Main Component ──────────────────────────────────────

const AIArchitecture: React.FC = () => {
  const { data: overview, isLoading: overviewLoading } = useQuery({
    queryKey: ['ai-architecture-overview'],
    queryFn: aiArchitectureApi.getOverview,
    staleTime: 60_000,
  });

  const { data: health, isLoading: healthLoading, refetch: refetchHealth } = useQuery({
    queryKey: ['ai-architecture-health'],
    queryFn: aiArchitectureApi.getHealth,
    staleTime: 30_000,
  });

  const { data: containersData } = useQuery({
    queryKey: ['ai-architecture-containers'],
    queryFn: aiArchitectureApi.getContainers,
    staleTime: 30_000,
  });

  const { data: mcpTopology } = useQuery({
    queryKey: ['ai-architecture-mcp-topology'],
    queryFn: aiArchitectureApi.getMcpTopology,
    staleTime: 60_000,
  });

  const mcpFlowData = useMemo(() => {
    if (!mcpTopology?.tools) return { nodes: [], edges: [] };
    return buildMcpNodes(mcpTopology.tools);
  }, [mcpTopology]);

  if (overviewLoading) {
    return <Card><div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /><div style={{ marginTop: 16 }}>아키텍처 정보 로딩 중...</div></div></Card>;
  }

  const sw = overview?.sw_architecture;
  const hw = overview?.hw_infrastructure;
  const infra = overview?.container_infrastructure;

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* Header */}
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: 600 }}>
              <ClusterOutlined style={{ color: '#722ed1', marginRight: 12, fontSize: 28 }} />
              AI 인프라 & 아키텍처
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 15 }}>
              AAR-002: AI S/W 아키텍처 (MCP & Agent) + H/W 인프라 + Container 기반 확장 환경
            </Paragraph>
          </Col>
          <Col>
            <Space>
              {health && (
                <Badge
                  status={health.overall === 'healthy' ? 'success' : 'warning'}
                  text={<span style={{ fontWeight: 500 }}>{health.overall === 'healthy' ? '전체 정상' : '일부 이상'}</span>}
                />
              )}
              <Button icon={<ReloadOutlined />} onClick={() => refetchHealth()} loading={healthLoading} size="small">
                새로고침
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Summary Statistics */}
      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic title="S/W 컴포넌트" value={sw?.components.length || 0} prefix={<ApiOutlined />} suffix="개" valueStyle={{ color: '#722ed1' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="GPU 자원" value={hw?.total_gpu_memory_gb?.toFixed(1) || 0} prefix={<ThunderboltOutlined />} suffix="GB" valueStyle={{ color: '#fa8c16' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="컨테이너 서비스" value={infra?.total_services || 0} prefix={<CloudServerOutlined />} suffix="개" valueStyle={{ color: '#1890ff' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic title="MCP Tools" value={7} prefix={<RobotOutlined />} suffix="개" valueStyle={{ color: '#52c41a' }} />
          </Card>
        </Col>
      </Row>

      {/* Tabs */}
      <Card>
        <Tabs
          defaultActiveKey="sw"
          size="large"
          items={[
            {
              key: 'sw',
              label: <span><ApiOutlined /> S/W 아키텍처</span>,
              children: (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  {/* Design Patterns */}
                  <Card title="아키텍처 패턴" size="small">
                    <List
                      dataSource={sw?.patterns || []}
                      renderItem={(item) => (
                        <List.Item>
                          <Text><CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />{item}</Text>
                        </List.Item>
                      )}
                    />
                  </Card>

                  {/* Component Cards */}
                  <Row gutter={[16, 16]}>
                    {sw?.components.map((comp) => (
                      <Col span={8} key={comp.id}>
                        <Card
                          size="small"
                          title={
                            <Space>
                              <Tag color={TYPE_COLORS[comp.type] || '#999'}>{comp.type}</Tag>
                              <Text strong>{comp.name}</Text>
                            </Space>
                          }
                          extra={health?.components?.[comp.id] && (
                            <Tag color={STATUS_MAP[health.components[comp.id]?.status]?.color || 'default'}>
                              {health.components[comp.id]?.status}
                            </Tag>
                          )}
                        >
                          <Text type="secondary" style={{ fontSize: 13 }}>{comp.description}</Text>
                          {comp.endpoint && (
                            <div style={{ marginTop: 8 }}>
                              <Tag color="blue">{comp.endpoint}</Tag>
                            </div>
                          )}
                          {comp.model && (
                            <div style={{ marginTop: 4 }}>
                              <Text code style={{ fontSize: 11 }}>{comp.model}</Text>
                            </div>
                          )}
                          {comp.providers && (
                            <div style={{ marginTop: 4 }}>
                              {comp.providers.map((p, i) => (
                                <Tag key={i} style={{ fontSize: 11, marginTop: 2 }}>{p}</Tag>
                              ))}
                            </div>
                          )}
                          {comp.nodes && (
                            <div style={{ marginTop: 4 }}>
                              {comp.nodes.map((n, i) => (
                                <Tag key={i} color="green" style={{ fontSize: 11, marginTop: 2 }}>{n}</Tag>
                              ))}
                            </div>
                          )}
                        </Card>
                      </Col>
                    ))}
                  </Row>
                </Space>
              ),
            },
            {
              key: 'mcp',
              label: <span><RobotOutlined /> MCP 토폴로지</span>,
              children: (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  <Alert
                    message="MCP (Model Context Protocol)"
                    description="7개 도구가 Qdrant RAG, OMOP CDM, LLM 서비스에 실시간 연결되어 있습니다. 모든 도구가 실데이터를 반환합니다."
                    type="info"
                    showIcon
                  />
                  <div style={{ width: '100%', height: 500 }}>
                    <ReactFlow
                      nodes={mcpFlowData.nodes}
                      edges={mcpFlowData.edges}
                      fitView
                      nodesDraggable={false}
                      nodesConnectable={false}
                      proOptions={{ hideAttribution: true }}
                    >
                      <Background color="#f0f0f0" gap={20} />
                      <Controls showInteractive={false} />
                    </ReactFlow>
                  </div>
                  {/* Tool details table */}
                  {mcpTopology?.tools && (
                    <Table
                      dataSource={mcpTopology.tools}
                      rowKey="name"
                      size="small"
                      pagination={false}
                      columns={[
                        { title: 'Tool', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                        { title: '카테고리', dataIndex: 'category', key: 'category', render: (v: string) => <Tag>{v}</Tag> },
                        { title: '백엔드 서비스', dataIndex: 'backend_service', key: 'backend_service' },
                        { title: '데이터 소스', dataIndex: 'data_source', key: 'data_source', ellipsis: true },
                        { title: '상태', dataIndex: 'status', key: 'status', render: (v: string) => <Tag color="green">{v}</Tag> },
                      ]}
                    />
                  )}
                </Space>
              ),
            },
            {
              key: 'gpu',
              label: <span><ThunderboltOutlined /> GPU 자원</span>,
              children: (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  <Row gutter={[16, 16]}>
                    {hw?.gpu_resources.map((gpu) => (
                      <Col span={8} key={gpu.id}>
                        <Card size="small" title={<><HddOutlined /> {gpu.name}</>}>
                          <Descriptions column={1} size="small">
                            <Descriptions.Item label="디바이스">{gpu.device}</Descriptions.Item>
                            <Descriptions.Item label="모델">{gpu.model_loaded}</Descriptions.Item>
                            <Descriptions.Item label="메모리">{gpu.memory_gb} GB</Descriptions.Item>
                            <Descriptions.Item label="포트">{String(gpu.port)}</Descriptions.Item>
                          </Descriptions>
                        </Card>
                      </Col>
                    ))}
                  </Row>
                  <Card title="GPU 메모리 할당 현황" size="small">
                    {hw?.gpu_resources.map((gpu) => {
                      const pct = Math.min(100, (gpu.memory_gb / (hw.total_gpu_memory_gb || 1)) * 100);
                      return (
                        <div key={gpu.id} style={{ marginBottom: 12 }}>
                          <Row justify="space-between">
                            <Text strong>{gpu.model_loaded}</Text>
                            <Text>{gpu.memory_gb} GB</Text>
                          </Row>
                          <div style={{ background: '#f0f0f0', borderRadius: 4, height: 20, marginTop: 4, overflow: 'hidden' }}>
                            <div style={{ background: '#fa8c16', height: '100%', width: `${pct}%`, borderRadius: 4, transition: 'width 0.3s' }} />
                          </div>
                        </div>
                      );
                    })}
                  </Card>
                </Space>
              ),
            },
            {
              key: 'containers',
              label: <span><CloudServerOutlined /> 컨테이너 인프라</span>,
              children: (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  {/* Stack overview */}
                  <Row gutter={[16, 16]}>
                    {infra?.stacks.map((stack) => (
                      <Col span={8} key={stack.stack}>
                        <Card size="small" title={<><DatabaseOutlined /> {stack.stack}</>} extra={<Tag>{stack.compose}</Tag>}>
                          {stack.services.map((svc) => {
                            const container = containersData?.containers?.find((c: ContainerInfo) => c.name === svc);
                            const running = container?.status?.includes('Up');
                            return (
                              <div key={svc} style={{ marginBottom: 4 }}>
                                <Badge status={running ? 'success' : 'default'} />
                                <Text style={{ fontSize: 12, marginLeft: 4 }}>{svc}</Text>
                              </div>
                            );
                          })}
                        </Card>
                      </Col>
                    ))}
                  </Row>

                  {/* Live container list */}
                  {containersData?.containers && (
                    <Card title={`실행 중인 컨테이너 (${containersData.count}개)`} size="small">
                      <Table
                        dataSource={containersData.containers}
                        rowKey="name"
                        size="small"
                        pagination={false}
                        scroll={{ y: 400 }}
                        columns={[
                          { title: '컨테이너명', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                          {
                            title: '상태', dataIndex: 'status', key: 'status',
                            render: (v: string) => <Tag color={v?.includes('Up') ? 'green' : 'red'}>{v}</Tag>,
                          },
                          { title: '이미지', dataIndex: 'image', key: 'image', ellipsis: true, render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> },
                          { title: '포트', dataIndex: 'ports', key: 'ports', ellipsis: true, render: (v: string) => <Text style={{ fontSize: 11 }}>{v}</Text> },
                        ]}
                      />
                    </Card>
                  )}
                </Space>
              ),
            },
            {
              key: 'health',
              label: <span><CheckCircleOutlined /> 헬스체크</span>,
              children: health ? (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  <Alert
                    message={`전체 상태: ${health.overall}`}
                    description={`마지막 확인: ${health.checked_at}`}
                    type={health.overall === 'healthy' ? 'success' : 'warning'}
                    showIcon
                  />
                  <Row gutter={[16, 16]}>
                    {Object.entries(health.components).map(([key, val]) => {
                      const st = STATUS_MAP[val.status] || STATUS_MAP.error;
                      return (
                        <Col span={6} key={key}>
                          <Card size="small">
                            <Space direction="vertical" style={{ width: '100%' }}>
                              <Row justify="space-between" align="middle">
                                <Text strong style={{ fontSize: 13 }}>{key}</Text>
                                <Tag color={st.color} icon={st.icon}>{val.status}</Tag>
                              </Row>
                              {val.execution_time_ms !== undefined && (
                                <Text type="secondary" style={{ fontSize: 11 }}>응답: {val.execution_time_ms.toFixed(0)}ms</Text>
                              )}
                              {val.port !== undefined && (
                                <Text type="secondary" style={{ fontSize: 11 }}>포트: {val.port}</Text>
                              )}
                              {val.error && (
                                <Text type="danger" style={{ fontSize: 11 }}>{val.error}</Text>
                              )}
                            </Space>
                          </Card>
                        </Col>
                      );
                    })}
                  </Row>
                </Space>
              ) : (
                <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>
              ),
            },
          ]}
        />
      </Card>
    </Space>
  );
};

export default AIArchitecture;
