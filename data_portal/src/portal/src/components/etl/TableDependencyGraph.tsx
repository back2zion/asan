/**
 * DIR-001: 테이블 종속관계 그래프 탭
 * ReactFlow 기반 dependency graph + FK 자동 감지 + 위상정렬
 */
import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  BackgroundVariant,
  Handle,
  Position,
  ReactFlowProvider,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  App, Card, Button, Space, Typography, Row, Col, Tag, Spin, Empty,
  Form, Input, Select, List, Badge, Drawer, Alert,
} from 'antd';
import {
  ApartmentOutlined, PlusOutlined, ThunderboltOutlined,
  OrderedListOutlined, ReloadOutlined, DeleteOutlined,
} from '@ant-design/icons';
import { fetchDelete, fetchPost } from '../../services/apiUtils';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetchPost(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetchDelete(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// Custom Node
const DependencyNode = ({ data }: any) => {
  const color = data.color || '#718096';
  return (
    <div style={{
      background: '#fff',
      border: `2px solid ${color}`,
      borderRadius: 8,
      padding: '8px 14px',
      minWidth: 140,
      boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
    }}>
      <Handle type="target" position={Position.Left} style={{ background: color, width: 8, height: 8 }} />
      <div style={{ fontWeight: 600, fontSize: 13, color }}>{data.label}</div>
      {data.rowCount > 0 && (
        <div style={{ fontSize: 11, color: '#888', marginTop: 2 }}>
          {data.rowCount.toLocaleString()} rows
        </div>
      )}
      <Handle type="source" position={Position.Right} style={{ background: color, width: 8, height: 8 }} />
    </div>
  );
};

const nodeTypes = { default: DependencyNode };

interface Dependency {
  dep_id: number;
  source_table: string;
  target_table: string;
  relationship: string | null;
  dep_type: string;
}

interface ExecOrderItem {
  order: number;
  table: string;
}

const InnerGraph: React.FC<{ graphData: { nodes: Node[]; edges: Edge[] } | null }> = ({ graphData }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (graphData) {
      setNodes(graphData.nodes);
      setEdges(graphData.edges);
    }
  }, [graphData, setNodes, setEdges]);

  if (!graphData || graphData.nodes.length === 0) {
    return <Empty description="종속관계 데이터 없음. 'FK 자동 감지'를 실행하세요." style={{ padding: 60 }} />;
  }

  return (
    <div style={{ height: 500 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        <Controls />
        <MiniMap nodeStrokeWidth={3} />
      </ReactFlow>
    </div>
  );
};

const TableDependencyGraph: React.FC = () => {
  const { message } = App.useApp();
  const [graphData, setGraphData] = useState<{ nodes: Node[]; edges: Edge[] } | null>(null);
  const [dependencies, setDependencies] = useState<Dependency[]>([]);
  const [executionOrder, setExecutionOrder] = useState<ExecOrderItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [detecting, setDetecting] = useState(false);
  const [orderDrawer, setOrderDrawer] = useState(false);
  const [addForm] = Form.useForm();

  const loadGraph = useCallback(async () => {
    setLoading(true);
    try {
      const [gData, dData] = await Promise.all([
        fetchJSON(`${API_BASE}/dependencies/graph`),
        fetchJSON(`${API_BASE}/dependencies`),
      ]);
      setGraphData(gData);
      setDependencies(dData.dependencies || []);
    } catch { message.error('그래프 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadGraph(); }, [loadGraph]);

  const handleAutoDetect = async () => {
    setDetecting(true);
    try {
      const data = await postJSON(`${API_BASE}/dependencies/auto-detect`, {});
      message.success(`${data.detected_count}개 종속관계 감지됨`);
      loadGraph();
    } catch { message.error('자동 감지 실패'); }
    finally { setDetecting(false); }
  };

  const handleLoadOrder = async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/dependencies/execution-order`);
      setExecutionOrder(data.execution_order || []);
      setOrderDrawer(true);
    } catch { message.error('실행순서 로드 실패'); }
  };

  const handleAddDependency = async () => {
    const values = await addForm.validateFields();
    try {
      await postJSON(`${API_BASE}/dependencies`, values);
      message.success('종속관계 추가됨');
      addForm.resetFields();
      loadGraph();
    } catch (e: any) {
      message.error(e.message || '추가 실패');
    }
  };

  const handleDeleteDep = async (depId: number) => {
    try {
      await deleteJSON(`${API_BASE}/dependencies/${depId}`);
      message.success('삭제됨');
      loadGraph();
    } catch { message.error('삭제 실패'); }
  };

  return (
    <Spin spinning={loading}>
      <Row gutter={16}>
        <Col xs={24} lg={18}>
          <Card
            size="small"
            title={<><ApartmentOutlined /> 테이블 종속관계 그래프</>}
            extra={
              <Space>
                <Button icon={<ThunderboltOutlined />} type="primary" onClick={handleAutoDetect} loading={detecting}>
                  FK 자동 감지
                </Button>
                <Button icon={<OrderedListOutlined />} onClick={handleLoadOrder}>실행 순서</Button>
                <Button icon={<ReloadOutlined />} onClick={loadGraph}>새로고침</Button>
              </Space>
            }
          >
            <div style={{ marginBottom: 8 }}>
              <Space>
                <Tag color="blue">실선 = FK</Tag>
                <Tag>점선 = 수동/파생</Tag>
                <Badge count={dependencies.length} style={{ backgroundColor: '#005BAC' }} overflowCount={999}>
                  <Tag>종속관계</Tag>
                </Badge>
              </Space>
            </div>
            <ReactFlowProvider>
              <InnerGraph graphData={graphData} />
            </ReactFlowProvider>
          </Card>
        </Col>

        <Col xs={24} lg={6}>
          <Card size="small" title="수동 종속관계 추가" style={{ marginBottom: 16 }}>
            <Form form={addForm} layout="vertical" size="small">
              <Form.Item name="source_table" label="부모 테이블" rules={[{ required: true }]}>
                <Input placeholder="예: person" />
              </Form.Item>
              <Form.Item name="target_table" label="자식 테이블" rules={[{ required: true }]}>
                <Input placeholder="예: condition_occurrence" />
              </Form.Item>
              <Form.Item name="relationship" label="관계 설명">
                <Input placeholder="예: person_id" />
              </Form.Item>
              <Form.Item name="dep_type" label="유형" initialValue="manual">
                <Select options={[
                  { value: 'fk', label: 'FK' },
                  { value: 'manual', label: '수동' },
                  { value: 'derived', label: '파생' },
                ]} />
              </Form.Item>
              <Button type="primary" icon={<PlusOutlined />} onClick={handleAddDependency} block>추가</Button>
            </Form>
          </Card>

          <Card size="small" title={`종속관계 목록 (${dependencies.length})`} styles={{ body: { maxHeight: 300, overflow: 'auto' } }}>
            {dependencies.length > 0 ? (
              <List
                size="small"
                dataSource={dependencies}
                renderItem={d => (
                  <List.Item
                    actions={[
                      <Button key="del" size="small" type="text" icon={<DeleteOutlined />} danger onClick={() => handleDeleteDep(d.dep_id)} />,
                    ]}
                  >
                    <div>
                      <Text code style={{ fontSize: 11 }}>{d.source_table}</Text>
                      <Text type="secondary"> → </Text>
                      <Text code style={{ fontSize: 11 }}>{d.target_table}</Text>
                      <div><Tag style={{ fontSize: 10 }}>{d.dep_type}</Tag></div>
                    </div>
                  </List.Item>
                )}
              />
            ) : <Empty description="없음" image={Empty.PRESENTED_IMAGE_SIMPLE} />}
          </Card>
        </Col>
      </Row>

      <Drawer
        title={<><OrderedListOutlined /> 위상정렬 실행 순서</>}
        open={orderDrawer}
        onClose={() => setOrderDrawer(false)}
        width={400}
      >
        {executionOrder.length > 0 ? (
          <List
            size="small"
            dataSource={executionOrder}
            renderItem={item => (
              <List.Item>
                <Badge count={item.order} style={{ backgroundColor: item.order === 1 ? '#52c41a' : '#005BAC' }} />
                <Text strong style={{ marginLeft: 12 }}>{item.table}</Text>
              </List.Item>
            )}
          />
        ) : <Empty description="종속관계를 먼저 추가하세요" />}
        <Alert
          style={{ marginTop: 16 }}
          type="info"
          showIcon
          message="위상정렬 기반 실행 순서"
          description="부모 테이블이 먼저 적재되도록 Kahn's algorithm으로 정렬합니다. person이 최상위에 위치합니다."
        />
      </Drawer>
    </Spin>
  );
};

export default TableDependencyGraph;
