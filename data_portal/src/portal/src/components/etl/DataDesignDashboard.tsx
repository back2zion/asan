/**
 * DIT-001: 통합적인 데이터 분석, 구성 체계 수립
 * Zone 관리, ERD 시각화, 명명 규칙, 비정형 데이터 매핑
 */
import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap, BackgroundVariant,
  Handle, Position, ReactFlowProvider, useNodesState, useEdgesState,
  type Node, type Edge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Segmented, Spin, Empty, Select, Descriptions, Drawer, Input,
  Form, List, Progress, Badge,
} from 'antd';
import {
  DatabaseOutlined, FolderOutlined, ApartmentOutlined,
  CheckCircleOutlined, CloseCircleOutlined, ReloadOutlined,
  FileTextOutlined, ExperimentOutlined, SafetyCertificateOutlined,
  SearchOutlined, CloudOutlined,
} from '@ant-design/icons';

const { Text, Title } = Typography;

const API_BASE = '/api/v1/data-design';

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

const ZONE_CONFIG: Record<string, { color: string; label: string; icon: React.ReactNode }> = {
  source: { color: '#E53E3E', label: 'Source', icon: <DatabaseOutlined /> },
  bronze: { color: '#DD6B20', label: 'Bronze', icon: <FolderOutlined /> },
  silver: { color: '#805AD5', label: 'Silver', icon: <SafetyCertificateOutlined /> },
  gold: { color: '#D69E2E', label: 'Gold', icon: <ExperimentOutlined /> },
  mart: { color: '#38A169', label: 'Mart', icon: <CloudOutlined /> },
};

// ERD Custom Node
const ERDNode = ({ data }: any) => {
  const color = data.color || '#718096';
  return (
    <div style={{
      background: '#fff',
      border: `2px solid ${color}`,
      borderRadius: 8,
      padding: '6px 12px',
      minWidth: 160,
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
    }}>
      <Handle type="target" position={Position.Left} style={{ background: color, width: 6, height: 6 }} />
      <div style={{ fontWeight: 600, fontSize: 12, color }}>{data.label}</div>
      <div style={{ fontSize: 10, color: '#888' }}>
        {data.entityType === 'external' ? <Tag color="red" style={{ fontSize: 9 }}>외부</Tag> : null}
        <Tag style={{ fontSize: 9 }} color={ZONE_CONFIG[data.zone]?.color}>{ZONE_CONFIG[data.zone]?.label || data.zone}</Tag>
        {data.columnCount > 0 && <span>{data.columnCount}cols</span>}
        {data.rowCount > 0 && <span> / {data.rowCount.toLocaleString()}rows</span>}
      </div>
      <Handle type="source" position={Position.Right} style={{ background: color, width: 6, height: 6 }} />
    </div>
  );
};

const erdNodeTypes = { default: ERDNode };

const InnerERD: React.FC<{ graphData: { nodes: Node[]; edges: Edge[] } | null }> = ({ graphData }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  useEffect(() => {
    if (graphData) { setNodes(graphData.nodes); setEdges(graphData.edges); }
  }, [graphData, setNodes, setEdges]);
  if (!graphData || graphData.nodes.length === 0) return <Empty description="ERD 데이터 로딩 중..." style={{ padding: 60 }} />;
  return (
    <div style={{ height: 500 }}>
      <ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
        nodeTypes={erdNodeTypes} fitView fitViewOptions={{ padding: 0.2 }}>
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        <Controls />
        <MiniMap nodeStrokeWidth={3} />
      </ReactFlow>
    </div>
  );
};

const DataDesignDashboard: React.FC = () => {
  const { message } = App.useApp();
  const [section, setSection] = useState<string>('zones');
  const [overview, setOverview] = useState<any>(null);
  const [zones, setZones] = useState<any[]>([]);
  const [entities, setEntities] = useState<any[]>([]);
  const [erdGraph, setErdGraph] = useState<{ nodes: Node[]; edges: Edge[] } | null>(null);
  const [namingRules, setNamingRules] = useState<any[]>([]);
  const [unstructured, setUnstructured] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [zoneFilter, setZoneFilter] = useState<string | undefined>();
  const [entityDrawer, setEntityDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });
  const [checkNames, setCheckNames] = useState('');
  const [checkResults, setCheckResults] = useState<any>(null);

  const loadZones = useCallback(async () => {
    setLoading(true);
    try {
      const [zData, oData] = await Promise.all([
        fetchJSON(`${API_BASE}/zones`),
        fetchJSON(`${API_BASE}/overview`),
      ]);
      setZones(zData.zones || []);
      setOverview(oData);
    } catch { message.error('데이터 영역 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadERD = useCallback(async () => {
    setLoading(true);
    try {
      const params = zoneFilter ? `?zone_type=${zoneFilter}` : '';
      const [eData, gData] = await Promise.all([
        fetchJSON(`${API_BASE}/entities${params ? `?${zoneFilter ? `zone_id=` : ''}` : ''}`),
        fetchJSON(`${API_BASE}/erd-graph${params}`),
      ]);
      setEntities(eData.entities || []);
      setErdGraph(gData);
    } catch { message.error('ERD 로드 실패'); }
    finally { setLoading(false); }
  }, [zoneFilter]);

  const loadNaming = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/naming-rules`);
      setNamingRules(data.rules || []);
    } catch { message.error('명명 규칙 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  const loadUnstructured = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/unstructured-mappings`);
      setUnstructured(data.mappings || []);
    } catch { message.error('비정형 매핑 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    if (section === 'zones') loadZones();
    else if (section === 'erd') loadERD();
    else if (section === 'naming') loadNaming();
    else if (section === 'unstructured') loadUnstructured();
  }, [section, loadZones, loadERD, loadNaming, loadUnstructured]);

  const handleViewEntity = async (entityId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/entities/${entityId}`);
      setEntityDrawer({ open: true, data });
    } catch { message.error('엔티티 조회 실패'); }
  };

  const handleCheckNaming = async () => {
    if (!checkNames.trim()) { message.warning('검증할 이름을 입력하세요'); return; }
    const names = checkNames.split(/[,\n]+/).map(n => n.trim()).filter(Boolean);
    try {
      const data = await postJSON(`${API_BASE}/naming-check`, { names, target: 'table' });
      setCheckResults(data);
    } catch { message.error('명명 검증 실패'); }
  };

  // ── Zone Overview ──
  const renderZones = () => (
    <>
      {overview && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={8} md={4}><Card size="small"><Statistic title="데이터 영역" value={Object.keys(overview.zones).length} prefix={<FolderOutlined />} /></Card></Col>
          <Col xs={8} md={4}><Card size="small"><Statistic title="엔티티" value={overview.entities.total} prefix={<DatabaseOutlined />} /></Card></Col>
          <Col xs={8} md={4}><Card size="small"><Statistic title="도메인" value={overview.entities.domains} prefix={<ApartmentOutlined />} /></Card></Col>
          <Col xs={8} md={4}><Card size="small"><Statistic title="관계" value={overview.relations} prefix={<ApartmentOutlined />} /></Card></Col>
          <Col xs={8} md={4}><Card size="small"><Statistic title="명명 준수율" value={overview.naming_compliance} suffix="%" valueStyle={{ color: overview.naming_compliance >= 90 ? '#3f8600' : '#cf1322' }} /></Card></Col>
          <Col xs={8} md={4}><Card size="small"><Statistic title="비정형 매핑" value={`${overview.unstructured.active}/${overview.unstructured.total}`} prefix={<FileTextOutlined />} /></Card></Col>
        </Row>
      )}

      <Card size="small" title="데이터 영역 (Zone) 구성 체계" extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadZones}>새로고침</Button>}>
        <Row gutter={[16, 16]}>
          {zones.map(z => {
            const cfg = ZONE_CONFIG[z.zone_type] || ZONE_CONFIG.source;
            return (
              <Col xs={24} md={12} lg={8} key={z.zone_id}>
                <Card
                  size="small"
                  style={{ borderLeft: `4px solid ${cfg.color}` }}
                  title={<Space>{cfg.icon}<Text strong>{z.zone_name}</Text><Tag color={cfg.color}>{cfg.label}</Tag></Space>}
                >
                  <Descriptions column={1} size="small">
                    <Descriptions.Item label="저장소"><Tag>{z.storage_type}</Tag></Descriptions.Item>
                    <Descriptions.Item label="경로"><Text code style={{ fontSize: 11 }}>{z.storage_path}</Text></Descriptions.Item>
                    <Descriptions.Item label="포맷"><Tag color="blue">{z.file_format?.toUpperCase()}</Tag></Descriptions.Item>
                    <Descriptions.Item label="보존기간">{z.retention_days}일</Descriptions.Item>
                    <Descriptions.Item label="파티션 전략">{z.partition_strategy || '-'}</Descriptions.Item>
                    <Descriptions.Item label="엔티티 수"><Badge count={z.entity_count} style={{ backgroundColor: cfg.color }} showZero /></Descriptions.Item>
                    <Descriptions.Item label="총 행 수">{z.total_rows?.toLocaleString() || 0}</Descriptions.Item>
                  </Descriptions>
                  <div style={{ marginTop: 8, fontSize: 12, color: '#888' }}>{z.description}</div>
                </Card>
              </Col>
            );
          })}
        </Row>
      </Card>
    </>
  );

  // ── ERD ──
  const renderERD = () => (
    <>
      <Card
        size="small"
        title={<><ApartmentOutlined /> 논리/물리 ERD</>}
        extra={
          <Space>
            <Select
              allowClear placeholder="영역 필터" size="small" style={{ width: 140 }}
              value={zoneFilter} onChange={setZoneFilter}
              options={Object.entries(ZONE_CONFIG).map(([k, v]) => ({ value: k, label: v.label }))}
            />
            <Button icon={<ReloadOutlined />} size="small" onClick={loadERD}>새로고침</Button>
          </Space>
        }
      >
        <div style={{ marginBottom: 8 }}>
          <Space wrap>
            {Object.entries(ZONE_CONFIG).map(([k, v]) => (
              <Tag key={k} color={v.color}>{v.icon} {v.label}</Tag>
            ))}
            <Tag color="blue">실선 = FK</Tag>
            <Tag color="red">점선 = ETL 변환</Tag>
          </Space>
        </div>
        <ReactFlowProvider>
          <InnerERD graphData={erdGraph} />
        </ReactFlowProvider>
      </Card>

      <Card size="small" title={`엔티티 목록 (${entities.length})`} style={{ marginTop: 16 }}>
        <Table
          dataSource={entities.map(e => ({ ...e, key: e.entity_id }))}
          size="small"
          pagination={{ pageSize: 15 }}
          onRow={(r) => ({ onClick: () => handleViewEntity(r.entity_id), style: { cursor: 'pointer' } })}
          columns={[
            { title: '물리명', dataIndex: 'entity_name', key: 'name', width: 180, render: (v: string) => <Text strong code>{v}</Text> },
            { title: '논리명', dataIndex: 'logical_name', key: 'lname', width: 120 },
            {
              title: '영역', key: 'zone', width: 100,
              render: (_: any, r: any) => {
                const cfg = ZONE_CONFIG[r.zone_type];
                return <Tag color={cfg?.color}>{cfg?.label || r.zone_type}</Tag>;
              },
            },
            { title: '도메인', dataIndex: 'domain', key: 'domain', width: 80, render: (v: string) => <Tag>{v}</Tag> },
            { title: '유형', dataIndex: 'entity_type', key: 'type', width: 80, render: (v: string) => <Tag color={v === 'external' ? 'red' : 'default'}>{v}</Tag> },
            { title: '컬럼', dataIndex: 'column_count', key: 'cols', width: 70 },
            { title: '행 수', dataIndex: 'row_count', key: 'rows', width: 110, render: (v: number) => v > 0 ? v.toLocaleString() : '-' },
          ]}
        />
      </Card>
    </>
  );

  // ── Naming ──
  const renderNaming = () => (
    <Row gutter={16}>
      <Col xs={24} lg={14}>
        <Card size="small" title="용어 표준/명명 규칙" extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadNaming}>새로고침</Button>}>
          <Table
            dataSource={namingRules.map(r => ({ ...r, key: r.rule_id }))}
            size="small"
            pagination={false}
            columns={[
              { title: '규칙명', dataIndex: 'rule_name', key: 'name', width: 160, render: (v: string) => <Text strong>{v}</Text> },
              { title: '대상', dataIndex: 'target', key: 'target', width: 80, render: (v: string) => <Tag>{v}</Tag> },
              { title: '패턴', dataIndex: 'pattern', key: 'pattern', width: 200, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
              { title: '예시', dataIndex: 'example', key: 'example', width: 200, render: (v: string) => <Text type="secondary">{v}</Text> },
              { title: '설명', dataIndex: 'description', key: 'desc', ellipsis: true },
            ]}
          />
        </Card>
      </Col>
      <Col xs={24} lg={10}>
        <Card size="small" title={<><SearchOutlined /> 명명 규칙 검증</>}>
          <Input.TextArea
            rows={4}
            value={checkNames}
            onChange={e => setCheckNames(e.target.value)}
            placeholder="검증할 이름을 입력 (쉼표 또는 줄바꿈 구분)&#10;예: person, visit_occurrence, BadTableName"
          />
          <Button type="primary" style={{ marginTop: 8 }} onClick={handleCheckNaming} block>검증 실행</Button>
          {checkResults && (
            <div style={{ marginTop: 16 }}>
              <Row gutter={16} style={{ marginBottom: 8 }}>
                <Col span={8}><Statistic title="검증" value={checkResults.summary.total} /></Col>
                <Col span={8}><Statistic title="통과" value={checkResults.summary.valid} valueStyle={{ color: '#3f8600' }} /></Col>
                <Col span={8}><Statistic title="준수율" value={checkResults.summary.compliance_rate} suffix="%" /></Col>
              </Row>
              <List
                size="small"
                dataSource={checkResults.results}
                renderItem={(r: any) => (
                  <List.Item>
                    <Space>
                      {r.valid ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#ff4d4f' }} />}
                      <Text code>{r.name}</Text>
                      {!r.valid && r.violations.map((v: any, i: number) => <Tag key={i} color="red">{v.rule}</Tag>)}
                    </Space>
                  </List.Item>
                )}
              />
            </div>
          )}
        </Card>
      </Col>
    </Row>
  );

  // ── Unstructured ──
  const renderUnstructured = () => (
    <Card size="small" title={<><FileTextOutlined /> 비정형 데이터 구조화 매핑</>} extra={<Button icon={<ReloadOutlined />} size="small" onClick={loadUnstructured}>새로고침</Button>}>
      <Table
        dataSource={unstructured.map(u => ({ ...u, key: u.mapping_id }))}
        size="small"
        pagination={false}
        columns={[
          { title: '소스 유형', dataIndex: 'source_type', key: 'src', width: 140, render: (v: string) => <Text strong>{v}</Text> },
          { title: '설명', dataIndex: 'source_description', key: 'desc', width: 250 },
          { title: '타겟 테이블', dataIndex: 'target_table', key: 'target', width: 150, render: (v: string) => <Text code>{v}</Text> },
          { title: '추출 방법', dataIndex: 'extraction_method', key: 'method', width: 150, render: (v: string) => <Tag color="blue">{v}</Tag> },
          { title: 'NLP 모델', dataIndex: 'nlp_model', key: 'model', width: 180, render: (v: string) => v ? <Text code style={{ fontSize: 11 }}>{v}</Text> : '-' },
          {
            title: '출력 컬럼', dataIndex: 'output_columns', key: 'cols', width: 200,
            render: (v: string[]) => <Space wrap size={2}>{(v || []).map(c => <Tag key={c} style={{ fontSize: 10 }}>{c}</Tag>)}</Space>,
          },
          {
            title: '상태', dataIndex: 'status', key: 'status', width: 80,
            render: (v: string) => {
              const colors: Record<string, string> = { active: 'green', testing: 'blue', planned: 'default', deprecated: 'red' };
              return <Tag color={colors[v] || 'default'}>{v}</Tag>;
            },
          },
        ]}
      />
    </Card>
  );

  return (
    <Spin spinning={loading}>
      <Segmented
        block
        options={[
          { label: '데이터 영역', value: 'zones', icon: <FolderOutlined /> },
          { label: 'ERD (논리/물리)', value: 'erd', icon: <ApartmentOutlined /> },
          { label: '명명 규칙', value: 'naming', icon: <SafetyCertificateOutlined /> },
          { label: '비정형 구조화', value: 'unstructured', icon: <FileTextOutlined /> },
        ]}
        value={section}
        onChange={(v) => setSection(v as string)}
        style={{ marginBottom: 16 }}
      />

      {section === 'zones' && renderZones()}
      {section === 'erd' && renderERD()}
      {section === 'naming' && renderNaming()}
      {section === 'unstructured' && renderUnstructured()}

      {/* Entity Detail Drawer */}
      <Drawer
        title={entityDrawer.data ? `${entityDrawer.data.entity_name} (${entityDrawer.data.logical_name})` : ''}
        open={entityDrawer.open}
        onClose={() => setEntityDrawer({ open: false, data: null })}
        width={600}
      >
        {entityDrawer.data && (
          <>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="물리명">{entityDrawer.data.entity_name}</Descriptions.Item>
              <Descriptions.Item label="논리명">{entityDrawer.data.logical_name}</Descriptions.Item>
              <Descriptions.Item label="영역"><Tag color={ZONE_CONFIG[entityDrawer.data.zone_type]?.color}>{entityDrawer.data.zone_type}</Tag></Descriptions.Item>
              <Descriptions.Item label="도메인">{entityDrawer.data.domain}</Descriptions.Item>
              <Descriptions.Item label="유형">{entityDrawer.data.entity_type}</Descriptions.Item>
              <Descriptions.Item label="행 수">{entityDrawer.data.row_count?.toLocaleString()}</Descriptions.Item>
            </Descriptions>
            {entityDrawer.data.description && (
              <div style={{ margin: '12px 0', color: '#666' }}>{entityDrawer.data.description}</div>
            )}
            <Card size="small" title={`컬럼 (${entityDrawer.data.columns?.length || 0})`} style={{ marginTop: 16 }}>
              <Table
                dataSource={(entityDrawer.data.columns || []).map((c: any, i: number) => ({ ...c, key: i }))}
                size="small"
                pagination={false}
                columns={[
                  { title: '컬럼명', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                  { title: '타입', dataIndex: 'type', key: 'type', render: (v: string) => <Tag>{v}</Tag> },
                  { title: 'Nullable', dataIndex: 'nullable', key: 'null', render: (v: boolean) => v ? 'YES' : <Text type="danger">NO</Text> },
                ]}
              />
            </Card>
          </>
        )}
      </Drawer>
    </Spin>
  );
};

export default DataDesignDashboard;
