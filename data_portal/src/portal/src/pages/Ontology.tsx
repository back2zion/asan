/**
 * OMOP CDM 의료 온톨로지 Knowledge Graph — 3D/2D 인터랙티브 시각화 + 관리
 *
 * Causality 기반 점진적 증강 온톨로지 자동 구축 기술
 * - OMOP CDM 스키마 -> 의료 지식 그래프 자동 변환
 * - SNOMED CT / ICD-10 / LOINC / RxNorm 표준 용어 연계
 * - 치료 관계, 동반질환, 인과 체인 시각화
 * - Neo4j Cypher 내보내기 / RDF Triple 뷰어
 * - 노드 주석 관리 (CRUD)
 *
 * Thin orchestrator — components live in ../components/ontology/
 */

import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
  Card, Row, Col, Typography, Space, Button, App, Drawer, List, Input, Popconfirm, Tag, Empty,
} from 'antd';
import {
  DeploymentUnitOutlined, NodeIndexOutlined,
  DownloadOutlined, ReloadOutlined, ClearOutlined,
  EditOutlined, DeleteOutlined, PlusOutlined, CommentOutlined,
} from '@ant-design/icons';
import { fetchPost, fetchPut, fetchDelete } from '../services/apiUtils';
import {
  GraphData, OntologyNode,
  NODE_TYPE_META, CDM_DOMAIN_META,
} from '../components/ontology/types';
import { StatsCards } from '../components/ontology/GraphControls';
import GraphControlsPanel from '../components/ontology/GraphControls';
import GraphCanvas from '../components/ontology/GraphCanvas';
import NodeDetailPanel from '../components/ontology/NodeDetailDrawer';
import { TripleDrawer, CypherExportDrawer } from '../components/ontology/TripleViewer';

const { Title, Paragraph, Text } = Typography;

interface Annotation {
  annotation_id: number;
  node_id: string;
  note: string;
  author: string;
  created_at: string;
}

const Ontology: React.FC = () => {
  const { message } = App.useApp();
  const graphRef = useRef<any>(null);

  // ── State ──────────────────────────────────────
  const [loading, setLoading] = useState(true);
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [viewMode, setViewMode] = useState<string>('schema');
  const [selectedNode, setSelectedNode] = useState<OntologyNode | null>(null);
  const [neighborData, setNeighborData] = useState<any>(null);
  const [searchResults, setSearchResults] = useState<OntologyNode[]>([]);
  const [activeNodeTypes, setActiveNodeTypes] = useState<Set<string>>(new Set(Object.keys(NODE_TYPE_META)));
  const [activeDomains, setActiveDomains] = useState<Set<string>>(new Set(Object.keys(CDM_DOMAIN_META)));
  const [showLabels, setShowLabels] = useState(true);
  const [showArrows, setShowArrows] = useState(true);
  const highlightNodesRef = useRef<Set<string>>(new Set());
  const highlightLinksRef = useRef<Set<string>>(new Set());
  const pendingNavigateRef = useRef<string | null>(null);
  const [tripleDrawerOpen, setTripleDrawerOpen] = useState(false);
  const [cypherDrawerOpen, setCypherDrawerOpen] = useState(false);
  const [cypherScript, setCypherScript] = useState('');
  const [engineRunning, setEngineRunning] = useState(true);

  // Annotation state
  const [annotationDrawerOpen, setAnnotationDrawerOpen] = useState(false);
  const [annotations, setAnnotations] = useState<Annotation[]>([]);
  const [annotationLoading, setAnnotationLoading] = useState(false);
  const [newAnnotation, setNewAnnotation] = useState('');
  const [editingAnnotation, setEditingAnnotation] = useState<number | null>(null);
  const [editAnnotationText, setEditAnnotationText] = useState('');

  // Cache management
  const [cacheRefreshing, setCacheRefreshing] = useState(false);

  // ── API calls ──────────────────────────────────
  const fetchGraph = useCallback(async (type: string = 'full', forceRefresh = false) => {
    setLoading(true);
    try {
      const res = await fetch(`/api/v1/ontology/graph?graph_type=${type}&force_refresh=${forceRefresh}`);
      if (!res.ok) throw new Error('API 오류');
      setGraphData(await res.json());
      setEngineRunning(true);
    } catch (e: any) {
      message.error(`온톨로지 로드 실패: ${e.message}`);
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => { fetchGraph(viewMode); }, [viewMode]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Annotation API ─────────────────────────────
  const fetchAnnotations = useCallback(async (nodeId?: string) => {
    setAnnotationLoading(true);
    try {
      const url = nodeId
        ? `/api/v1/ontology/annotations?node_id=${encodeURIComponent(nodeId)}`
        : '/api/v1/ontology/annotations';
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        setAnnotations(data.annotations || []);
      }
    } catch { /* ignore */ }
    finally { setAnnotationLoading(false); }
  }, []);

  const handleAddAnnotation = async () => {
    if (!selectedNode || !newAnnotation.trim()) return;
    try {
      const res = await fetchPost('/api/v1/ontology/annotations', { node_id: selectedNode.id, note: newAnnotation });
      if (!res.ok) throw new Error();
      message.success('주석이 추가되었습니다');
      setNewAnnotation('');
      fetchAnnotations(selectedNode.id);
    } catch {
      message.error('주석 추가 실패');
    }
  };

  const handleUpdateAnnotation = async (id: number) => {
    try {
      const res = await fetchPut(`/api/v1/ontology/annotations/${id}`, { note: editAnnotationText });
      if (!res.ok) throw new Error();
      message.success('주석이 수정되었습니다');
      setEditingAnnotation(null);
      if (selectedNode) fetchAnnotations(selectedNode.id);
      else fetchAnnotations();
    } catch {
      message.error('주석 수정 실패');
    }
  };

  const handleDeleteAnnotation = async (id: number) => {
    try {
      const res = await fetchDelete(`/api/v1/ontology/annotations/${id}`);
      if (!res.ok) throw new Error();
      message.success('주석이 삭제되었습니다');
      if (selectedNode) fetchAnnotations(selectedNode.id);
      else fetchAnnotations();
    } catch {
      message.error('주석 삭제 실패');
    }
  };

  const handleCacheRefresh = async () => {
    setCacheRefreshing(true);
    try {
      const res = await fetchPost('/api/v1/ontology/cache-refresh');
      if (!res.ok) throw new Error();
      const data = await res.json();
      message.success(`캐시 갱신 완료 (${data.elapsed_seconds}s, ${data.nodes} nodes)`);
      fetchGraph(viewMode, true);
    } catch {
      message.error('캐시 갱신 실패');
    } finally {
      setCacheRefreshing(false);
    }
  };

  const handleCacheClear = async () => {
    try {
      const res = await fetchPost('/api/v1/ontology/cache-clear');
      if (!res.ok) throw new Error();
      message.success('캐시가 초기화되었습니다');
    } catch {
      message.error('캐시 초기화 실패');
    }
  };

  // ── Derived data ───────────────────────────────
  const filteredGraph = useMemo(() => {
    if (!graphData) return { nodes: [], links: [] };
    const activeNodes = graphData.nodes.filter(n => {
      if (!activeNodeTypes.has(n.type)) return false;
      if (viewMode === 'schema' && n.type === 'domain' && n.domain) return activeDomains.has(n.domain);
      return true;
    });
    const activeIds = new Set(activeNodes.map(n => n.id));
    const activeLinks = graphData.links.filter(
      l => activeIds.has(typeof l.source === 'string' ? l.source : (l.source as any).id)
        && activeIds.has(typeof l.target === 'string' ? l.target : (l.target as any).id)
    );
    return { nodes: activeNodes, links: activeLinks };
  }, [graphData, activeNodeTypes, viewMode, activeDomains]);

  const nodeTypeCounts = useMemo(() => {
    if (!graphData) return {};
    const counts: Record<string, number> = {};
    graphData.nodes.forEach(n => { counts[n.type] = (counts[n.type] || 0) + 1; });
    return counts;
  }, [graphData]);

  const domainCounts = useMemo(() => {
    if (!graphData) return {};
    const result: Record<string, { count: number; rows: number }> = {};
    graphData.nodes.filter(n => n.type === 'domain' && n.domain).forEach(n => {
      const d = n.domain!;
      if (!result[d]) result[d] = { count: 0, rows: 0 };
      result[d].count++;
      result[d].rows += (n.row_count || 0);
    });
    return result;
  }, [graphData]);

  // ── Handlers ───────────────────────────────────
  const handleNodeClick = useCallback(async (node: any) => {
    setSelectedNode(node);
    try {
      const res = await fetch(`/api/v1/ontology/node/${encodeURIComponent(node.id)}`);
      if (res.ok) setNeighborData(await res.json());
    } catch { /* ignore */ }
  }, []);

  const handleNodeHover = useCallback((node: any) => {
    if (!node) {
      highlightNodesRef.current = new Set();
      highlightLinksRef.current = new Set();
      return;
    }
    const neighbors = new Set<string>();
    const links = new Set<string>();
    neighbors.add(node.id);
    const fg = graphRef.current;
    if (fg) {
      const gd = fg.graphData?.() || { links: [] };
      (gd.links || []).forEach((l: any) => {
        const src = typeof l.source === 'string' ? l.source : l.source?.id;
        const tgt = typeof l.target === 'string' ? l.target : l.target?.id;
        if (src === node.id) { neighbors.add(tgt); links.add(`${src}_${tgt}`); }
        if (tgt === node.id) { neighbors.add(src); links.add(`${src}_${tgt}`); }
      });
    }
    highlightNodesRef.current = neighbors;
    highlightLinksRef.current = links;
  }, []);

  const handleSearch = useCallback(async (value: string) => {
    if (!value.trim()) {
      setSearchResults([]);
      highlightNodesRef.current = new Set();
      return;
    }
    try {
      const res = await fetch(`/api/v1/ontology/search?q=${encodeURIComponent(value)}`);
      if (res.ok) {
        const data = await res.json();
        setSearchResults(data.results || []);
        highlightNodesRef.current = new Set<string>((data.results || []).map((r: any) => r.id as string));
        if (data.results.length > 0 && graphRef.current) {
          const firstId = data.results[0].id;
          const gd = graphRef.current.graphData?.() || { nodes: [] };
          const targetNode = (gd.nodes || []).find((n: any) => n.id === firstId);
          if (targetNode && targetNode.x != null) {
            graphRef.current.centerAt(targetNode.x, targetNode.y, 800);
            graphRef.current.zoom(2.5, 800);
          }
        }
      }
    } catch { /* ignore */ }
  }, []);

  const handleNavigateToNode = useCallback((nodeId: string) => {
    if (!graphRef.current) return;
    // Use force-graph's internal graphData (has x/y positions) instead of filteredGraph
    const gd = graphRef.current.graphData?.() || { nodes: [] };
    const target = (gd.nodes || []).find((n: any) => n.id === nodeId);
    if (target && target.x != null) {
      graphRef.current.centerAt(target.x, target.y, 600);
      graphRef.current.zoom(3, 600);
      handleNodeClick(target);
    } else {
      // Node not in current view — switch to full graph and retry after load
      pendingNavigateRef.current = nodeId;
      setViewMode('full');
    }
  }, [handleNodeClick]);

  // Pending navigation: after graph loads in new view mode, navigate to target node
  useEffect(() => {
    if (!pendingNavigateRef.current || loading) return;
    const nodeId = pendingNavigateRef.current;
    const timer = setTimeout(() => {
      if (!graphRef.current) return;
      const gd = graphRef.current.graphData?.() || { nodes: [] };
      const target = (gd.nodes || []).find((n: any) => n.id === nodeId);
      if (target && target.x != null) {
        graphRef.current.centerAt(target.x, target.y, 600);
        graphRef.current.zoom(3, 600);
        handleNodeClick(target);
      }
      pendingNavigateRef.current = null;
    }, 1500);
    return () => clearTimeout(timer);
  }, [loading, graphData, handleNodeClick]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleExportCypher = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/ontology/neo4j-export');
      if (res.ok) {
        const data = await res.json();
        setCypherScript(data.cypher);
        setCypherDrawerOpen(true);
      }
    } catch {
      message.error('Export 실패');
    }
  }, [message]);

  const openAnnotationDrawer = () => {
    if (selectedNode) {
      fetchAnnotations(selectedNode.id);
    } else {
      fetchAnnotations();
    }
    setAnnotationDrawerOpen(true);
  };

  // ── Render ─────────────────────────────────────
  return (
    <div>
      {/* Header */}
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <DeploymentUnitOutlined style={{ color: '#006241', marginRight: 12, fontSize: 28 }} />
              Medical Ontology Knowledge Graph
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: 14, color: '#6c757d' }}>
              OMOP CDM 기반 온톨로지 · SNOMED CT · ICD-10 · LOINC · RxNorm
            </Paragraph>
          </Col>
          <Col>
            <Space>
              <Button icon={<CommentOutlined />} onClick={openAnnotationDrawer}>주석 관리</Button>
              <Button icon={<ClearOutlined />} onClick={handleCacheClear}>캐시 초기화</Button>
              <Button icon={<DownloadOutlined />} onClick={handleExportCypher}>Neo4j Export</Button>
              <Button icon={<NodeIndexOutlined />} onClick={() => setTripleDrawerOpen(true)}>Triples</Button>
              <Button icon={<ReloadOutlined />} onClick={handleCacheRefresh} loading={cacheRefreshing}>캐시 갱신</Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Stats */}
      {graphData?.stats && <StatsCards stats={graphData.stats} />}

      {/* Main Graph Area */}
      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} md={5} lg={4}>
          <GraphControlsPanel
            viewMode={viewMode}
            onViewModeChange={setViewMode}
            searchResults={searchResults}
            onSearch={handleSearch}
            onNavigateToNode={handleNavigateToNode}
            activeNodeTypes={activeNodeTypes}
            onToggleNodeType={(type) => setActiveNodeTypes(prev => {
              const next = new Set(prev);
              if (next.has(type)) next.delete(type); else next.add(type);
              return next;
            })}
            nodeTypeCounts={nodeTypeCounts}
            activeDomains={activeDomains}
            onToggleDomain={(domain) => setActiveDomains(prev => {
              const next = new Set(prev);
              if (next.has(domain)) next.delete(domain); else next.add(domain);
              return next;
            })}
            domainCounts={domainCounts}
            showLabels={showLabels}
            onShowLabelsChange={setShowLabels}
            showArrows={showArrows}
            onShowArrowsChange={setShowArrows}
            onZoomToFit={() => graphRef.current?.zoomToFit(400, 60)}
            onZoomIn={() => graphRef.current?.zoom(graphRef.current.zoom() * 1.5, 300)}
            onZoomOut={() => graphRef.current?.zoom(graphRef.current.zoom() / 1.5, 300)}
          />
        </Col>

        <Col xs={24} md={selectedNode ? 13 : 19} lg={selectedNode ? 14 : 20}
          style={{ transition: 'all 0.3s ease' }}
        >
          <GraphCanvas
            loading={loading}
            filteredGraph={filteredGraph}
            viewMode={viewMode}
            showLabels={showLabels}
            showArrows={showArrows}
            highlightNodesRef={highlightNodesRef}
            highlightLinksRef={highlightLinksRef}
            graphRef={graphRef}
            onNodeClick={handleNodeClick}
            onNodeHover={handleNodeHover}
            onEngineStop={() => setEngineRunning(false)}
            engineRunning={engineRunning}
          />
        </Col>

        {selectedNode && (
          <Col xs={24} md={6} lg={6}
            style={{ transition: 'all 0.3s ease' }}
          >
            <NodeDetailPanel
              node={selectedNode}
              neighbors={neighborData}
              onClose={() => { setSelectedNode(null); setNeighborData(null); }}
              onNavigate={handleNavigateToNode}
            />
          </Col>
        )}
      </Row>

      <TripleDrawer
        open={tripleDrawerOpen}
        onClose={() => setTripleDrawerOpen(false)}
        graphData={graphData}
        onNodeClick={handleNavigateToNode}
      />

      <CypherExportDrawer
        open={cypherDrawerOpen}
        onClose={() => setCypherDrawerOpen(false)}
        cypherScript={cypherScript}
        onCopy={() => {
          navigator.clipboard.writeText(cypherScript);
          message.success('Copied to clipboard');
        }}
      />

      {/* Annotation Drawer */}
      <Drawer
        title={
          selectedNode
            ? <><CommentOutlined /> 노드 주석: <Text code>{selectedNode.label}</Text></>
            : <><CommentOutlined /> 전체 주석 관리</>
        }
        placement="right"
        width={420}
        open={annotationDrawerOpen}
        onClose={() => setAnnotationDrawerOpen(false)}
      >
        {selectedNode && (
          <div style={{ marginBottom: 16 }}>
            <Space.Compact style={{ width: '100%' }}>
              <Input.TextArea
                rows={2}
                value={newAnnotation}
                onChange={e => setNewAnnotation(e.target.value)}
                placeholder="노드에 대한 주석을 입력하세요..."
              />
            </Space.Compact>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={handleAddAnnotation}
              style={{ marginTop: 8 }}
              disabled={!newAnnotation.trim()}
              block
            >
              주석 추가
            </Button>
          </div>
        )}

        <List
          loading={annotationLoading}
          dataSource={annotations}
          locale={{ emptyText: <Empty description="주석이 없습니다" /> }}
          renderItem={(item) => (
            <List.Item
              actions={[
                editingAnnotation === item.annotation_id ? (
                  <Button size="small" type="primary" onClick={() => handleUpdateAnnotation(item.annotation_id)}>저장</Button>
                ) : (
                  <Button size="small" type="text" icon={<EditOutlined />} onClick={() => {
                    setEditingAnnotation(item.annotation_id);
                    setEditAnnotationText(item.note);
                  }} />
                ),
                <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDeleteAnnotation(item.annotation_id)} okText="삭제" cancelText="취소">
                  <Button size="small" type="text" danger icon={<DeleteOutlined />} />
                </Popconfirm>,
              ]}
            >
              <List.Item.Meta
                title={
                  <Space>
                    <Tag color="blue">{item.node_id.substring(0, 30)}{item.node_id.length > 30 ? '...' : ''}</Tag>
                    <Text type="secondary" style={{ fontSize: 11 }}>{item.author}</Text>
                  </Space>
                }
                description={
                  editingAnnotation === item.annotation_id ? (
                    <Input.TextArea
                      rows={2}
                      value={editAnnotationText}
                      onChange={e => setEditAnnotationText(e.target.value)}
                    />
                  ) : (
                    <>
                      <Text style={{ fontSize: 13 }}>{item.note}</Text>
                      <br />
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {item.created_at ? new Date(item.created_at).toLocaleString('ko-KR') : ''}
                      </Text>
                    </>
                  )
                }
              />
            </List.Item>
          )}
        />
      </Drawer>
    </div>
  );
};

export default Ontology;
