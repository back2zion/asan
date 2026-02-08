/**
 * OMOP CDM 의료 온톨로지 Knowledge Graph — 3D/2D 인터랙티브 시각화
 *
 * Causality 기반 점진적 증강 온톨로지 자동 구축 기술
 * - OMOP CDM 스키마 -> 의료 지식 그래프 자동 변환
 * - SNOMED CT / ICD-10 / LOINC / RxNorm 표준 용어 연계
 * - 치료 관계, 동반질환, 인과 체인 시각화
 * - Neo4j Cypher 내보내기 / RDF Triple 뷰어
 *
 * Thin orchestrator — components live in ../components/ontology/
 */

import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { Card, Row, Col, Typography, Space, Button, App } from 'antd';
import {
  DeploymentUnitOutlined, NodeIndexOutlined,
  DownloadOutlined, ReloadOutlined,
} from '@ant-design/icons';

import {
  GraphData, OntologyNode,
  NODE_TYPE_META, CDM_DOMAIN_META,
} from '../components/ontology/types';
import { StatsCards } from '../components/ontology/GraphControls';
import GraphControlsPanel from '../components/ontology/GraphControls';
import GraphCanvas from '../components/ontology/GraphCanvas';
import NodeDetailPanel from '../components/ontology/NodeDetailDrawer';
import { TripleDrawer, CypherExportDrawer } from '../components/ontology/TripleViewer';

const { Title, Paragraph } = Typography;

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
  const [tripleDrawerOpen, setTripleDrawerOpen] = useState(false);
  const [cypherDrawerOpen, setCypherDrawerOpen] = useState(false);
  const [cypherScript, setCypherScript] = useState('');
  const [engineRunning, setEngineRunning] = useState(true);

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
    if (!graphRef.current || !filteredGraph.nodes) return;
    const target = filteredGraph.nodes.find(n => n.id === nodeId);
    if (target && (target as any).x != null) {
      graphRef.current.centerAt((target as any).x, (target as any).y, 600);
      graphRef.current.zoom(3, 600);
      handleNodeClick(target);
    }
  }, [filteredGraph.nodes, handleNodeClick]);

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

  // ── Render ─────────────────────────────────────
  return (
    <div style={{ padding: 0, minHeight: '100vh', background: '#f0f2f5' }}>
      {/* Header */}
      <Card
        style={{
          marginBottom: 16,
          background: 'linear-gradient(135deg, #1A365D 0%, #005BAC 40%, #2D3748 100%)',
          border: 'none', borderRadius: 12,
        }}
        styles={{ body: { padding: '20px 24px' } }}
      >
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#ffffff' }}>
              <DeploymentUnitOutlined style={{ marginRight: 12 }} />
              Medical Ontology Knowledge Graph
            </Title>
            <Paragraph style={{ margin: '8px 0 0 36px', color: 'rgba(255,255,255,0.75)', fontSize: 13 }}>
              OMOP CDM 기반 Causality 점진적 증강 온톨로지 &middot; SNOMED CT &middot; ICD-10 &middot; LOINC &middot; RxNorm
            </Paragraph>
          </Col>
          <Col>
            <Space>
              <Button
                icon={<DownloadOutlined />}
                onClick={handleExportCypher}
                style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', color: '#fff' }}
              >
                Neo4j Export
              </Button>
              <Button
                icon={<NodeIndexOutlined />}
                onClick={() => setTripleDrawerOpen(true)}
                style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', color: '#fff' }}
              >
                Triples
              </Button>
              <Button
                icon={<ReloadOutlined />}
                onClick={() => fetchGraph(viewMode, true)}
                loading={loading}
                style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', color: '#fff' }}
              >
                Refresh
              </Button>
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
    </div>
  );
};

export default Ontology;
