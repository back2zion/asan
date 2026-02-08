/**
 * The force-directed graph visualization area (ForceGraph2D)
 * plus the bottom-left overlay showing node/edge counts.
 */

import React, { useRef, useEffect, useState, MutableRefObject } from 'react';
import { Card, Spin, Empty, Space, Typography } from 'antd';
import ForceGraph2D from 'react-force-graph-2d';

import { OntologyNode, OntologyLink, drawNode, LINK_TYPE_COLORS, VIEW_OPTIONS } from './types';

const { Text } = Typography;

interface FilteredGraph {
  nodes: OntologyNode[];
  links: OntologyLink[];
}

interface GraphCanvasProps {
  loading: boolean;
  filteredGraph: FilteredGraph;
  viewMode: string;
  showLabels: boolean;
  showArrows: boolean;
  highlightNodesRef: MutableRefObject<Set<string>>;
  highlightLinksRef: MutableRefObject<Set<string>>;
  graphRef: MutableRefObject<any>;
  onNodeClick: (node: any) => void;
  onNodeHover: (node: any) => void;
  onEngineStop: () => void;
  engineRunning: boolean;
}

const GraphCanvas: React.FC<GraphCanvasProps> = ({
  loading,
  filteredGraph,
  viewMode,
  showLabels,
  showArrows,
  highlightNodesRef,
  highlightLinksRef,
  graphRef,
  onNodeClick,
  onNodeHover,
  onEngineStop,
  engineRunning,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [graphDimensions, setGraphDimensions] = useState({ width: 800, height: 600 });

  // Resize observer
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setGraphDimensions({
          width: entry.contentRect.width || 800,
          height: Math.max(entry.contentRect.height, 500),
        });
      }
    });
    ro.observe(container);
    return () => ro.disconnect();
  }, []);

  // Adjust d3 forces based on node count — fewer nodes need stronger repulsion
  useEffect(() => {
    const fg = graphRef.current;
    if (!fg || filteredGraph.nodes.length === 0) return;
    const n = filteredGraph.nodes.length;
    if (n < 50) {
      // Schema / small views: spread out
      fg.d3Force('charge')?.strength(-500);
      fg.d3Force('link')?.distance(140);
    } else if (n < 200) {
      // Medical / causality: moderate spacing, not too sparse
      fg.d3Force('charge')?.strength(-120);
      fg.d3Force('link')?.distance(45);
    } else {
      // Full view: tight clustering
      fg.d3Force('charge')?.strength(-60);
      fg.d3Force('link')?.distance(35);
    }
    fg.d3ReheatSimulation();
  }, [filteredGraph.nodes.length, viewMode]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <Card
      size="small"
      style={{ borderRadius: 10, overflow: 'hidden', minHeight: 650 }}
      styles={{ body: { padding: 0 } }}
    >
      <div ref={containerRef} style={{ width: '100%', height: 650, position: 'relative', background: '#f8f9fa' }}>
        {loading ? (
          <Spin size="large" tip="Building Knowledge Graph..." fullscreen={false}>
            <div style={{ width: '100%', height: 650 }} />
          </Spin>
        ) : filteredGraph.nodes.length === 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
            <Empty description={<Text style={{ color: '#999' }}>No data</Text>} />
          </div>
        ) : (
          <ForceGraph2D
            ref={graphRef}
            graphData={filteredGraph}
            width={graphDimensions.width}
            height={650}
            backgroundColor="#f8f9fa"
            // Node rendering
            nodeCanvasObject={showLabels ? drawNode : undefined}
            nodeRelSize={6}
            nodeColor={(node: any) => {
              if (highlightNodesRef.current.size > 0 && !highlightNodesRef.current.has(node.id)) return 'rgba(180,180,180,0.3)';
              return node.color || '#718096';
            }}
            nodeVal={(node: any) => node.size || 6}
            // Link rendering
            linkColor={(link: any) => {
              const src = typeof link.source === 'string' ? link.source : link.source?.id;
              const tgt = typeof link.target === 'string' ? link.target : link.target?.id;
              if (highlightLinksRef.current.size > 0 && !highlightLinksRef.current.has(`${src}_${tgt}`)) return 'rgba(200,200,200,0.2)';
              return LINK_TYPE_COLORS[link.type] || 'rgba(160,174,192,0.5)';
            }}
            linkWidth={(link: any) => {
              const src = typeof link.source === 'string' ? link.source : link.source?.id;
              const tgt = typeof link.target === 'string' ? link.target : link.target?.id;
              if (highlightLinksRef.current.size > 0 && highlightLinksRef.current.has(`${src}_${tgt}`)) return 2.5;
              return link.type === 'treatment' || link.type === 'comorbidity' ? 1.5 : 0.5;
            }}
            linkDirectionalArrowLength={showArrows ? 4 : 0}
            linkDirectionalArrowRelPos={1}
            linkCurvature={0.1}
            linkLabel={(link: any) => `${link.label || ''}\n${link.description || ''}`}
            // Interaction
            onNodeClick={onNodeClick}
            onNodeHover={onNodeHover}
            onNodeDrag={() => {
              graphRef.current?.d3ReheatSimulation();
            }}
            onBackgroundClick={() => {
              highlightNodesRef.current = new Set();
              highlightLinksRef.current = new Set();
            }}
            // Physics
            warmupTicks={80}
            cooldownTicks={200}
            d3AlphaDecay={0.05}
            d3VelocityDecay={0.3}
            onEngineStop={() => {
              graphRef.current?.zoomToFit(400, 60);
              onEngineStop();
            }}
            // Zoom
            enableZoomInteraction={true}
            enablePanInteraction={true}
            minZoom={0.2}
            maxZoom={12}
          />
        )}

        {/* Overlay info */}
        {!loading && filteredGraph.nodes.length > 0 && (
          <div style={{
            position: 'absolute', bottom: 12, left: 12,
            background: 'rgba(255,255,255,0.9)', borderRadius: 8, padding: '8px 14px',
            color: '#333', fontSize: 11, border: '1px solid #e0e0e0',
            boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
          }}>
            <Space split={<span style={{ color: '#ccc' }}>|</span>}>
              <span>{filteredGraph.nodes.length} nodes</span>
              <span>{filteredGraph.links.length} edges</span>
              <span>View: {VIEW_OPTIONS.find(v => v.value === viewMode)?.label}</span>
              {viewMode === 'schema' && <span style={{ fontWeight: 600 }}>OMOP CDM v5.4</span>}
              <span>{engineRunning ? '⏳ 배치 중...' : '✓ 안정화'}</span>
            </Space>
          </div>
        )}
      </div>
    </Card>
  );
};

export default GraphCanvas;
