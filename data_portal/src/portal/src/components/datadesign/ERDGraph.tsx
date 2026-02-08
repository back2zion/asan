/**
 * DataDesign - ERD Canvas Graph Component
 * Canvas-based entity-relationship diagram with zone-grouped layout
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Card, Typography, Empty } from 'antd';
import { NodeIndexOutlined } from '@ant-design/icons';
import { ERDNode, ERDEdge, ZONE_COLORS } from './types';

const { Text } = Typography;

interface ERDGraphProps {
  erdNodes: ERDNode[];
  erdEdges: ERDEdge[];
}

const ERDGraph: React.FC<ERDGraphProps> = ({ erdNodes, erdEdges }) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);

  // Layout: compute positions in a force-directed-like grid
  const computeLayout = useCallback(() => {
    if (erdNodes.length === 0) return { nodes: [], edges: erdEdges };

    // Group by zone
    const zoneOrder = ['source', 'bronze', 'silver', 'gold', 'mart'];
    const grouped: Record<string, ERDNode[]> = {};
    erdNodes.forEach(n => {
      const z = n.data.zone || 'silver';
      if (!grouped[z]) grouped[z] = [];
      grouped[z].push(n);
    });

    const nodePositions: Record<string, { x: number; y: number; node: ERDNode }> = {};
    const xBase: Record<string, number> = {};
    zoneOrder.forEach((z, i) => { xBase[z] = 80 + i * 220; });

    zoneOrder.forEach(z => {
      const nodes = grouped[z] || [];
      nodes.forEach((n, i) => {
        nodePositions[n.id] = {
          x: xBase[z],
          y: 60 + i * 60,
          node: n,
        };
      });
    });

    return { nodePositions, edges: erdEdges };
  }, [erdNodes, erdEdges]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);

    const { nodePositions, edges } = computeLayout();
    if (!nodePositions) return;

    // Clear
    ctx.clearRect(0, 0, rect.width, rect.height);

    // Draw zone backgrounds
    const zoneOrder = ['source', 'bronze', 'silver', 'gold', 'mart'];
    const zoneLabels: Record<string, string> = { source: 'Source (ODS)', bronze: 'Bronze (ODS)', silver: 'Silver (DW)', gold: 'Gold (DW)', mart: 'Mart (DM)' };
    zoneOrder.forEach((z, i) => {
      const x = 40 + i * 220;
      ctx.fillStyle = (ZONE_COLORS[z] || '#ccc') + '08';
      ctx.strokeStyle = (ZONE_COLORS[z] || '#ccc') + '30';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.roundRect(x, 10, 200, rect.height - 20, 8);
      ctx.fill();
      ctx.stroke();
      // Zone label
      ctx.fillStyle = ZONE_COLORS[z] || '#666';
      ctx.font = 'bold 11px "Noto Sans KR", sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(zoneLabels[z] || z, x + 100, 30);
    });

    // Draw edges
    edges.forEach(e => {
      const src = nodePositions[e.source];
      const tgt = nodePositions[e.target];
      if (!src || !tgt) return;

      ctx.strokeStyle = e.style?.stroke || '#ccc';
      ctx.lineWidth = 1.5;
      if (e.animated || e.style?.strokeDasharray !== 'none') {
        ctx.setLineDash([4, 4]);
      } else {
        ctx.setLineDash([]);
      }
      ctx.beginPath();
      // Bezier curve
      const sx = src.x + 80;
      const sy = src.y + 12;
      const tx = tgt.x;
      const ty = tgt.y + 12;
      const cx = (sx + tx) / 2;
      ctx.moveTo(sx, sy);
      ctx.bezierCurveTo(cx, sy, cx, ty, tx, ty);
      ctx.stroke();
      ctx.setLineDash([]);

      // Arrow head
      const angle = Math.atan2(ty - sy, tx - (cx));
      ctx.fillStyle = e.style?.stroke || '#ccc';
      ctx.beginPath();
      ctx.moveTo(tx, ty);
      ctx.lineTo(tx - 6 * Math.cos(angle - 0.4), ty - 6 * Math.sin(angle - 0.4));
      ctx.lineTo(tx - 6 * Math.cos(angle + 0.4), ty - 6 * Math.sin(angle + 0.4));
      ctx.closePath();
      ctx.fill();
    });

    // Draw nodes
    Object.entries(nodePositions).forEach(([id, { x, y, node }]) => {
      const isHovered = hoveredNode === id;
      const w = 160;
      const h = 24;

      // Node background
      ctx.fillStyle = isHovered ? (node.data.color + '30') : '#ffffff';
      ctx.strokeStyle = node.data.color || '#005BAC';
      ctx.lineWidth = isHovered ? 2 : 1;
      ctx.beginPath();
      ctx.roundRect(x, y, w, h, 4);
      ctx.fill();
      ctx.stroke();

      // Entity name
      ctx.fillStyle = '#333';
      ctx.font = `${isHovered ? 'bold ' : ''}11px "Noto Sans KR", monospace`;
      ctx.textAlign = 'left';
      const label = node.data.label.length > 22 ? node.data.label.substring(0, 22) + '...' : node.data.label;
      ctx.fillText(label, x + 6, y + 16);

      // Row count badge
      if (node.data.rowCount > 0) {
        const rc = node.data.rowCount >= 1000000 ? `${(node.data.rowCount / 1000000).toFixed(1)}M` : node.data.rowCount >= 1000 ? `${(node.data.rowCount / 1000).toFixed(0)}K` : `${node.data.rowCount}`;
        ctx.fillStyle = node.data.color + '20';
        const tw = ctx.measureText(rc).width + 8;
        ctx.beginPath();
        ctx.roundRect(x + w - tw - 4, y + 4, tw, 16, 3);
        ctx.fill();
        ctx.fillStyle = node.data.color;
        ctx.font = '9px monospace';
        ctx.textAlign = 'right';
        ctx.fillText(rc, x + w - 8, y + 15);
      }
    });

  }, [erdNodes, erdEdges, hoveredNode, computeLayout]);

  // Mouse hover
  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const { nodePositions } = computeLayout();
    if (!nodePositions) return;

    let found: string | null = null;
    for (const [id, { x, y }] of Object.entries(nodePositions)) {
      if (mx >= x && mx <= x + 160 && my >= y && my <= y + 24) {
        found = id;
        break;
      }
    }
    setHoveredNode(found);
    canvas.style.cursor = found ? 'pointer' : 'default';
  };

  const maxY = erdNodes.length > 0 ? Math.max(300, 60 + Math.max(...Object.values(
    erdNodes.reduce((acc, n) => {
      const z = n.data.zone || 'silver';
      acc[z] = (acc[z] || 0) + 1;
      return acc;
    }, {} as Record<string, number>)
  )) * 60 + 40) : 400;

  return (
    <Card
      title={<><NodeIndexOutlined /> ERD 관계 그래프</>}
      size="small"
      style={{ marginBottom: 16 }}
      extra={<Text type="secondary" style={{ fontSize: 11 }}>Zone별 엔티티 배치 + 관계 화살표 (ETL 관계: 점선 빨강)</Text>}
    >
      {erdNodes.length === 0 ? (
        <Empty description="ERD 데이터를 로딩 중..." />
      ) : (
        <canvas
          ref={canvasRef}
          onMouseMove={handleMouseMove}
          style={{ width: '100%', height: maxY, display: 'block', borderRadius: 8, background: '#fafbfc' }}
        />
      )}
    </Card>
  );
};

export default ERDGraph;
