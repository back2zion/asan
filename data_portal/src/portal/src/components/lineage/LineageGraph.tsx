/**
 * 데이터 계보 시각화 컴포넌트
 * DPR-002: Data Lineage 시각화
 * API 연동으로 OMOP 데이터 기반 리니지 표시
 */

import React, { useEffect, useCallback } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  BackgroundVariant,
  MarkerType,
  Handle,
  Position,
  ReactFlowProvider,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Spin, Empty } from 'antd';
import { useQuery } from '@tanstack/react-query';
import {
  DatabaseOutlined,
  BranchesOutlined,
  TableOutlined,
  CloudServerOutlined,
  FilterOutlined,
} from '@ant-design/icons';
import { semanticApi } from '../../services/api';

// 아산병원 브랜드 컬러
const BRAND = {
  BLUE: '#005BAC',
  SKY: '#D0E1F9',
  NAVY: '#182C4E',
  GRAY: '#F8F9FA',
};

// 노드 타입별 아이콘 및 스타일
const nodeConfig: Record<string, { icon: React.ReactNode; color: string; bgColor: string }> = {
  source: {
    icon: <CloudServerOutlined />,
    color: '#1890ff',
    bgColor: '#e6f7ff'
  },
  bronze: {
    icon: <DatabaseOutlined />,
    color: '#CD7F32',
    bgColor: '#fff7e6'
  },
  silver: {
    icon: <FilterOutlined />,
    color: '#C0C0C0',
    bgColor: '#f5f5f5'
  },
  gold: {
    icon: <TableOutlined />,
    color: '#FFD700',
    bgColor: '#fffbe6'
  },
  process: {
    icon: <BranchesOutlined />,
    color: '#52c41a',
    bgColor: '#f6ffed'
  },
};

// 커스텀 노드 컴포넌트
const LineageNode = ({ data, selected }: any) => {
  const config = nodeConfig[data.nodeType] || nodeConfig.source;

  return (
    <div
      style={{
        background: config.bgColor,
        borderRadius: '10px',
        border: `2px solid ${selected ? BRAND.BLUE : config.color}`,
        padding: '16px 20px',
        minWidth: '180px',
        maxWidth: '220px',
        boxShadow: selected
          ? `0 0 16px ${BRAND.BLUE}40`
          : '0 4px 12px rgba(0,0,0,0.12)',
        transition: 'all 0.2s ease',
        cursor: 'pointer',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: config.color, width: 8, height: 8 }}
      />

      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
        <span style={{ color: config.color, fontSize: 20 }}>{config.icon}</span>
        <span style={{
          fontSize: 11,
          fontWeight: 700,
          color: '#666',
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
        }}>
          {data.layer}
        </span>
      </div>

      <div style={{
        fontSize: 14,
        fontWeight: 700,
        color: '#333',
        marginBottom: 6,
        wordBreak: 'break-all',
      }}>
        {data.label}
      </div>

      {data.description && (
        <div style={{ fontSize: 12, color: '#666' }}>
          {data.description}
        </div>
      )}

      <Handle
        type="source"
        position={Position.Right}
        style={{ background: config.color, width: 8, height: 8 }}
      />
    </div>
  );
};

const nodeTypes = { lineageNode: LineageNode };

// 노드 클릭 시 전달되는 세부 정보
export interface LineageNodeDetail {
  id: string;
  label: string;
  layer: string;
  nodeType: string;
  description: string;
  metadata?: Record<string, any>;
}

interface LineageGraphProps {
  tableName: string;
  physicalName: string;
  height?: number | string;
  onNodeClick?: (node: LineageNodeDetail | null, lineageMeta: any) => void;
}

// 내부 Flow 컴포넌트
const LineageFlow: React.FC<{
  lineageData: any;
  onNodeClick?: (node: LineageNodeDetail | null, lineageMeta: any) => void;
}> = ({ lineageData, onNodeClick }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!lineageData?.lineage?.nodes) return;

    const apiNodes = lineageData.lineage.nodes;
    const apiEdges = lineageData.lineage.edges;

    const flowNodes: Node[] = apiNodes.map((n: any, idx: number) => ({
      id: n.id,
      type: 'lineageNode',
      position: { x: idx * 260, y: 100 },
      data: {
        label: n.label,
        layer: n.layer,
        nodeType: n.type,
        description: n.description,
        metadata: n.metadata,
      },
    }));

    const flowEdges: Edge[] = apiEdges.map((e: any) => ({
      id: `${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
      animated: true,
      label: e.label,
      style: { stroke: '#888', strokeWidth: 2 },
      markerEnd: { type: MarkerType.ArrowClosed, color: '#888' },
    }));

    setNodes(flowNodes);
    setEdges(flowEdges);
  }, [lineageData, setNodes, setEdges]);

  const handleNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    if (!onNodeClick) return;
    const detail: LineageNodeDetail = {
      id: node.id,
      label: node.data.label,
      layer: node.data.layer,
      nodeType: node.data.nodeType,
      description: node.data.description,
      metadata: node.data.metadata,
    };
    onNodeClick(detail, lineageData?.lineage);
  }, [onNodeClick, lineageData]);

  const handlePaneClick = useCallback(() => {
    onNodeClick?.(null, lineageData?.lineage);
  }, [onNodeClick, lineageData]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onNodeClick={handleNodeClick}
      onPaneClick={handlePaneClick}
      fitView
      fitViewOptions={{ padding: 0.3, minZoom: 0.4, maxZoom: 1.5 }}
      nodesDraggable={true}
      nodesConnectable={false}
      elementsSelectable={true}
      zoomOnScroll={true}
      panOnScroll={true}
      minZoom={0.2}
      maxZoom={2}
    >
      <Background
        color="#d1d5db"
        gap={16}
        size={1}
        variant={BackgroundVariant.Dots}
      />
      <Controls position="bottom-right" showInteractive={false} />
      <MiniMap
        style={{ height: 60, width: 90 }}
        position="bottom-left"
        nodeColor={(node) => {
          const config = nodeConfig[node.data?.nodeType] || nodeConfig.source;
          return config.color;
        }}
      />
    </ReactFlow>
  );
};

const LineageGraph: React.FC<LineageGraphProps> = ({ tableName, physicalName, height = 500, onNodeClick }) => {
  const { data: lineageData, isLoading, isError } = useQuery({
    queryKey: ['lineage', physicalName],
    queryFn: () => semanticApi.getLineage(physicalName),
    enabled: !!physicalName,
    staleTime: 5 * 60 * 1000,
  });

  if (isLoading) {
    return (
      <div style={{
        width: '100%',
        height,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        background: '#fafafa',
        borderRadius: 8,
        border: '1px solid #e5e7eb',
        gap: 12,
      }}>
        <Spin />
        <span style={{ color: '#888', fontSize: 13 }}>리니지 데이터 로딩 중...</span>
      </div>
    );
  }

  if (isError || !lineageData?.lineage?.nodes) {
    return (
      <div style={{
        width: '100%',
        height,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: '#fafafa',
        borderRadius: 8,
        border: '1px solid #e5e7eb',
      }}>
        <Empty description="리니지 데이터를 불러올 수 없습니다" />
      </div>
    );
  }

  return (
    <ReactFlowProvider>
      <div style={{
        width: '100%',
        height,
        border: '1px solid #e5e7eb',
        borderRadius: 8,
        background: '#fafafa',
        position: 'relative',
      }}>
        <LineageFlow lineageData={lineageData} onNodeClick={onNodeClick} />
      </div>
    </ReactFlowProvider>
  );
};

export default LineageGraph;
