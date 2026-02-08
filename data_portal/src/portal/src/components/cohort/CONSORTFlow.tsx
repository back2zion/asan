import React, { useMemo, useCallback } from 'react';
import ReactFlow, { Background, Controls, Node, Edge } from 'reactflow';
import 'reactflow/dist/style.css';

interface FlowStepResult {
  step_type: string;
  label: string;
  remaining_count: number;
  excluded_count: number;
}

interface CONSORTFlowProps {
  totalPopulation: number;
  steps: FlowStepResult[];
  onNodeClick?: (stepIndex: number) => void;
}

const nodeStyle = (bg: string): React.CSSProperties => ({
  padding: '12px 20px',
  borderRadius: 8,
  border: `2px solid ${bg}`,
  background: '#fff',
  textAlign: 'center',
  fontSize: 13,
  fontWeight: 500,
  cursor: 'pointer',
  minWidth: 200,
});

const CONSORTFlow: React.FC<CONSORTFlowProps> = ({ totalPopulation, steps, onNodeClick }) => {
  const { nodes, edges } = useMemo(() => {
    const ns: Node[] = [];
    const es: Edge[] = [];
    const X_MAIN = 50;
    const X_SIDE = 330;
    const Y_GAP = 140;

    // Top node: total population
    ns.push({
      id: 'total',
      position: { x: X_MAIN, y: 0 },
      data: { label: `전체 모수: ${totalPopulation.toLocaleString()}명` },
      style: nodeStyle('#52c41a'),
    });

    let prevId = 'total';
    steps.forEach((step, idx) => {
      const stepId = `step-${idx}`;
      const y = (idx + 1) * Y_GAP;
      const color = step.step_type === 'inclusion' ? '#1890ff' : '#fa8c16';

      // Main node
      ns.push({
        id: stepId,
        position: { x: X_MAIN, y },
        data: {
          label: `${step.label}: ${step.remaining_count.toLocaleString()}명`,
        },
        style: nodeStyle(color),
      });

      // Edge from prev
      es.push({
        id: `e-${prevId}-${stepId}`,
        source: prevId,
        target: stepId,
        animated: true,
        style: { stroke: color, strokeWidth: 2 },
      });

      // Side node: excluded count
      if (step.excluded_count > 0) {
        const sideId = `side-${idx}`;
        ns.push({
          id: sideId,
          position: { x: X_SIDE, y },
          data: { label: `탈락: ${step.excluded_count.toLocaleString()}명` },
          style: { ...nodeStyle('#ff4d4f'), minWidth: 150 },
        });
        es.push({
          id: `e-${stepId}-${sideId}`,
          source: stepId,
          target: sideId,
          style: { stroke: '#ff4d4f', strokeWidth: 1.5, strokeDasharray: '5 3' },
        });
      }

      prevId = stepId;
    });

    return { nodes: ns, edges: es };
  }, [totalPopulation, steps]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (!onNodeClick) return;
      const match = node.id.match(/^step-(\d+)$/);
      if (match) {
        onNodeClick(parseInt(match[1], 10));
      }
    },
    [onNodeClick],
  );

  return (
    <div style={{ width: '100%', height: 500 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodeClick={handleNodeClick}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#f0f0f0" gap={20} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
};

export default CONSORTFlow;
