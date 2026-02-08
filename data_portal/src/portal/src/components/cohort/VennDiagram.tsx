import React from 'react';

interface VennDiagramProps {
  countA: number;
  countB: number;
  countOverlap: number;
  labelA: string;
  labelB: string;
}

const VennDiagram: React.FC<VennDiagramProps> = ({ countA, countB, countOverlap, labelA, labelB }) => {
  const aOnly = countA - countOverlap;
  const bOnly = countB - countOverlap;

  return (
    <svg width={400} height={300} viewBox="0 0 400 300">
      {/* Circle A */}
      <circle cx={155} cy={150} r={100} fill="#1890ff" fillOpacity={0.3} stroke="#1890ff" strokeWidth={2} />
      {/* Circle B */}
      <circle cx={245} cy={150} r={100} fill="#fa8c16" fillOpacity={0.3} stroke="#fa8c16" strokeWidth={2} />

      {/* Labels */}
      <text x={100} y={30} textAnchor="middle" fontSize={14} fontWeight="bold" fill="#1890ff">{labelA}</text>
      <text x={300} y={30} textAnchor="middle" fontSize={14} fontWeight="bold" fill="#fa8c16">{labelB}</text>

      {/* A only */}
      <text x={110} y={155} textAnchor="middle" fontSize={18} fontWeight="bold" fill="#1890ff">
        {aOnly.toLocaleString()}
      </text>

      {/* Overlap */}
      <text x={200} y={155} textAnchor="middle" fontSize={18} fontWeight="bold" fill="#333">
        {countOverlap.toLocaleString()}
      </text>

      {/* B only */}
      <text x={290} y={155} textAnchor="middle" fontSize={18} fontWeight="bold" fill="#fa8c16">
        {bOnly.toLocaleString()}
      </text>

      {/* Totals below */}
      <text x={110} y={180} textAnchor="middle" fontSize={11} fill="#666">A only</text>
      <text x={200} y={180} textAnchor="middle" fontSize={11} fill="#666">교집합</text>
      <text x={290} y={180} textAnchor="middle" fontSize={11} fill="#666">B only</text>
    </svg>
  );
};

export default VennDiagram;
