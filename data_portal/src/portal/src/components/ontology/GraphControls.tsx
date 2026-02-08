/**
 * Left sidebar controls for the Ontology Knowledge Graph:
 * - StatsCards: summary statistics row
 * - GraphLegend: node-type filter/legend
 * - SchemaLegend: CDM 5.4 domain-group filter/legend
 * - GraphControlsPanel: view mode, search, legends, zoom buttons
 */

import React from 'react';
import {
  Card, Row, Col, Typography, Statistic, Tag, Space, Button, Input,
  Tooltip, Badge, Divider, Radio, Switch,
} from 'antd';
import {
  DeploymentUnitOutlined, NodeIndexOutlined, BranchesOutlined,
  MedicineBoxOutlined, FundOutlined,
  ZoomInOutlined, ZoomOutOutlined, FullscreenOutlined,
} from '@ant-design/icons';

import { OntologyNode, NODE_TYPE_META, CDM_DOMAIN_META, VIEW_OPTIONS } from './types';

const { Text } = Typography;
const { Search } = Input;

// ═══════════════════════════════════════════════════
//  StatsCards
// ═══════════════════════════════════════════════════

export const StatsCards: React.FC<{ stats: any }> = ({ stats }) => {
  if (!stats) return null;
  const items = [
    { title: 'Nodes', value: stats.total_nodes || 0, color: '#005BAC', icon: <DeploymentUnitOutlined /> },
    { title: 'Edges', value: stats.total_links || 0, color: '#38A169', icon: <BranchesOutlined /> },
    { title: 'Triples', value: stats.total_triples || 0, color: '#805AD5', icon: <NodeIndexOutlined /> },
    { title: 'Patients', value: stats.total_patients || stats.demographics?.total_patients || 0, color: '#E53E3E', icon: <MedicineBoxOutlined /> },
    { title: 'Records', value: stats.total_records || 0, color: '#DD6B20', icon: <FundOutlined /> },
  ];

  return (
    <Row gutter={[12, 12]}>
      {items.map((item) => (
        <Col key={item.title} xs={12} sm={8} md={4} lg={4}>
          <Card size="small" style={{ borderLeft: `3px solid ${item.color}`, borderRadius: 8 }}>
            <Statistic
              title={<Text style={{ fontSize: 11, color: '#8c8c8c' }}>{item.title}</Text>}
              value={item.value}
              prefix={item.icon}
              valueStyle={{ fontSize: 18, fontWeight: 700, color: item.color }}
            />
          </Card>
        </Col>
      ))}
    </Row>
  );
};

// ═══════════════════════════════════════════════════
//  GraphLegend — node-type toggle filter
// ═══════════════════════════════════════════════════

export const GraphLegend: React.FC<{
  activeTypes: Set<string>;
  onToggleType: (type: string) => void;
  nodeTypeCounts: Record<string, number>;
}> = ({ activeTypes, onToggleType, nodeTypeCounts }) => {
  const totalTypes = Object.entries(NODE_TYPE_META).filter(([k]) => (nodeTypeCounts[k] || 0) > 0);
  const allActive = totalTypes.every(([k]) => activeTypes.has(k));
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <Text strong style={{ fontSize: 13 }}>Node Types</Text>
        {!allActive && (
          <Text
            style={{ fontSize: 10, color: '#1677ff', cursor: 'pointer' }}
            onClick={() => totalTypes.forEach(([k]) => { if (!activeTypes.has(k)) onToggleType(k); })}
          >전체 선택</Text>
        )}
      </div>
      <div style={{ marginTop: 4 }}>
        {totalTypes.map(([key, meta]) => {
          const isActive = activeTypes.has(key);
          return (
            <div
              key={key}
              onClick={() => onToggleType(key)}
              style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '4px 8px', marginBottom: 2, cursor: 'pointer',
                borderRadius: 6, transition: 'all 0.2s',
                backgroundColor: isActive ? `${meta.color}18` : 'transparent',
                border: isActive ? `1.5px solid ${meta.color}` : '1.5px solid transparent',
                opacity: isActive ? 1 : 0.4,
              }}
            >
              <div style={{
                width: 14, height: 14, borderRadius: '50%', backgroundColor: meta.color,
                border: isActive ? '2px solid #fff' : '1px solid #ccc',
                boxShadow: isActive ? `0 0 0 1.5px ${meta.color}` : 'none',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 8, color: '#fff', flexShrink: 0,
              }}>
                {isActive && '✓'}
              </div>
              <Text style={{ fontSize: 11, fontWeight: isActive ? 600 : 400, flex: 1 }}>{meta.label}</Text>
              <Badge count={nodeTypeCounts[key] || 0} style={{ fontSize: 9, backgroundColor: isActive ? meta.color : '#bbb' }} />
            </div>
          );
        })}
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════
//  SchemaLegend — CDM 5.4 domain-group toggle
// ═══════════════════════════════════════════════════

export const SchemaLegend: React.FC<{
  activeDomains: Set<string>;
  onToggleDomain: (domain: string) => void;
  domainCounts: Record<string, { count: number; rows: number }>;
}> = ({ activeDomains, onToggleDomain, domainCounts }) => {
  const allDomains = Object.entries(CDM_DOMAIN_META).filter(([k]) => domainCounts[k]);
  const allActive = allDomains.every(([k]) => activeDomains.has(k));
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <Text strong style={{ fontSize: 13 }}>CDM 5.4 Domain</Text>
        {!allActive && (
          <Text
            style={{ fontSize: 10, color: '#1677ff', cursor: 'pointer' }}
            onClick={() => allDomains.forEach(([k]) => { if (!activeDomains.has(k)) onToggleDomain(k); })}
          >전체 선택</Text>
        )}
      </div>
      <div>
        {allDomains.map(([key, meta]) => {
          const isActive = activeDomains.has(key);
          const info = domainCounts[key] || { count: 0, rows: 0 };
          return (
            <div
              key={key}
              onClick={() => onToggleDomain(key)}
              style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '5px 8px', marginBottom: 3, cursor: 'pointer',
                borderRadius: 6, transition: 'all 0.2s',
                backgroundColor: isActive ? `${meta.color}18` : 'transparent',
                border: isActive ? `1.5px solid ${meta.color}` : '1.5px solid transparent',
                opacity: isActive ? 1 : 0.35,
              }}
            >
              <div style={{
                width: 16, height: 16, borderRadius: 4, backgroundColor: meta.color,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 9, color: '#fff', flexShrink: 0,
                border: isActive ? '2px solid #fff' : 'none',
                boxShadow: isActive ? `0 0 0 1.5px ${meta.color}` : 'none',
              }}>
                {isActive && '✓'}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <Text style={{ fontSize: 11, fontWeight: isActive ? 600 : 400, display: 'block', lineHeight: '16px' }}>
                  {meta.label}
                </Text>
                <Text style={{ fontSize: 9, color: '#888', display: 'block', lineHeight: '12px' }}>
                  {info.count}개 테이블 · {info.rows > 1000000 ? `${(info.rows / 1000000).toFixed(1)}M` : info.rows > 1000 ? `${(info.rows / 1000).toFixed(0)}K` : info.rows} rows
                </Text>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════
//  GraphControlsPanel — the full left sidebar card
// ═══════════════════════════════════════════════════

interface GraphControlsPanelProps {
  viewMode: string;
  onViewModeChange: (mode: string) => void;
  searchResults: OntologyNode[];
  onSearch: (value: string) => void;
  onNavigateToNode: (nodeId: string) => void;
  activeNodeTypes: Set<string>;
  onToggleNodeType: (type: string) => void;
  nodeTypeCounts: Record<string, number>;
  activeDomains: Set<string>;
  onToggleDomain: (domain: string) => void;
  domainCounts: Record<string, { count: number; rows: number }>;
  showLabels: boolean;
  onShowLabelsChange: (v: boolean) => void;
  showArrows: boolean;
  onShowArrowsChange: (v: boolean) => void;
  onZoomToFit: () => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
}

const GraphControlsPanel: React.FC<GraphControlsPanelProps> = ({
  viewMode, onViewModeChange,
  searchResults, onSearch, onNavigateToNode,
  activeNodeTypes, onToggleNodeType, nodeTypeCounts,
  activeDomains, onToggleDomain, domainCounts,
  showLabels, onShowLabelsChange,
  showArrows, onShowArrowsChange,
  onZoomToFit, onZoomIn, onZoomOut,
}) => {
  return (
    <Card size="small" style={{ borderRadius: 10 }} styles={{ body: { padding: 12 } }}>
      {/* View Mode */}
      <Text strong style={{ fontSize: 13 }}>View Mode</Text>
      <Radio.Group
        value={viewMode}
        onChange={(e) => onViewModeChange(e.target.value)}
        style={{ display: 'flex', flexDirection: 'column', gap: 4, marginTop: 8, marginBottom: 16 }}
        size="small"
      >
        {VIEW_OPTIONS.map(opt => (
          <Radio.Button
            key={opt.value}
            value={opt.value}
            style={{ textAlign: 'left', fontSize: 12, height: 30, lineHeight: '28px' }}
          >
            <Space size={4}>{opt.icon}{opt.label}</Space>
          </Radio.Button>
        ))}
      </Radio.Group>

      <Divider style={{ margin: '12px 0' }} />

      {/* Search */}
      <Search
        placeholder="노드 검색..."
        size="small"
        onSearch={onSearch}
        onChange={(e) => { if (!e.target.value) onSearch(''); }}
        allowClear
        style={{ marginBottom: 12 }}
      />

      {searchResults.length > 0 && (
        <div style={{ maxHeight: 150, overflowY: 'auto', marginBottom: 12 }}>
          {searchResults.slice(0, 8).map(r => (
            <div
              key={r.id}
              onClick={() => onNavigateToNode(r.id)}
              style={{
                padding: '4px 8px', cursor: 'pointer', fontSize: 11,
                borderRadius: 4, display: 'flex', alignItems: 'center', gap: 6,
              }}
              onMouseEnter={(e) => { (e.currentTarget as HTMLDivElement).style.background = '#f0f0f0'; }}
              onMouseLeave={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'transparent'; }}
            >
              <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: NODE_TYPE_META[r.type]?.color || '#718096' }} />
              <Text ellipsis style={{ fontSize: 11, maxWidth: 150 }}>{r.label}</Text>
            </div>
          ))}
        </div>
      )}

      <Divider style={{ margin: '12px 0' }} />

      {/* Legend / Node Type Filter */}
      {viewMode === 'schema' ? (
        <SchemaLegend
          activeDomains={activeDomains}
          onToggleDomain={onToggleDomain}
          domainCounts={domainCounts}
        />
      ) : (
        <GraphLegend
          activeTypes={activeNodeTypes}
          onToggleType={onToggleNodeType}
          nodeTypeCounts={nodeTypeCounts}
        />
      )}

      <Divider style={{ margin: '12px 0' }} />

      {/* Graph Controls */}
      <Text strong style={{ fontSize: 13 }}>Controls</Text>
      <div style={{ marginTop: 8 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
          <Text style={{ fontSize: 11 }}>Labels</Text>
          <Switch size="small" checked={showLabels} onChange={onShowLabelsChange} />
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
          <Text style={{ fontSize: 11 }}>Arrows</Text>
          <Switch size="small" checked={showArrows} onChange={onShowArrowsChange} />
        </div>
        <Space style={{ marginTop: 8 }}>
          <Tooltip title="Zoom to Fit">
            <Button size="small" icon={<FullscreenOutlined />} onClick={onZoomToFit} />
          </Tooltip>
          <Tooltip title="Zoom In">
            <Button size="small" icon={<ZoomInOutlined />} onClick={onZoomIn} />
          </Tooltip>
          <Tooltip title="Zoom Out">
            <Button size="small" icon={<ZoomOutOutlined />} onClick={onZoomOut} />
          </Tooltip>
        </Space>
      </div>
    </Card>
  );
};

export default GraphControlsPanel;
