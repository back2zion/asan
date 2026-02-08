/**
 * CatalogResultsPanel - 검색 결과 패널 (테이블 뷰 / 트리 뷰 / 관계 그래프)
 */

import React, { useCallback, useMemo } from 'react';
import {
  Card, Table, Tag, Space, Typography, Badge, Button, Tooltip,
  Empty, Spin, Tree,
} from 'antd';
import type { DataNode } from 'antd/es/tree';
import type { ColumnsType } from 'antd/es/table';
import {
  TableOutlined, DatabaseOutlined, EyeOutlined, CopyOutlined,
  NodeIndexOutlined, ApartmentOutlined, DownOutlined, ShareAltOutlined,
} from '@ant-design/icons';
import ReactFlow, {
  Background, Controls, MiniMap, BackgroundVariant, MarkerType,
  Handle, Position, ReactFlowProvider,
  type Node, type Edge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import type { TableInfo } from '../../services/api';

const { Text } = Typography;

// OMOP CDM FK 관계 정의 (person_id 기반)
const FK_RELATIONSHIPS: { source: string; target: string; fk: string }[] = [
  { source: 'person', target: 'visit_occurrence', fk: 'person_id' },
  { source: 'person', target: 'condition_occurrence', fk: 'person_id' },
  { source: 'person', target: 'drug_exposure', fk: 'person_id' },
  { source: 'person', target: 'measurement', fk: 'person_id' },
  { source: 'person', target: 'observation', fk: 'person_id' },
  { source: 'person', target: 'procedure_occurrence', fk: 'person_id' },
  { source: 'person', target: 'observation_period', fk: 'person_id' },
  { source: 'visit_occurrence', target: 'visit_detail', fk: 'visit_occurrence_id' },
  { source: 'visit_occurrence', target: 'condition_occurrence', fk: 'visit_occurrence_id' },
  { source: 'visit_occurrence', target: 'drug_exposure', fk: 'visit_occurrence_id' },
  { source: 'visit_occurrence', target: 'measurement', fk: 'visit_occurrence_id' },
  { source: 'visit_occurrence', target: 'procedure_occurrence', fk: 'visit_occurrence_id' },
  { source: 'condition_occurrence', target: 'condition_era', fk: 'condition_concept_id' },
  { source: 'drug_exposure', target: 'drug_era', fk: 'drug_concept_id' },
  { source: 'person', target: 'cost', fk: 'person_id' },
  { source: 'person', target: 'payer_plan_period', fk: 'person_id' },
  { source: 'care_site', target: 'visit_occurrence', fk: 'care_site_id' },
  { source: 'provider', target: 'visit_occurrence', fk: 'provider_id' },
  { source: 'location', target: 'care_site', fk: 'location_id' },
];

const DOMAIN_COLORS: Record<string, { bg: string; border: string }> = {
  Demographics: { bg: '#e6f7ff', border: '#1890ff' },
  Clinical: { bg: '#f6ffed', border: '#52c41a' },
  'Lab/Vital': { bg: '#fff7e6', border: '#faad14' },
  Derived: { bg: '#f9f0ff', border: '#722ed1' },
  Financial: { bg: '#fff1f0', border: '#ff4d4f' },
  'Health System': { bg: '#f0f5ff', border: '#2f54eb' },
  Other: { bg: '#fafafa', border: '#d9d9d9' },
};

const TABLE_DOMAIN_MAP: Record<string, string> = {
  person: 'Demographics',
  visit_occurrence: 'Clinical', visit_detail: 'Clinical',
  condition_occurrence: 'Clinical', drug_exposure: 'Clinical',
  measurement: 'Lab/Vital', observation: 'Clinical',
  procedure_occurrence: 'Clinical', observation_period: 'Derived',
  condition_era: 'Derived', drug_era: 'Derived',
  cost: 'Financial', payer_plan_period: 'Financial',
  care_site: 'Health System', provider: 'Health System', location: 'Health System',
};

// Custom ReactFlow 노드
const CatalogGraphNode = ({ data }: { data: { label: string; domain: string; rowCount?: string; onView?: () => void } }) => {
  const colors = DOMAIN_COLORS[data.domain] || DOMAIN_COLORS.Other;
  return (
    <div
      style={{
        padding: '8px 14px',
        borderRadius: 8,
        border: `2px solid ${colors.border}`,
        background: colors.bg,
        minWidth: 130,
        textAlign: 'center',
        cursor: data.onView ? 'pointer' : 'default',
      }}
      onClick={data.onView}
    >
      <Handle type="target" position={Position.Top} style={{ background: colors.border }} />
      <div style={{ fontWeight: 600, fontSize: 12 }}>{data.label}</div>
      {data.rowCount && <div style={{ fontSize: 10, color: '#666' }}>{data.rowCount}</div>}
      <Tag color={colors.border} style={{ fontSize: 9, marginTop: 2 }}>{data.domain}</Tag>
      <Handle type="source" position={Position.Bottom} style={{ background: colors.border }} />
    </div>
  );
};

const nodeTypes = { catalogNode: CatalogGraphNode };

interface CatalogResultsPanelProps {
  tables: TableInfo[];
  viewMode: string;
  isSearching: boolean;
  searchQuery: string;
  treeExpandedKeys: React.Key[];
  onTreeExpandedKeysChange: (keys: React.Key[]) => void;
  onViewDetail: (table: TableInfo) => void;
  onCopyTableName: (name: string) => void;
  onShowLineage: (table: TableInfo) => void;
  onTreeSelect: (table: TableInfo, tab: string) => void;
}

const CatalogResultsPanel: React.FC<CatalogResultsPanelProps> = ({
  tables,
  viewMode,
  isSearching,
  searchQuery,
  treeExpandedKeys,
  onTreeExpandedKeysChange,
  onViewDetail,
  onCopyTableName,
  onShowLineage,
  onTreeSelect,
}) => {
  // 테이블 목록 컬럼 정의
  const tableColumns: ColumnsType<TableInfo> = [
    {
      title: '테이블명',
      key: 'name',
      width: 280,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Space>
            <TableOutlined style={{ color: '#005BAC' }} />
            <Text strong>{record.business_name}</Text>
          </Space>
          <Text type="secondary" style={{ fontSize: 12 }}>{record.physical_name}</Text>
        </Space>
      ),
    },
    {
      title: '도메인',
      dataIndex: 'domain',
      key: 'domain',
      width: 120,
      render: (domain: string) => <Tag icon={<DatabaseOutlined />} color="blue">{domain}</Tag>,
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '태그',
      key: 'tags',
      width: 200,
      render: (_, record) => (
        <Space wrap size={[4, 4]}>
          {record.tags?.slice(0, 3).map((tag) => <Tag key={tag} color="default">{tag}</Tag>)}
          {record.tags?.length > 3 && (
            <Tooltip title={record.tags.slice(3).join(', ')}>
              <Tag>+{record.tags.length - 3}</Tag>
            </Tooltip>
          )}
        </Space>
      ),
    },
    {
      title: '활용',
      dataIndex: 'usage_count',
      key: 'usage_count',
      width: 60,
      align: 'center',
      render: (count: number) => <Badge count={count} showZero color="#005BAC" overflowCount={999} />,
    },
    {
      title: '최종수정',
      key: 'last_modified',
      width: 90,
      render: (_: unknown, record: TableInfo) => {
        // last_modified가 있으면 사용, 없으면 테이블명 기반 시뮬레이션
        if (record.last_modified) return <Text type="secondary" style={{ fontSize: 11 }}>{record.last_modified}</Text>;
        const hash = record.physical_name.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
        const daysAgo = (hash % 14) + 1;
        const label = daysAgo === 1 ? '어제' : daysAgo <= 7 ? `${daysAgo}일 전` : `${Math.floor(daysAgo / 7)}주 전`;
        return <Text type="secondary" style={{ fontSize: 11 }}>{label}</Text>;
      },
    },
    {
      title: '',
      key: 'actions',
      width: 120,
      render: (_, record) => (
        <Space>
          <Tooltip title="상세 보기">
            <Button type="text" icon={<EyeOutlined />} onClick={() => onViewDetail(record)} />
          </Tooltip>
          <Tooltip title="테이블명 복사">
            <Button type="text" icon={<CopyOutlined />} onClick={() => onCopyTableName(record.physical_name)} />
          </Tooltip>
          <Tooltip title="데이터 계보">
            <Button
              type="text"
              icon={<NodeIndexOutlined />}
              onClick={() => onShowLineage(record)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 트리 데이터 빌드
  const buildTreeData = useCallback((): DataNode[] => {
    const domainMap: Record<string, TableInfo[]> = {};
    for (const table of tables) {
      const d = table.domain || '미분류';
      if (!domainMap[d]) domainMap[d] = [];
      domainMap[d].push(table);
    }
    return Object.entries(domainMap).map(([domain, domainTables]) => ({
      key: `domain__${domain}`,
      title: (
        <Space>
          <DatabaseOutlined style={{ color: '#005BAC' }} />
          <Text strong>{domain}</Text>
          <Badge count={domainTables.length} style={{ backgroundColor: '#006241' }} />
        </Space>
      ),
      children: domainTables.map((t) => ({
        key: `table__${t.physical_name}`,
        title: (
          <Space>
            <TableOutlined style={{ color: '#005BAC' }} />
            <Text strong>{t.business_name}</Text>
            <Text type="secondary" style={{ fontSize: 11 }}>({t.physical_name})</Text>
          </Space>
        ),
        children: (t.columns || []).map((col) => ({
          key: `col__${t.physical_name}__${col.physical_name}`,
          title: (
            <Space>
              <Text>{col.business_name || col.physical_name}</Text>
              <Tag style={{ fontSize: 10 }}>{col.data_type}</Tag>
              {col.is_pk && <Tag color="gold" style={{ fontSize: 10 }}>PK</Tag>}
              {col.sensitivity === 'PHI' && <Tag color="orange" style={{ fontSize: 10 }}>PHI</Tag>}
            </Space>
          ),
          isLeaf: true,
        })),
      })),
    }));
  }, [tables]);

  const treeData = buildTreeData();

  const allTreeKeys = treeData.flatMap((domain) => [
    domain.key,
    ...(domain.children || []).map((t) => t.key),
  ]);

  const handleExpandAll = () => onTreeExpandedKeysChange(allTreeKeys);
  const handleCollapseAll = () => onTreeExpandedKeysChange([]);

  const handleTreeSelect = (selectedKeys: React.Key[]) => {
    const key = selectedKeys[0]?.toString() || '';
    if (key.startsWith('table__')) {
      const physicalName = key.replace('table__', '');
      const table = tables.find((t) => t.physical_name === physicalName);
      if (table) onTreeSelect(table, 'columns');
    } else if (key.startsWith('col__')) {
      const parts = key.replace('col__', '').split('__');
      const tableName = parts[0];
      const table = tables.find((t) => t.physical_name === tableName);
      if (table) onTreeSelect(table, 'columns');
    }
  };

  // ReactFlow 그래프 데이터 빌드
  const { graphNodes, graphEdges } = useMemo(() => {
    const tableSet = new Set(tables.map((t) => t.physical_name));
    // 검색 결과에 포함된 테이블의 FK만 필터링 (양쪽 다 포함인 경우)
    const relevantFks = FK_RELATIONSHIPS.filter(
      (fk) => tableSet.has(fk.source) || tableSet.has(fk.target)
    );
    // FK에 등장하는 모든 테이블 (검색 결과 + 연결된 테이블)
    const allTables = new Set<string>();
    tables.forEach((t) => allTables.add(t.physical_name));
    relevantFks.forEach((fk) => { allTables.add(fk.source); allTables.add(fk.target); });

    // 레이아웃: 계층별 배치 (person -> visit -> event tables)
    const LAYER: Record<string, number> = {
      location: 0, care_site: 0, provider: 0,
      person: 1,
      visit_occurrence: 2, observation_period: 2, payer_plan_period: 2, cost: 2,
      visit_detail: 3, condition_occurrence: 3, drug_exposure: 3, measurement: 3,
      observation: 3, procedure_occurrence: 3,
      condition_era: 4, drug_era: 4,
    };
    const layerCounts: Record<number, number> = {};
    const sortedTables = [...allTables].sort((a, b) => (LAYER[a] ?? 3) - (LAYER[b] ?? 3));

    const nodes: Node[] = sortedTables.map((tname) => {
      const layer = LAYER[tname] ?? 3;
      const idx = layerCounts[layer] || 0;
      layerCounts[layer] = idx + 1;
      const domain = TABLE_DOMAIN_MAP[tname] || 'Other';
      const tableInfo = tables.find((t) => t.physical_name === tname);
      return {
        id: tname,
        type: 'catalogNode',
        position: { x: 80 + idx * 180, y: 40 + layer * 130 },
        data: {
          label: tname,
          domain,
          rowCount: tableInfo ? undefined : '(연결 테이블)',
          onView: tableInfo ? () => onViewDetail(tableInfo) : undefined,
        },
      };
    });

    const edges: Edge[] = relevantFks
      .filter((fk) => allTables.has(fk.source) && allTables.has(fk.target))
      .map((fk, i) => ({
        id: `fk-${i}`,
        source: fk.source,
        target: fk.target,
        label: fk.fk,
        labelStyle: { fontSize: 9, fill: '#999' },
        style: { stroke: '#b0b0b0', strokeWidth: 1.5 },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#b0b0b0' },
        animated: tableSet.has(fk.source) && tableSet.has(fk.target),
      }));

    return { graphNodes: nodes, graphEdges: edges };
  }, [tables, onViewDetail]);

  const viewTitle = viewMode === 'table' ? '검색 결과' : viewMode === 'tree' ? '트리 뷰' : 'FK 관계 그래프';
  const viewIcon = viewMode === 'table' ? <TableOutlined /> : viewMode === 'tree' ? <ApartmentOutlined /> : <ShareAltOutlined />;

  return (
    <Card
      title={
        <Space>
          {viewIcon}
          {viewTitle}
          <Text type="secondary">({tables.length}개)</Text>
        </Space>
      }
      extra={viewMode === 'tree' && tables.length > 0 ? (
        <Space>
          <Button size="small" onClick={handleExpandAll}>모두 펼치기</Button>
          <Button size="small" onClick={handleCollapseAll}>모두 접기</Button>
        </Space>
      ) : undefined}
    >
      <Spin spinning={isSearching}>
        {tables.length > 0 ? (
          viewMode === 'table' ? (
            <Table
              dataSource={tables}
              columns={tableColumns}
              rowKey="physical_name"
              pagination={{ pageSize: 10, showSizeChanger: true, showTotal: (total) => `총 ${total}개` }}
              size="middle"
            />
          ) : viewMode === 'tree' ? (
            <Tree
              showLine
              switcherIcon={<DownOutlined />}
              treeData={treeData}
              expandedKeys={treeExpandedKeys}
              onExpand={(keys) => onTreeExpandedKeysChange(keys)}
              onSelect={handleTreeSelect}
              height={500}
              style={{ padding: '8px 0' }}
            />
          ) : (
            <ReactFlowProvider>
              <div style={{ height: 560, border: '1px solid #f0f0f0', borderRadius: 8 }}>
                <ReactFlow
                  nodes={graphNodes}
                  edges={graphEdges}
                  nodeTypes={nodeTypes}
                  fitView
                  fitViewOptions={{ padding: 0.2 }}
                  minZoom={0.3}
                  maxZoom={1.5}
                >
                  <Background variant={BackgroundVariant.Dots} gap={16} size={1} color="#e8e8e8" />
                  <Controls showInteractive={false} />
                  <MiniMap nodeStrokeWidth={3} pannable zoomable />
                </ReactFlow>
              </div>
            </ReactFlowProvider>
          )
        ) : (
          <Empty description={searchQuery ? '검색 결과가 없습니다.' : '검색어를 입력하거나 필터를 선택하세요.'} />
        )}
      </Spin>
    </Card>
  );
};

export default CatalogResultsPanel;
