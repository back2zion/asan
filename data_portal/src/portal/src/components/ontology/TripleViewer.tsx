/**
 * RDF Triple Viewer table + Neo4j Cypher Export drawer.
 */

import React, { useState, useMemo } from 'react';
import {
  Typography, Tag, Space, Button, Select, Drawer, Alert, Table,
} from 'antd';
import {
  NodeIndexOutlined, DownloadOutlined, CopyOutlined,
} from '@ant-design/icons';

import { OntologyNode, Triple, GraphData, NODE_TYPE_META } from './types';

const { Text } = Typography;

// ═══════════════════════════════════════════════════
//  Helper: resolve triple subject/object
// ═══════════════════════════════════════════════════

const resolveTripleNode = (
  val: string | { id: string; label: string; type: string },
  nodeMap: Map<string, OntologyNode>,
) => {
  if (typeof val === 'object' && val.id) return val;
  const node = nodeMap.get(val as string);
  return node
    ? { id: node.id, label: node.label, type: node.type }
    : { id: val as string, label: val as string, type: 'unknown' };
};

// ═══════════════════════════════════════════════════
//  TripleTable — the actual triple table
// ═══════════════════════════════════════════════════

const TripleTable: React.FC<{
  triples: Triple[];
  nodes: OntologyNode[];
  onNodeClick: (id: string) => void;
}> = ({ triples, nodes, onNodeClick }) => {
  const [filterType, setFilterType] = useState<string>('all');

  const nodeMap = useMemo(() => {
    const m = new Map<string, OntologyNode>();
    nodes.forEach(n => m.set(n.id, n));
    return m;
  }, [nodes]);

  const resolved = useMemo(() =>
    triples.map(t => ({
      subject: resolveTripleNode(t.subject, nodeMap),
      predicate: t.predicate,
      object: resolveTripleNode(t.object, nodeMap),
      triple_type: t.triple_type || t.type || 'unknown',
      description: t.description || '',
    })),
    [triples, nodeMap],
  );

  const tripleTypes = useMemo(() => {
    const types = new Set(resolved.map(t => t.triple_type));
    return ['all', ...Array.from(types).sort()];
  }, [resolved]);

  const filtered = filterType === 'all' ? resolved : resolved.filter(t => t.triple_type === filterType);

  const columns = [
    {
      title: 'Subject',
      key: 'subject',
      width: 180,
      render: (_: any, t: any) => (
        <Tag
          color={NODE_TYPE_META[t.subject.type]?.color || '#718096'}
          style={{ cursor: 'pointer', fontSize: 11 }}
          onClick={() => onNodeClick(t.subject.id)}
        >
          {(t.subject.label || '').substring(0, 24)}
        </Tag>
      ),
    },
    {
      title: 'Predicate',
      key: 'predicate',
      width: 130,
      render: (_: any, t: any) => (
        <Text code style={{ fontSize: 10 }}>{t.predicate}</Text>
      ),
    },
    {
      title: 'Object',
      key: 'object',
      width: 180,
      render: (_: any, t: any) => (
        <Tag
          color={NODE_TYPE_META[t.object.type]?.color || '#718096'}
          style={{ cursor: 'pointer', fontSize: 11 }}
          onClick={() => onNodeClick(t.object.id)}
        >
          {(t.object.label || '').substring(0, 24)}
        </Tag>
      ),
    },
    {
      title: 'Type',
      key: 'type',
      width: 110,
      render: (_: any, t: any) => (
        <Tag style={{ fontSize: 10 }}>{t.triple_type}</Tag>
      ),
    },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 12 }}>
        <Text strong style={{ fontSize: 13 }}>RDF Triples ({filtered.length})</Text>
        <Select
          size="small"
          value={filterType}
          onChange={setFilterType}
          style={{ width: 160, fontSize: 11 }}
          options={tripleTypes.map(t => ({ value: t, label: t === 'all' ? '전체' : t }))}
        />
      </Space>
      <Table
        dataSource={filtered.slice(0, 200)}
        columns={columns}
        size="small"
        pagination={{ pageSize: 50, size: 'small', showTotal: (total) => `${total}개` }}
        rowKey={(r: any) => `${r.subject?.id}_${r.predicate}_${r.object?.id}`}
        scroll={{ y: 'calc(35vh - 160px)' }}
        style={{ fontSize: 11 }}
      />
    </div>
  );
};

// ═══════════════════════════════════════════════════
//  TripleDrawer — wraps the triple table in a drawer
// ═══════════════════════════════════════════════════

export const TripleDrawer: React.FC<{
  open: boolean;
  onClose: () => void;
  graphData: GraphData | null;
  onNodeClick: (id: string) => void;
}> = ({ open, onClose, graphData, onNodeClick }) => (
  <Drawer
    title={
      <Space>
        <NodeIndexOutlined />
        <Text strong>RDF Triple Viewer</Text>
        <Tag color="purple">{graphData?.triples?.length || 0} triples</Tag>
      </Space>
    }
    open={open}
    onClose={onClose}
    placement="bottom"
    height="35vh"
  >
    {graphData?.triples && (
      <TripleTable triples={graphData.triples} nodes={graphData.nodes} onNodeClick={onNodeClick} />
    )}
  </Drawer>
);

// ═══════════════════════════════════════════════════
//  CypherExportDrawer — Neo4j Cypher script drawer
// ═══════════════════════════════════════════════════

export const CypherExportDrawer: React.FC<{
  open: boolean;
  onClose: () => void;
  cypherScript: string;
  onCopy: () => void;
}> = ({ open, onClose, cypherScript, onCopy }) => (
  <Drawer
    title={
      <Space>
        <DownloadOutlined />
        <Text strong>Neo4j Cypher Export</Text>
      </Space>
    }
    open={open}
    onClose={onClose}
    width={700}
    extra={
      <Button icon={<CopyOutlined />} size="small" onClick={onCopy}>
        Copy
      </Button>
    }
  >
    <Alert
      message="Neo4j Import Instructions"
      description={
        <ol style={{ paddingLeft: 16, margin: '8px 0', fontSize: 12 }}>
          <li>Neo4j Desktop/Aura에서 데이터베이스 생성</li>
          <li>Neo4j Browser 접속 후 아래 Cypher 스크립트 실행</li>
          <li><code>CALL db.schema.visualization()</code> 으로 스키마 확인</li>
          <li><code>MATCH (n)-[r]-&gt;(m) RETURN n,r,m LIMIT 300</code> 으로 시각화</li>
        </ol>
      }
      type="info"
      showIcon
      style={{ marginBottom: 16 }}
    />
    <pre style={{
      background: '#1a202c', color: '#e2e8f0', padding: 16, borderRadius: 8,
      fontSize: 11, lineHeight: 1.6, maxHeight: 'calc(100vh - 300px)',
      overflow: 'auto', whiteSpace: 'pre-wrap',
    }}>
      {cypherScript}
    </pre>
  </Drawer>
);

export default TripleTable;
