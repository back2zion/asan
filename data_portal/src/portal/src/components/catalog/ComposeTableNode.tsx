/**
 * ReactFlow 커스텀 테이블 노드 (DPR-002: 데이터 조합)
 * 테이블명 헤더 + 컬럼 체크박스 목록
 */

import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';
import type { NodeProps } from 'reactflow';
import { Checkbox, Typography, Tag, Button } from 'antd';
import { CloseOutlined, DatabaseOutlined } from '@ant-design/icons';

const { Text } = Typography;

export interface TableNodeData {
  tableName: string;
  columns: Array<{ name: string; type: string; pk?: boolean }>;
  selectedColumns: string[];
  onColumnToggle: (tableName: string, columnName: string) => void;
  onRemove: (tableName: string) => void;
}

const ComposeTableNode: React.FC<NodeProps<TableNodeData>> = ({ data }) => {
  const { tableName, columns, selectedColumns, onColumnToggle, onRemove } = data;

  return (
    <div style={{
      background: '#fff',
      border: '2px solid #005BAC',
      borderRadius: 8,
      minWidth: 200,
      maxWidth: 240,
      boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
    }}>
      <Handle type="target" position={Position.Left} style={{ background: '#005BAC' }} />
      <Handle type="source" position={Position.Right} style={{ background: '#005BAC' }} />

      {/* Header */}
      <div style={{
        background: '#005BAC',
        color: '#fff',
        padding: '6px 10px',
        borderRadius: '6px 6px 0 0',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 13, fontWeight: 600 }}>
          <DatabaseOutlined />
          {tableName}
        </span>
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined style={{ color: '#fff', fontSize: 10 }} />}
          onClick={() => onRemove(tableName)}
          style={{ minWidth: 0, padding: '0 4px' }}
        />
      </div>

      {/* Columns */}
      <div style={{ padding: '6px 10px', maxHeight: 200, overflowY: 'auto' }}>
        {columns.map((col) => (
          <div key={col.name} style={{ display: 'flex', alignItems: 'center', padding: '2px 0', gap: 4 }}>
            <Checkbox
              checked={selectedColumns.includes(col.name)}
              onChange={() => onColumnToggle(tableName, col.name)}
              style={{ fontSize: 11 }}
            >
              <Text style={{ fontSize: 11 }}>{col.name}</Text>
            </Checkbox>
            {col.pk && <Tag color="gold" style={{ fontSize: 9, lineHeight: '14px', padding: '0 3px' }}>PK</Tag>}
          </div>
        ))}
      </div>
    </div>
  );
};

export default memo(ComposeTableNode);
