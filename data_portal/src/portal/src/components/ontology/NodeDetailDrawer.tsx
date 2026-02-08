/**
 * Inline right panel showing detailed information about a selected ontology node
 * and its connections (outgoing/incoming links).
 *
 * Replaces the previous Drawer overlay — now part of the grid layout so the
 * graph canvas shrinks to make room instead of being hidden.
 */

import React from 'react';
import {
  Typography, Tag, Space, Card, Descriptions, Alert, Divider, List, Button,
} from 'antd';
import { CloseOutlined } from '@ant-design/icons';

import { OntologyNode, NODE_TYPE_META } from './types';

const { Text } = Typography;

interface NodeDetailPanelProps {
  node: OntologyNode | null;
  neighbors: any;
  onClose: () => void;
  onNavigate: (nodeId: string) => void;
}

const NodeDetailPanel: React.FC<NodeDetailPanelProps> = ({ node, neighbors, onClose, onNavigate }) => {
  if (!node) return null;
  const meta = NODE_TYPE_META[node.type] || { label: node.type, color: '#718096', icon: null };

  return (
    <Card
      size="small"
      title={
        <Space size={6} style={{ maxWidth: '100%', overflow: 'hidden' }}>
          <div style={{ width: 14, height: 14, borderRadius: '50%', backgroundColor: meta.color, flexShrink: 0 }} />
          <Text strong style={{ fontSize: 13, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {node.label}
          </Text>
          <Tag color={meta.color} style={{ fontSize: 10, margin: 0 }}>{meta.label}</Tag>
        </Space>
      }
      extra={<Button type="text" size="small" icon={<CloseOutlined />} onClick={onClose} />}
      style={{ height: '100%', overflow: 'hidden', borderRadius: 8 }}
      styles={{ body: { padding: '8px 12px', overflow: 'auto', maxHeight: 'calc(100vh - 340px)' } }}
    >
      {node.full_label && node.full_label !== node.label && (
        <Alert message={node.full_label} type="info" style={{ marginBottom: 8, fontSize: 11 }} />
      )}

      <Descriptions column={1} size="small" bordered style={{ fontSize: 11 }}>
        <Descriptions.Item label="ID">{node.id}</Descriptions.Item>
        <Descriptions.Item label="Type">
          <Tag color={meta.color} style={{ fontSize: 10 }}>{meta.label}</Tag>
        </Descriptions.Item>
        {node.concept_id != null && (
          <Descriptions.Item label="Concept ID">{node.concept_id}</Descriptions.Item>
        )}
        {node.row_count != null && (
          <Descriptions.Item label="Row Count">{node.row_count.toLocaleString()}</Descriptions.Item>
        )}
        {node.record_count != null && (
          <Descriptions.Item label="Records">{node.record_count.toLocaleString()}</Descriptions.Item>
        )}
        {node.patient_count != null && (
          <Descriptions.Item label="Patients">{node.patient_count.toLocaleString()}</Descriptions.Item>
        )}
        {node.description && (
          <Descriptions.Item label="Description">
            <Text style={{ fontSize: 11 }}>{node.description}</Text>
          </Descriptions.Item>
        )}
        {node.domain && (
          <Descriptions.Item label="Domain">{node.domain}</Descriptions.Item>
        )}
      </Descriptions>

      {neighbors && (
        <>
          <Divider orientation="left" style={{ fontSize: 11, margin: '12px 0 8px' }}>
            Connections ({(neighbors.outgoing_links?.length || 0) + (neighbors.incoming_links?.length || 0)})
          </Divider>

          {neighbors.outgoing_links?.length > 0 && (
            <>
              <Text type="secondary" style={{ fontSize: 10 }}>Outgoing ({neighbors.outgoing_links.length})</Text>
              <List
                size="small"
                dataSource={neighbors.outgoing_links.slice(0, 15)}
                renderItem={(link: any) => {
                  const targetNode = neighbors.neighbors?.find((n: any) => n.id === link.target);
                  return (
                    <List.Item
                      style={{ padding: '3px 0', cursor: 'pointer' }}
                      onClick={() => onNavigate(link.target)}
                    >
                      <Space size={4} wrap>
                        <Tag color="blue" style={{ fontSize: 9, margin: 0 }}>{link.label}</Tag>
                        <Text style={{ fontSize: 11 }}>{targetNode?.label || link.target}</Text>
                      </Space>
                    </List.Item>
                  );
                }}
              />
            </>
          )}

          {neighbors.incoming_links?.length > 0 && (
            <>
              <Text type="secondary" style={{ fontSize: 10 }}>Incoming ({neighbors.incoming_links.length})</Text>
              <List
                size="small"
                dataSource={neighbors.incoming_links.slice(0, 15)}
                renderItem={(link: any) => {
                  const sourceNode = neighbors.neighbors?.find((n: any) => n.id === link.source);
                  return (
                    <List.Item
                      style={{ padding: '3px 0', cursor: 'pointer' }}
                      onClick={() => onNavigate(link.source)}
                    >
                      <Space size={4} wrap>
                        <Text style={{ fontSize: 11 }}>{sourceNode?.label || link.source}</Text>
                        <Tag color="green" style={{ fontSize: 9, margin: 0 }}>{link.label}</Tag>
                      </Space>
                    </List.Item>
                  );
                }}
              />
            </>
          )}
        </>
      )}
    </Card>
  );
};

export default NodeDetailPanel;
