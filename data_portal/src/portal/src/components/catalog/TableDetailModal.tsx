/**
 * 테이블 상세 모달 (DPR-002: 지능형 카탈로그)
 */

import React from 'react';
import {
  Table, Tag, Space, Typography, Row, Col,
  Button, Descriptions, Modal, Tabs, Badge, Divider, Avatar,
} from 'antd';
import {
  TableOutlined, DatabaseOutlined, CopyOutlined,
  UserOutlined, TeamOutlined, CodeOutlined,
  BranchesOutlined, FileTextOutlined, ThunderboltOutlined, ApiOutlined,
} from '@ant-design/icons';
import type { TableInfo, ColumnInfo } from '../../services/api';
import type { ColumnsType } from 'antd/es/table';
import LineageGraph from '../lineage/LineageGraph';
import RelatedTables from './RelatedTables';
import QualityInfo from './QualityInfo';
import { sensitivityColors, sensitivityLabels, generateSqlCode, generatePythonCode, generateApiEndpoint } from './constants';

const { Title, Text } = Typography;

interface TableDetailModalProps {
  table: TableInfo | null;
  visible: boolean;
  activeTab: string;
  onTabChange: (key: string) => void;
  onClose: () => void;
  onCopyTableName: (name: string) => void;
  onCopyText: (text: string, label: string) => void;
}

const columnColumns: ColumnsType<ColumnInfo> = [
  {
    title: '컬럼명',
    key: 'name',
    width: 200,
    render: (_, record) => (
      <Space direction="vertical" size={0}>
        <Text strong>{record.business_name}</Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          {record.physical_name}
        </Text>
      </Space>
    ),
  },
  {
    title: '타입',
    dataIndex: 'data_type',
    key: 'data_type',
    width: 100,
    render: (type: string) => <Tag color="geekblue">{type}</Tag>,
  },
  {
    title: '설명',
    dataIndex: 'description',
    key: 'description',
    ellipsis: true,
  },
  {
    title: 'PK',
    dataIndex: 'is_pk',
    key: 'is_pk',
    width: 60,
    align: 'center',
    render: (isPk: boolean) => (isPk ? <Tag color="gold">PK</Tag> : '-'),
  },
  {
    title: '민감도',
    dataIndex: 'sensitivity',
    key: 'sensitivity',
    width: 80,
    render: (sensitivity: string) => (
      <Tag color={sensitivityColors[sensitivity] || 'default'}>
        {sensitivityLabels[sensitivity] || sensitivity}
      </Tag>
    ),
  },
];

const preStyle: React.CSSProperties = {
  background: '#f5f5f5',
  padding: 12,
  borderRadius: 4,
  fontSize: 12,
  overflow: 'auto',
};

const TableDetailModal: React.FC<TableDetailModalProps> = ({
  table,
  visible,
  activeTab,
  onTabChange,
  onClose,
  onCopyTableName,
  onCopyText,
}) => {
  if (!table) return null;

  return (
    <Modal
      title={
        <Space>
          <TableOutlined style={{ color: '#005BAC' }} />
          {table.business_name}
          <Tag color="blue">{table.domain}</Tag>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width={1000}
      footer={null}
    >
      {/* 기본 정보 (DGR-003: 오너쉽 정보 포함) */}
      <Descriptions bordered column={3} size="small" style={{ marginBottom: 16 }}>
        <Descriptions.Item label="물리명">
          <Space>
            <Text code>{table.physical_name}</Text>
            <Button
              type="text"
              size="small"
              icon={<CopyOutlined />}
              onClick={() => onCopyTableName(table.physical_name)}
            />
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="오너">
          <Space>
            <Avatar size="small" icon={<UserOutlined />} />
            <Text>데이터관리팀</Text>
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="활용 횟수">
          <Badge count={table.usage_count} showZero color="#005BAC" />
        </Descriptions.Item>
        <Descriptions.Item label="설명" span={3}>
          {table.description}
        </Descriptions.Item>
        <Descriptions.Item label="태그" span={2}>
          <Space wrap>
            {table.tags?.map((tag) => (
              <Tag key={tag}>{tag}</Tag>
            ))}
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="공유 범위">
          <Tag icon={<TeamOutlined />} color="green">전체 공개</Tag>
        </Descriptions.Item>
      </Descriptions>

      {/* 탭 기반 상세 정보 */}
      <Tabs
        activeKey={activeTab}
        onChange={onTabChange}
        items={[
          {
            key: 'columns',
            label: <><TableOutlined /> 컬럼 정보</>,
            children: (
              <Table
                dataSource={table.columns || []}
                columns={columnColumns}
                rowKey="physical_name"
                pagination={false}
                size="small"
                scroll={{ y: 280 }}
              />
            ),
          },
          {
            key: 'lineage',
            label: <><BranchesOutlined /> 데이터 계보</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                <Title level={5} style={{ marginBottom: 16 }}>Data Lineage (DPR-002)</Title>
                <LineageGraph
                  key={`lineage-${table.physical_name}`}
                  tableName={table.business_name}
                  physicalName={table.physical_name}
                  height={400}
                />
                <Divider />
                <Title level={5}>연관 테이블 (DPR-002: 연관 데이터셋 추천)</Title>
                <RelatedTables tableName={table.physical_name} />
              </div>
            ),
          },
          {
            key: 'code',
            label: <><CodeOutlined /> 코드 샘플</>,
            children: (
              <div style={{ padding: '16px 0' }}>
                <div style={{ marginBottom: 16 }}>
                  <Space style={{ marginBottom: 8 }}>
                    <DatabaseOutlined />
                    <Text strong>SQL Query</Text>
                    <Button size="small" icon={<CopyOutlined />} onClick={() => onCopyText(generateSqlCode(table), 'SQL')}>
                      복사
                    </Button>
                  </Space>
                  <pre style={preStyle}>{generateSqlCode(table)}</pre>
                </div>
                <div style={{ marginBottom: 16 }}>
                  <Space style={{ marginBottom: 8 }}>
                    <ThunderboltOutlined />
                    <Text strong>Python (pandas)</Text>
                    <Button size="small" icon={<CopyOutlined />} onClick={() => onCopyText(generatePythonCode(table), 'Python 코드')}>
                      복사
                    </Button>
                  </Space>
                  <pre style={preStyle}>{generatePythonCode(table)}</pre>
                </div>
                <div>
                  <Space style={{ marginBottom: 8 }}>
                    <ApiOutlined />
                    <Text strong>REST API</Text>
                    <Button size="small" icon={<CopyOutlined />} onClick={() => onCopyText(generateApiEndpoint(table), 'API 정보')}>
                      복사
                    </Button>
                  </Space>
                  <pre style={preStyle}>{generateApiEndpoint(table)}</pre>
                </div>
              </div>
            ),
          },
          {
            key: 'quality',
            label: <><FileTextOutlined /> 품질 정보</>,
            children: <QualityInfo tableName={table.physical_name} />,
          },
        ]}
      />
    </Modal>
  );
};

export default TableDetailModal;
