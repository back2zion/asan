/**
 * TableDetailModal 정적 설정 (컬럼 정의, 스타일, 코드 환경 옵션)
 */
import React from 'react';
import { Space, Typography, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { ColumnInfo } from '../../services/api';
import { sensitivityColors, sensitivityLabels } from './constants';

const { Text } = Typography;

export const columnColumns: ColumnsType<ColumnInfo> = [
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

export const preStyle: React.CSSProperties = {
  background: '#f5f5f5',
  padding: 12,
  borderRadius: 4,
  fontSize: 12,
  overflow: 'auto',
};

export const CODE_ENVS = [
  { label: 'SQL', value: 'sql' },
  { label: 'Python', value: 'python' },
  { label: 'R', value: 'r' },
  { label: 'REST API', value: 'api' },
  { label: 'JupyterLab', value: 'jupyter' },
];
