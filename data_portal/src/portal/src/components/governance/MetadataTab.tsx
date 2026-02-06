/**
 * 메타데이터 탭 컴포넌트
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col,
  Alert, Spin, Descriptions, Button,
} from 'antd';
import { EyeOutlined, ReloadOutlined } from '@ant-design/icons';
import { semanticApi } from '../../services/api';

const { Title, Text } = Typography;

const MetadataTab: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tables, setTables] = useState<any[]>([]);
  const [selectedTable, setSelectedTable] = useState<any>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const loadTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await semanticApi.facetedSearch({ limit: 100 });
      setTables(resp.data?.tables || resp.data || []);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadTables(); }, [loadTables]);

  const handleTableClick = async (physicalName: string) => {
    setDetailLoading(true);
    try {
      const resp = await semanticApi.getTableMetadata(physicalName);
      setSelectedTable(resp.data || resp);
    } catch {
      setSelectedTable(null);
    } finally {
      setDetailLoading(false);
    }
  };

  const tableColumns = [
    {
      title: '물리명',
      dataIndex: 'physical_name',
      key: 'physical_name',
      render: (v: string) => (
        <Button type="link" size="small" onClick={() => handleTableClick(v)} style={{ fontFamily: 'monospace', padding: 0 }}>
          {v}
        </Button>
      ),
    },
    { title: '비즈니스명', dataIndex: 'business_name', key: 'business_name' },
    { title: '도메인', dataIndex: 'domain', key: 'domain', render: (v: string) => v ? <Tag color="blue">{v}</Tag> : '-' },
    {
      title: '태그',
      dataIndex: 'tags',
      key: 'tags',
      render: (tags: string[]) => tags?.map((t: string) => <Tag key={t}>{t}</Tag>) || '-',
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {error && <Alert message={error} type="error" showIcon />}

      <Row gutter={16}>
        <Col xs={24} lg={selectedTable ? 14 : 24}>
          <Card
            title={`테이블 목록 (${tables.length})`}
            extra={<Button icon={<ReloadOutlined />} onClick={loadTables} loading={loading}>새로고침</Button>}
          >
            <Table
              columns={tableColumns}
              dataSource={tables}
              rowKey="physical_name"
              size="small"
              loading={loading}
              pagination={{ pageSize: 12, showSizeChanger: true }}
            />
          </Card>
        </Col>

        {selectedTable && (
          <Col xs={24} lg={10}>
            <Card
              title={
                <Space>
                  <EyeOutlined />
                  <Text strong>{selectedTable.physical_name}</Text>
                  {selectedTable.business_name && <Text type="secondary">({selectedTable.business_name})</Text>}
                </Space>
              }
              extra={<Button type="text" size="small" onClick={() => setSelectedTable(null)}>닫기</Button>}
            >
              <Spin spinning={detailLoading}>
                {selectedTable.description && (
                  <Descriptions column={1} size="small" style={{ marginBottom: 16 }}>
                    <Descriptions.Item label="설명">{selectedTable.description}</Descriptions.Item>
                    {selectedTable.domain && <Descriptions.Item label="도메인">{selectedTable.domain}</Descriptions.Item>}
                  </Descriptions>
                )}
                <Title level={5}>컬럼 정보</Title>
                <Table
                  columns={[
                    { title: '컬럼명', dataIndex: 'physical_name', key: 'physical_name', render: (v: string) => <Text code>{v}</Text> },
                    { title: '비즈니스명', dataIndex: 'business_name', key: 'business_name' },
                    { title: '타입', dataIndex: 'data_type', key: 'data_type', render: (v: string) => <Tag>{v}</Tag> },
                    { title: 'PK', dataIndex: 'is_pk', key: 'is_pk', render: (v: boolean) => v ? <Tag color="gold">PK</Tag> : null },
                    {
                      title: '민감도',
                      dataIndex: 'sensitivity',
                      key: 'sensitivity',
                      render: (v: string) => {
                        const colorMap: Record<string, string> = { high: 'red', medium: 'orange', low: 'green' };
                        return v ? <Tag color={colorMap[v] || 'default'}>{v}</Tag> : <Tag>일반</Tag>;
                      },
                    },
                  ]}
                  dataSource={selectedTable.columns || []}
                  rowKey="physical_name"
                  size="small"
                  pagination={false}
                  scroll={{ y: 400 }}
                />
              </Spin>
            </Card>
          </Col>
        )}
      </Row>
    </Space>
  );
};

export default MetadataTab;
