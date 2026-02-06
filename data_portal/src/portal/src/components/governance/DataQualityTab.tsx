/**
 * 데이터 품질 탭 컴포넌트
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Alert, Progress, Button,
} from 'antd';
import {
  CheckCircleOutlined, DatabaseOutlined, TableOutlined,
  SafetyCertificateOutlined, ReloadOutlined,
} from '@ant-design/icons';
import { executeSQL } from './helpers';
import type { TableQuality } from './types';

const { Text } = Typography;

const DataQualityTab: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [qualityData, setQualityData] = useState<TableQuality[]>([]);

  const loadQuality = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const sql = `
SELECT
  t.table_name,
  (SELECT reltuples::bigint FROM pg_class WHERE relname = t.table_name) AS row_count,
  (SELECT COUNT(*) FROM information_schema.columns c2
   WHERE c2.table_schema = 'public' AND c2.table_name = t.table_name AND c2.is_nullable = 'YES')
  * 100.0 /
  NULLIF((SELECT COUNT(*) FROM information_schema.columns c3
   WHERE c3.table_schema = 'public' AND c3.table_name = t.table_name), 0) AS nullable_col_pct
FROM information_schema.tables t
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY t.table_name`;

      const result = await executeSQL(sql);
      const rows: TableQuality[] = result.results.map((r) => {
        const rowCount = Number(r[1]) || 0;
        const nullRate = Number(r[2]) || 0;
        const completeness = 100 - nullRate;
        return {
          table_name: String(r[0]),
          row_count: rowCount,
          null_rate: Math.round(nullRate * 10) / 10,
          completeness: Math.round(completeness * 10) / 10,
          status: completeness >= 80 ? 'good' : completeness >= 60 ? 'warning' : 'error',
        };
      });
      setQualityData(rows);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadQuality(); }, [loadQuality]);

  const totalRows = qualityData.reduce((s, r) => s + r.row_count, 0);
  const avgCompleteness = qualityData.length
    ? Math.round(qualityData.reduce((s, r) => s + r.completeness, 0) / qualityData.length * 10) / 10
    : 0;
  const goodCount = qualityData.filter((r) => r.status === 'good').length;

  const columns = [
    {
      title: '테이블명',
      dataIndex: 'table_name',
      key: 'table_name',
      render: (v: string) => <Text strong style={{ fontFamily: 'monospace' }}>{v}</Text>,
    },
    {
      title: '행 수',
      dataIndex: 'row_count',
      key: 'row_count',
      sorter: (a: TableQuality, b: TableQuality) => a.row_count - b.row_count,
      render: (v: number) => v.toLocaleString(),
    },
    {
      title: 'Nullable 컬럼 비율',
      dataIndex: 'null_rate',
      key: 'null_rate',
      sorter: (a: TableQuality, b: TableQuality) => a.null_rate - b.null_rate,
      render: (v: number) => `${v}%`,
    },
    {
      title: '완전성 점수',
      dataIndex: 'completeness',
      key: 'completeness',
      sorter: (a: TableQuality, b: TableQuality) => a.completeness - b.completeness,
      render: (v: number) => (
        <Progress
          percent={v}
          size="small"
          status={v >= 80 ? 'success' : v >= 60 ? 'normal' : 'exception'}
          format={(p) => `${p}%`}
        />
      ),
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      filters: [
        { text: '양호', value: 'good' },
        { text: '주의', value: 'warning' },
        { text: '위험', value: 'error' },
      ],
      onFilter: (value: any, record: TableQuality) => record.status === value,
      render: (v: string) => {
        const map: Record<string, { color: string; text: string }> = {
          good: { color: 'green', text: '양호' },
          warning: { color: 'orange', text: '주의' },
          error: { color: 'red', text: '위험' },
        };
        const m = map[v] || map.good;
        return <Tag color={m.color}>{m.text}</Tag>;
      },
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 테이블" value={qualityData.length} suffix="개" prefix={<TableOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 레코드" value={totalRows} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="평균 완전성"
              value={avgCompleteness}
              suffix="%"
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: avgCompleteness >= 80 ? '#3f8600' : '#cf1322' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic
              title="양호 테이블"
              value={goodCount}
              suffix={`/ ${qualityData.length}`}
              prefix={<SafetyCertificateOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
      </Row>

      {error && <Alert message={error} type="error" showIcon />}

      <Card
        title="테이블별 품질 지표"
        extra={<Button icon={<ReloadOutlined />} onClick={loadQuality} loading={loading}>새로고침</Button>}
      >
        <Table
          columns={columns}
          dataSource={qualityData}
          rowKey="table_name"
          size="small"
          loading={loading}
          pagination={{ pageSize: 15, showSizeChanger: true, showTotal: (t) => `총 ${t}개 테이블` }}
        />
      </Card>
    </Space>
  );
};

export default DataQualityTab;
