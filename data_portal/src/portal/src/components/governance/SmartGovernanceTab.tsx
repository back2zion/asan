/**
 * DGR-009: 지능형 거버넌스 및 자동화 확장
 * 2개 서브탭: 성능 최적화 / 지능형 메타데이터
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Tabs, Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Progress, Spin, App,
} from 'antd';
import {
  ThunderboltOutlined, DatabaseOutlined, ClockCircleOutlined,
  WarningOutlined, NodeIndexOutlined, HddOutlined,
  BarChartOutlined, ExperimentOutlined, AlertOutlined,
} from '@ant-design/icons';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
} from 'recharts';
import { governanceApi } from '../../services/governanceApi';

const { Text } = Typography;

// ═══════════════════════════════════════════
//  Tab A: 성능 최적화
// ═══════════════════════════════════════════

const OptimizationSection: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<any>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await governanceApi.getSmartOptimization();
      setData(res);
    } catch {
      message.error('최적화 데이터 로드 실패');
    }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading && !data) return <Spin tip="분석 중..."><div style={{ minHeight: 200 }} /></Spin>;
  if (!data) return null;

  const qp = data.query_patterns || {};
  const indexRecs = data.index_recommendations || [];
  const cacheSugg = data.cache_suggestions || [];
  const peakHours = data.peak_hours || [];

  const peakHour = peakHours.reduce(
    (max: any, h: any) => (h.count > (max?.count || 0) ? h : max), peakHours[0]
  );

  const priorityColor: Record<string, string> = { high: 'red', medium: 'orange', low: 'blue' };
  const priorityLabel: Record<string, string> = { high: '긴급', medium: '보통', low: '낮음' };

  const indexColumns = [
    { title: '테이블', dataIndex: 'table', key: 'table', render: (v: string) => <Text strong>{v}</Text> },
    { title: 'Seq Scan', dataIndex: 'seq_scan', key: 'seq_scan', render: (v: number) => v?.toLocaleString() },
    { title: 'Idx Scan', dataIndex: 'idx_scan', key: 'idx_scan', render: (v: number) => v?.toLocaleString() },
    {
      title: 'Seq 비율', dataIndex: 'ratio', key: 'ratio',
      render: (v: number) => <Progress percent={v} size="small" strokeColor={v > 90 ? '#ff4d4f' : v > 80 ? '#faad14' : '#1890ff'} style={{ width: 100 }} />,
    },
    { title: '행 수', dataIndex: 'rows', key: 'rows', render: (v: number) => v?.toLocaleString() },
    {
      title: '우선순위', dataIndex: 'priority', key: 'priority',
      render: (v: string) => <Tag color={priorityColor[v]}>{priorityLabel[v]}</Tag>,
    },
  ];

  const cacheColumns = [
    {
      title: '테이블 조합', dataIndex: 'tables', key: 'tables',
      render: (tables: string[]) => (
        <Space>{tables?.map((t: string) => <Tag key={t} color="geekblue">{t}</Tag>)}</Space>
      ),
    },
    { title: '빈도', dataIndex: 'frequency', key: 'frequency', render: (v: number) => `${v}회` },
    { title: '추천', dataIndex: 'recommendation', key: 'recommendation', ellipsis: true },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 쿼리" value={qp.total_queries || 0} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="평균 응답시간" value={qp.avg_execution_time_ms || 0} suffix="ms" prefix={<ClockCircleOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="인덱스 추천" value={indexRecs.length} prefix={<NodeIndexOutlined />} valueStyle={{ color: indexRecs.length > 0 ? '#cf1322' : '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="캐시 후보" value={cacheSugg.length} prefix={<HddOutlined />} valueStyle={{ color: '#1890ff' }} />
          </Card>
        </Col>
      </Row>

      <Card title={<><NodeIndexOutlined /> 인덱스 추천</>} size="small">
        <Table
          dataSource={indexRecs}
          columns={indexColumns}
          rowKey="table"
          size="small"
          pagination={{ pageSize: 8 }}
        />
      </Card>

      <Card title={<><HddOutlined /> 조인 패턴 캐시 추천</>} size="small">
        <Table
          dataSource={cacheSugg}
          columns={cacheColumns}
          rowKey={(r: any) => r.tables?.join('-')}
          size="small"
          pagination={false}
        />
      </Card>

      <Card title={<><BarChartOutlined /> 시간대별 쿼리 분포</>} size="small">
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={peakHours}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="hour" tickFormatter={(h: number) => `${h}시`} />
            <YAxis />
            <Tooltip formatter={(v: number) => [`${v}건`, '쿼리 수']} labelFormatter={(h: number) => `${h}시`} />
            <Bar dataKey="count" radius={[4, 4, 0, 0]}>
              {peakHours.map((entry: any, idx: number) => (
                <Cell key={idx} fill={entry.hour === peakHour?.hour ? '#ff4d4f' : '#1890ff'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        {peakHour && peakHour.count > 0 && (
          <Text type="secondary">피크 시간: {peakHour.hour}시 ({peakHour.count}건)</Text>
        )}
      </Card>
    </Space>
  );
};


// ═══════════════════════════════════════════
//  Tab B: 지능형 메타데이터
// ═══════════════════════════════════════════

const MetadataIntelSection: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<any>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await governanceApi.getSmartMetadata();
      setData(res);
    } catch {
      message.error('메타데이터 분석 로드 실패');
    }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading && !data) return <Spin tip="분석 중..."><div style={{ minHeight: 200 }} /></Spin>;
  if (!data) return null;

  const anomalies = data.quality_anomalies || [];
  const impactGraph = data.impact_graph || [];
  const retentionRecs = data.retention_recommendations || [];

  const highImpact = impactGraph.filter((t: any) => t.impact_level === 'high').length;

  const severityColor: Record<string, string> = { high: 'red', medium: 'orange', low: 'blue' };
  const severityLabel: Record<string, string> = { high: '심각', medium: '주의', low: '정보' };
  const typeLabel: Record<string, string> = {
    vacuum_needed: 'VACUUM 필요', stale_stats: '통계 오래됨', stale_data: '정적 데이터',
    high_null: 'NULL 비율 높음',
  };
  const impactColor: Record<string, string> = { high: 'red', medium: 'orange', low: 'green' };
  const retentionColor: Record<string, string> = { '아카이브': 'volcano', '6개월': 'orange', '12개월': 'blue' };

  const anomalyColumns = [
    { title: '테이블', dataIndex: 'table', key: 'table', render: (v: string) => <Text strong>{v}</Text> },
    {
      title: '유형', dataIndex: 'type', key: 'type',
      render: (v: string) => <Tag>{typeLabel[v] || v}</Tag>,
    },
    {
      title: '심각도', dataIndex: 'severity', key: 'severity',
      render: (v: string) => <Tag color={severityColor[v]}>{severityLabel[v]}</Tag>,
    },
    { title: '상세', dataIndex: 'detail', key: 'detail', ellipsis: true },
  ];

  const impactColumns = [
    { title: '테이블', dataIndex: 'table', key: 'table', render: (v: string) => <Text strong>{v}</Text> },
    { title: '행 수', dataIndex: 'row_count', key: 'row_count', render: (v: number) => v?.toLocaleString() },
    {
      title: '연관 테이블', dataIndex: 'dependent_tables', key: 'deps',
      render: (tables: string[]) => (
        <Space wrap>{tables?.map((t: string) => <Tag key={t} color="cyan">{t}</Tag>)}</Space>
      ),
    },
    {
      title: '영향도', dataIndex: 'impact_level', key: 'impact_level',
      render: (v: string) => <Tag color={impactColor[v]}>{v === 'high' ? '높음' : v === 'medium' ? '보통' : '낮음'}</Tag>,
    },
  ];

  const retentionColumns = [
    { title: '테이블', dataIndex: 'table', key: 'table', render: (v: string) => <Text strong>{v}</Text> },
    { title: '행 수', dataIndex: 'row_count', key: 'row_count', render: (v: number) => v?.toLocaleString() },
    { title: '쿼리 빈도', dataIndex: 'query_frequency', key: 'qf', render: (v: number) => `${v}회` },
    {
      title: '추천 기간', dataIndex: 'recommended_retention', key: 'ret',
      render: (v: string) => <Tag color={retentionColor[v] || 'default'}>{v}</Tag>,
    },
    { title: '사유', dataIndex: 'reason', key: 'reason', ellipsis: true },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        <Col xs={12} sm={8}>
          <Card size="small">
            <Statistic title="품질 이상" value={anomalies.length} prefix={<AlertOutlined />}
              valueStyle={{ color: anomalies.length > 0 ? '#cf1322' : '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8}>
          <Card size="small">
            <Statistic title="고영향 테이블" value={highImpact} prefix={<ExperimentOutlined />}
              valueStyle={{ color: highImpact > 0 ? '#faad14' : '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8}>
          <Card size="small">
            <Statistic title="아카이브 추천" value={retentionRecs.filter((r: any) => r.recommended_retention === '아카이브').length}
              prefix={<HddOutlined />} valueStyle={{ color: '#722ed1' }} />
          </Card>
        </Col>
      </Row>

      <Card title={<><WarningOutlined /> 품질 이상 감지</>} size="small">
        <Table
          dataSource={anomalies}
          columns={anomalyColumns}
          rowKey={(r: any) => `${r.table}-${r.type}`}
          size="small"
          pagination={{ pageSize: 8 }}
        />
      </Card>

      <Card title={<><NodeIndexOutlined /> 변경 영향도 분석</>} size="small">
        <Table
          dataSource={impactGraph}
          columns={impactColumns}
          rowKey="table"
          size="small"
          pagination={{ pageSize: 8 }}
        />
      </Card>

      <Card title={<><HddOutlined /> 보관 주기 추천</>} size="small">
        <Table
          dataSource={retentionRecs}
          columns={retentionColumns}
          rowKey="table"
          size="small"
          pagination={{ pageSize: 8 }}
        />
      </Card>
    </Space>
  );
};


// ═══════════════════════════════════════════
//  Main Tab Component
// ═══════════════════════════════════════════

const SmartGovernanceTab: React.FC = () => {
  return (
    <Tabs
      defaultActiveKey="optimization"
      items={[
        {
          key: 'optimization',
          label: <span><ThunderboltOutlined /> 성능 최적화</span>,
          children: <OptimizationSection />,
        },
        {
          key: 'metadata-intel',
          label: <span><ExperimentOutlined /> 지능형 메타데이터</span>,
          children: <MetadataIntelSection />,
        },
      ]}
    />
  );
};

export default SmartGovernanceTab;
