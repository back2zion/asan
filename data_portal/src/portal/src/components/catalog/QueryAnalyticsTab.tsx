/**
 * 쿼리 분석 탭 (DPR-002: 쿼리 패턴 분석)
 * - Top 검색어/테이블 바 차트 (recharts)
 * - 테이블 공동사용 네트워크 (테이블 형태)
 * - 시간대별 쿼리 볼륨 라인 차트
 * - 기간 선택 (7일/30일/전체)
 */

import React, { useState } from 'react';
import { Card, Row, Col, Segmented, Spin, Empty, Table, Typography, Tag, Space } from 'antd';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Cell,
} from 'recharts';
import { BarChartOutlined, ClockCircleOutlined, LinkOutlined, SearchOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { catalogAnalyticsApi } from '../../services/catalogAnalyticsApi';

const { Text, Title } = Typography;

const PERIOD_OPTIONS = [
  { label: '7일', value: 7 },
  { label: '30일', value: 30 },
  { label: '전체', value: 365 },
];

const CHART_COLORS = ['#005BAC', '#1890FF', '#36CFC9', '#73D13D', '#FADB14', '#FF7A45', '#F759AB', '#B37FEB', '#597EF7', '#52C41A'];

const QueryAnalyticsTab: React.FC = () => {
  const [days, setDays] = useState(7);

  const { data, isLoading } = useQuery({
    queryKey: ['query-patterns', days],
    queryFn: () => catalogAnalyticsApi.getQueryPatterns(days),
  });

  if (isLoading) {
    return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /></div>;
  }

  if (!data) {
    return <Empty description="분석 데이터가 없습니다" />;
  }

  const topTables = (data.top_tables || []).map((item: { table: string; count: number }) => ({
    name: item.table,
    count: item.count,
  }));

  const topSearches = (data.top_searches || []).map((item: { term: string; count: number }) => ({
    name: item.term,
    count: item.count,
  }));

  const hourlyData = (data.hourly_distribution || []).map((item: { hour: number; count: number }) => ({
    hour: `${item.hour}시`,
    count: item.count,
  }));

  const coUsage = (data.co_usage || []).map((item: { table_a: string; table_b: string; count: number }, idx: number) => ({
    key: idx,
    table_a: item.table_a,
    table_b: item.table_b,
    count: item.count,
  }));

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <BarChartOutlined style={{ fontSize: 18, color: '#005BAC' }} />
          <Title level={5} style={{ margin: 0 }}>쿼리 패턴 분석</Title>
          <Tag color="blue">총 {data.total_queries?.toLocaleString() || 0}건</Tag>
        </Space>
        <Segmented
          options={PERIOD_OPTIONS}
          value={days}
          onChange={(val) => setDays(val as number)}
        />
      </div>

      <Row gutter={[16, 16]}>
        {/* Top 테이블 */}
        <Col span={12}>
          <Card title={<><SearchOutlined /> Top 10 조회 테이블</>} size="small">
            {topTables.length > 0 ? (
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={topTables} layout="vertical" margin={{ left: 80, right: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" />
                  <YAxis type="category" dataKey="name" width={80} tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Bar dataKey="count" name="조회 수">
                    {topTables.map((_: unknown, idx: number) => (
                      <Cell key={idx} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="데이터 없음" />
            )}
          </Card>
        </Col>

        {/* Top 검색어 */}
        <Col span={12}>
          <Card title={<><SearchOutlined /> Top 10 검색어</>} size="small">
            {topSearches.length > 0 ? (
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={topSearches} layout="vertical" margin={{ left: 80, right: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" />
                  <YAxis type="category" dataKey="name" width={80} tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Bar dataKey="count" name="검색 횟수" fill="#36CFC9" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="데이터 없음" />
            )}
          </Card>
        </Col>

        {/* 시간대별 쿼리 볼륨 */}
        <Col span={14}>
          <Card title={<><ClockCircleOutlined /> 시간대별 쿼리 볼륨</>} size="small">
            {hourlyData.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <LineChart data={hourlyData} margin={{ left: 10, right: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="count" stroke="#005BAC" strokeWidth={2} dot={{ r: 3 }} name="쿼리 수" />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="데이터 없음" />
            )}
          </Card>
        </Col>

        {/* 테이블 공동사용 */}
        <Col span={10}>
          <Card title={<><LinkOutlined /> 테이블 공동사용</>} size="small">
            <Table
              dataSource={coUsage}
              columns={[
                {
                  title: '테이블 A',
                  dataIndex: 'table_a',
                  key: 'table_a',
                  render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
                },
                {
                  title: '테이블 B',
                  dataIndex: 'table_b',
                  key: 'table_b',
                  render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text>,
                },
                {
                  title: '횟수',
                  dataIndex: 'count',
                  key: 'count',
                  width: 60,
                  align: 'center' as const,
                  render: (v: number) => <Tag color="blue">{v}</Tag>,
                },
              ]}
              pagination={false}
              size="small"
              scroll={{ y: 200 }}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default QueryAnalyticsTab;
