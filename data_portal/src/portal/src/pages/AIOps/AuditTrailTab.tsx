/**
 * Tab 4: 감사 로그
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Table, Tag, Statistic,
  Button, Space, Input, Tooltip, Spin, DatePicker, Select, Alert, App,
} from 'antd';
import {
  FileSearchOutlined, ReloadOutlined, DownloadOutlined,
} from '@ant-design/icons';
import {
  PieChart, Pie, Cell,
  Tooltip as RTooltip, ResponsiveContainer, BarChart, Bar,
} from 'recharts';
import dayjs from 'dayjs';

import { API_BASE, COLORS_CHART, fetchJSON } from './helpers';

const { Text } = Typography;
const { RangePicker } = DatePicker;

const AuditTrailTab: React.FC = () => {
  const { message } = App.useApp();
  const [logs, setLogs] = useState<any[]>([]);
  const [stats, setStats] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [filters, setFilters] = useState<{
    model?: string; user?: string; dateRange?: [any, any];
  }>({});

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      let url = `${API_BASE}/audit-logs?page=${page}&page_size=15`;
      if (filters.model) url += `&model=${filters.model}`;
      if (filters.user) url += `&user=${encodeURIComponent(filters.user)}`;
      if (filters.dateRange?.[0]) url += `&date_from=${filters.dateRange[0].format('YYYY-MM-DD')}`;
      if (filters.dateRange?.[1]) url += `&date_to=${filters.dateRange[1].format('YYYY-MM-DD')}`;

      const [logData, statsData] = await Promise.all([
        fetchJSON(url),
        fetchJSON(`${API_BASE}/audit-logs/stats`),
      ]);
      setLogs(logData.logs || []);
      setTotal(logData.total || 0);
      setStats(statsData);
    } catch {
      message.error('감사 로그 로드 실패');
    } finally {
      setLoading(false);
    }
  }, [page, filters]);

  useEffect(() => { loadData(); }, [loadData]);

  const handleExport = () => {
    let url = `${API_BASE}/audit/export?`;
    if (filters.model) url += `model=${filters.model}&`;
    if (filters.dateRange?.[0]) url += `date_from=${filters.dateRange[0].format('YYYY-MM-DD')}&`;
    if (filters.dateRange?.[1]) url += `date_to=${filters.dateRange[1].format('YYYY-MM-DD')}&`;
    window.open(url, '_blank');
  };

  const columns = [
    { title: '시간', dataIndex: 'timestamp', key: 'timestamp', width: 160,
      render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm:ss') : '-' },
    { title: '사용자', dataIndex: 'user', key: 'user', width: 100 },
    { title: '모델', dataIndex: 'model', key: 'model', width: 120,
      render: (v: string) => <Tag color="blue">{v || '-'}</Tag> },
    { title: '유형', dataIndex: 'query_type', key: 'query_type', width: 80,
      render: (v: string) => <Tag>{v || '-'}</Tag> },
    { title: '지연(ms)', dataIndex: 'latency_ms', key: 'latency_ms', width: 90,
      render: (v: number) => v ? <Text>{v.toLocaleString()}</Text> : '-' },
    { title: '토큰', dataIndex: 'tokens', key: 'tokens', width: 70 },
    { title: 'PII', dataIndex: 'pii_count', key: 'pii_count', width: 60,
      render: (v: number) => v > 0 ? <Tag color="red">{v}</Tag> : <Text type="secondary">0</Text> },
    { title: '환각', dataIndex: 'hallucination_status', key: 'hallucination_status', width: 80,
      render: (v: string) => {
        if (v === 'pass') return <Tag color="green">Pass</Tag>;
        if (v === 'warning') return <Tag color="orange">Warning</Tag>;
        if (v === 'fail') return <Tag color="red">Fail</Tag>;
        return <Tag>-</Tag>;
      } },
    { title: '질의', dataIndex: 'query', key: 'query', ellipsis: true,
      render: (v: string) => <Tooltip title={v}><Text>{v?.substring(0, 50) || '-'}</Text></Tooltip> },
  ];

  const modelDistData = stats?.model_distribution
    ? Object.entries(stats.model_distribution).map(([k, v]) => ({ name: k, value: v }))
    : [];

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} md={6}>
          <Card size="small"><Statistic title="총 질의 수" value={stats?.total_queries || 0} prefix={<FileSearchOutlined />} /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card size="small"><Statistic title="평균 지연" value={stats?.avg_latency_ms || 0} suffix="ms" /></Card>
        </Col>
        <Col xs={12} md={6}>
          <Card size="small">
            <Text type="secondary" style={{ fontSize: 12 }}>모델별 분포</Text>
            {modelDistData.length > 0 ? (
              <ResponsiveContainer width="100%" height={80}>
                <PieChart>
                  <Pie data={modelDistData} cx="50%" cy="50%" innerRadius={20} outerRadius={35} dataKey="value">
                    {modelDistData.map((_, idx) => <Cell key={idx} fill={COLORS_CHART[idx % COLORS_CHART.length]} />)}
                  </Pie>
                  <RTooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : <Text type="secondary">데이터 없음</Text>}
          </Card>
        </Col>
        <Col xs={12} md={6}>
          <Card size="small">
            <Text type="secondary" style={{ fontSize: 12 }}>일별 추이</Text>
            {stats?.daily_counts?.length > 0 ? (
              <ResponsiveContainer width="100%" height={80}>
                <BarChart data={stats.daily_counts}><Bar dataKey="count" fill="#005BAC" /></BarChart>
              </ResponsiveContainer>
            ) : <Text type="secondary">데이터 없음</Text>}
          </Card>
        </Col>
      </Row>

      {stats?.note && <Alert message={stats.note} type="info" showIcon style={{ marginBottom: 16 }} />}

      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Select placeholder="모델 필터" allowClear style={{ width: 160 }}
            onChange={(v) => { setFilters((f) => ({ ...f, model: v })); setPage(1); }}
            options={[
              { value: 'xiyan-sql', label: 'XiYanSQL' },
              { value: 'qwen3-32b', label: 'Qwen3-32B' },
              { value: 'bioclinical-bert', label: 'BioClinicalBERT' },
            ]}
          />
          <Input placeholder="사용자" allowClear style={{ width: 140 }}
            onChange={(e) => { setFilters((f) => ({ ...f, user: e.target.value })); setPage(1); }}
          />
          <RangePicker size="middle"
            onChange={(dates) => { setFilters((f) => ({ ...f, dateRange: dates as any })); setPage(1); }}
          />
          <Button icon={<ReloadOutlined />} onClick={loadData}>새로고침</Button>
          <Button icon={<DownloadOutlined />} onClick={handleExport}>CSV 내보내기</Button>
        </Space>
      </Card>

      <Card size="small">
        <Table
          dataSource={logs} columns={columns} rowKey="id" size="small" loading={loading}
          pagination={{
            current: page, pageSize: 15, total, showSizeChanger: false,
            showTotal: (t) => `총 ${t}건`,
            onChange: (p) => setPage(p),
          }}
          scroll={{ x: 900 }}
        />
      </Card>
    </div>
  );
};

export default AuditTrailTab;
