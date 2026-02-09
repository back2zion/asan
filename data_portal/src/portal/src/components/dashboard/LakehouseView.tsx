/**
 * CDW/EDW 통합 데이터 레이크하우스 뷰 (DPR-001)
 * 데이터 자산 전체 현황, 추천 데이터셋, 도메인별 분포
 */
import React, { useState, useEffect } from 'react';
import { Card, Row, Col, Statistic, Tag, Table, Progress, Typography, Space, Badge, Tooltip, Spin, Empty, Button } from 'antd';
import {
  DatabaseOutlined, TeamOutlined, TableOutlined, CloudServerOutlined,
  StarOutlined, ArrowRightOutlined, PieChartOutlined, FundOutlined,
  HddOutlined, ExperimentOutlined, DollarOutlined, FileTextOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { PieChart, Pie, BarChart, Bar, XAxis, YAxis, CartesianGrid, Cell, ResponsiveContainer, Tooltip as RTooltip, Treemap } from 'recharts';
import { VISIT_TYPE_COLORS } from './dashboardConstants';

const { Title, Text, Paragraph } = Typography;

// 도메인별 색상
const DOMAIN_COLORS: Record<string, string> = {
  'Clinical': '#006241',
  'Lab/Vital': '#00A0B0',
  'Demographics': '#005BAC',
  'Derived': '#52A67D',
  'Financial': '#FF6F00',
  'Health System': '#722ED1',
  'Unstructured': '#EB2F96',
  'Other': '#8c8c8c',
};

const DOMAIN_ICONS: Record<string, React.ReactNode> = {
  'Clinical': <ExperimentOutlined />,
  'Lab/Vital': <FundOutlined />,
  'Demographics': <TeamOutlined />,
  'Derived': <PieChartOutlined />,
  'Financial': <DollarOutlined />,
  'Health System': <HddOutlined />,
  'Unstructured': <FileTextOutlined />,
};

interface LakehouseData {
  total_tables: number;
  total_rows: number;
  total_rows_label: string;
  patient_count: number;
  assets: { table_name: string; domain: string; row_count: number; size_label: string }[];
  domain_distribution: { domain: string; row_count: number; percentage: number }[];
  recommended_datasets: { table_name: string; reason: string; usage_score: number }[];
  cdw_tables: number;
  edw_tables: number;
  unstructured_tables: number;
}

const LakehouseView: React.FC = () => {
  const navigate = useNavigate();
  const [data, setData] = useState<LakehouseData | null>(null);
  const [loading, setLoading] = useState(true);
  const [visitTypeData, setVisitTypeData] = useState<{type: string; count: number}[]>([]);

  useEffect(() => {
    (async () => {
      try {
        const [lhRes, dashRes] = await Promise.all([
          fetch('/api/v1/catalog-ext/lakehouse-overview'),
          fetch('/api/v1/datamart/dashboard-stats'),
        ]);
        if (lhRes.ok) setData(await lhRes.json());
        if (dashRes.ok) {
          const d = await dashRes.json();
          if (d.visit_type_distribution?.length) setVisitTypeData(d.visit_type_distribution);
        }
      } catch { /* fallback */ }
      setLoading(false);
    })();
  }, []);

  if (loading) return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large"><span style={{ display: 'block', marginTop: 12, color: '#8c8c8c' }}>데이터 레이크하우스 현황 로딩...</span></Spin></div>;
  if (!data) return <Empty description="데이터를 불러올 수 없습니다" />;

  const treemapData = data.assets.filter(a => a.row_count > 0).map(a => ({
    name: a.table_name,
    size: a.row_count,
    domain: a.domain,
    label: a.size_label,
  }));

  const assetColumns = [
    {
      title: '테이블',
      dataIndex: 'table_name',
      key: 'table_name',
      render: (name: string) => (
        <Button type="link" size="small" style={{ padding: 0, color: '#005BAC' }} onClick={() => navigate(`/catalog?q=${name}`)}>
          <TableOutlined /> {name}
        </Button>
      ),
    },
    {
      title: '도메인',
      dataIndex: 'domain',
      key: 'domain',
      width: 120,
      render: (d: string) => <Tag color={DOMAIN_COLORS[d] || '#8c8c8c'}>{d}</Tag>,
    },
    {
      title: '데이터 규모',
      dataIndex: 'size_label',
      key: 'size_label',
      width: 100,
      align: 'right' as const,
      render: (label: string, record: any) => (
        <Text strong>{label}</Text>
      ),
    },
    {
      title: '비중',
      key: 'ratio',
      width: 150,
      render: (_: any, record: any) => {
        const pct = data.total_rows > 0 ? (record.row_count / data.total_rows * 100) : 0;
        return <Progress percent={Math.round(pct * 10) / 10} size="small" strokeColor={DOMAIN_COLORS[record.domain] || '#8c8c8c'} />;
      },
    },
  ];

  return (
    <>
      {/* KPI 카드 */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable style={{ borderLeft: '4px solid #006241' }}>
            <Statistic
              title={<Text type="secondary">총 데이터 자산</Text>}
              value={data.total_tables}
              suffix="테이블"
              prefix={<DatabaseOutlined style={{ color: '#006241' }} />}
            />
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>총 {data.total_rows_label} 레코드</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable style={{ borderLeft: '4px solid #005BAC' }}>
            <Statistic
              title={<Text type="secondary">등록 환자 수</Text>}
              value={data.patient_count}
              suffix="명"
              prefix={<TeamOutlined style={{ color: '#005BAC' }} />}
            />
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>OMOP CDM 기준 합성 데이터</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable style={{ borderLeft: '4px solid #00A0B0' }}>
            <Statistic
              title={<Text type="secondary">CDW 임상 테이블</Text>}
              value={data.cdw_tables}
              suffix="개"
              prefix={<ExperimentOutlined style={{ color: '#00A0B0' }} />}
            />
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>임상·Lab·인구통계·파생</Text>
            </div>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable style={{ borderLeft: '4px solid #FF6F00' }}>
            <Statistic
              title={<Text type="secondary">EDW 운영 테이블</Text>}
              value={data.edw_tables + data.unstructured_tables}
              suffix="개"
              prefix={<CloudServerOutlined style={{ color: '#FF6F00' }} />}
            />
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>재무·의료시스템·비정형</Text>
            </div>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        {/* 도메인별 분포 Pie Chart */}
        <Col xs={24} lg={8}>
          <Card
            title={<><PieChartOutlined /> 도메인별 데이터 분포</>}
            size="small"
            style={{ height: '100%' }}
          >
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie
                  data={data.domain_distribution}
                  dataKey="row_count"
                  nameKey="domain"
                  cx="50%"
                  cy="50%"
                  outerRadius={80}
                  innerRadius={45}
                  label={({ domain, percentage }) => `${domain} ${percentage}%`}
                  labelLine={true}
                >
                  {data.domain_distribution.map((entry, idx) => (
                    <Cell key={idx} fill={DOMAIN_COLORS[entry.domain] || '#8c8c8c'} />
                  ))}
                </Pie>
                <RTooltip formatter={(value: number, name: string) => [`${(value / 1_000_000).toFixed(1)}M 건`, name]} />
              </PieChart>
            </ResponsiveContainer>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, justifyContent: 'center', marginTop: 4 }}>
              {data.domain_distribution.map((d) => (
                <Tag key={d.domain} color={DOMAIN_COLORS[d.domain] || '#8c8c8c'} style={{ fontSize: 10 }}>
                  {DOMAIN_ICONS[d.domain]} {d.domain} ({d.percentage}%)
                </Tag>
              ))}
            </div>
          </Card>
        </Col>

        {/* 진료유형별 분포 Bar Chart */}
        <Col xs={24} lg={8}>
          <Card
            title={<><FundOutlined style={{ color: '#006241' }} /> 진료유형별 분포</>}
            size="small"
            style={{ height: '100%' }}
          >
            {visitTypeData.length > 0 ? (
              <>
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={visitTypeData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                    <XAxis dataKey="type" tick={{ fill: '#64748b', fontSize: 13, fontWeight: 600 }} axisLine={false} tickLine={false} />
                    <YAxis tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={(v) => v >= 1_000_000 ? `${(v / 1_000_000).toFixed(1)}M` : v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v} />
                    <RTooltip formatter={(value: number) => [`${value.toLocaleString()} 건`, '건수']} />
                    <Bar dataKey="count" name="건수" radius={[6, 6, 0, 0]} barSize={60}>
                      {visitTypeData.map((entry, idx) => (
                        <Cell key={idx} fill={VISIT_TYPE_COLORS[entry.type] || '#94a3b8'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, fontFamily: 'monospace', marginTop: 4 }}>
                  <Text style={{ color: '#006241' }}>총 {visitTypeData.reduce((s, d) => s + d.count, 0).toLocaleString()}건</Text>
                  <Text type="secondary">{visitTypeData.length}개 유형</Text>
                </div>
              </>
            ) : (
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 240 }}>
                <Text type="secondary">데이터 로딩 중...</Text>
              </div>
            )}
          </Card>
        </Col>

        {/* 추천 데이터셋 */}
        <Col xs={24} lg={8}>
          <Card
            title={<><StarOutlined style={{ color: '#FF6F00' }} /> 추천 데이터셋</>}
            size="small"
            extra={<Button type="link" size="small" onClick={() => navigate('/catalog')}>전체 보기 <ArrowRightOutlined /></Button>}
            style={{ height: '100%' }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {data.recommended_datasets.map((rec) => (
                <div
                  key={rec.table_name}
                  style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '10px 14px', background: '#fafafa', borderRadius: 8, cursor: 'pointer',
                    border: '1px solid #f0f0f0', transition: 'all 0.2s',
                  }}
                  onClick={() => navigate(`/catalog?q=${rec.table_name}`)}
                  onMouseEnter={(e) => { (e.currentTarget as HTMLDivElement).style.background = '#f0f7ff'; (e.currentTarget as HTMLDivElement).style.borderColor = '#005BAC'; }}
                  onMouseLeave={(e) => { (e.currentTarget as HTMLDivElement).style.background = '#fafafa'; (e.currentTarget as HTMLDivElement).style.borderColor = '#f0f0f0'; }}
                >
                  <Space>
                    <Badge count={rec.usage_score} style={{ backgroundColor: rec.usage_score >= 90 ? '#006241' : '#005BAC', fontSize: 10 }} />
                    <div>
                      <Text strong style={{ color: '#005BAC' }}>{rec.table_name}</Text>
                      <br />
                      <Text type="secondary" style={{ fontSize: 12 }}>{rec.reason}</Text>
                    </div>
                  </Space>
                  <ArrowRightOutlined style={{ color: '#ccc' }} />
                </div>
              ))}
            </div>
          </Card>
        </Col>
      </Row>

      {/* 데이터 자산 Treemap + Table */}
      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} lg={12}>
          <Card title={<><DatabaseOutlined /> 데이터 맵 (규모 기준)</>} size="small">
            <ResponsiveContainer width="100%" height={300}>
              <Treemap
                data={treemapData}
                dataKey="size"
                nameKey="name"
                aspectRatio={4 / 3}
                stroke="#fff"
              >
                {treemapData.map((entry, idx) => (
                  <Cell key={idx} fill={DOMAIN_COLORS[entry.domain] || '#8c8c8c'} />
                ))}
                <RTooltip formatter={(value: number, name: string) => [`${(value / 1_000_000).toFixed(2)}M`, name]} />
              </Treemap>
            </ResponsiveContainer>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title={<><TableOutlined /> 전체 데이터 자산 목록</>} size="small">
            <Table
              dataSource={data.assets}
              columns={assetColumns}
              rowKey="table_name"
              size="small"
              pagination={{ pageSize: 8, size: 'small' }}
              scroll={{ y: 260 }}
            />
          </Card>
        </Col>
      </Row>
    </>
  );
};

export default LakehouseView;
