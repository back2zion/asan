/**
 * AI 운영관리 페이지 (AAR-003)
 * 4개 탭: 모델 관리, 리소스 모니터링, AI 안전성, 감사 로그
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Tabs, Typography, Row, Col, Table, Tag, Badge, Statistic, Progress,
  Button, Space, Switch, Input, Tooltip, Spin, message, DatePicker,
  Select, Descriptions, Modal,
} from 'antd';
import {
  SettingOutlined, RobotOutlined, DashboardOutlined, SafetyCertificateOutlined,
  FileSearchOutlined, CheckCircleOutlined, WarningOutlined, CloseCircleOutlined,
  ReloadOutlined, DownloadOutlined, CloudServerOutlined, ThunderboltOutlined,
  EyeOutlined, EyeInvisibleOutlined, ExperimentOutlined,
} from '@ant-design/icons';
import {
  PieChart, Pie, Cell, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip as RTooltip, Legend, ResponsiveContainer, BarChart, Bar,
} from 'recharts';
import dayjs from 'dayjs';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;
const { RangePicker } = DatePicker;

const API_BASE = '/api/v1/ai-ops';

// --- API helpers ---
async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function postJSON(url: string, body: any) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function putJSON(url: string, body: any) {
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// --- Colors ---
const COLORS_CHART = ['#52c41a', '#faad14', '#ff4d4f', '#1890ff', '#722ed1'];
const STATUS_COLOR: Record<string, string> = {
  healthy: 'green',
  unhealthy: 'orange',
  offline: 'red',
  unknown: 'default',
};
const HALL_COLOR: Record<string, string> = {
  pass: '#52c41a',
  warning: '#faad14',
  fail: '#ff4d4f',
};

// ============================================================
//  Tab 1: AI 모델 관리
// ============================================================
const ModelLifecycleTab: React.FC = () => {
  const [models, setModels] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [metricsModal, setMetricsModal] = useState<any>(null);
  const [metrics, setMetrics] = useState<any>(null);

  const loadModels = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/models`);
      setModels(data.models || []);
    } catch {
      message.error('모델 목록 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadModels(); }, [loadModels]);

  const openMetrics = async (modelId: string) => {
    setMetricsModal(modelId);
    try {
      const data = await fetchJSON(`${API_BASE}/models/${modelId}/metrics`);
      setMetrics(data);
    } catch {
      message.error('지표 로드 실패');
    }
  };

  const ftStageTag = (stage: string) => {
    const map: Record<string, { color: string; label: string }> = {
      completed: { color: 'green', label: '완료' },
      in_progress: { color: 'blue', label: '진행 중' },
      planned: { color: 'default', label: '계획' },
    };
    const s = map[stage] || { color: 'default', label: stage };
    return <Tag color={s.color}>{s.label}</Tag>;
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Text type="secondary">등록된 AI 모델의 상태와 성능을 관리합니다.</Text>
        <Button icon={<ReloadOutlined />} onClick={loadModels} loading={loading}>새로고침</Button>
      </div>

      <Row gutter={[16, 16]}>
        {models.map((m) => (
          <Col xs={24} md={8} key={m.id}>
            <Card
              hoverable
              title={
                <Space>
                  <Badge status={m.status === 'healthy' ? 'success' : m.status === 'offline' ? 'error' : 'warning'} />
                  <span>{m.name}</span>
                </Space>
              }
              extra={<Tag color={STATUS_COLOR[m.status]}>{m.status}</Tag>}
              actions={[
                <Tooltip title="성능 지표" key="metrics">
                  <DashboardOutlined onClick={() => openMetrics(m.id)} />
                </Tooltip>,
              ]}
            >
              <Descriptions column={1} size="small">
                <Descriptions.Item label="유형">{m.type}</Descriptions.Item>
                <Descriptions.Item label="버전">{m.version}</Descriptions.Item>
                <Descriptions.Item label="파라미터">{m.parameters}</Descriptions.Item>
                <Descriptions.Item label="GPU 메모리">{(m.gpu_memory_mb / 1024).toFixed(1)} GB</Descriptions.Item>
                <Descriptions.Item label="Fine-tuning">{ftStageTag(m.fine_tuning?.stage)}</Descriptions.Item>
              </Descriptions>
              {m.fine_tuning?.accuracy && (
                <div style={{ marginTop: 8 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>정확도</Text>
                  <Progress percent={m.fine_tuning.accuracy} size="small" strokeColor="#005BAC" />
                </div>
              )}
            </Card>
          </Col>
        ))}
      </Row>

      {loading && <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>}

      <Modal
        title={`모델 성능 지표 — ${metricsModal}`}
        open={!!metricsModal}
        onCancel={() => { setMetricsModal(null); setMetrics(null); }}
        footer={null}
        width={600}
      >
        {metrics ? (
          <div>
            <Row gutter={16}>
              <Col span={8}><Statistic title="총 요청" value={metrics.total_requests} /></Col>
              <Col span={8}><Statistic title="평균 지연" value={metrics.avg_latency_ms} suffix="ms" /></Col>
              <Col span={8}><Statistic title="오류율" value={metrics.error_rate} suffix="%" valueStyle={{ color: metrics.error_rate > 5 ? '#ff4d4f' : '#52c41a' }} /></Col>
            </Row>
            {metrics.daily_trend?.length > 0 && (
              <div style={{ marginTop: 24 }}>
                <Text strong>일별 요청 추이</Text>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={metrics.daily_trend}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                    <YAxis />
                    <RTooltip />
                    <Bar dataKey="count" fill="#005BAC" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>
        )}
      </Modal>
    </div>
  );
};

// ============================================================
//  Tab 2: 리소스 모니터링
// ============================================================
const ResourceMonitoringTab: React.FC = () => {
  const [overview, setOverview] = useState<any>(null);
  const [quotas, setQuotas] = useState<any[]>([]);
  const [usageHistory, setUsageHistory] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const [ov, qt, uh] = await Promise.all([
        fetchJSON(`${API_BASE}/resources/overview`),
        fetchJSON(`${API_BASE}/resources/quotas`),
        fetchJSON(`${API_BASE}/resources/usage-history?hours=24`),
      ]);
      setOverview(ov);
      setQuotas(qt.quotas || []);
      setUsageHistory(uh.data || []);
    } catch {
      message.error('리소스 데이터 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  const quotaColumns = [
    { title: '역할', dataIndex: 'role', key: 'role', render: (v: string) => <Tag color="blue">{v}</Tag> },
    {
      title: 'GPU 시간', key: 'gpu',
      render: (_: any, r: any) => (
        <Progress percent={Math.round(r.used_gpu_hours / r.max_gpu_hours * 100)} size="small"
          format={() => `${r.used_gpu_hours}/${r.max_gpu_hours}h`} />
      ),
    },
    {
      title: '일일 쿼리', key: 'queries',
      render: (_: any, r: any) => (
        <Progress percent={Math.round(r.used_queries_day / r.max_queries_day * 100)} size="small"
          format={() => `${r.used_queries_day}/${r.max_queries_day}`} />
      ),
    },
    {
      title: '스토리지', key: 'storage',
      render: (_: any, r: any) => (
        <Progress percent={Math.round(r.used_storage_gb / r.max_storage_gb * 100)} size="small"
          format={() => `${r.used_storage_gb}/${r.max_storage_gb}GB`} />
      ),
    },
  ];

  if (loading) return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /></div>;

  const sys = overview?.system;
  const gpuModels = overview?.gpu_models || [];

  return (
    <div>
      {/* 시스템 리소스 요약 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="CPU 사용률" value={sys?.cpu_percent || 0} suffix="%" prefix={<ThunderboltOutlined />} />
            <Progress percent={sys?.cpu_percent || 0} showInfo={false} strokeColor="#005BAC" style={{ marginTop: 4 }} />
            <Text type="secondary" style={{ fontSize: 12 }}>{sys?.cpu_cores || 0} 코어</Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="메모리 사용률" value={sys?.memory_percent || 0} suffix="%" prefix={<CloudServerOutlined />} />
            <Progress percent={sys?.memory_percent || 0} showInfo={false} strokeColor="#52c41a" style={{ marginTop: 4 }} />
            <Text type="secondary" style={{ fontSize: 12 }}>{sys?.memory_used_gb || 0} / {sys?.memory_total_gb || 0} GB</Text>
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="디스크 사용률" value={sys?.disk_percent || 0} suffix="%" prefix={<DashboardOutlined />} />
            <Progress percent={sys?.disk_percent || 0} showInfo={false} strokeColor="#faad14" style={{ marginTop: 4 }} />
            <Text type="secondary" style={{ fontSize: 12 }}>{sys?.disk_used_gb || 0} / {sys?.disk_total_gb || 0} GB</Text>
          </Card>
        </Col>
      </Row>

      {/* 모델별 GPU 메모리 */}
      <Card size="small" title="모델별 GPU 메모리 할당" style={{ marginBottom: 16 }}>
        {gpuModels.map((gm: any, idx: number) => (
          <div key={idx} style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Text>{gm.model}</Text>
              <Text type="secondary">{(gm.gpu_memory_mb / 1024).toFixed(1)} GB</Text>
            </div>
            <Progress
              percent={Math.round(gm.gpu_memory_mb / (overview?.total_gpu_allocated_mb || 1) * 100)}
              showInfo={false}
              strokeColor={COLORS_CHART[idx % COLORS_CHART.length]}
            />
          </div>
        ))}
        <Text type="secondary" style={{ fontSize: 12 }}>
          총 할당: {((overview?.total_gpu_allocated_mb || 0) / 1024).toFixed(1)} GB
        </Text>
      </Card>

      {/* 시간대별 사용량 트렌드 */}
      <Card size="small" title="24시간 사용량 트렌드" style={{ marginBottom: 16 }}>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={usageHistory}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" tick={{ fontSize: 10 }} interval={2} />
            <YAxis unit="%" />
            <RTooltip />
            <Legend />
            <Line type="monotone" dataKey="cpu_percent" stroke="#005BAC" name="CPU" dot={false} strokeWidth={2} />
            <Line type="monotone" dataKey="memory_percent" stroke="#52c41a" name="메모리" dot={false} strokeWidth={2} />
            <Line type="monotone" dataKey="gpu_percent" stroke="#faad14" name="GPU" dot={false} strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </Card>

      {/* 역할별 쿼터 */}
      <Card size="small" title="역할별 리소스 쿼터">
        <Table dataSource={quotas} columns={quotaColumns} rowKey="role_en" pagination={false} size="small" />
      </Card>
    </div>
  );
};

// ============================================================
//  Tab 3: AI 안전성
// ============================================================
const SafetyTab: React.FC = () => {
  const [patterns, setPatterns] = useState<any[]>([]);
  const [hallStats, setHallStats] = useState<any>(null);
  const [testText, setTestText] = useState('');
  const [testResult, setTestResult] = useState<any>(null);
  const [testLoading, setTestLoading] = useState(false);
  const [loading, setLoading] = useState(true);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [p, h] = await Promise.all([
        fetchJSON(`${API_BASE}/safety/pii-patterns`),
        fetchJSON(`${API_BASE}/safety/hallucination-stats`),
      ]);
      setPatterns(p.patterns || []);
      setHallStats(h);
    } catch {
      message.error('안전성 데이터 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  const togglePattern = async (patternId: string, enabled: boolean) => {
    try {
      await putJSON(`${API_BASE}/safety/pii-patterns/${patternId}`, { enabled });
      setPatterns((prev) => prev.map((p) => p.id === patternId ? { ...p, enabled } : p));
      message.success(`패턴 ${enabled ? '활성화' : '비활성화'}됨`);
    } catch {
      message.error('패턴 업데이트 실패');
    }
  };

  const runPIITest = async () => {
    if (!testText.trim()) return;
    setTestLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/safety/test-pii`, { text: testText });
      setTestResult(data);
    } catch {
      message.error('PII 테스트 실패');
    } finally {
      setTestLoading(false);
    }
  };

  const piiColumns = [
    { title: '이름', dataIndex: 'name', key: 'name' },
    { title: 'ID', dataIndex: 'id', key: 'id', render: (v: string) => <Tag>{v}</Tag> },
    { title: '설명', dataIndex: 'description', key: 'description' },
    { title: '치환값', dataIndex: 'replacement', key: 'replacement', render: (v: string) => <Text code>{v}</Text> },
    {
      title: '활성화', key: 'enabled',
      render: (_: any, r: any) => (
        <Switch checked={r.enabled} onChange={(v) => togglePattern(r.id, v)} size="small" />
      ),
    },
  ];

  // 환각 검증 파이 차트 데이터
  const hallPieData = hallStats ? [
    { name: 'Pass', value: hallStats.pass_count, color: HALL_COLOR.pass },
    { name: 'Warning', value: hallStats.warning_count, color: HALL_COLOR.warning },
    { name: 'Fail', value: hallStats.fail_count, color: HALL_COLOR.fail },
  ] : [];

  if (loading) return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /></div>;

  return (
    <div>
      <Row gutter={[16, 16]}>
        {/* PII 마스킹 패턴 */}
        <Col span={24}>
          <Card size="small" title={<><EyeInvisibleOutlined /> PII 탐지/마스킹 패턴</>}>
            <Table dataSource={patterns} columns={piiColumns} rowKey="id" pagination={false} size="small" />
          </Card>
        </Col>

        {/* PII 테스트 */}
        <Col xs={24} md={12}>
          <Card size="small" title={<><ExperimentOutlined /> PII 탐지 테스트</>}>
            <TextArea
              rows={4}
              placeholder="테스트할 텍스트를 입력하세요...&#10;예: 환자 홍길동 주민번호 880101-1234567, 전화 010-1234-5678"
              value={testText}
              onChange={(e) => setTestText(e.target.value)}
            />
            <Button
              type="primary"
              onClick={runPIITest}
              loading={testLoading}
              style={{ marginTop: 8 }}
              icon={<SafetyCertificateOutlined />}
            >
              PII 탐지 실행
            </Button>

            {testResult && (
              <div style={{ marginTop: 16 }}>
                <Text strong>탐지 결과: {testResult.pii_count}건</Text>
                {testResult.findings?.map((f: any, i: number) => (
                  <Tag color="red" key={i} style={{ display: 'block', margin: '4px 0' }}>
                    {f.pattern_name}: {f.matched_text}
                  </Tag>
                ))}
                <div style={{ marginTop: 8, padding: 12, background: '#f6ffed', borderRadius: 6, border: '1px solid #b7eb8f' }}>
                  <Text strong style={{ color: '#389e0d' }}>마스킹 결과:</Text>
                  <div style={{ marginTop: 4 }}>{testResult.masked_text}</div>
                </div>
              </div>
            )}
          </Card>
        </Col>

        {/* 환각 검증 통계 */}
        <Col xs={24} md={12}>
          <Card size="small" title={<><SafetyCertificateOutlined /> 환각 검증 통계</>}>
            {hallStats && (
              <>
                <Row gutter={8} style={{ marginBottom: 16 }}>
                  <Col span={8}>
                    <Statistic title="검증 완료" value={hallStats.total_verified} valueStyle={{ fontSize: 20 }} />
                  </Col>
                  <Col span={8}>
                    <Statistic title="통과율" value={hallStats.pass_rate} suffix="%" valueStyle={{ color: '#52c41a', fontSize: 20 }} />
                  </Col>
                  <Col span={8}>
                    <Statistic title="실패" value={hallStats.fail_count} valueStyle={{ color: '#ff4d4f', fontSize: 20 }} />
                  </Col>
                </Row>
                <ResponsiveContainer width="100%" height={200}>
                  <PieChart>
                    <Pie
                      data={hallPieData}
                      cx="50%"
                      cy="50%"
                      innerRadius={50}
                      outerRadius={80}
                      dataKey="value"
                      label={({ name, value }) => `${name}: ${value}`}
                    >
                      {hallPieData.map((entry, idx) => (
                        <Cell key={idx} fill={entry.color} />
                      ))}
                    </Pie>
                    <RTooltip />
                  </PieChart>
                </ResponsiveContainer>
              </>
            )}
          </Card>
        </Col>
      </Row>
    </div>
  );
};

// ============================================================
//  Tab 4: 감사 로그
// ============================================================
const AuditTrailTab: React.FC = () => {
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
    let url = `${API_BASE}/audit-logs/export?`;
    if (filters.model) url += `model=${filters.model}&`;
    if (filters.dateRange?.[0]) url += `date_from=${filters.dateRange[0].format('YYYY-MM-DD')}&`;
    if (filters.dateRange?.[1]) url += `date_to=${filters.dateRange[1].format('YYYY-MM-DD')}&`;
    window.open(url, '_blank');
  };

  const columns = [
    {
      title: '시간', dataIndex: 'timestamp', key: 'timestamp', width: 160,
      render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm:ss') : '-',
    },
    { title: '사용자', dataIndex: 'user', key: 'user', width: 100 },
    {
      title: '모델', dataIndex: 'model', key: 'model', width: 120,
      render: (v: string) => <Tag color="blue">{v || '-'}</Tag>,
    },
    {
      title: '유형', dataIndex: 'query_type', key: 'query_type', width: 80,
      render: (v: string) => <Tag>{v || '-'}</Tag>,
    },
    {
      title: '지연(ms)', dataIndex: 'latency_ms', key: 'latency_ms', width: 90,
      render: (v: number) => v ? <Text>{v.toLocaleString()}</Text> : '-',
    },
    { title: '토큰', dataIndex: 'tokens', key: 'tokens', width: 70 },
    {
      title: 'PII', dataIndex: 'pii_count', key: 'pii_count', width: 60,
      render: (v: number) => v > 0 ? <Tag color="red">{v}</Tag> : <Text type="secondary">0</Text>,
    },
    {
      title: '환각', dataIndex: 'hallucination_status', key: 'hallucination_status', width: 80,
      render: (v: string) => {
        if (v === 'pass') return <Tag color="green">Pass</Tag>;
        if (v === 'warning') return <Tag color="orange">Warning</Tag>;
        if (v === 'fail') return <Tag color="red">Fail</Tag>;
        return <Tag>-</Tag>;
      },
    },
    {
      title: '질의', dataIndex: 'query', key: 'query', ellipsis: true,
      render: (v: string) => <Tooltip title={v}><Text>{v?.substring(0, 50) || '-'}</Text></Tooltip>,
    },
  ];

  // 모델 분포 차트 데이터
  const modelDistData = stats?.model_distribution
    ? Object.entries(stats.model_distribution).map(([k, v]) => ({ name: k, value: v }))
    : [];

  return (
    <div>
      {/* 통계 요약 */}
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
                    {modelDistData.map((_, idx) => (
                      <Cell key={idx} fill={COLORS_CHART[idx % COLORS_CHART.length]} />
                    ))}
                  </Pie>
                  <RTooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <Text type="secondary">데이터 없음</Text>
            )}
          </Card>
        </Col>
        <Col xs={12} md={6}>
          <Card size="small">
            <Text type="secondary" style={{ fontSize: 12 }}>일별 추이</Text>
            {stats?.daily_counts?.length > 0 ? (
              <ResponsiveContainer width="100%" height={80}>
                <BarChart data={stats.daily_counts}>
                  <Bar dataKey="count" fill="#005BAC" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <Text type="secondary">데이터 없음</Text>
            )}
          </Card>
        </Col>
      </Row>

      {/* 필터 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Select
            placeholder="모델 필터"
            allowClear
            style={{ width: 160 }}
            onChange={(v) => { setFilters((f) => ({ ...f, model: v })); setPage(1); }}
            options={[
              { value: 'xiyan-sql', label: 'XiYanSQL' },
              { value: 'qwen3-32b', label: 'Qwen3-32B' },
              { value: 'bioclinical-bert', label: 'BioClinicalBERT' },
            ]}
          />
          <Input
            placeholder="사용자"
            allowClear
            style={{ width: 140 }}
            onChange={(e) => { setFilters((f) => ({ ...f, user: e.target.value })); setPage(1); }}
          />
          <RangePicker
            size="middle"
            onChange={(dates) => { setFilters((f) => ({ ...f, dateRange: dates as any })); setPage(1); }}
          />
          <Button icon={<ReloadOutlined />} onClick={loadData}>새로고침</Button>
          <Button icon={<DownloadOutlined />} onClick={handleExport}>CSV 내보내기</Button>
        </Space>
      </Card>

      {/* 로그 테이블 */}
      <Card size="small">
        <Table
          dataSource={logs}
          columns={columns}
          rowKey="id"
          size="small"
          loading={loading}
          pagination={{
            current: page,
            pageSize: 15,
            total,
            showSizeChanger: false,
            showTotal: (t) => `총 ${t}건`,
            onChange: (p) => setPage(p),
          }}
          scroll={{ x: 900 }}
        />
      </Card>
    </div>
  );
};

// ============================================================
//  메인 페이지
// ============================================================
const tabItems = [
  {
    key: 'models',
    label: <span><RobotOutlined /> AI 모델 관리</span>,
    children: <ModelLifecycleTab />,
  },
  {
    key: 'resources',
    label: <span><DashboardOutlined /> 리소스 모니터링</span>,
    children: <ResourceMonitoringTab />,
  },
  {
    key: 'safety',
    label: <span><SafetyCertificateOutlined /> AI 안전성</span>,
    children: <SafetyTab />,
  },
  {
    key: 'audit',
    label: <span><FileSearchOutlined /> 감사 로그</span>,
    children: <AuditTrailTab />,
  },
];

const AIOps: React.FC = () => {
  return (
    <div>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#005BAC', marginRight: '12px', fontSize: '28px' }} />
              AI 운영관리
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              AI 모델 라이프사이클 관리 · 리소스 모니터링 · 안전성 · 감사 로그
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Card style={{ marginTop: 16 }}>
        <Tabs items={tabItems} defaultActiveKey="models" destroyOnHidden />
      </Card>
    </div>
  );
};

export default AIOps;
