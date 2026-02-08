/**
 * Tab 2: 리소스 모니터링 (실제 psutil 데이터)
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Table, Tag, Statistic, Progress,
  Button, Spin, App,
} from 'antd';
import {
  DashboardOutlined, ReloadOutlined, CloudServerOutlined, ThunderboltOutlined,
} from '@ant-design/icons';

import { API_BASE, COLORS_CHART, fetchJSON } from './helpers';

const { Text } = Typography;

const ResourceMonitoringTab: React.FC = () => {
  const { message } = App.useApp();
  const [overview, setOverview] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const ov = await fetchJSON(`${API_BASE}/resources/overview`);
      setOverview(ov);
    } catch {
      message.error('리소스 데이터 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  if (loading) return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /></div>;

  const sys = overview?.system;
  const gpuModels = overview?.gpu_models || [];
  const topProcs = overview?.top_processes || [];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 16 }}>
        <Button icon={<ReloadOutlined />} onClick={loadAll}>새로고침</Button>
      </div>

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

      <Card size="small" title="활성 프로세스 (CPU > 1%)">
        <Table
          size="small"
          dataSource={topProcs.map((p: any, i: number) => ({ ...p, key: i }))}
          pagination={false}
          columns={[
            { title: 'PID', dataIndex: 'pid', width: 80 },
            { title: '프로세스', dataIndex: 'name', ellipsis: true },
            { title: 'CPU %', dataIndex: 'cpu_percent', width: 80, render: (v: number) => <Tag color={v > 50 ? 'red' : v > 20 ? 'orange' : 'green'}>{v?.toFixed(1)}%</Tag> },
            { title: 'Memory %', dataIndex: 'memory_percent', width: 100, render: (v: number) => `${v?.toFixed(1)}%` },
          ]}
        />
      </Card>
    </div>
  );
};

export default ResourceMonitoringTab;
