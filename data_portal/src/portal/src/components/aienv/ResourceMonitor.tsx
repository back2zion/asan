import React from 'react';
import { Card, Row, Col, Tag, Progress, Button, Space, Spin } from 'antd';
import {
  ReloadOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
} from '@ant-design/icons';

interface SystemResources {
  cpu: { percent: number; cores: number; used_cores: number };
  memory: { total_gb: number; used_gb: number; available_gb: number; percent: number };
  disk: { total_gb: number; used_gb: number; free_gb: number; percent: number };
}

interface GpuInfo {
  index: number;
  name: string;
  utilization_percent: number;
  memory_used_mb: number;
  memory_total_mb: number;
  memory_percent: number;
  temperature: number;
}

interface ResourceMonitorProps {
  systemRes: SystemResources | null;
  gpus: GpuInfo[];
  gpuAvailable: boolean;
  loading: boolean;
  onRefresh: () => void;
}

const ResourceMonitor: React.FC<ResourceMonitorProps> = ({
  systemRes,
  gpus,
  gpuAvailable,
  loading,
  onRefresh,
}) => {
  if (loading) {
    return <Spin tip="리소스 정보 로딩 중..."><div style={{ minHeight: 200 }} /></Spin>;
  }

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<ReloadOutlined />} onClick={onRefresh}>새로고침</Button>
        <Tag icon={<CheckCircleOutlined />} color="success">10초 자동 갱신</Tag>
      </Space>
      <Row gutter={[16, 16]}>
        <Col span={12}>
          <Card title="CPU 사용률">
            <Progress percent={systemRes?.cpu.percent ?? 0} status="active" />
            <p>{systemRes?.cpu.used_cores ?? 0} cores / {systemRes?.cpu.cores ?? 0} cores 사용 중</p>
          </Card>
        </Col>
        <Col span={12}>
          <Card title="메모리 사용률">
            <Progress percent={systemRes?.memory.percent ?? 0} status="active" />
            <p>{systemRes?.memory.used_gb ?? 0} GB / {systemRes?.memory.total_gb ?? 0} GB 사용 중</p>
          </Card>
        </Col>
        <Col span={12}>
          <Card title="디스크 사용률">
            <Progress percent={systemRes?.disk.percent ?? 0} status="active" />
            <p>{systemRes?.disk.used_gb ?? 0} GB / {systemRes?.disk.total_gb ?? 0} GB 사용 중</p>
          </Card>
        </Col>
        {gpuAvailable ? gpus.map((gpu) => (
          <Col span={12} key={gpu.index}>
            <Card title={`GPU #${gpu.index}: ${gpu.name}`}>
              <Progress percent={gpu.utilization_percent} strokeColor="#52c41a" />
              <p>VRAM: {Math.round(gpu.memory_used_mb)} MB / {Math.round(gpu.memory_total_mb)} MB ({gpu.memory_percent}%)</p>
              <p>온도: {gpu.temperature}°C</p>
            </Card>
          </Col>
        )) : (
          <Col span={12}>
            <Card title="GPU">
              <Tag icon={<CloseCircleOutlined />} color="default">GPU를 감지할 수 없습니다 (nvidia-smi 없음)</Tag>
            </Card>
          </Col>
        )}
      </Row>
    </div>
  );
};

export default ResourceMonitor;
