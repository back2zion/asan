import React, { useState, useEffect, useCallback } from 'react';
import { Card, Tabs, Row, Col, Statistic, Typography } from 'antd';
import {
  CodeOutlined,
  CloudServerOutlined,
  ThunderboltOutlined,
  RobotOutlined,
  SwapOutlined,
  ProjectOutlined,
} from '@ant-design/icons';

import ContainerManager from '../components/aienv/ContainerManager';
import TemplateSelector from '../components/aienv/TemplateSelector';
import type { TemplateInfo } from '../components/aienv/TemplateSelector';
import ResourceMonitor from '../components/aienv/ResourceMonitor';
import NotebookManager from '../components/aienv/NotebookManager';
import DatasetManager from '../components/aienv/DatasetManager';
import ProjectManager from '../components/aienv/ProjectManager';

const { Title, Paragraph } = Typography;

const API_BASE = '/api/v1/ai-environment';

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

const AIEnvironment: React.FC = () => {
  // Top-level stats: containers
  const [containerStats, setContainerStats] = useState({ running: 0, total: 0 });

  // Top-level stats: resources (shared between stats cards and ResourceMonitor tab)
  const [systemRes, setSystemRes] = useState<SystemResources | null>(null);
  const [gpus, setGpus] = useState<GpuInfo[]>([]);
  const [gpuAvailable, setGpuAvailable] = useState(false);
  const [resourceLoading, setResourceLoading] = useState(true);

  // Templates (shared between TemplateSelector tab and ContainerManager create-from-template)
  const [templates, setTemplates] = useState<TemplateInfo[]>([]);
  const [templatesLoading, setTemplatesLoading] = useState(true);

  const fetchResources = useCallback(async () => {
    try {
      const [sysR, gpuR] = await Promise.all([
        fetch(`${API_BASE}/resources/system`),
        fetch(`${API_BASE}/resources/gpu`),
      ]);
      if (sysR.ok) {
        const sysRes = await sysR.json();
        setSystemRes(sysRes);
      }
      if (gpuR.ok) {
        const gpuRes = await gpuR.json();
        setGpus(gpuRes.gpus || []);
        setGpuAvailable(gpuRes.available || false);
      }
    } catch {
      /* polling 실패 무시 — 다음 주기에 재시도 */
    } finally {
      setResourceLoading(false);
    }
  }, []);

  const fetchTemplates = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/templates`);
      if (!res.ok) return;
      const data = await res.json();
      setTemplates(data.templates || []);
    } catch {
      /* 템플릿 로드 실패 — 빈 목록 유지 */
    } finally {
      setTemplatesLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchResources();
    fetchTemplates();
  }, [fetchResources, fetchTemplates]);

  useEffect(() => {
    const interval = setInterval(fetchResources, 10000);
    return () => clearInterval(interval);
  }, [fetchResources]);

  const handleContainerStatsChange = useCallback((running: number, total: number) => {
    setContainerStats({ running, total });
  }, []);

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <RobotOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              AI 데이터 분석환경
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              컨테이너 기반 AI 분석환경 및 리소스 관리
            </Paragraph>
          </Col>
        </Row>
      </Card>

      {/* Top stats cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="활성 컨테이너"
              value={containerStats.running}
              suffix={`/ ${containerStats.total}`}
              prefix={<CloudServerOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="CPU 사용률"
              value={systemRes?.cpu.percent ?? '-'}
              suffix="%"
              prefix={<ThunderboltOutlined />}
              valueStyle={{ color: '#1890ff' }}
              loading={resourceLoading}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="메모리"
              value={systemRes ? `${systemRes.memory.used_gb} / ${systemRes.memory.total_gb} GB` : '-'}
              prefix={<CloudServerOutlined />}
              loading={resourceLoading}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="디스크"
              value={systemRes ? `${systemRes.disk.used_gb} / ${systemRes.disk.total_gb} GB` : '-'}
              prefix={<CodeOutlined />}
              loading={resourceLoading}
            />
          </Card>
        </Col>
      </Row>

      <Card title="AI 데이터 분석환경">
        <Tabs
          defaultActiveKey="containers"
          items={[
            {
              key: 'containers',
              label: '컨테이너 관리',
              children: <ContainerManager onStatsChange={handleContainerStatsChange} />,
            },
            {
              key: 'templates',
              label: '분석 템플릿',
              children: (
                <TemplateSelector
                  templates={templates}
                  loading={templatesLoading}
                  onTemplateCreate={(template) => {
                    // Template create opens the container create modal in ContainerManager
                    // For simplicity, this triggers a page-level alert; the ContainerManager handles creation internally
                    window.dispatchEvent(new CustomEvent('aienv:template-create', { detail: template }));
                  }}
                />
              ),
            },
            {
              key: 'monitoring',
              label: '리소스 모니터링',
              children: (
                <ResourceMonitor
                  systemRes={systemRes}
                  gpus={gpus}
                  gpuAvailable={gpuAvailable}
                  loading={resourceLoading}
                  onRefresh={fetchResources}
                />
              ),
            },
            {
              key: 'sharing',
              label: '분석 작업 공유',
              children: <NotebookManager />,
            },
            {
              key: 'datasets',
              label: <span><SwapOutlined /> 데이터셋 이관</span>,
              children: <DatasetManager />,
            },
            {
              key: 'projects',
              label: <span><ProjectOutlined /> 프로젝트 관리</span>,
              children: <ProjectManager />,
            },
          ]}
        />
      </Card>
    </div>
  );
};

export default AIEnvironment;
