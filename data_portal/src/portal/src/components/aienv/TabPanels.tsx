/**
 * AI 분석 환경 탭 패널들 (templates, ai_analysis, monitoring, projects)
 */

import React from 'react';
import { Card, Button, Typography, Space, Row, Col, Alert, Tag } from 'antd';
import { PlayCircleOutlined, FileTextOutlined } from '@ant-design/icons';
import type { AnalysisTemplate } from '../../services/aiEnvironmentService';

const { Text } = Typography;

export const TemplatesPanel: React.FC<{ templates: AnalysisTemplate[] }> = ({ templates }) => (
  <Card title="분석 템플릿 라이브러리">
    <Alert
      message="템플릿 기반 분석 환경"
      description="탐색적 데이터 분석, 자연어 분석, 공간 분석을 위한 사전 정의된 분석 절차를 제공합니다."
      type="info"
      showIcon
      style={{ borderRadius: '6px', backgroundColor: '#f0f8f3', marginBottom: '16px' }}
    />
    <Row gutter={[16, 16]}>
      {templates.map(template => (
        <Col xs={24} md={8} key={template.id}>
          <Card
            hoverable
            size="small"
            style={{ borderRadius: '8px', border: '1px solid #e9ecef' }}
            actions={[
              <Button type="link" icon={<PlayCircleOutlined />}>실행</Button>
            ]}
          >
            <Card.Meta
              avatar={<FileTextOutlined style={{ color: '#006241', fontSize: '24px' }} />}
              title={template.name}
              description={
                <Space direction="vertical" size="small">
                  <Text>{template.description}</Text>
                  <Space>
                    <Tag color={template.category === 'basic' ? 'green' : 'orange'}>
                      {template.category === 'basic' ? '기본' : '고급'}
                    </Tag>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      예상 시간: {template.estimated_runtime_minutes}분
                    </Text>
                  </Space>
                </Space>
              }
            />
          </Card>
        </Col>
      ))}
    </Row>
  </Card>
);

export const AIAnalysisPanel: React.FC = () => (
  <Card title="생성형 AI 분석 기능">
    <Alert
      message="SOTA LLM 기반 지능형 분석"
      description="GraphRAG와 LangGraph를 활용한 고도화된 AI 분석 기능을 제공합니다. Qwen3-Next, MiniMax M1 등 최신 모델을 지원합니다."
      type="success"
      showIcon
      style={{ borderRadius: '6px', backgroundColor: '#f0f8f3', marginBottom: '16px' }}
    />
    <Row gutter={[16, 16]}>
      <Col xs={24} md={12}>
        <Card type="inner" title="GraphRAG 시스템" size="small">
          <Space direction="vertical" style={{ width: '100%' }}>
            <div style={{ padding: '8px', background: '#f6ffed', borderRadius: '4px' }}>
              <Text strong>지식 그래프 기반 검색</Text>
              <div><Text type="secondary" style={{ fontSize: '12px' }}>의료 지식베이스 연동</Text></div>
            </div>
            <div style={{ padding: '8px', background: '#fff7e6', borderRadius: '4px' }}>
              <Text strong>벡터 검색 증강</Text>
              <div><Text type="secondary" style={{ fontSize: '12px' }}>컨텍스트 기반 응답 생성</Text></div>
            </div>
          </Space>
        </Card>
      </Col>
      <Col xs={24} md={12}>
        <Card type="inner" title="LangGraph 에이전트" size="small">
          <Space direction="vertical" style={{ width: '100%' }}>
            <div style={{ padding: '8px', background: '#e6f7ff', borderRadius: '4px' }}>
              <Text strong>다중 LLM 통합</Text>
              <div><Text type="secondary" style={{ fontSize: '12px' }}>에이전트 기반 워크플로우</Text></div>
            </div>
            <div style={{ padding: '8px', background: '#f9f0ff', borderRadius: '4px' }}>
              <Text strong>도구 연동 시스템</Text>
              <div><Text type="secondary" style={{ fontSize: '12px' }}>데이터베이스 능동 상호작용</Text></div>
            </div>
          </Space>
        </Card>
      </Col>
    </Row>
  </Card>
);

export const MonitoringPanel: React.FC = () => (
  <Card title="실시간 리소스 모니터링">
    <Alert
      message="프로젝트별 리소스 관리"
      description="CPU/Memory/GPU 사용량을 실시간으로 모니터링하고 프로젝트별 리소스 할당을 관리합니다."
      type="warning"
      showIcon
      style={{ borderRadius: '6px', backgroundColor: '#fff8f0', marginBottom: '16px' }}
    />
    <Text>실시간 모니터링 대시보드가 여기에 표시됩니다.</Text>
  </Card>
);

export const ProjectsPanel: React.FC = () => (
  <Card title="프로젝트 및 공유 관리">
    <Alert
      message="분석 작업 공유 및 결과 활용"
      description="분석 프로젝트를 공유하고 권한을 관리할 수 있습니다. 분석 결과를 다양한 포맷으로 다운로드할 수 있습니다."
      type="info"
      showIcon
      style={{ borderRadius: '6px', backgroundColor: '#f0f8f3', marginBottom: '16px' }}
    />
    <Text>프로젝트 관리 기능이 여기에 표시됩니다.</Text>
  </Card>
);
