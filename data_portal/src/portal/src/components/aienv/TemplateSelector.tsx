import React from 'react';
import { Card, Row, Col, Tag, Button, Space, Spin } from 'antd';
import {
  FundProjectionScreenOutlined,
  ThunderboltOutlined,
  RobotOutlined,
  ExperimentOutlined,
  EnvironmentOutlined,
  LineChartOutlined,
} from '@ant-design/icons';

const ICON_MAP: Record<string, React.ReactNode> = {
  FundProjectionScreenOutlined: <FundProjectionScreenOutlined />,
  ThunderboltOutlined: <ThunderboltOutlined />,
  RobotOutlined: <RobotOutlined />,
  ExperimentOutlined: <ExperimentOutlined />,
  EnvironmentOutlined: <EnvironmentOutlined />,
  LineChartOutlined: <LineChartOutlined />,
};

export interface TemplateInfo {
  id: string;
  name: string;
  description: string;
  libraries: string[];
  icon: string;
  default_memory: string;
  default_cpu: number;
}

interface TemplateSelectorProps {
  templates: TemplateInfo[];
  loading: boolean;
  onTemplateCreate: (template: TemplateInfo) => void;
}

const TemplateSelector: React.FC<TemplateSelectorProps> = ({
  templates,
  loading,
  onTemplateCreate,
}) => {
  if (loading) {
    return <Spin tip="로딩 중..."><div style={{ minHeight: 200 }} /></Spin>;
  }

  return (
    <Row gutter={[16, 16]}>
      {templates.map((template) => (
        <Col span={12} key={template.id}>
          <Card>
            <Space direction="vertical" style={{ width: '100%' }}>
              <Space>
                {ICON_MAP[template.icon] || <ExperimentOutlined />}
                <strong>{template.name}</strong>
              </Space>
              <p>{template.description}</p>
              <Space wrap>
                {template.libraries.map((lib, i) => (
                  <Tag key={i}>{lib}</Tag>
                ))}
              </Space>
              <div style={{ fontSize: 12, color: '#888' }}>
                기본 설정: CPU {template.default_cpu} cores, 메모리 {template.default_memory}
              </div>
              <Button type="primary" block onClick={() => onTemplateCreate(template)}>
                이 템플릿으로 시작
              </Button>
            </Space>
          </Card>
        </Col>
      ))}
    </Row>
  );
};

export default TemplateSelector;
