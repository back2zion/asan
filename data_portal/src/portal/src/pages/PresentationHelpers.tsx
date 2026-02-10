import React from 'react';
import {
  Card,
  Typography,
  Upload,
  Row,
  Col,
  Space,
  Progress,
  Tag,
  Spin,
  Button,
} from 'antd';
import {
  FileTextOutlined,
  UploadOutlined,
  ExperimentOutlined,
  LoadingOutlined,
  CheckCircleOutlined,
  StopOutlined,
} from '@ant-design/icons';
import type { UploadFile } from 'antd';

const { Title, Text, Paragraph } = Typography;
const { Dragger } = Upload;

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

export const templates = [
  { value: 'asan-medical', label: '아산병원 의료 (기본)', color: '#005BAC' },
  { value: 'academic', label: '학술 발표', color: '#2F4858' },
  { value: 'research', label: '연구 보고서', color: '#1A4D3E' },
  { value: 'clinical', label: '임상 케이스', color: '#C41E3A' },
  { value: 'minimal', label: '미니멀', color: '#333333' },
];

export const steps = [
  { title: '문서 업로드', icon: <UploadOutlined /> },
  { title: '설정', icon: <ExperimentOutlined /> },
  { title: '생성 중', icon: <LoadingOutlined /> },
  { title: '완료', icon: <CheckCircleOutlined /> },
];

/* ------------------------------------------------------------------ */
/*  Sub-components                                                     */
/* ------------------------------------------------------------------ */

interface UploadStepProps {
  fileList: UploadFile[];
  handleUpload: (info: any) => void;
}

export const UploadStep: React.FC<UploadStepProps> = ({ fileList, handleUpload }) => (
  <Card>
    <Row gutter={24}>
      <Col span={16}>
        <Dragger
          accept=".pdf"
          fileList={fileList}
          onChange={handleUpload}
          beforeUpload={() => false}
          multiple={false}
        >
          <p className="ant-upload-drag-icon">
            <FileTextOutlined style={{ fontSize: 48, color: '#005BAC' }} />
          </p>
          <p className="ant-upload-text">
            PDF 문서를 이 영역에 드래그하거나 클릭하여 업로드하세요
          </p>
          <p className="ant-upload-hint">
            지원 형식: PDF (연구 논문, 기술 보고서 등)
          </p>
        </Dragger>
      </Col>
      <Col span={8}>
        <Card size="small" title="지원 문서 유형" style={{ height: '100%' }}>
          <Space direction="vertical" size="small">
            <Tag color="blue">연구 논문</Tag>
            <Tag color="green">기술 보고서</Tag>
            <Tag color="purple">임상 연구 결과</Tag>
            <Tag color="orange">학위 논문</Tag>
            <Tag color="cyan">프로젝트 제안서</Tag>
          </Space>
        </Card>
      </Col>
    </Row>
  </Card>
);

interface GeneratingStepProps {
  outputType: 'slides' | 'poster';
  progress: number;
  jobId: string | null;
  handleCancel: () => void;
}

export const GeneratingStep: React.FC<GeneratingStepProps> = ({
  outputType,
  progress,
  jobId,
  handleCancel,
}) => (
  <Card>
    <div style={{ textAlign: 'center', padding: '40px 0' }}>
      <Spin
        indicator={<LoadingOutlined style={{ fontSize: 48 }} spin />}
        size="large"
      />
      <Title level={4} style={{ marginTop: 24 }}>
        AI가 {outputType === 'poster' ? '포스터를' : '프레젠테이션을'} 생성하고 있습니다...
      </Title>
      <Progress
        percent={progress}
        status="active"
        style={{ maxWidth: 400, margin: '24px auto' }}
      />
      <Paragraph type="secondary">
        문서를 분석하고 핵심 내용을 추출하여 {outputType === 'poster' ? '포스터를' : '슬라이드를'} 구성합니다.
      </Paragraph>
      {jobId && (
        <Text type="secondary" style={{ fontSize: 12 }}>작업 ID: {jobId}</Text>
      )}
      <div style={{ marginTop: 24 }}>
        <Button danger icon={<StopOutlined />} onClick={handleCancel}>
          취소
        </Button>
      </div>
    </div>
  </Card>
);
