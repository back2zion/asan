import React, { useState, useRef } from 'react';
import {
  Card,
  Typography,
  Upload,
  Button,
  Row,
  Col,
  Steps,
  Space,
  Select,
  Input,
  Progress,
  Alert,
  Tag,
  Divider,
  Radio,
  Spin,
  App,
} from 'antd';
import {
  UploadOutlined,
  FileTextOutlined,
  FilePptOutlined,
  PictureOutlined,
  RocketOutlined,
  DownloadOutlined,
  CheckCircleOutlined,
  LoadingOutlined,
  BulbOutlined,
  ExperimentOutlined,
  StopOutlined,
} from '@ant-design/icons';
import type { UploadFile } from 'antd';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const { Dragger } = Upload;

const Presentation: React.FC = () => {
  const { message: messageApi } = App.useApp();
  const [currentStep, setCurrentStep] = useState(0);
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [outputType, setOutputType] = useState<'slides' | 'poster'>('slides');
  const [template, setTemplate] = useState<string>('asan-medical');
  const [slideCount, setSlideCount] = useState<number>(10);
  const [generating, setGenerating] = useState(false);
  const [progress, setProgress] = useState(0);
  const [additionalPrompt, setAdditionalPrompt] = useState('');
  const [jobId, setJobId] = useState<string | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const templates = [
    { value: 'asan-medical', label: '아산병원 의료 (기본)', color: '#005BAC' },
    { value: 'academic', label: '학술 발표', color: '#2F4858' },
    { value: 'research', label: '연구 보고서', color: '#1A4D3E' },
    { value: 'clinical', label: '임상 케이스', color: '#C41E3A' },
    { value: 'minimal', label: '미니멀', color: '#333333' },
  ];

  const handleUpload = (info: any) => {
    const { fileList: newFileList } = info;
    setFileList(newFileList);
    if (newFileList.length > 0) {
      setCurrentStep(1);
    }
  };

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const handleGenerate = async () => {
    if (fileList.length === 0) {
      messageApi.warning('문서를 먼저 업로드해주세요.');
      return;
    }

    setGenerating(true);
    setProgress(0);
    setCurrentStep(2);
    setJobId(null);

    try {
      const formData = new FormData();
      const file = fileList[0].originFileObj;
      if (!file) {
        throw new Error('파일을 찾을 수 없습니다.');
      }
      formData.append('file', file);
      formData.append('output_type', outputType);
      formData.append('template', template);
      formData.append('slide_count', slideCount.toString());
      formData.append('additional_prompt', additionalPrompt);

      // 1. 작업 시작
      const response = await fetch('/api/v1/presentation/generate', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || '프레젠테이션 생성에 실패했습니다.');
      }

      const result = await response.json();
      const jid = result.job_id;
      setJobId(jid);
      setProgress(10);
      messageApi.info('Paper2Slides 작업이 시작되었습니다. 1~5분 소요됩니다.');

      // 2. 상태 폴링
      let attempts = 0;
      const maxAttempts = 120; // 최대 10분 대기

      pollingRef.current = setInterval(async () => {
        attempts++;
        if (attempts > maxAttempts) {
          stopPolling();
          setGenerating(false);
          setCurrentStep(1);
          messageApi.error('시간 초과: 작업이 너무 오래 걸립니다.');
          return;
        }

        setProgress(Math.min(10 + attempts * 1.5, 95));

        try {
          const statusRes = await fetch(`/api/v1/presentation/status/${jid}`);
          if (!statusRes.ok) return;

          const status = await statusRes.json();

          if (status.status === 'completed') {
            stopPolling();
            setProgress(100);
            setGenerating(false);
            setCurrentStep(3);
            messageApi.success(
              outputType === 'poster'
                ? '포스터가 생성되었습니다!'
                : '프레젠테이션이 생성되었습니다!'
            );
          } else if (status.status === 'failed') {
            stopPolling();
            setGenerating(false);
            setCurrentStep(1);
            messageApi.error(status.message || '생성에 실패했습니다.');
          }
        } catch {
          // 폴링 실패는 무시
        }
      }, 5000);
    } catch (error: any) {
      stopPolling();
      setGenerating(false);
      setCurrentStep(1);
      messageApi.error(error.message || 'AI 분석 중 오류가 발생했습니다.');
    }
  };

  const handleCancel = () => {
    stopPolling();
    setGenerating(false);
    setCurrentStep(1);
    messageApi.info('작업이 취소되었습니다.');
  };

  const handleDownloadPdf = async () => {
    if (!jobId) return;
    try {
      const response = await fetch(`/api/v1/presentation/download/${jobId}`);
      if (!response.ok) throw new Error('다운로드 실패');

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `presentation_${jobId}.pdf`;
      a.click();
      URL.revokeObjectURL(url);
      messageApi.success('PDF 파일이 다운로드되었습니다.');
    } catch (error: any) {
      messageApi.error(error.message || '다운로드 중 오류가 발생했습니다.');
    }
  };

  const handleDownloadPptx = async () => {
    if (!jobId) return;
    try {
      messageApi.loading('PPTX 변환 중...');
      const response = await fetch(`/api/v1/presentation/pptx/${jobId}`);
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || 'PPTX 변환 실패');
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `presentation_${jobId}.pptx`;
      a.click();
      URL.revokeObjectURL(url);
      messageApi.success('PPTX 파일이 다운로드되었습니다.');
    } catch (error: any) {
      messageApi.error(error.message || 'PPTX 다운로드 중 오류가 발생했습니다.');
    }
  };

  const renderUploadStep = () => (
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

  const renderSettingsStep = () => (
    <Card>
      <Row gutter={24}>
        <Col span={12}>
          <Space direction="vertical" style={{ width: '100%' }} size="large">
            <div>
              <Text strong>출력 형식</Text>
              <Radio.Group
                value={outputType}
                onChange={e => setOutputType(e.target.value)}
                style={{ marginTop: 8, display: 'block' }}
              >
                <Radio.Button value="slides">
                  <FilePptOutlined /> 프레젠테이션 슬라이드
                </Radio.Button>
                <Radio.Button value="poster">
                  <PictureOutlined /> 학술 포스터
                </Radio.Button>
              </Radio.Group>
            </div>

            <div>
              <Text strong>템플릿 선택</Text>
              <Select
                value={template}
                onChange={setTemplate}
                style={{ width: '100%', marginTop: 8 }}
                options={templates.map(t => ({
                  value: t.value,
                  label: (
                    <Space>
                      <div
                        style={{
                          width: 16,
                          height: 16,
                          background: t.color,
                          borderRadius: 4,
                        }}
                      />
                      {t.label}
                    </Space>
                  ),
                }))}
              />
            </div>

            {outputType === 'slides' && (
              <div>
                <Text strong>슬라이드 수 (권장: 8-15장)</Text>
                <Select
                  value={slideCount}
                  onChange={setSlideCount}
                  style={{ width: '100%', marginTop: 8 }}
                  options={[
                    { value: 5, label: '5장 (요약)' },
                    { value: 10, label: '10장 (표준)' },
                    { value: 15, label: '15장 (상세)' },
                    { value: 20, label: '20장 (풀버전)' },
                  ]}
                />
              </div>
            )}
          </Space>
        </Col>
        <Col span={12}>
          <div>
            <Text strong>추가 지시사항 (선택)</Text>
            <TextArea
              value={additionalPrompt}
              onChange={e => setAdditionalPrompt(e.target.value)}
              placeholder="예: 방법론 섹션을 더 자세히 설명해주세요, 그래프를 많이 포함해주세요..."
              rows={6}
              style={{ marginTop: 8 }}
            />
          </div>
        </Col>
      </Row>
      <Divider />
      <div style={{ textAlign: 'center' }}>
        <Button
          type="primary"
          size="large"
          icon={<RocketOutlined />}
          onClick={handleGenerate}
          loading={generating}
        >
          {outputType === 'poster' ? '포스터 생성' : '프레젠테이션 생성'}
        </Button>
      </div>
    </Card>
  );

  const renderGeneratingStep = () => (
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

  const renderResultStep = () => (
    <Row gutter={16}>
      <Col span={16}>
        <Card
          title={
            <Space>
              <CheckCircleOutlined style={{ color: '#52c41a' }} />
              {outputType === 'poster' ? '포스터 생성 완료' : '프레젠테이션 생성 완료'}
            </Space>
          }
        >
          <div
            style={{
              background: 'linear-gradient(135deg, #005BAC 0%, #00A0B0 100%)',
              borderRadius: 8,
              padding: 48,
              minHeight: 400,
              color: 'white',
              textAlign: 'center',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              alignItems: 'center',
            }}
          >
            <CheckCircleOutlined style={{ fontSize: 64, marginBottom: 24 }} />
            <Title level={2} style={{ color: 'white', margin: 0 }}>
              {outputType === 'poster' ? '학술 포스터가 생성되었습니다!' : '프레젠테이션이 생성되었습니다!'}
            </Title>
            <Paragraph style={{ color: 'rgba(255,255,255,0.9)', fontSize: 16, marginTop: 16 }}>
              Paper2Slides AI가 문서를 분석하여 파일을 생성했습니다.
            </Paragraph>
            <Text style={{ color: 'rgba(255,255,255,0.7)', marginTop: 8 }}>
              작업 ID: {jobId}
            </Text>
          </div>
        </Card>
      </Col>
      <Col span={8}>
        <Card title="다운로드">
          <Space direction="vertical" style={{ width: '100%' }}>
            {outputType === 'slides' && (
              <Button
                type="primary"
                icon={<FilePptOutlined />}
                block
                size="large"
                onClick={handleDownloadPptx}
              >
                PowerPoint (.pptx) 다운로드
              </Button>
            )}
            <Button
              icon={<DownloadOutlined />}
              block
              size="large"
              onClick={handleDownloadPdf}
            >
              {outputType === 'poster' ? '포스터 PDF 다운로드' : 'PDF 다운로드'}
            </Button>
          </Space>
        </Card>
        <Card title="정보" style={{ marginTop: 16 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Alert
              message="생성 완료"
              description={outputType === 'poster' ? '학술 포스터가 생성되었습니다.' : '프레젠테이션이 생성되었습니다.'}
              type="success"
              showIcon
              icon={<CheckCircleOutlined />}
            />
            <Divider style={{ margin: '12px 0' }} />
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Text>출력 형식</Text>
              <Text strong>{outputType === 'poster' ? '학술 포스터' : '프레젠테이션'}</Text>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <Text>템플릿</Text>
              <Text strong>{templates.find(t => t.value === template)?.label}</Text>
            </div>
            {outputType === 'slides' && (
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>슬라이드 수</Text>
                <Text strong>{slideCount}장</Text>
              </div>
            )}
            {jobId && (
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text>작업 ID</Text>
                <Text code>{jobId}</Text>
              </div>
            )}
          </Space>
        </Card>
      </Col>
    </Row>
  );

  const steps = [
    { title: '문서 업로드', icon: <UploadOutlined /> },
    { title: '설정', icon: <ExperimentOutlined /> },
    { title: '생성 중', icon: <LoadingOutlined /> },
    { title: '완료', icon: <CheckCircleOutlined /> },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Card style={{ marginBottom: 24 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Space>
              <FilePptOutlined style={{ fontSize: 24, color: '#005BAC' }} />
              <div>
                <Title level={3} style={{ margin: 0 }}>
                  프레젠테이션 자동 생성
                </Title>
                <Text type="secondary">
                  연구 논문, 기술 보고서를 전문가 수준의 슬라이드로 변환
                </Text>
              </div>
            </Space>
          </Col>
          <Col>
            <Tag color="processing" icon={<ExperimentOutlined />}>
              Paper2Slides AI
            </Tag>
          </Col>
        </Row>
      </Card>

      <Card style={{ marginBottom: 24 }}>
        <Steps
          current={currentStep}
          items={steps.map((s, i) => ({
            title: s.title,
            icon: currentStep === i && generating ? <LoadingOutlined /> : s.icon,
          }))}
        />
      </Card>

      {currentStep === 0 && renderUploadStep()}
      {currentStep === 1 && renderSettingsStep()}
      {currentStep === 2 && renderGeneratingStep()}
      {currentStep === 3 && renderResultStep()}

      {currentStep > 0 && currentStep < 3 && !generating && (
        <div style={{ marginTop: 16, textAlign: 'center' }}>
          <Button onClick={() => setCurrentStep(currentStep - 1)}>
            이전 단계
          </Button>
        </div>
      )}

      {currentStep === 3 && (
        <div style={{ marginTop: 16, textAlign: 'center' }}>
          <Button
            type="primary"
            onClick={() => {
              setCurrentStep(0);
              setFileList([]);
              setJobId(null);
              setProgress(0);
            }}
          >
            {outputType === 'poster' ? '새 포스터 만들기' : '새 프레젠테이션 만들기'}
          </Button>
        </div>
      )}
    </div>
  );
};

export default Presentation;
