/**
 * 새 분석 환경 생성 모달
 */

import React from 'react';
import {
  Button, Space, Row, Col, Alert, Modal, Form, Input, Select,
} from 'antd';
import type { SystemResourceMetrics, AnalysisTemplate } from '../../services/aiEnvironmentService';

const { Option } = Select;

interface CreateEnvironmentModalProps {
  visible: boolean;
  loading: boolean;
  templates: AnalysisTemplate[];
  resourceMetrics: SystemResourceMetrics | null;
  onSubmit: (values: any) => void;
  onCancel: () => void;
}

const CreateEnvironmentModal: React.FC<CreateEnvironmentModalProps> = ({
  visible,
  loading,
  templates,
  resourceMetrics,
  onSubmit,
  onCancel,
}) => {
  const [form] = Form.useForm();

  const handleCancel = () => {
    form.resetFields();
    onCancel();
  };

  const handleFinish = (values: any) => {
    onSubmit(values);
    form.resetFields();
  };

  return (
    <Modal
      title="새 분석 환경 생성"
      open={visible}
      onCancel={handleCancel}
      footer={null}
      width={600}
    >
      <Form form={form} layout="vertical" onFinish={handleFinish}>
        <Form.Item
          name="name"
          label="환경명"
          rules={[{ required: true, message: '환경명을 입력하세요!' }]}
        >
          <Input placeholder="예: Medical Deep Learning Lab" />
        </Form.Item>

        <Form.Item name="description" label="설명">
          <Input.TextArea placeholder="이 분석 환경에 대한 간단한 설명을 입력하세요" rows={2} />
        </Form.Item>

        <Form.Item name="template" label="분석 템플릿">
          <Select placeholder="템플릿을 선택하세요 (선택사항)" allowClear>
            {templates.map(template => (
              <Option key={template.id} value={template.id}>
                {template.name} - {template.description}
              </Option>
            ))}
          </Select>
        </Form.Item>

        <Row gutter={16}>
          <Col span={6}>
            <Form.Item name="cpu" label="CPU 코어" rules={[{ required: true, message: 'CPU 코어 수를 선택하세요!' }]}>
              <Select>
                <Option value={2}>2 코어</Option>
                <Option value={4}>4 코어</Option>
                <Option value={8}>8 코어</Option>
                <Option value={16}>16 코어</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={6}>
            <Form.Item name="memory" label="메모리" rules={[{ required: true, message: '메모리 크기를 선택하세요!' }]}>
              <Select>
                <Option value="4GB">4GB</Option>
                <Option value="8GB">8GB</Option>
                <Option value="16GB">16GB</Option>
                <Option value="32GB">32GB</Option>
                <Option value="64GB">64GB</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={6}>
            <Form.Item name="gpu" label="GPU" initialValue={0}>
              <Select>
                <Option value={0}>없음</Option>
                <Option value={1}>1개</Option>
                <Option value={2}>2개</Option>
                <Option value={4}>4개</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={6}>
            <Form.Item name="disk" label="디스크 용량" rules={[{ required: true, message: '디스크 용량을 선택하세요!' }]} initialValue={50}>
              <Select>
                <Option value={20}>20GB</Option>
                <Option value={50}>50GB</Option>
                <Option value={100}>100GB</Option>
                <Option value={200}>200GB</Option>
              </Select>
            </Form.Item>
          </Col>
        </Row>

        {resourceMetrics && (
          <Alert
            message="현재 리소스 상황"
            description={
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <div>
                  CPU: {resourceMetrics.cpu.used_cores}/{resourceMetrics.cpu.total_cores} 코어 사용중
                  ({resourceMetrics.cpu.utilization_percent.toFixed(1)}%)
                </div>
                <div>
                  메모리: {resourceMetrics.memory.used_gb}GB/{resourceMetrics.memory.total_gb}GB 사용중
                  ({resourceMetrics.memory.utilization_percent.toFixed(1)}%)
                </div>
                <div>
                  GPU: {resourceMetrics.gpu.used_count}/{resourceMetrics.gpu.total_count}개 사용중
                  ({resourceMetrics.gpu.utilization_percent.toFixed(1)}%)
                </div>
              </Space>
            }
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}

        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit" loading={loading}>생성</Button>
            <Button onClick={handleCancel}>취소</Button>
          </Space>
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default CreateEnvironmentModal;
