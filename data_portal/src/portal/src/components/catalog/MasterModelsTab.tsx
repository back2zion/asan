/**
 * 마스터 모델 관리 탭 (DPR-002: 분석 모델 CRUD)
 * - 모델 카드 그리드
 * - 모델 생성 모달
 * - 모델 상세 Drawer (실행 → SQL 생성 → 결과 미리보기)
 */

import React, { useState } from 'react';
import {
  Card, Row, Col, Button, Space, Typography, Tag, Modal, Form, Input, Select, Drawer,
  Spin, Empty, App, Badge, Descriptions, Divider, Table,
} from 'antd';
import {
  PlusOutlined, ExperimentOutlined, PlayCircleOutlined,
  CopyOutlined, DeleteOutlined, EditOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { catalogAnalyticsApi } from '../../services/catalogAnalyticsApi';
import { catalogComposeApi } from '../../services/catalogComposeApi';
import type { MasterModel } from '../../services/catalogAnalyticsApi';

const { Text, Title, Paragraph } = Typography;
const { TextArea } = Input;

const MODEL_TYPE_LABELS: Record<string, { label: string; color: string }> = {
  cohort: { label: '코호트 분석', color: 'blue' },
  trend: { label: '시간 추이', color: 'green' },
  comparison: { label: '비교 분석', color: 'orange' },
  correlation: { label: '상관관계', color: 'purple' },
};

const MasterModelsTab: React.FC = () => {
  const { message } = App.useApp();
  const queryClient = useQueryClient();
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [detailDrawerVisible, setDetailDrawerVisible] = useState(false);
  const [selectedModel, setSelectedModel] = useState<MasterModel | null>(null);
  const [previewResult, setPreviewResult] = useState<{ columns: string[]; rows: Record<string, unknown>[] } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [form] = Form.useForm();

  const { data, isLoading } = useQuery({
    queryKey: ['master-models'],
    queryFn: () => catalogAnalyticsApi.getMasterModels(),
  });

  const models: MasterModel[] = data?.models || [];

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      await catalogAnalyticsApi.createMasterModel({
        name: values.name,
        description: values.description,
        model_type: values.model_type,
        base_tables: values.base_tables || [],
        query_template: values.query_template,
      });
      message.success('마스터 모델이 생성되었습니다');
      setCreateModalVisible(false);
      form.resetFields();
      queryClient.invalidateQueries({ queryKey: ['master-models'] });
    } catch {
      // form validation error
    }
  };

  const handleDelete = async (modelId: string) => {
    try {
      await catalogAnalyticsApi.deleteMasterModel(modelId);
      message.success('모델이 삭제되었습니다');
      queryClient.invalidateQueries({ queryKey: ['master-models'] });
    } catch {
      message.error('삭제에 실패했습니다');
    }
  };

  const handleOpenDetail = async (model: MasterModel) => {
    try {
      const detail = await catalogAnalyticsApi.getMasterModel(model.model_id);
      setSelectedModel(detail);
    } catch {
      setSelectedModel(model);
    }
    setPreviewResult(null);
    setDetailDrawerVisible(true);
  };

  const handleExecutePreview = async () => {
    if (!selectedModel?.query_template) return;
    setPreviewLoading(true);
    try {
      // 파라미터 치환 (기본값 사용)
      let sql = selectedModel.query_template;
      const params = selectedModel.parameters || {};
      for (const [key, def] of Object.entries(params)) {
        const defVal = (def as Record<string, unknown>)?.default;
        if (defVal !== undefined) {
          sql = sql.replace(`:${key}`, String(defVal));
        }
      }
      const result = await catalogComposeApi.preview(sql, 20);
      setPreviewResult(result);
    } catch (err: unknown) {
      const errMsg = err instanceof Error ? err.message : '미리보기 실행 오류';
      message.error(errMsg);
    }
    setPreviewLoading(false);
  };

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    message.success('클립보드에 복사되었습니다');
  };

  if (isLoading) {
    return <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /></div>;
  }

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <ExperimentOutlined style={{ fontSize: 18, color: '#005BAC' }} />
          <Title level={5} style={{ margin: 0 }}>마스터 분석 모델</Title>
          <Tag color="blue">{models.length}개</Tag>
        </Space>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setCreateModalVisible(true); }}>
          새 모델 등록
        </Button>
      </div>

      {models.length === 0 ? (
        <Empty description="등록된 분석 모델이 없습니다" />
      ) : (
        <Row gutter={[16, 16]}>
          {models.map((model) => {
            const typeInfo = MODEL_TYPE_LABELS[model.model_type] || { label: model.model_type, color: 'default' };
            return (
              <Col key={model.model_id} xs={24} sm={12} lg={8}>
                <Card
                  hoverable
                  size="small"
                  onClick={() => handleOpenDetail(model)}
                  actions={[
                    <PlayCircleOutlined key="run" onClick={(e) => { e.stopPropagation(); handleOpenDetail(model); }} />,
                    <EditOutlined key="edit" onClick={(e) => { e.stopPropagation(); handleOpenDetail(model); }} />,
                    <DeleteOutlined key="del" onClick={(e) => { e.stopPropagation(); handleDelete(model.model_id); }} />,
                  ]}
                >
                  <Card.Meta
                    title={
                      <Space>
                        <Text strong>{model.name}</Text>
                        <Tag color={typeInfo.color}>{typeInfo.label}</Tag>
                      </Space>
                    }
                    description={
                      <div>
                        <Paragraph ellipsis={{ rows: 2 }} style={{ marginBottom: 8, fontSize: 12, color: '#666' }}>
                          {model.description || '설명 없음'}
                        </Paragraph>
                        <Space size={4} wrap>
                          {model.base_tables.slice(0, 3).map((t) => (
                            <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>
                          ))}
                          {model.base_tables.length > 3 && <Tag style={{ fontSize: 10 }}>+{model.base_tables.length - 3}</Tag>}
                        </Space>
                        <div style={{ marginTop: 8, display: 'flex', justifyContent: 'space-between' }}>
                          <Text type="secondary" style={{ fontSize: 11 }}>{model.creator}</Text>
                          <Badge count={model.usage_count} size="small" showZero color="#005BAC" title="사용 횟수" />
                        </div>
                      </div>
                    }
                  />
                </Card>
              </Col>
            );
          })}
        </Row>
      )}

      {/* 모델 생성 모달 */}
      <Modal
        title="새 분석 모델 등록"
        open={createModalVisible}
        onOk={handleCreate}
        onCancel={() => setCreateModalVisible(false)}
        width={640}
        okText="등록"
        cancelText="취소"
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="모델 이름" rules={[{ required: true, message: '이름을 입력하세요' }]}>
            <Input placeholder="예: 코호트 분석 모델" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <TextArea rows={2} placeholder="모델의 목적과 사용 방법을 설명하세요" />
          </Form.Item>
          <Form.Item name="model_type" label="모델 유형" initialValue="cohort">
            <Select
              options={Object.entries(MODEL_TYPE_LABELS).map(([key, val]) => ({
                value: key, label: val.label,
              }))}
            />
          </Form.Item>
          <Form.Item name="base_tables" label="기본 테이블">
            <Select
              mode="multiple"
              placeholder="사용할 테이블 선택"
              options={[
                'person', 'visit_occurrence', 'condition_occurrence', 'measurement',
                'drug_exposure', 'observation', 'procedure_occurrence', 'visit_detail',
                'condition_era', 'drug_era', 'cost', 'payer_plan_period',
              ].map((t) => ({ value: t, label: t }))}
            />
          </Form.Item>
          <Form.Item name="query_template" label="SQL 템플릿">
            <TextArea
              rows={6}
              placeholder="SELECT ... FROM ... WHERE :parameter_name ..."
              style={{ fontFamily: 'monospace', fontSize: 12 }}
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 모델 상세 Drawer */}
      <Drawer
        title={
          <Space>
            <ExperimentOutlined style={{ color: '#005BAC' }} />
            {selectedModel?.name}
            {selectedModel && (
              <Tag color={MODEL_TYPE_LABELS[selectedModel.model_type]?.color}>
                {MODEL_TYPE_LABELS[selectedModel.model_type]?.label}
              </Tag>
            )}
          </Space>
        }
        placement="right"
        width={680}
        open={detailDrawerVisible}
        onClose={() => setDetailDrawerVisible(false)}
      >
        {selectedModel && (
          <>
            <Descriptions column={2} size="small" bordered>
              <Descriptions.Item label="작성자">{selectedModel.creator}</Descriptions.Item>
              <Descriptions.Item label="사용 횟수">
                <Badge count={selectedModel.usage_count} showZero color="#005BAC" />
              </Descriptions.Item>
              <Descriptions.Item label="기본 테이블" span={2}>
                <Space wrap>
                  {selectedModel.base_tables.map((t) => <Tag key={t}>{t}</Tag>)}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="설명" span={2}>
                {selectedModel.description || '설명 없음'}
              </Descriptions.Item>
            </Descriptions>

            {/* 파라미터 */}
            {Object.keys(selectedModel.parameters || {}).length > 0 && (
              <>
                <Divider orientation="left" style={{ fontSize: 13 }}>파라미터</Divider>
                <Descriptions column={1} size="small" bordered>
                  {Object.entries(selectedModel.parameters).map(([key, def]) => {
                    const d = def as Record<string, unknown>;
                    return (
                      <Descriptions.Item key={key} label={<Text code>{key}</Text>}>
                        {(d?.label as string) || key} — 기본값: <Tag>{String(d?.default ?? 'N/A')}</Tag>
                      </Descriptions.Item>
                    );
                  })}
                </Descriptions>
              </>
            )}

            {/* SQL 템플릿 */}
            {selectedModel.query_template && (
              <>
                <Divider orientation="left" style={{ fontSize: 13 }}>SQL 템플릿</Divider>
                <div style={{ position: 'relative' }}>
                  <pre style={{
                    background: '#f5f5f5', padding: 12, borderRadius: 4,
                    fontSize: 12, overflow: 'auto', maxHeight: 200,
                  }}>
                    {selectedModel.query_template}
                  </pre>
                  <Button
                    size="small"
                    icon={<CopyOutlined />}
                    style={{ position: 'absolute', top: 8, right: 8 }}
                    onClick={() => handleCopy(selectedModel.query_template || '')}
                  />
                </div>

                <div style={{ marginTop: 12, textAlign: 'center' }}>
                  <Button
                    type="primary"
                    icon={<PlayCircleOutlined />}
                    loading={previewLoading}
                    onClick={handleExecutePreview}
                  >
                    기본값으로 실행 (미리보기)
                  </Button>
                </div>
              </>
            )}

            {/* 미리보기 결과 */}
            {previewResult && (
              <>
                <Divider orientation="left" style={{ fontSize: 13 }}>미리보기 결과 ({previewResult.rows?.length || 0}행)</Divider>
                <Table
                  dataSource={(previewResult.rows || []).map((r: Record<string, unknown>, i: number) => ({ ...r, _key: i }))}
                  columns={(previewResult.columns || []).map((col: string) => ({
                    title: col,
                    dataIndex: col,
                    key: col,
                    ellipsis: true,
                    width: 120,
                    render: (v: unknown) => <Text style={{ fontSize: 11 }}>{v != null ? String(v) : <Text type="secondary">NULL</Text>}</Text>,
                  }))}
                  rowKey="_key"
                  pagination={false}
                  size="small"
                  scroll={{ x: 'max-content', y: 250 }}
                  bordered
                />
              </>
            )}
          </>
        )}
      </Drawer>
    </div>
  );
};

export default MasterModelsTab;
