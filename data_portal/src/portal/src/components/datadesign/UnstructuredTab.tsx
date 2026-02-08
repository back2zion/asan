/**
 * DataDesign - Tab 5: 비정형 데이터 구조화
 * Unstructured data mapping table and add mapping drawer
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography,
  Drawer, Input, Form, App,
} from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { UnstructuredMapping, STATUS_COLORS } from './types';

const { Text } = Typography;

const UnstructuredTab: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [unstructuredMappings, setUnstructuredMappings] = useState<UnstructuredMapping[]>([]);

  // Mapping drawer
  const [mappingDrawerOpen, setMappingDrawerOpen] = useState(false);
  const [mappingForm] = Form.useForm();

  const fetchUnstructured = useCallback(async () => {
    setLoading(true);
    try {
      const data = await dataDesignApi.getUnstructuredMappings();
      setUnstructuredMappings(data.mappings || []);
    } catch {
      message.error('비정형 매핑 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    fetchUnstructured();
  }, [fetchUnstructured]);

  const handleCreateMapping = async () => {
    try {
      const values = await mappingForm.validateFields();
      await dataDesignApi.createUnstructuredMapping(values);
      message.success('매핑 생성 완료');
      setMappingDrawerOpen(false);
      mappingForm.resetFields();
      fetchUnstructured();
    } catch {
      message.error('매핑 생성 실패');
    }
  };

  const unstructuredColumns = [
    { title: '소스 유형', dataIndex: 'source_type', key: 'source_type', render: (v: string) => <Text strong>{v}</Text> },
    { title: '소스 설명', dataIndex: 'source_description', key: 'source_description', ellipsis: true },
    {
      title: '대상 테이블',
      dataIndex: 'target_table',
      key: 'target_table',
      render: (v: string) => <Text code>{v}</Text>,
    },
    { title: '추출 방법', dataIndex: 'extraction_method', key: 'extraction_method' },
    {
      title: 'NLP 모델',
      dataIndex: 'nlp_model',
      key: 'nlp_model',
      render: (v: string) => v || '-',
      ellipsis: true,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      render: (v: string) => <Tag color={STATUS_COLORS[v] || 'default'}>{v}</Tag>,
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Text type="secondary">{unstructuredMappings.length}개 비정형 매핑</Text>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setMappingDrawerOpen(true)}>매핑 추가</Button>
      </div>

      <Table
        dataSource={unstructuredMappings}
        columns={unstructuredColumns}
        rowKey="mapping_id"
        size="small"
        pagination={false}
        expandable={{
          expandedRowRender: (record) => (
            <Space wrap>
              {(record.output_columns || []).map((col, i) => (
                <Tag key={i} color="blue">{col}</Tag>
              ))}
              {(!record.output_columns || record.output_columns.length === 0) && <Text type="secondary">출력 컬럼 없음</Text>}
            </Space>
          ),
        }}
      />

      {/* Mapping Add Drawer */}
      <Drawer
        title="비정형 매핑 추가"
        open={mappingDrawerOpen}
        onClose={() => { setMappingDrawerOpen(false); mappingForm.resetFields(); }}
        width={480}
        extra={<Button type="primary" onClick={handleCreateMapping}>생성</Button>}
      >
        <Form form={mappingForm} layout="vertical">
          <Form.Item name="source_type" label="소스 유형" rules={[{ required: true }]}>
            <Input placeholder="e.g. 병리검사 보고서" />
          </Form.Item>
          <Form.Item name="source_description" label="소스 설명" rules={[{ required: true }]}>
            <Input.TextArea rows={2} placeholder="e.g. 암병원 병리 검사 결과 보고서 (PDF/텍스트)" />
          </Form.Item>
          <Form.Item name="target_table" label="대상 테이블" rules={[{ required: true }]}>
            <Input placeholder="e.g. observation" />
          </Form.Item>
          <Form.Item name="extraction_method" label="추출 방법" rules={[{ required: true }]}>
            <Input placeholder="e.g. NLP 구조화 추출" />
          </Form.Item>
          <Form.Item name="nlp_model" label="NLP 모델">
            <Input placeholder="e.g. BioClinicalBERT + 규칙 기반" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

export default UnstructuredTab;
