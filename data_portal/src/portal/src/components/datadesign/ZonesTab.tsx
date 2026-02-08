/**
 * DataDesign - Tab 2: 데이터 영역 (Zone) 관리
 * Zone cards display and add zone modal
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Tag, Space, Button, Typography, Row, Col,
  Descriptions, Form, Modal, Input, Select, App,
} from 'antd';
import { PlusOutlined, CodeOutlined } from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { Zone, ZONE_COLORS, ZONE_LABELS } from './types';

const { Text } = Typography;

const ZonesTab: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [zones, setZones] = useState<Zone[]>([]);
  const [zoneModalOpen, setZoneModalOpen] = useState(false);
  const [zoneForm] = Form.useForm();

  const fetchZones = useCallback(async () => {
    setLoading(true);
    try {
      const data = await dataDesignApi.getZones();
      setZones(data.zones || []);
    } catch {
      message.error('Zone 데이터 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    fetchZones();
  }, [fetchZones]);

  const handleCreateZone = async () => {
    try {
      const values = await zoneForm.validateFields();
      await dataDesignApi.createZone(values);
      message.success('Zone 생성 완료');
      setZoneModalOpen(false);
      zoneForm.resetFields();
      fetchZones();
    } catch {
      message.error('Zone 생성 실패');
    }
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Text type="secondary">{zones.length}개 데이터 영역</Text>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setZoneModalOpen(true)}>Zone 추가</Button>
      </div>
      <Row gutter={[16, 16]}>
        {zones.map(z => (
          <Col xs={24} sm={12} lg={8} xl={6} key={z.zone_id}>
            <Card
              size="small"
              hoverable
              style={{ borderTop: `3px solid ${ZONE_COLORS[z.zone_type] || '#718096'}` }}
            >
              <Space direction="vertical" style={{ width: '100%' }} size="small">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Tag color={ZONE_COLORS[z.zone_type]}>{ZONE_LABELS[z.zone_type] || z.zone_type}</Tag>
                  <Text type="secondary" style={{ fontSize: 11 }}>{z.file_format}</Text>
                </div>
                <Text strong style={{ fontSize: 14 }}>{z.zone_name}</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>{z.description}</Text>
                <Descriptions size="small" column={2} style={{ marginTop: 8 }}>
                  <Descriptions.Item label="저장소">{z.storage_type === 'object_storage' ? 'Object' : z.storage_type === 'rdbms' ? 'RDBMS' : 'Hybrid'}</Descriptions.Item>
                  <Descriptions.Item label="보존">{z.retention_days}일</Descriptions.Item>
                  <Descriptions.Item label="엔티티">{z.entity_count}개</Descriptions.Item>
                  <Descriptions.Item label="총 행수">{(z.total_rows || 0).toLocaleString()}</Descriptions.Item>
                </Descriptions>
                {z.partition_strategy && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    <CodeOutlined /> 파티션: {z.partition_strategy}
                  </Text>
                )}
              </Space>
            </Card>
          </Col>
        ))}
      </Row>

      {/* Zone Add Modal */}
      <Modal
        title="새 데이터 영역 추가"
        open={zoneModalOpen}
        onOk={handleCreateZone}
        onCancel={() => { setZoneModalOpen(false); zoneForm.resetFields(); }}
        okText="생성"
        cancelText="취소"
      >
        <Form form={zoneForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="zone_name" label="영역 이름" rules={[{ required: true }]}>
            <Input placeholder="e.g. 원천 데이터 (Source)" />
          </Form.Item>
          <Form.Item name="zone_type" label="영역 유형" rules={[{ required: true }]}>
            <Select options={[
              { value: 'source', label: 'Source' },
              { value: 'bronze', label: 'Bronze' },
              { value: 'silver', label: 'Silver' },
              { value: 'gold', label: 'Gold' },
              { value: 'mart', label: 'Mart' },
            ]} />
          </Form.Item>
          <Form.Item name="storage_type" label="저장소 유형" initialValue="object_storage">
            <Select options={[
              { value: 'object_storage', label: 'Object Storage' },
              { value: 'rdbms', label: 'RDBMS' },
              { value: 'hybrid', label: 'Hybrid' },
            ]} />
          </Form.Item>
          <Form.Item name="file_format" label="파일 포맷" initialValue="parquet">
            <Select options={[
              { value: 'parquet', label: 'Parquet' },
              { value: 'orc', label: 'ORC' },
              { value: 'avro', label: 'Avro' },
              { value: 'delta', label: 'Delta' },
              { value: 'iceberg', label: 'Iceberg' },
              { value: 'csv', label: 'CSV' },
              { value: 'json', label: 'JSON' },
            ]} />
          </Form.Item>
          <Form.Item name="retention_days" label="보존 기간 (일)" initialValue={365}>
            <Input type="number" />
          </Form.Item>
          <Form.Item name="partition_strategy" label="파티션 전략">
            <Input placeholder="e.g. YYYY/MM/DD" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  );
};

export default ZonesTab;
