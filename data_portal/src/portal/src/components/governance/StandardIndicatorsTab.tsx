/**
 * DGR-001: 표준 지표 관리 탭
 * 지표 정의, 산출식, 측정 주기, 담당 부서
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Spin, Button, Select, Drawer, Form, Input, Popconfirm, message, Empty,
  Tooltip, Descriptions,
} from 'antd';
import {
  BarChartOutlined, PlusOutlined, EditOutlined, DeleteOutlined,
  ReloadOutlined, DashboardOutlined, TeamOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import { governanceApi } from '../../services/api';

const { Text } = Typography;

interface StandardIndicator {
  indicator_id: number;
  name: string;
  definition: string;
  formula: string;
  unit: string;
  frequency: string;
  owner_dept: string;
  data_source: string;
  category: string;
  status: string;
  created_at: string | null;
}

const CATEGORY_COLORS: Record<string, string> = {
  '임상': 'blue', '품질': 'green', '운영': 'orange', '안전': 'red',
  '보안': 'volcano', '데이터': 'purple',
};

const FREQUENCY_OPTIONS = ['일간', '주간', '월간', '분기', '반기', '연간'];
const CATEGORY_OPTIONS = ['임상', '품질', '운영', '안전', '보안', '데이터'];

const StandardIndicatorsTab: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [indicators, setIndicators] = useState<StandardIndicator[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [total, setTotal] = useState(0);
  const [categoryFilter, setCategoryFilter] = useState<string | undefined>(undefined);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editingIndicator, setEditingIndicator] = useState<StandardIndicator | null>(null);
  const [form] = Form.useForm();

  const fetchIndicators = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string> = {};
      if (categoryFilter) params.category = categoryFilter;
      const data = await governanceApi.getStandardIndicators(params);
      setIndicators(data.indicators || []);
      setTotal(data.total || 0);
      setCategories(data.categories || []);
    } catch {
      message.error('표준 지표 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [categoryFilter]);

  useEffect(() => {
    fetchIndicators();
  }, [fetchIndicators]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      if (editingIndicator) {
        await governanceApi.updateStandardIndicator(editingIndicator.indicator_id, values);
        message.success('지표 수정 완료');
      } else {
        await governanceApi.createStandardIndicator(values);
        message.success('지표 등록 완료');
      }
      setDrawerOpen(false);
      setEditingIndicator(null);
      form.resetFields();
      fetchIndicators();
    } catch {
      // validation error
    }
  };

  const handleDelete = async (indicatorId: number) => {
    try {
      await governanceApi.deleteStandardIndicator(indicatorId);
      message.success('지표 삭제 완료');
      fetchIndicators();
    } catch {
      message.error('지표 삭제 실패');
    }
  };

  const openEdit = (ind: StandardIndicator) => {
    setEditingIndicator(ind);
    form.setFieldsValue({
      name: ind.name,
      definition: ind.definition,
      formula: ind.formula,
      unit: ind.unit,
      frequency: ind.frequency,
      owner_dept: ind.owner_dept,
      data_source: ind.data_source,
      category: ind.category,
    });
    setDrawerOpen(true);
  };

  const openCreate = () => {
    setEditingIndicator(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  // Category stats
  const catStats = indicators.reduce<Record<string, number>>((acc, i) => {
    acc[i.category] = (acc[i.category] || 0) + 1;
    return acc;
  }, {});

  // Frequency stats
  const freqStats = indicators.reduce<Record<string, number>>((acc, i) => {
    acc[i.frequency] = (acc[i.frequency] || 0) + 1;
    return acc;
  }, {});

  // Dept stats
  const deptCount = new Set(indicators.map(i => i.owner_dept)).size;

  const columns = [
    {
      title: '지표명',
      dataIndex: 'name',
      key: 'name',
      render: (v: string) => <Text strong>{v}</Text>,
      width: 140,
    },
    {
      title: '분류',
      dataIndex: 'category',
      key: 'category',
      width: 70,
      render: (v: string) => <Tag color={CATEGORY_COLORS[v] || 'default'}>{v}</Tag>,
    },
    {
      title: '정의',
      dataIndex: 'definition',
      key: 'definition',
      ellipsis: true,
    },
    {
      title: '단위',
      dataIndex: 'unit',
      key: 'unit',
      width: 50,
      render: (v: string) => <Tag>{v}</Tag>,
    },
    {
      title: '주기',
      dataIndex: 'frequency',
      key: 'frequency',
      width: 60,
      render: (v: string) => <Text type="secondary">{v}</Text>,
    },
    {
      title: '담당 부서',
      dataIndex: 'owner_dept',
      key: 'owner_dept',
      width: 120,
    },
    {
      title: '작업',
      key: 'action',
      width: 80,
      render: (_: unknown, record: StandardIndicator) => (
        <Space size="small">
          <Tooltip title="수정">
            <Button size="small" type="text" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          </Tooltip>
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(record.indicator_id)} okText="삭제" cancelText="취소">
            <Button size="small" type="text" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Spin spinning={loading}>
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        {/* Stats */}
        <Row gutter={[16, 16]}>
          <Col xs={8} md={4}>
            <Card size="small">
              <Statistic title="전체 지표" value={total} prefix={<BarChartOutlined />} valueStyle={{ color: '#005BAC' }} />
            </Card>
          </Col>
          <Col xs={8} md={4}>
            <Card size="small">
              <Statistic title="담당 부서" value={deptCount} prefix={<TeamOutlined />} valueStyle={{ color: '#38A169' }} />
            </Card>
          </Col>
          <Col xs={8} md={6}>
            <Card size="small">
              <div style={{ fontSize: 13, color: '#666', marginBottom: 4 }}>분류별</div>
              <Space size={4} wrap>
                {Object.entries(catStats).map(([c, n]) => (
                  <Tag key={c} color={CATEGORY_COLORS[c] || 'default'}>{c}: {n}</Tag>
                ))}
              </Space>
            </Card>
          </Col>
          <Col xs={24} md={10}>
            <Card size="small">
              <div style={{ fontSize: 13, color: '#666', marginBottom: 4 }}>측정 주기</div>
              <Space size={4} wrap>
                {Object.entries(freqStats).map(([f, n]) => (
                  <Tag key={f} icon={<ClockCircleOutlined />}>{f}: {n}</Tag>
                ))}
              </Space>
            </Card>
          </Col>
        </Row>

        {/* Filter + Actions */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <Select
              placeholder="분류 필터"
              value={categoryFilter}
              onChange={setCategoryFilter}
              allowClear
              style={{ width: 140 }}
              options={categories.map(c => ({ value: c, label: c }))}
            />
            <Button icon={<ReloadOutlined />} onClick={fetchIndicators} />
          </Space>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>지표 등록</Button>
        </div>

        {/* Table */}
        <Table
          dataSource={indicators}
          columns={columns}
          rowKey="indicator_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true, showTotal: (t) => `총 ${t}개` }}
          expandable={{
            expandedRowRender: (record) => (
              <Descriptions size="small" column={2} bordered>
                <Descriptions.Item label="산출식" span={2}>
                  <Text code style={{ fontSize: 12 }}>{record.formula}</Text>
                </Descriptions.Item>
                <Descriptions.Item label="데이터 소스">{record.data_source}</Descriptions.Item>
                <Descriptions.Item label="담당 부서">{record.owner_dept}</Descriptions.Item>
                <Descriptions.Item label="측정 주기">{record.frequency}</Descriptions.Item>
                <Descriptions.Item label="단위">{record.unit}</Descriptions.Item>
              </Descriptions>
            ),
          }}
          locale={{ emptyText: <Empty description="지표가 없습니다" /> }}
        />

        {/* Drawer */}
        <Drawer
          title={editingIndicator ? '표준 지표 수정' : '표준 지표 등록'}
          open={drawerOpen}
          onClose={() => { setDrawerOpen(false); setEditingIndicator(null); form.resetFields(); }}
          width={520}
          extra={<Button type="primary" onClick={handleCreate}>{editingIndicator ? '수정' : '등록'}</Button>}
        >
          <Form form={form} layout="vertical">
            <Form.Item name="name" label="지표명" rules={[{ required: true, message: '지표명을 입력하세요' }]}>
              <Input placeholder="e.g. 평균 재원일수" />
            </Form.Item>
            <Form.Item name="category" label="분류" rules={[{ required: true }]} initialValue="임상">
              <Select options={CATEGORY_OPTIONS.map(c => ({ value: c, label: c }))} />
            </Form.Item>
            <Form.Item name="definition" label="정의" rules={[{ required: true, message: '정의를 입력하세요' }]}>
              <Input.TextArea rows={2} placeholder="지표의 비즈니스 정의" />
            </Form.Item>
            <Form.Item name="formula" label="산출식" rules={[{ required: true, message: '산출식을 입력하세요' }]}>
              <Input.TextArea rows={2} placeholder="e.g. SUM(재원일수) / COUNT(퇴원건수)" />
            </Form.Item>
            <Row gutter={16}>
              <Col span={12}>
                <Form.Item name="unit" label="단위" rules={[{ required: true }]}>
                  <Input placeholder="e.g. %, 건, 일" />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item name="frequency" label="측정 주기" rules={[{ required: true }]}>
                  <Select options={FREQUENCY_OPTIONS.map(f => ({ value: f, label: f }))} />
                </Form.Item>
              </Col>
            </Row>
            <Form.Item name="owner_dept" label="담당 부서" rules={[{ required: true }]}>
              <Input placeholder="e.g. QI실, 의료정보실" />
            </Form.Item>
            <Form.Item name="data_source" label="데이터 소스" rules={[{ required: true }]}>
              <Input placeholder="e.g. visit_occurrence, condition_occurrence" />
            </Form.Item>
          </Form>
        </Drawer>
      </Space>
    </Spin>
  );
};

export default StandardIndicatorsTab;
