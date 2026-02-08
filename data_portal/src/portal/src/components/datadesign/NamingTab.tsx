/**
 * DataDesign - Tab 4: 용어 표준 & 명명 규칙
 * CRUD 관리 + 명명 검증 도구
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col,
  Input, Select, App, Modal, Form, Popconfirm, Tooltip,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, SearchOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined,
} from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { NamingRule, NamingCheckResult } from './types';

const { Text } = Typography;

const TARGET_OPTIONS = [
  { value: 'table', label: '테이블' },
  { value: 'column', label: '컬럼' },
  { value: 'schema', label: '스키마' },
  { value: 'index', label: '인덱스' },
  { value: 'constraint', label: '제약조건' },
];

const TARGET_COLORS: Record<string, string> = {
  table: 'blue', column: 'green', schema: 'purple', index: 'orange', constraint: 'red',
};

const NamingTab: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [namingRules, setNamingRules] = useState<NamingRule[]>([]);

  // Modal state
  const [modalOpen, setModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<NamingRule | null>(null);
  const [saving, setSaving] = useState(false);
  const [form] = Form.useForm();

  // Naming check state
  const [namingCheckInput, setNamingCheckInput] = useState('');
  const [namingCheckTarget, setNamingCheckTarget] = useState('table');
  const [namingCheckResults, setNamingCheckResults] = useState<NamingCheckResult[]>([]);
  const [namingCheckLoading, setNamingCheckLoading] = useState(false);

  const fetchNamingRules = useCallback(async () => {
    setLoading(true);
    try {
      const data = await dataDesignApi.getNamingRules();
      setNamingRules(data.rules || []);
    } catch {
      message.error('명명 규칙 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    fetchNamingRules();
  }, [fetchNamingRules]);

  const handleNamingCheck = async () => {
    const names = namingCheckInput.split(',').map(s => s.trim()).filter(Boolean);
    if (names.length === 0) { message.warning('이름을 입력하세요 (쉼표 구분)'); return; }
    setNamingCheckLoading(true);
    try {
      const data = await dataDesignApi.checkNaming(names, namingCheckTarget);
      setNamingCheckResults(data.results || []);
    } catch {
      message.error('명명 검증 실패');
    } finally {
      setNamingCheckLoading(false);
    }
  };

  const openAddModal = () => {
    setEditingRule(null);
    form.resetFields();
    setModalOpen(true);
  };

  const openEditModal = (rule: NamingRule) => {
    setEditingRule(rule);
    form.setFieldsValue(rule);
    setModalOpen(true);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);
      if (editingRule) {
        await dataDesignApi.updateNamingRule(editingRule.rule_id, values);
        message.success('규칙이 수정되었습니다');
      } else {
        await dataDesignApi.createNamingRule(values);
        message.success('규칙이 추가되었습니다');
      }
      setModalOpen(false);
      fetchNamingRules();
    } catch {
      if (saving) message.error('저장 실패');
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (ruleId: number) => {
    try {
      await dataDesignApi.deleteNamingRule(ruleId);
      message.success('규칙이 삭제되었습니다');
      fetchNamingRules();
    } catch {
      message.error('삭제 실패');
    }
  };

  // ─── Column definitions ───

  const namingRuleColumns = [
    { title: '규칙 이름', dataIndex: 'rule_name', key: 'rule_name', render: (v: string) => <Text strong>{v}</Text> },
    {
      title: '대상',
      dataIndex: 'target',
      key: 'target',
      width: 90,
      render: (v: string) => <Tag color={TARGET_COLORS[v] || 'default'}>{v}</Tag>,
    },
    {
      title: '패턴',
      dataIndex: 'pattern',
      key: 'pattern',
      render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text>,
    },
    { title: '예시', dataIndex: 'example', key: 'example', render: (v: string) => <Text type="secondary">{v}</Text> },
    { title: '설명', dataIndex: 'description', key: 'description', ellipsis: true },
    {
      title: '관리',
      key: 'actions',
      width: 100,
      render: (_: any, record: NamingRule) => (
        <Space size={4}>
          <Tooltip title="수정">
            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => openEditModal(record)} />
          </Tooltip>
          <Popconfirm title="이 규칙을 삭제하시겠습니까?" onConfirm={() => handleDelete(record.rule_id)} okText="삭제" cancelText="취소">
            <Tooltip title="삭제">
              <Button type="text" size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const namingCheckColumns = [
    {
      title: '이름',
      dataIndex: 'name',
      key: 'name',
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: '결과',
      dataIndex: 'valid',
      key: 'valid',
      render: (v: boolean) => v
        ? <Tag icon={<CheckCircleOutlined />} color="success">통과</Tag>
        : <Tag icon={<CloseCircleOutlined />} color="error">위반</Tag>,
    },
    {
      title: '위반 사항',
      dataIndex: 'violations',
      key: 'violations',
      render: (violations: NamingCheckResult['violations']) =>
        violations.length === 0 ? '-' : (
          <Space direction="vertical" size={2}>
            {violations.map((v, i) => (
              <Text key={i} type="secondary" style={{ fontSize: 12 }}>
                <CloseCircleOutlined style={{ color: '#ff4d4f', marginRight: 4 }} />
                {v.rule}: <Text code style={{ fontSize: 11 }}>{v.pattern}</Text>
              </Text>
            ))}
          </Space>
        ),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* Rules table */}
      <Card
        title={`명명 규칙 (${namingRules.length})`}
        size="small"
        extra={
          <Button type="primary" size="small" icon={<PlusOutlined />} onClick={openAddModal}>
            규칙 추가
          </Button>
        }
      >
        <Table
          dataSource={namingRules}
          columns={namingRuleColumns}
          rowKey="rule_id"
          size="small"
          loading={loading}
          pagination={false}
        />
      </Card>

      {/* Naming check tool */}
      <Card title="명명 검증 도구" size="small">
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Row gutter={8} align="middle">
            <Col flex="auto">
              <Input
                placeholder="이름 입력 (쉼표로 구분, e.g. person, Visit_Occurrence, 123_test)"
                value={namingCheckInput}
                onChange={(e) => setNamingCheckInput(e.target.value)}
                onPressEnter={handleNamingCheck}
                prefix={<SearchOutlined />}
              />
            </Col>
            <Col>
              <Select
                value={namingCheckTarget}
                onChange={setNamingCheckTarget}
                style={{ width: 120 }}
                options={[
                  { value: 'table', label: '테이블' },
                  { value: 'column', label: '컬럼' },
                  { value: 'schema', label: '스키마' },
                ]}
              />
            </Col>
            <Col>
              <Button type="primary" onClick={handleNamingCheck} loading={namingCheckLoading}>검증</Button>
            </Col>
          </Row>
          {namingCheckResults.length > 0 && (
            <Table
              dataSource={namingCheckResults}
              columns={namingCheckColumns}
              rowKey="name"
              size="small"
              pagination={false}
            />
          )}
        </Space>
      </Card>

      {/* Add/Edit Modal */}
      <Modal
        title={editingRule ? '명명 규칙 수정' : '명명 규칙 추가'}
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        onOk={handleSave}
        confirmLoading={saving}
        okText={editingRule ? '수정' : '추가'}
        cancelText="취소"
        destroyOnHidden
      >
        <Form form={form} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="rule_name" label="규칙 이름" rules={[{ required: true, message: '규칙 이름을 입력하세요' }]}>
            <Input placeholder="예: 테이블 명명 규칙" />
          </Form.Item>
          <Form.Item name="target" label="대상" rules={[{ required: true, message: '대상을 선택하세요' }]}>
            <Select options={TARGET_OPTIONS} placeholder="대상 선택" />
          </Form.Item>
          <Form.Item name="pattern" label="정규식 패턴" rules={[{ required: true, message: '패턴을 입력하세요' }]}>
            <Input placeholder="예: ^[a-z][a-z0-9_]*$" />
          </Form.Item>
          <Form.Item name="example" label="예시">
            <Input placeholder="예: person, visit_occurrence" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="규칙 설명" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  );
};

export default NamingTab;
