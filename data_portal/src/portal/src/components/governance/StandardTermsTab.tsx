/**
 * DGR-001: 표준 용어 사전 탭
 * 용어 검색, 등록/수정, 동의어·약어 매핑
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Spin, Button, Input, Select, Drawer, Form, Popconfirm, message, Empty,
  Tooltip,
} from 'antd';
import {
  BookOutlined, SearchOutlined, PlusOutlined, EditOutlined,
  DeleteOutlined, ReloadOutlined, TagsOutlined,
} from '@ant-design/icons';
import { governanceApi } from '../../services/api';

const { Text } = Typography;

interface StandardTerm {
  term_id: number;
  standard_name: string;
  definition: string;
  domain: string;
  synonyms: string[];
  abbreviation: string | null;
  english_name: string | null;
  status: string;
  created_at: string | null;
}

const DOMAIN_COLORS: Record<string, string> = {
  '환자': 'blue', '진료': 'green', '처방': 'orange', '검사': 'cyan',
  '시술': 'purple', '기록': 'geekblue', '보안': 'red', '표준': 'gold',
  '설계': 'magenta',
};

const StandardTermsTab: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [terms, setTerms] = useState<StandardTerm[]>([]);
  const [domains, setDomains] = useState<string[]>([]);
  const [total, setTotal] = useState(0);
  const [domainFilter, setDomainFilter] = useState<string | undefined>(undefined);
  const [searchText, setSearchText] = useState('');
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editingTerm, setEditingTerm] = useState<StandardTerm | null>(null);
  const [form] = Form.useForm();

  const fetchTerms = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string> = {};
      if (domainFilter) params.domain = domainFilter;
      if (searchText.trim()) params.search = searchText.trim();
      const data = await governanceApi.getStandardTerms(params);
      setTerms(data.terms || []);
      setTotal(data.total || 0);
      setDomains(data.domains || []);
    } catch {
      message.error('표준 용어 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [domainFilter, searchText]);

  useEffect(() => {
    fetchTerms();
  }, [fetchTerms]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      if (values.synonyms_text) {
        values.synonyms = values.synonyms_text.split(',').map((s: string) => s.trim()).filter(Boolean);
      }
      delete values.synonyms_text;

      if (editingTerm) {
        await governanceApi.updateStandardTerm(editingTerm.term_id, values);
        message.success('용어 수정 완료');
      } else {
        await governanceApi.createStandardTerm(values);
        message.success('용어 등록 완료');
      }
      setDrawerOpen(false);
      setEditingTerm(null);
      form.resetFields();
      fetchTerms();
    } catch {
      // validation error
    }
  };

  const handleDelete = async (termId: number) => {
    try {
      await governanceApi.deleteStandardTerm(termId);
      message.success('용어 삭제 완료');
      fetchTerms();
    } catch {
      message.error('용어 삭제 실패');
    }
  };

  const openEdit = (term: StandardTerm) => {
    setEditingTerm(term);
    form.setFieldsValue({
      standard_name: term.standard_name,
      definition: term.definition,
      domain: term.domain,
      synonyms_text: (term.synonyms || []).join(', '),
      abbreviation: term.abbreviation,
      english_name: term.english_name,
    });
    setDrawerOpen(true);
  };

  const openCreate = () => {
    setEditingTerm(null);
    form.resetFields();
    setDrawerOpen(true);
  };

  // Domain stats
  const domainStats = terms.reduce<Record<string, number>>((acc, t) => {
    acc[t.domain] = (acc[t.domain] || 0) + 1;
    return acc;
  }, {});

  const columns = [
    {
      title: '표준 용어',
      dataIndex: 'standard_name',
      key: 'standard_name',
      render: (v: string) => <Text strong>{v}</Text>,
      width: 120,
    },
    {
      title: '영문명',
      dataIndex: 'english_name',
      key: 'english_name',
      render: (v: string | null) => v ? <Text type="secondary">{v}</Text> : '-',
      width: 140,
    },
    {
      title: '약어',
      dataIndex: 'abbreviation',
      key: 'abbreviation',
      render: (v: string | null) => v ? <Tag>{v}</Tag> : '-',
      width: 70,
    },
    {
      title: '도메인',
      dataIndex: 'domain',
      key: 'domain',
      render: (v: string) => <Tag color={DOMAIN_COLORS[v] || 'default'}>{v}</Tag>,
      width: 80,
    },
    {
      title: '정의',
      dataIndex: 'definition',
      key: 'definition',
      ellipsis: true,
    },
    {
      title: '동의어',
      dataIndex: 'synonyms',
      key: 'synonyms',
      width: 200,
      render: (syns: string[]) => syns && syns.length > 0
        ? <Space size={2} wrap>{syns.map((s, i) => <Tag key={i} color="processing" style={{ fontSize: 11 }}>{s}</Tag>)}</Space>
        : <Text type="secondary">-</Text>,
    },
    {
      title: '작업',
      key: 'action',
      width: 80,
      render: (_: unknown, record: StandardTerm) => (
        <Space size="small">
          <Tooltip title="수정">
            <Button size="small" type="text" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          </Tooltip>
          <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(record.term_id)} okText="삭제" cancelText="취소">
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
          <Col xs={8} md={6}>
            <Card size="small">
              <Statistic title="전체 용어" value={total} prefix={<BookOutlined />} valueStyle={{ color: '#005BAC' }} />
            </Card>
          </Col>
          <Col xs={8} md={6}>
            <Card size="small">
              <Statistic title="도메인 수" value={domains.length} prefix={<TagsOutlined />} valueStyle={{ color: '#38A169' }} />
            </Card>
          </Col>
          <Col xs={8} md={12}>
            <Card size="small">
              <div style={{ fontSize: 13, color: '#666', marginBottom: 4 }}>도메인별 분포</div>
              <Space size={4} wrap>
                {Object.entries(domainStats).map(([d, c]) => (
                  <Tag key={d} color={DOMAIN_COLORS[d] || 'default'}>{d}: {c}</Tag>
                ))}
              </Space>
            </Card>
          </Col>
        </Row>

        {/* Filters */}
        <Card size="small">
          <Row gutter={8} align="middle">
            <Col flex="auto">
              <Input
                placeholder="용어, 영문명, 약어, 동의어 검색..."
                prefix={<SearchOutlined />}
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                onPressEnter={fetchTerms}
                allowClear
              />
            </Col>
            <Col>
              <Select
                placeholder="도메인 필터"
                value={domainFilter}
                onChange={setDomainFilter}
                allowClear
                style={{ width: 140 }}
                options={domains.map(d => ({ value: d, label: d }))}
              />
            </Col>
            <Col>
              <Button icon={<ReloadOutlined />} onClick={fetchTerms}>검색</Button>
            </Col>
            <Col>
              <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>용어 등록</Button>
            </Col>
          </Row>
        </Card>

        {/* Table */}
        <Table
          dataSource={terms}
          columns={columns}
          rowKey="term_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true, showTotal: (t) => `총 ${t}개` }}
          locale={{ emptyText: <Empty description="용어가 없습니다" /> }}
        />

        {/* Drawer */}
        <Drawer
          title={editingTerm ? '표준 용어 수정' : '표준 용어 등록'}
          open={drawerOpen}
          onClose={() => { setDrawerOpen(false); setEditingTerm(null); form.resetFields(); }}
          width={480}
          extra={<Button type="primary" onClick={handleCreate}>{editingTerm ? '수정' : '등록'}</Button>}
        >
          <Form form={form} layout="vertical">
            <Form.Item name="standard_name" label="표준 용어 (한글)" rules={[{ required: true, message: '용어명을 입력하세요' }]}>
              <Input placeholder="e.g. 환자" />
            </Form.Item>
            <Form.Item name="english_name" label="영문명">
              <Input placeholder="e.g. Patient" />
            </Form.Item>
            <Form.Item name="abbreviation" label="약어">
              <Input placeholder="e.g. Pt" />
            </Form.Item>
            <Form.Item name="domain" label="도메인" rules={[{ required: true, message: '도메인을 선택하세요' }]}>
              <Select
                placeholder="도메인 선택"
                options={[
                  { value: '환자', label: '환자' }, { value: '진료', label: '진료' },
                  { value: '처방', label: '처방' }, { value: '검사', label: '검사' },
                  { value: '시술', label: '시술' }, { value: '기록', label: '기록' },
                  { value: '보안', label: '보안' }, { value: '표준', label: '표준' },
                  { value: '설계', label: '설계' },
                ]}
              />
            </Form.Item>
            <Form.Item name="definition" label="정의" rules={[{ required: true, message: '정의를 입력하세요' }]}>
              <Input.TextArea rows={3} placeholder="용어의 비즈니스 정의를 입력하세요" />
            </Form.Item>
            <Form.Item name="synonyms_text" label="동의어 (쉼표 구분)">
              <Input placeholder="e.g. 피검자, 수진자, 대상자" />
            </Form.Item>
          </Form>
        </Drawer>
      </Space>
    </Spin>
  );
};

export default StandardTermsTab;
