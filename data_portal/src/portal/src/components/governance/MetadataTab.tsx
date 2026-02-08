/**
 * 메타데이터 탭 컴포넌트 - 테이블/컬럼 메타데이터 관리
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col,
  Alert, Spin, Descriptions, Button, Modal, Form, Input, Select, message,
} from 'antd';
import {
  EyeOutlined, ReloadOutlined, EditOutlined, UserOutlined, TagsOutlined,
  RobotOutlined, CheckOutlined, CloseOutlined,
} from '@ant-design/icons';
import { semanticApi, governanceApi } from '../../services/api';

const { Title, Text } = Typography;

const DOMAIN_OPTIONS = ['환자', '진료', '검사', '처방', '시술', '관찰', '영상', '의료진', '기관'];

interface MetaOverride {
  table_name: string;
  business_name?: string;
  description?: string;
  domain?: string;
  tags?: string[];
  owner?: string;
}

interface ColMetaOverride {
  table_name: string;
  column_name: string;
  business_name?: string;
  description?: string;
}

const MetadataTab: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tables, setTables] = useState<any[]>([]);
  const [selectedTable, setSelectedTable] = useState<any>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // Override 데이터
  const [tableOverrides, setTableOverrides] = useState<Record<string, MetaOverride>>({});
  const [columnOverrides, setColumnOverrides] = useState<Record<string, ColMetaOverride>>({});

  // 테이블 편집 Modal
  const [tableModalOpen, setTableModalOpen] = useState(false);
  const [editingTableName, setEditingTableName] = useState<string>('');
  const [tableForm] = Form.useForm();

  // 컬럼 편집 Modal
  const [colModalOpen, setColModalOpen] = useState(false);
  const [editingCol, setEditingCol] = useState<{ table: string; column: string } | null>(null);
  const [colForm] = Form.useForm();

  // AI 태그 추천 (AAR-001)
  const [tagSuggestion, setTagSuggestion] = useState<{
    table_name: string;
    recommended_tags: string[];
    new_tags: string[];
    recommended_domain: string;
    reasoning: string;
    source: string;
  } | null>(null);
  const [tagSuggestionLoading, setTagSuggestionLoading] = useState(false);

  const loadTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await semanticApi.facetedSearch({ limit: 100 });
      setTables(resp.data?.tables || resp.data || []);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadOverrides = useCallback(async () => {
    try {
      const resp = await governanceApi.getMetadataOverrides();
      const tMap: Record<string, MetaOverride> = {};
      for (const t of resp.tables || []) {
        tMap[t.table_name] = t;
      }
      setTableOverrides(tMap);

      const cMap: Record<string, ColMetaOverride> = {};
      for (const c of resp.columns || []) {
        cMap[`${c.table_name}.${c.column_name}`] = c;
      }
      setColumnOverrides(cMap);
    } catch {
      // override 로드 실패 시 무시 (기본값 사용)
    }
  }, []);

  useEffect(() => {
    loadTables();
    loadOverrides();
  }, [loadTables, loadOverrides]);

  // override 적용된 값 반환
  const getTableValue = (table: any, field: string) => {
    const ov = tableOverrides[table.physical_name];
    if (ov && ov[field as keyof MetaOverride] !== undefined && ov[field as keyof MetaOverride] !== null) {
      return ov[field as keyof MetaOverride];
    }
    return table[field];
  };

  const getColumnValue = (tableName: string, col: any, field: string) => {
    const key = `${tableName}.${col.physical_name}`;
    const ov = columnOverrides[key];
    if (ov && ov[field as keyof ColMetaOverride] !== undefined && ov[field as keyof ColMetaOverride] !== null) {
      return ov[field as keyof ColMetaOverride];
    }
    return col[field];
  };

  const handleTableClick = async (physicalName: string) => {
    setDetailLoading(true);
    try {
      const resp = await semanticApi.getTableMetadata(physicalName);
      setSelectedTable(resp.data || resp);
    } catch {
      setSelectedTable(null);
    } finally {
      setDetailLoading(false);
    }
  };

  // ── 테이블 메타데이터 편집 ──
  const openTableEdit = (table: any) => {
    setEditingTableName(table.physical_name);
    const ov: Partial<MetaOverride> = tableOverrides[table.physical_name] || {};
    tableForm.setFieldsValue({
      business_name: ov.business_name || table.business_name || '',
      description: ov.description || table.description || '',
      domain: ov.domain || table.domain || '',
      tags: ov.tags || table.tags || [],
      owner: ov.owner || '',
    });
    setTableModalOpen(true);
  };

  const handleTableMetaSave = async () => {
    try {
      const values = await tableForm.validateFields();
      await governanceApi.updateTableMetadata({ table_name: editingTableName, ...values });
      message.success(`${editingTableName} 메타데이터 수정 완료`);
      setTableModalOpen(false);
      await loadOverrides();
    } catch (err: any) {
      if (err?.response?.data?.detail) message.error(err.response.data.detail);
    }
  };

  // ── 컬럼 메타데이터 편집 ──
  const openColEdit = (tableName: string, col: any) => {
    setEditingCol({ table: tableName, column: col.physical_name });
    const key = `${tableName}.${col.physical_name}`;
    const ov: Partial<ColMetaOverride> = columnOverrides[key] || {};
    colForm.setFieldsValue({
      business_name: ov.business_name || col.business_name || '',
      description: ov.description || col.description || '',
    });
    setColModalOpen(true);
  };

  // ── AI 태그 추천 (AAR-001) ──
  const handleSuggestTags = async (tableName: string) => {
    setTagSuggestionLoading(true);
    setTagSuggestion(null);
    try {
      const resp = await governanceApi.suggestTags(tableName);
      setTagSuggestion(resp);
    } catch {
      message.error('태그 추천 실패');
    } finally {
      setTagSuggestionLoading(false);
    }
  };

  const handleApplySuggestedTags = async () => {
    if (!tagSuggestion) return;
    try {
      const existingTags = (selectedTable?.tags || []) as string[];
      const merged = [...new Set([...existingTags, ...tagSuggestion.recommended_tags, ...tagSuggestion.new_tags])];
      await governanceApi.updateTableMetadata({
        table_name: tagSuggestion.table_name,
        tags: merged,
        domain: tagSuggestion.recommended_domain,
      });
      message.success(`${tagSuggestion.table_name}에 태그가 적용되었습니다`);
      setTagSuggestion(null);
      await loadOverrides();
    } catch {
      message.error('태그 적용 실패');
    }
  };

  const handleColMetaSave = async () => {
    if (!editingCol) return;
    try {
      const values = await colForm.validateFields();
      await governanceApi.updateColumnMetadata({
        table_name: editingCol.table,
        column_name: editingCol.column,
        ...values,
      });
      message.success(`${editingCol.table}.${editingCol.column} 메타데이터 수정 완료`);
      setColModalOpen(false);
      await loadOverrides();
    } catch (err: any) {
      if (err?.response?.data?.detail) message.error(err.response.data.detail);
    }
  };

  const tableColumns = [
    {
      title: '물리명',
      dataIndex: 'physical_name',
      key: 'physical_name',
      render: (v: string) => (
        <Button type="link" size="small" onClick={() => handleTableClick(v)} style={{ fontFamily: 'monospace', padding: 0 }}>
          {v}
        </Button>
      ),
    },
    {
      title: '비즈니스명',
      dataIndex: 'business_name',
      key: 'business_name',
      render: (_: string, r: any) => {
        const val = getTableValue(r, 'business_name');
        const ov = tableOverrides[r.physical_name];
        return (
          <Space size={4}>
            <Text>{val as string}</Text>
            {ov?.business_name && <Tag color="blue" style={{ fontSize: 10 }}>수정됨</Tag>}
          </Space>
        );
      },
    },
    {
      title: '도메인',
      dataIndex: 'domain',
      key: 'domain',
      render: (_: string, r: any) => {
        const val = getTableValue(r, 'domain') as string;
        return val ? <Tag color="blue">{val}</Tag> : '-';
      },
    },
    {
      title: '태그',
      dataIndex: 'tags',
      key: 'tags',
      render: (_: string[], r: any) => {
        const tags = (getTableValue(r, 'tags') || []) as string[];
        return tags.length > 0
          ? tags.slice(0, 3).map((t: string) => <Tag key={t}>{t}</Tag>)
          : '-';
      },
    },
    {
      title: '담당자',
      key: 'owner',
      width: 100,
      render: (_: any, r: any) => {
        const owner = tableOverrides[r.physical_name]?.owner;
        return owner ? <Tag icon={<UserOutlined />} color="purple">{owner}</Tag> : <Text type="secondary">-</Text>;
      },
    },
    {
      title: '편집',
      key: 'edit',
      width: 60,
      render: (_: any, r: any) => (
        <Button size="small" icon={<EditOutlined />} onClick={() => openTableEdit(r)} />
      ),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {error && <Alert message={error} type="error" showIcon />}

      <Row gutter={16}>
        <Col xs={24} lg={selectedTable ? 14 : 24}>
          <Card
            title={`테이블 목록 (${tables.length})`}
            extra={<Button icon={<ReloadOutlined />} onClick={() => { loadTables(); loadOverrides(); }} loading={loading}>새로고침</Button>}
          >
            <Table
              columns={tableColumns}
              dataSource={tables}
              rowKey="physical_name"
              size="small"
              loading={loading}
              pagination={{ pageSize: 12, showSizeChanger: true }}
            />
          </Card>
        </Col>

        {selectedTable && (
          <Col xs={24} lg={10}>
            <Card
              title={
                <Space>
                  <EyeOutlined />
                  <Text strong>{selectedTable.physical_name}</Text>
                  {(getTableValue(selectedTable, 'business_name') as string) && (
                    <Text type="secondary">({getTableValue(selectedTable, 'business_name') as string})</Text>
                  )}
                </Space>
              }
              extra={
                <Space size={4}>
                  <Button
                    type="text"
                    size="small"
                    icon={<RobotOutlined />}
                    loading={tagSuggestionLoading}
                    onClick={() => handleSuggestTags(selectedTable.physical_name)}
                    style={{ color: '#1890ff' }}
                  >
                    AI 태그 추천
                  </Button>
                  <Button type="text" size="small" icon={<EditOutlined />} onClick={() => openTableEdit(selectedTable)}>편집</Button>
                  <Button type="text" size="small" onClick={() => setSelectedTable(null)}>닫기</Button>
                </Space>
              }
            >
              <Spin spinning={detailLoading}>
                <Descriptions column={1} size="small" style={{ marginBottom: 16 }}>
                  <Descriptions.Item label="설명">{getTableValue(selectedTable, 'description') as string || '-'}</Descriptions.Item>
                  <Descriptions.Item label="도메인">{getTableValue(selectedTable, 'domain') as string || '-'}</Descriptions.Item>
                  {tableOverrides[selectedTable.physical_name]?.owner && (
                    <Descriptions.Item label="담당자">
                      <Tag icon={<UserOutlined />} color="purple">{tableOverrides[selectedTable.physical_name].owner}</Tag>
                    </Descriptions.Item>
                  )}
                  {((getTableValue(selectedTable, 'tags') || []) as string[]).length > 0 && (
                    <Descriptions.Item label="태그">
                      {((getTableValue(selectedTable, 'tags') || []) as string[]).map((t: string) => <Tag key={t} icon={<TagsOutlined />}>{t}</Tag>)}
                    </Descriptions.Item>
                  )}
                </Descriptions>
                {/* AI 태그 추천 결과 (AAR-001) */}
                {tagSuggestion && tagSuggestion.table_name === selectedTable.physical_name && (
                  <Card
                    size="small"
                    style={{ marginBottom: 16, borderLeft: '3px solid #1890ff' }}
                    title={
                      <Space>
                        <RobotOutlined style={{ color: '#1890ff' }} />
                        <Text strong>AI 태그 추천</Text>
                        <Tag color={tagSuggestion.source === 'llm' ? 'blue' : 'default'}>
                          {tagSuggestion.source === 'llm' ? 'LLM' : '규칙 기반'}
                        </Tag>
                      </Space>
                    }
                    extra={
                      <Space size={4}>
                        <Button type="primary" size="small" icon={<CheckOutlined />} onClick={handleApplySuggestedTags}>적용</Button>
                        <Button size="small" icon={<CloseOutlined />} onClick={() => setTagSuggestion(null)}>무시</Button>
                      </Space>
                    }
                  >
                    <Space direction="vertical" size={8} style={{ width: '100%' }}>
                      <div>
                        <Text type="secondary" style={{ fontSize: 12 }}>추천 태그:</Text>
                        <div style={{ marginTop: 4 }}>
                          {tagSuggestion.recommended_tags.map((tag) => (
                            <Tag key={tag} color="blue" style={{ marginBottom: 4 }}>{tag}</Tag>
                          ))}
                        </div>
                      </div>
                      {tagSuggestion.new_tags.length > 0 && (
                        <div>
                          <Text type="secondary" style={{ fontSize: 12 }}>신규 태그 제안:</Text>
                          <div style={{ marginTop: 4 }}>
                            {tagSuggestion.new_tags.map((tag) => (
                              <Tag key={tag} color="green" style={{ marginBottom: 4 }}>{tag}</Tag>
                            ))}
                          </div>
                        </div>
                      )}
                      {tagSuggestion.recommended_domain && (
                        <div>
                          <Text type="secondary" style={{ fontSize: 12 }}>추천 도메인: </Text>
                          <Tag color="purple">{tagSuggestion.recommended_domain}</Tag>
                        </div>
                      )}
                      {tagSuggestion.reasoning && (
                        <Text type="secondary" style={{ fontSize: 12 }}>{tagSuggestion.reasoning}</Text>
                      )}
                    </Space>
                  </Card>
                )}

                <Title level={5}>컬럼 정보</Title>
                <Table
                  columns={[
                    { title: '컬럼명', dataIndex: 'physical_name', key: 'physical_name', render: (v: string) => <Text code>{v}</Text> },
                    {
                      title: '비즈니스명',
                      dataIndex: 'business_name',
                      key: 'business_name',
                      render: (_: string, col: any) => {
                        const val = getColumnValue(selectedTable.physical_name, col, 'business_name');
                        const key = `${selectedTable.physical_name}.${col.physical_name}`;
                        const isOverridden = !!columnOverrides[key]?.business_name;
                        return (
                          <Space size={4}>
                            <Text>{val as string}</Text>
                            {isOverridden && <Tag color="blue" style={{ fontSize: 10 }}>수정됨</Tag>}
                          </Space>
                        );
                      },
                    },
                    { title: '타입', dataIndex: 'data_type', key: 'data_type', render: (v: string) => <Tag>{v}</Tag> },
                    { title: 'PK', dataIndex: 'is_pk', key: 'is_pk', width: 50, render: (v: boolean) => v ? <Tag color="gold">PK</Tag> : null },
                    {
                      title: '민감도',
                      dataIndex: 'sensitivity',
                      key: 'sensitivity',
                      width: 70,
                      render: (v: string) => {
                        const colorMap: Record<string, string> = { high: 'red', medium: 'orange', low: 'green', PHI: 'red', Normal: 'green' };
                        return v ? <Tag color={colorMap[v] || 'default'}>{v}</Tag> : <Tag>일반</Tag>;
                      },
                    },
                    {
                      title: '',
                      key: 'edit',
                      width: 40,
                      render: (_: any, col: any) => (
                        <Button size="small" type="text" icon={<EditOutlined />} onClick={() => openColEdit(selectedTable.physical_name, col)} />
                      ),
                    },
                  ]}
                  dataSource={selectedTable.columns || []}
                  rowKey="physical_name"
                  size="small"
                  pagination={false}
                  scroll={{ y: 400 }}
                />
              </Spin>
            </Card>
          </Col>
        )}
      </Row>

      {/* 테이블 메타데이터 편집 Modal */}
      <Modal
        title={<>테이블 메타데이터 편집 — <Text code>{editingTableName}</Text></>}
        open={tableModalOpen}
        onOk={handleTableMetaSave}
        onCancel={() => setTableModalOpen(false)}
        okText="저장"
        cancelText="취소"
        width={520}
      >
        <Form form={tableForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="business_name" label="비즈니스명 (한글명)">
            <Input placeholder="예: 환자 기본정보" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={3} placeholder="테이블에 대한 설명" />
          </Form.Item>
          <Form.Item name="domain" label="도메인">
            <Select
              options={DOMAIN_OPTIONS.map((d) => ({ value: d, label: d }))}
              placeholder="도메인 선택"
              allowClear
            />
          </Form.Item>
          <Form.Item name="tags" label="태그">
            <Select
              mode="tags"
              placeholder="태그 입력 (Enter로 추가)"
              tokenSeparators={[',']}
            />
          </Form.Item>
          <Form.Item name="owner" label="담당자">
            <Input prefix={<UserOutlined />} placeholder="예: 홍길동" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 컬럼 메타데이터 편집 Modal */}
      <Modal
        title={<>컬럼 메타데이터 편집 — <Text code>{editingCol ? `${editingCol.table}.${editingCol.column}` : ''}</Text></>}
        open={colModalOpen}
        onOk={handleColMetaSave}
        onCancel={() => setColModalOpen(false)}
        okText="저장"
        cancelText="취소"
        width={480}
      >
        <Form form={colForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="business_name" label="비즈니스명 (한글명)">
            <Input placeholder="예: 환자ID" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="컬럼에 대한 설명" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  );
};

export default MetadataTab;
