/**
 * DataDesign - Tab 3: 논리/물리 ERD
 * Entity table, relation table, ERD graph, entity/relation add drawers
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Segmented,
  Spin, Drawer, Select, Input, Form, Empty, App,
} from 'antd';
import { PlusOutlined, ArrowRightOutlined } from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { Entity, EntityDetail, Relation, Zone, ERDNode, ERDEdge, ZONE_COLORS, ZONE_LABELS } from './types';
import ERDGraph from './ERDGraph';

const { Text } = Typography;

const ERDTab: React.FC = () => {
  const { message } = App.useApp();

  // Data
  const [entities, setEntities] = useState<Entity[]>([]);
  const [relations, setRelations] = useState<Relation[]>([]);
  const [zones, setZones] = useState<Zone[]>([]);
  const [erdNodes, setErdNodes] = useState<ERDNode[]>([]);
  const [erdEdges, setErdEdges] = useState<ERDEdge[]>([]);

  // Filters
  const [zoneFilter, setZoneFilter] = useState<string>('all');
  const [erdZoneFilter, setErdZoneFilter] = useState<string | undefined>(undefined);

  // Entity detail expansion
  const [expandedEntityDetail, setExpandedEntityDetail] = useState<Record<number, EntityDetail | null>>({});

  // Entity add drawer
  const [entityDrawerOpen, setEntityDrawerOpen] = useState(false);
  const [entityForm] = Form.useForm();

  // Relation add drawer
  const [relationDrawerOpen, setRelationDrawerOpen] = useState(false);
  const [relationForm] = Form.useForm();

  // ─── API calls ───

  const fetchEntities = useCallback(async () => {
    try {
      const data = await dataDesignApi.getEntities();
      setEntities(data.entities || []);
    } catch {
      message.error('엔티티 데이터 로딩 실패');
    }
  }, [message]);

  const fetchRelations = useCallback(async () => {
    try {
      const data = await dataDesignApi.getRelations();
      setRelations(data.relations || []);
    } catch {
      message.error('관계 데이터 로딩 실패');
    }
  }, [message]);

  const fetchZones = useCallback(async () => {
    try {
      const data = await dataDesignApi.getZones();
      setZones(data.zones || []);
    } catch {
      // zones are optional for ERD tab, don't show error
    }
  }, []);

  const fetchERDGraph = useCallback(async (zoneType?: string) => {
    try {
      const data = await dataDesignApi.getERDGraph(zoneType);
      setErdNodes(data.nodes || []);
      setErdEdges(data.edges || []);
    } catch {
      // ERD graph is optional, don't show error
    }
  }, []);

  useEffect(() => {
    fetchEntities();
    fetchRelations();
    fetchZones();
    fetchERDGraph(erdZoneFilter);
  }, [fetchEntities, fetchRelations, fetchZones, fetchERDGraph, erdZoneFilter]);

  // ─── Entity detail expansion ───

  const loadEntityDetail = async (entityId: number) => {
    if (expandedEntityDetail[entityId]) return;
    try {
      const data = await dataDesignApi.getEntity(entityId);
      setExpandedEntityDetail(prev => ({ ...prev, [entityId]: data }));
    } catch {
      message.error('엔티티 상세 로딩 실패');
    }
  };

  // ─── Create handlers ───

  const handleCreateEntity = async () => {
    try {
      const values = await entityForm.validateFields();
      await dataDesignApi.createEntity(values);
      message.success('엔티티 생성 완료');
      setEntityDrawerOpen(false);
      entityForm.resetFields();
      fetchEntities();
    } catch {
      message.error('엔티티 생성 실패');
    }
  };

  const handleCreateRelation = async () => {
    try {
      const values = await relationForm.validateFields();
      await dataDesignApi.createRelation(values);
      message.success('관계 생성 완료');
      setRelationDrawerOpen(false);
      relationForm.resetFields();
      fetchRelations();
    } catch {
      message.error('관계 생성 실패');
    }
  };

  // ─── Filtered data ───

  const filteredEntities = zoneFilter === 'all'
    ? entities
    : entities.filter(e => e.zone_type === zoneFilter);

  // ─── Column definitions ───

  const entityColumns = [
    {
      title: '엔티티명',
      dataIndex: 'entity_name',
      key: 'entity_name',
      render: (v: string) => <Text strong style={{ fontFamily: 'monospace' }}>{v}</Text>,
    },
    { title: '논리명', dataIndex: 'logical_name', key: 'logical_name' },
    {
      title: 'Zone',
      dataIndex: 'zone_type',
      key: 'zone_type',
      render: (v: string) => v ? <Tag color={ZONE_COLORS[v]}>{ZONE_LABELS[v] || v}</Tag> : '-',
    },
    { title: '도메인', dataIndex: 'domain', key: 'domain', render: (v: string) => v || '-' },
    {
      title: '유형',
      dataIndex: 'entity_type',
      key: 'entity_type',
      render: (v: string) => <Tag>{v}</Tag>,
    },
    { title: '컬럼 수', dataIndex: 'column_count', key: 'column_count', align: 'right' as const },
    {
      title: '행 수',
      dataIndex: 'row_count',
      key: 'row_count',
      align: 'right' as const,
      render: (v: number) => (v || 0).toLocaleString(),
    },
  ];

  const relationColumns = [
    {
      title: '소스 엔티티',
      dataIndex: 'source_entity',
      key: 'source_entity',
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: '',
      key: 'arrow',
      width: 40,
      render: () => <ArrowRightOutlined style={{ color: '#999' }} />,
    },
    {
      title: '대상 엔티티',
      dataIndex: 'target_entity',
      key: 'target_entity',
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: '관계 유형',
      dataIndex: 'relation_type',
      key: 'relation_type',
      render: (v: string) => {
        const color = v === '1:1' ? 'blue' : v === '1:N' ? 'green' : v === 'N:M' ? 'orange' : 'purple';
        return <Tag color={color}>{v}</Tag>;
      },
    },
    {
      title: 'FK 컬럼',
      dataIndex: 'fk_columns',
      key: 'fk_columns',
      render: (v: string) => v ? <Text code style={{ fontSize: 12 }}>{v}</Text> : '-',
    },
    { title: '설명', dataIndex: 'description', key: 'description', ellipsis: true },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* Filters + Actions */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
        <Segmented
          value={zoneFilter}
          onChange={(v) => {
            const val = v as string;
            setZoneFilter(val);
            setErdZoneFilter(val === 'all' ? undefined : val);
          }}
          options={[
            { label: '전체', value: 'all' },
            { label: 'Source', value: 'source' },
            { label: 'Silver', value: 'silver' },
            { label: 'Gold', value: 'gold' },
          ]}
        />
        <Space>
          <Button icon={<PlusOutlined />} onClick={() => setEntityDrawerOpen(true)}>엔티티 추가</Button>
          <Button icon={<PlusOutlined />} onClick={() => setRelationDrawerOpen(true)}>관계 추가</Button>
        </Space>
      </div>

      {/* ERD Graph Visualization */}
      <ERDGraph erdNodes={erdNodes} erdEdges={erdEdges} />

      {/* Entities Table */}
      <Card title={`엔티티 (${filteredEntities.length})`} size="small">
        <Table
          dataSource={filteredEntities}
          columns={entityColumns}
          rowKey="entity_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true, showTotal: (t) => `총 ${t}개` }}
          expandable={{
            expandedRowRender: (record) => {
              const detail = expandedEntityDetail[record.entity_id];
              if (!detail) return <Spin size="small" />;
              if (!detail.columns || detail.columns.length === 0) return <Empty description="컬럼 정보 없음" />;
              return (
                <Table
                  dataSource={detail.columns}
                  rowKey="name"
                  size="small"
                  pagination={false}
                  columns={[
                    { title: '컬럼명', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                    { title: '데이터 타입', dataIndex: 'type', key: 'type', render: (v: string) => <Tag>{v}</Tag> },
                    {
                      title: 'Nullable',
                      dataIndex: 'nullable',
                      key: 'nullable',
                      render: (v: boolean) => v
                        ? <Tag color="default">NULL</Tag>
                        : <Tag color="red">NOT NULL</Tag>,
                    },
                  ]}
                />
              );
            },
            onExpand: (expanded, record) => {
              if (expanded) loadEntityDetail(record.entity_id);
            },
          }}
        />
      </Card>

      {/* Relations Table */}
      <Card title={`관계 (${relations.length})`} size="small">
        <Table
          dataSource={relations}
          columns={relationColumns}
          rowKey="relation_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true, showTotal: (t) => `총 ${t}개` }}
        />
      </Card>

      {/* Entity Add Drawer */}
      <Drawer
        title="엔티티 추가"
        open={entityDrawerOpen}
        onClose={() => { setEntityDrawerOpen(false); entityForm.resetFields(); }}
        width={420}
        extra={<Button type="primary" onClick={handleCreateEntity}>생성</Button>}
      >
        <Form form={entityForm} layout="vertical">
          <Form.Item name="entity_name" label="엔티티명 (물리)" rules={[{ required: true }]}>
            <Input placeholder="e.g. patient_summary" />
          </Form.Item>
          <Form.Item name="logical_name" label="논리명" rules={[{ required: true }]}>
            <Input placeholder="e.g. 환자 요약" />
          </Form.Item>
          <Form.Item name="zone_id" label="Zone">
            <Select
              allowClear
              placeholder="Zone 선택"
              options={zones.map(z => ({ value: z.zone_id, label: z.zone_name }))}
            />
          </Form.Item>
          <Form.Item name="domain" label="도메인">
            <Input placeholder="e.g. 환자, 진료, 검사" />
          </Form.Item>
          <Form.Item name="entity_type" label="유형" initialValue="table">
            <Select options={[
              { value: 'table', label: 'Table' },
              { value: 'view', label: 'View' },
              { value: 'materialized_view', label: 'Materialized View' },
              { value: 'external', label: 'External' },
            ]} />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={3} />
          </Form.Item>
        </Form>
      </Drawer>

      {/* Relation Add Drawer */}
      <Drawer
        title="관계 추가"
        open={relationDrawerOpen}
        onClose={() => { setRelationDrawerOpen(false); relationForm.resetFields(); }}
        width={420}
        extra={<Button type="primary" onClick={handleCreateRelation}>생성</Button>}
      >
        <Form form={relationForm} layout="vertical">
          <Form.Item name="source_entity" label="소스 엔티티" rules={[{ required: true }]}>
            <Select
              showSearch
              placeholder="소스 엔티티 선택"
              options={entities.map(e => ({ value: e.entity_name, label: `${e.entity_name} (${e.logical_name})` }))}
            />
          </Form.Item>
          <Form.Item name="target_entity" label="대상 엔티티" rules={[{ required: true }]}>
            <Select
              showSearch
              placeholder="대상 엔티티 선택"
              options={entities.map(e => ({ value: e.entity_name, label: `${e.entity_name} (${e.logical_name})` }))}
            />
          </Form.Item>
          <Form.Item name="relation_type" label="관계 유형" initialValue="1:N">
            <Select options={[
              { value: '1:1', label: '1:1' },
              { value: '1:N', label: '1:N' },
              { value: 'N:M', label: 'N:M' },
              { value: 'inheritance', label: 'Inheritance' },
            ]} />
          </Form.Item>
          <Form.Item name="fk_columns" label="FK 컬럼">
            <Input placeholder="e.g. person_id" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Drawer>
    </Space>
  );
};

export default ERDTab;
