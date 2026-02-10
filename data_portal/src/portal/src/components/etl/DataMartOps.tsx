/**
 * DIT-002: 데이터 마트 생성 및 운영 체계
 * 마트 카탈로그, 스키마 변경, Dimension 관리, 표준 지표, 데이터 흐름, 연결 최적화
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Segmented, Badge, List,
  Tooltip, Descriptions, Drawer, Popconfirm, Steps,
  Collapse, Tree,
} from 'antd';
import {
  AppstoreOutlined, PlusOutlined, DeleteOutlined,
  WarningOutlined, ReloadOutlined, BranchesOutlined,
  BookOutlined, ThunderboltOutlined,
  RocketOutlined, EyeOutlined,
  ApartmentOutlined, FundOutlined,
} from '@ant-design/icons';
import {
  API_BASE, fetchJSON, postJSON, putJSON, deleteJSON,
  ZONE_COLORS, CATEGORY_COLORS, STAGE_COLORS,
  buildTreeData, parseJsonField, FLOW_STEPS,
  ZONE_OPTIONS, CATEGORY_OPTIONS, CATALOG_VISIBLE_OPTIONS,
  CATALOG_SEARCH_COLUMNS, SUGGESTION_COLUMNS,
  SCHEMA_CHANGE_BASE_COLUMNS, DIMENSION_BASE_COLUMNS,
  METRICS_BASE_COLUMNS, OPTIMIZATION_BASE_COLUMNS,
  OVERVIEW_STATS, renderStageCard, renderMartDrawerContent,
} from './DataMartOpsHelpers';

const { Text } = Typography;

const DataMartOps: React.FC = () => {
  const { message } = App.useApp();
  const [view, setView] = useState<string>('catalog');
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [dimensionsLoading, setDimensionsLoading] = useState(false);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [flowLoading, setFlowLoading] = useState(false);
  const [optimizationLoading, setOptimizationLoading] = useState(false);
  const [overview, setOverview] = useState<any>(null);

  // -- Catalog state --
  const [marts, setMarts] = useState<any[]>([]);
  const [martModal, setMartModal] = useState(false);
  const [martForm] = Form.useForm();
  const [editMartId, setEditMartId] = useState<number | null>(null);
  const [martDrawer, setMartDrawer] = useState<{ open: boolean; data: any }>({ open: false, data: null });
  const [dupCheck, setDupCheck] = useState<any[] | null>(null);

  // -- Schema Changes --
  const [schemaChanges, setSchemaChanges] = useState<any[]>([]);

  // -- Dimensions --
  const [dimensions, setDimensions] = useState<any[]>([]);
  const [dimModal, setDimModal] = useState(false);
  const [dimForm] = Form.useForm();
  const [dimHierarchy, setDimHierarchy] = useState<{ open: boolean; data: any }>({ open: false, data: null });

  // -- Metrics --
  const [metrics, setMetrics] = useState<any[]>([]);
  const [metricModal, setMetricModal] = useState(false);
  const [metricForm] = Form.useForm();
  const [catalogSearch, setCatalogSearch] = useState('');
  const [catalogData, setCatalogData] = useState<any>(null);

  // -- Flow Stages --
  const [stages, setStages] = useState<any[]>([]);

  // -- Optimizations --
  const [optimizations, setOptimizations] = useState<any[]>([]);
  const [suggestions, setSuggestions] = useState<any[]>([]);

  const loadOverview = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/overview`);
      setOverview(data);
    } catch { /* ignore */ }
  }, []);

  const loadMarts = useCallback(async () => {
    setCatalogLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/marts`);
      setMarts(data);
    } catch { message.error('마트 목록 로드 실패'); }
    finally { setCatalogLoading(false); }
  }, [message]);

  const loadSchemaChanges = useCallback(async () => {
    setSchemaLoading(true);
    try { setSchemaChanges(await fetchJSON(`${API_BASE}/schema-changes`)); }
    catch { message.error('스키마 변경 로드 실패'); }
    finally { setSchemaLoading(false); }
  }, [message]);

  const loadDimensions = useCallback(async () => {
    setDimensionsLoading(true);
    try { setDimensions(await fetchJSON(`${API_BASE}/dimensions`)); }
    catch { message.error('Dimension 로드 실패'); }
    finally { setDimensionsLoading(false); }
  }, [message]);

  const loadMetrics = useCallback(async () => {
    setMetricsLoading(true);
    try { setMetrics(await fetchJSON(`${API_BASE}/metrics`)); }
    catch { message.error('지표 로드 실패'); }
    finally { setMetricsLoading(false); }
  }, [message]);

  const loadStages = useCallback(async () => {
    setFlowLoading(true);
    try { setStages(await fetchJSON(`${API_BASE}/flow-stages`)); }
    catch { message.error('흐름 단계 로드 실패'); }
    finally { setFlowLoading(false); }
  }, [message]);

  const loadOptimizations = useCallback(async () => {
    setOptimizationLoading(true);
    try {
      setOptimizations(await fetchJSON(`${API_BASE}/optimizations`));
      setSuggestions((await fetchJSON(`${API_BASE}/optimizations/suggestions`)).suggestions || []);
    } catch { message.error('최적화 로드 실패'); }
    finally { setOptimizationLoading(false); }
  }, [message]);

  useEffect(() => {
    loadOverview();
    if (view === 'catalog') loadMarts();
    else if (view === 'schema') loadSchemaChanges();
    else if (view === 'dimensions') loadDimensions();
    else if (view === 'metrics') loadMetrics();
    else if (view === 'flow') loadStages();
    else if (view === 'optimization') loadOptimizations();
  }, [view]);

  // -- Mart CRUD --
  const handleSaveMart = async () => {
    try {
      const vals = await martForm.validateFields();
      vals.source_tables = vals.source_tables_str ? vals.source_tables_str.split(',').map((s: string) => s.trim()).filter(Boolean) : [];
      delete vals.source_tables_str;
      if (vals.target_schema_str) {
        try { vals.target_schema = JSON.parse(vals.target_schema_str); } catch { vals.target_schema = null; }
      }
      delete vals.target_schema_str;

      if (editMartId) {
        await putJSON(`${API_BASE}/marts/${editMartId}`, vals);
        message.success('마트 수정 완료');
      } else {
        const result = await postJSON(`${API_BASE}/marts`, vals);
        if (result.duplicates?.length > 0) {
          setDupCheck(result.duplicates);
        }
        message.success('마트 생성 완료');
      }
      setMartModal(false);
      setEditMartId(null);
      martForm.resetFields();
      loadMarts();
    } catch { /* validation error */ }
  };

  const openMartDetail = async (martId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/marts/${martId}`);
      setMartDrawer({ open: true, data });
    } catch { message.error('마트 상세 로드 실패'); }
  };

  // -- Dimension hierarchy --
  const openHierarchy = async (dimId: number) => {
    try {
      const data = await fetchJSON(`${API_BASE}/dimensions/${dimId}/hierarchy`);
      setDimHierarchy({ open: true, data });
    } catch { message.error('계층 로드 실패'); }
  };

  // -- Catalog search --
  const searchCatalog = async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/metrics/catalog${catalogSearch ? `?search=${encodeURIComponent(catalogSearch)}` : ''}`);
      setCatalogData(data);
    } catch { message.error('카탈로그 검색 실패'); }
  };

  // ── Render sections ──

  const renderOverviewCards = () => {
    if (!overview) return null;
    return (
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        {OVERVIEW_STATS.map((s) => (
          <Col span={3} key={s.key}><Card size="small"><Statistic
            title={s.title} value={overview[s.key]} suffix={s.suffix}
            valueStyle={{ color: s.warnIfPositive && overview[s.key] > 0 ? '#faad14' : s.color }}
          /></Card></Col>
        ))}
      </Row>
    );
  };

  const renderCatalog = () => (
    <Spin spinning={catalogLoading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { setEditMartId(null); martForm.resetFields(); setMartModal(true); }}>마트 생성</Button>
        <Button icon={<ReloadOutlined />} onClick={loadMarts}>새로고침</Button>
      </Space>
      <Table
        dataSource={marts} rowKey="mart_id" size="small" pagination={false}
        columns={[
          { title: 'ID', dataIndex: 'mart_id', width: 50 },
          { title: '마트명', dataIndex: 'mart_name', width: 160,
            render: (v: string, r: any) => <a onClick={() => openMartDetail(r.mart_id)}>{v}</a> },
          { title: '용도', dataIndex: 'purpose', width: 200, ellipsis: true },
          { title: '영역', dataIndex: 'zone', width: 80, render: (v: string) => <Tag color={ZONE_COLORS[v]}>{v}</Tag> },
          { title: '소스 테이블', dataIndex: 'source_tables', width: 200,
            render: (v: any) => <Space wrap size={[4, 4]}>{parseJsonField(v).map((t: string) => <Tag key={t} style={{ fontSize: 11 }}>{t}</Tag>)}</Space>,
          },
          { title: '행 수', dataIndex: 'row_count', width: 100, render: (v: number) => v ? v.toLocaleString() : '-' },
          { title: '담당', dataIndex: 'owner', width: 90 },
          { title: '갱신 주기', dataIndex: 'refresh_schedule', width: 110 },
          { title: '상태', dataIndex: 'status', width: 70,
            render: (v: string) => <Tag color={v === 'active' ? 'green' : 'default'}>{v}</Tag> },
          { title: '대기 변경', dataIndex: 'pending_changes', width: 80,
            render: (v: number) => v > 0 ? <Badge count={v} /> : <Tag color="green">없음</Tag> },
          { title: '', width: 80, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="상세"><Button size="small" icon={<EyeOutlined />} onClick={() => openMartDetail(r.mart_id)} /></Tooltip>
              <Popconfirm title="아카이브?" onConfirm={async () => { await deleteJSON(`${API_BASE}/marts/${r.mart_id}`); loadMarts(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />

      {/* Duplicate check result */}
      {dupCheck && dupCheck.length > 0 && (
        <Card size="small" style={{ marginTop: 12, borderColor: '#faad14' }} title={<><WarningOutlined style={{ color: '#faad14' }} /> 유사 마트 감지</>}>
          <List size="small" dataSource={dupCheck} renderItem={(d: any) => (
            <List.Item>
              <Space>
                <Tag color="orange">{d.similarity}% 유사</Tag>
                <Text strong>{d.mart_name}</Text>
                {d.overlap_tables && <Text type="secondary">겹치는 테이블: {d.overlap_tables.join(', ')}</Text>}
                {d.reason === 'schema_hash_match' && <Tag color="red">스키마 동일</Tag>}
              </Space>
            </List.Item>
          )} />
          <Button size="small" onClick={() => setDupCheck(null)} style={{ marginTop: 8 }}>닫기</Button>
        </Card>
      )}

      {/* Mart Modal */}
      <Modal title={editMartId ? '마트 수정' : '마트 생성'} open={martModal} onOk={handleSaveMart}
        onCancel={() => { setMartModal(false); setEditMartId(null); }} width={600}>
        <Form form={martForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="mart_name" label="마트명" rules={[{ required: true }]}><Input /></Form.Item></Col>
            <Col span={12}><Form.Item name="zone" label="영역" initialValue="gold">
              <Select options={ZONE_OPTIONS} />
            </Form.Item></Col>
          </Row>
          <Form.Item name="purpose" label="용도" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="source_tables_str" label="소스 테이블 (쉼표 구분)"><Input placeholder="person,visit_occurrence,condition_occurrence" /></Form.Item>
          <Row gutter={16}>
            <Col span={8}><Form.Item name="owner" label="담당"><Input /></Form.Item></Col>
            <Col span={8}><Form.Item name="refresh_schedule" label="갱신 주기"><Input placeholder="0 2 * * *" /></Form.Item></Col>
            <Col span={8}><Form.Item name="retention_days" label="보관(일)" initialValue={365}><Input type="number" /></Form.Item></Col>
          </Row>
          <Form.Item name="target_schema_str" label="스키마 JSON (선택)"><Input.TextArea rows={2} placeholder='{"columns":["col1","col2"]}' /></Form.Item>
        </Form>
      </Modal>

      {/* Mart Detail Drawer */}
      <Drawer title={martDrawer.data?.mart_name} open={martDrawer.open} width={640}
        onClose={() => setMartDrawer({ open: false, data: null })}>
        {martDrawer.data && renderMartDrawerContent(martDrawer.data)}
      </Drawer>
    </Spin>
  );

  const renderSchemaChanges = () => (
    <Spin spinning={schemaLoading}>
      <Button icon={<ReloadOutlined />} onClick={loadSchemaChanges} style={{ marginBottom: 16 }}>새로고침</Button>
      <Table
        dataSource={schemaChanges} rowKey="change_id" size="small" pagination={false}
        columns={[
          ...SCHEMA_CHANGE_BASE_COLUMNS,
          { title: '', width: 120, render: (_: any, r: any) => r.status === 'pending' ? (
            <Space size={4}>
              <Popconfirm title="적용?" onConfirm={async () => { await putJSON(`${API_BASE}/schema-changes/${r.change_id}/apply`); loadSchemaChanges(); }}>
                <Button size="small" type="primary">적용</Button>
              </Popconfirm>
              <Popconfirm title="롤백?" onConfirm={async () => { await putJSON(`${API_BASE}/schema-changes/${r.change_id}/rollback`); loadSchemaChanges(); }}>
                <Button size="small" danger>롤백</Button>
              </Popconfirm>
            </Space>
          ) : null },
        ]}
      />
    </Spin>
  );

  const renderDimensions = () => (
    <Spin spinning={dimensionsLoading}>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<PlusOutlined />} type="primary" onClick={() => { dimForm.resetFields(); setDimModal(true); }}>Dimension 추가</Button>
        <Button icon={<ReloadOutlined />} onClick={loadDimensions}>새로고침</Button>
      </Space>
      <Table
        dataSource={dimensions} rowKey="dim_id" size="small" pagination={false}
        columns={[
          ...DIMENSION_BASE_COLUMNS,
          { title: '', width: 100, render: (_: any, r: any) => (
            <Space size={4}>
              <Tooltip title="계층 보기"><Button size="small" icon={<ApartmentOutlined />} onClick={() => openHierarchy(r.dim_id)} /></Tooltip>
              <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/dimensions/${r.dim_id}`); loadDimensions(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />

      {/* Dimension Modal */}
      <Modal title="Dimension 추가" open={dimModal} onCancel={() => setDimModal(false)}
        onOk={async () => {
          try {
            const vals = await dimForm.validateFields();
            vals.hierarchy_levels = vals.hierarchy_str ? JSON.parse(vals.hierarchy_str) : [];
            vals.attributes = vals.attributes_str ? JSON.parse(vals.attributes_str) : [];
            vals.mart_ids = vals.mart_ids_str ? vals.mart_ids_str.split(',').map((s: string) => parseInt(s.trim())).filter(Boolean) : [];
            delete vals.hierarchy_str; delete vals.attributes_str; delete vals.mart_ids_str;
            await postJSON(`${API_BASE}/dimensions`, vals);
            message.success('Dimension 생성 완료');
            setDimModal(false);
            loadDimensions();
          } catch { /* validation */ }
        }} width={560}>
        <Form form={dimForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="dimension_name" label="물리명" rules={[{ required: true }]}><Input placeholder="dim_time" /></Form.Item></Col>
            <Col span={12}><Form.Item name="logical_name" label="논리명" rules={[{ required: true }]}><Input placeholder="시간 Dimension" /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="hierarchy_str" label='계층 JSON (예: [{"level":"year","key":"year"}])'>
            <Input.TextArea rows={2} placeholder='[{"level":"year","key":"year"},{"level":"month","key":"month"}]' />
          </Form.Item>
          <Form.Item name="attributes_str" label='속성 JSON (예: [{"name":"is_weekend","type":"boolean"}])'>
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="mart_ids_str" label="연결 마트 ID (쉼표 구분)"><Input placeholder="1,2,3" /></Form.Item>
        </Form>
      </Modal>

      {/* Hierarchy Drawer */}
      <Drawer title="Dimension 계층 구조" open={dimHierarchy.open} width={480}
        onClose={() => setDimHierarchy({ open: false, data: null })}>
        {dimHierarchy.data && (<>
          <Descriptions column={1} size="small" bordered style={{ marginBottom: 16 }}>
            <Descriptions.Item label="물리명">{dimHierarchy.data.dimension?.dimension_name}</Descriptions.Item>
            <Descriptions.Item label="논리명">{dimHierarchy.data.dimension?.logical_name}</Descriptions.Item>
            <Descriptions.Item label="설명">{dimHierarchy.data.dimension?.description}</Descriptions.Item>
          </Descriptions>
          <Card size="small" title="계층 트리">
            <Tree
              showLine
              defaultExpandAll
              treeData={[buildTreeData(dimHierarchy.data.tree)]}
            />
          </Card>
        </>)}
      </Drawer>
    </Spin>
  );

  const renderMetrics = () => (
    <Spin spinning={metricsLoading}>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col flex="auto">
          <Space>
            <Button icon={<PlusOutlined />} type="primary" onClick={() => { metricForm.resetFields(); setMetricModal(true); }}>지표 추가</Button>
            <Button icon={<ReloadOutlined />} onClick={loadMetrics}>새로고침</Button>
          </Space>
        </Col>
        <Col>
          <Space>
            <Input.Search placeholder="카탈로그 검색" value={catalogSearch} onChange={e => setCatalogSearch(e.target.value)}
              onSearch={searchCatalog} enterButton={<><BookOutlined /> 카탈로그</>} style={{ width: 320 }} />
          </Space>
        </Col>
      </Row>

      {catalogData && (
        <Card size="small" style={{ marginBottom: 16, borderColor: '#1890ff' }}
          title={<><BookOutlined /> 카탈로그 검색 결과 ({catalogData.total}건)</>}
          extra={<Button size="small" onClick={() => setCatalogData(null)}>닫기</Button>}>
          {Object.entries(catalogData.categories || {}).map(([cat, items]: [string, any]) => (
            <div key={cat} style={{ marginBottom: 12 }}>
              <Tag color={CATEGORY_COLORS[cat]}>{cat}</Tag>
              <span style={{ fontWeight: 600 }}>{(items as any[]).length}건</span>
              <Table size="small" dataSource={items as any[]} rowKey="metric_id" pagination={false} style={{ marginTop: 4 }}
                columns={CATALOG_SEARCH_COLUMNS}
              />
            </div>
          ))}
        </Card>
      )}

      <Table
        dataSource={metrics} rowKey="metric_id" size="small" pagination={false}
        columns={[
          ...METRICS_BASE_COLUMNS,
          { title: '', width: 60, render: (_: any, r: any) => (
            <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/metrics/${r.metric_id}`); loadMetrics(); }}>
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          )},
        ]}
      />

      {/* Metric Modal */}
      <Modal title="표준 지표 추가" open={metricModal} onCancel={() => setMetricModal(false)}
        onOk={async () => {
          try {
            const vals = await metricForm.validateFields();
            vals.dimension_ids = vals.dimension_ids_str ? vals.dimension_ids_str.split(',').map((s: string) => parseInt(s.trim())).filter(Boolean) : [];
            delete vals.dimension_ids_str;
            await postJSON(`${API_BASE}/metrics`, vals);
            message.success('지표 생성 완료');
            setMetricModal(false);
            loadMetrics();
          } catch { /* validation */ }
        }} width={560}>
        <Form form={metricForm} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}><Form.Item name="metric_name" label="물리명" rules={[{ required: true }]}><Input placeholder="patient_count" /></Form.Item></Col>
            <Col span={12}><Form.Item name="logical_name" label="논리명" rules={[{ required: true }]}><Input placeholder="환자 수" /></Form.Item></Col>
          </Row>
          <Form.Item name="formula" label="산식" rules={[{ required: true }]}><Input placeholder="COUNT(DISTINCT person_id)" /></Form.Item>
          <Row gutter={16}>
            <Col span={8}><Form.Item name="unit" label="단위"><Input placeholder="명" /></Form.Item></Col>
            <Col span={8}><Form.Item name="category" label="분류" initialValue="clinical">
              <Select options={CATEGORY_OPTIONS} />
            </Form.Item></Col>
            <Col span={8}><Form.Item name="mart_id" label="마트 ID"><Input type="number" /></Form.Item></Col>
          </Row>
          <Form.Item name="description" label="설명"><Input.TextArea rows={2} /></Form.Item>
          <Form.Item name="dimension_ids_str" label="연결 Dimension ID (쉼표 구분)"><Input placeholder="1,2" /></Form.Item>
          <Form.Item name="catalog_visible" label="카탈로그 공개" initialValue={true}>
            <Select options={CATALOG_VISIBLE_OPTIONS} />
          </Form.Item>
        </Form>
      </Modal>
    </Spin>
  );

  const renderFlowStages = () => (
    <Spin spinning={flowLoading}>
      <Button icon={<ReloadOutlined />} onClick={loadStages} style={{ marginBottom: 16 }}>새로고침</Button>

      <Card size="small" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 0, overflowX: 'auto', padding: '16px 0' }}>
          {stages.map((s, i) => (
            <React.Fragment key={s.stage_id}>
              {renderStageCard(s)}
              {i < stages.length - 1 && <div style={{ fontSize: 24, color: '#999', padding: '0 8px' }}>→</div>}
            </React.Fragment>
          ))}
        </div>
      </Card>

      {/* Stage details */}
      <Collapse
        items={stages.map((s: any) => {
          const rules = typeof s.processing_rules === 'string' ? JSON.parse(s.processing_rules) : s.processing_rules;
          const meta = typeof s.meta_config === 'string' ? JSON.parse(s.meta_config) : s.meta_config;
          return {
            key: s.stage_id,
            label: <Space><Tag color={STAGE_COLORS[s.stage_type]}>{s.stage_type}</Tag><Text strong>{s.stage_name}</Text></Space>,
            children: (
              <Row gutter={16}>
                <Col span={12}>
                  <Descriptions column={1} size="small" bordered>
                    <Descriptions.Item label="저장소">{s.storage_type}</Descriptions.Item>
                    <Descriptions.Item label="파일 포맷">{s.file_format}</Descriptions.Item>
                    <Descriptions.Item label="설명">{s.description}</Descriptions.Item>
                  </Descriptions>
                </Col>
                <Col span={6}>
                  <Card size="small" title="처리 규칙">
                    {rules ? Object.entries(rules).map(([k, v]) => (
                      <div key={k}><Text code>{k}</Text>: <Text>{String(v)}</Text></div>
                    )) : <Empty description="없음" />}
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small" title="메타 구성">
                    {meta ? Object.entries(meta).map(([k, v]) => (
                      <div key={k}><Text code>{k}</Text>: <Text>{String(v)}</Text></div>
                    )) : <Empty description="없음" />}
                  </Card>
                </Col>
              </Row>
            ),
          };
        })}
      />

      {/* Flow description */}
      <Card size="small" style={{ marginTop: 16 }} title="비정형→정형 / 비표준→표준 변환 Flow">
        <Steps direction="vertical" size="small" items={FLOW_STEPS} />
      </Card>
    </Spin>
  );

  const renderOptimization = () => (
    <Spin spinning={optimizationLoading}>
      <Button icon={<ReloadOutlined />} onClick={loadOptimizations} style={{ marginBottom: 16 }}>새로고침</Button>

      {/* Suggestions */}
      {suggestions.length > 0 && (
        <Card size="small" style={{ marginBottom: 16, borderColor: '#722ed1' }}
          title={<><ThunderboltOutlined style={{ color: '#722ed1' }} /> 최적화 제안 ({suggestions.length}건)</>}>
          <Table size="small" dataSource={suggestions} rowKey={(r) => `${r.mart_name}-${r.opt_type}-${r.reason}`} pagination={false}
            columns={SUGGESTION_COLUMNS}
          />
        </Card>
      )}

      {/* Applied / proposed */}
      <Table
        dataSource={optimizations} rowKey="opt_id" size="small" pagination={false}
        columns={[
          ...OPTIMIZATION_BASE_COLUMNS,
          { title: '', width: 120, render: (_: any, r: any) => (
            <Space size={4}>
              {r.status === 'proposed' && (
                <Popconfirm title="적용?" onConfirm={async () => { await putJSON(`${API_BASE}/optimizations/${r.opt_id}/apply`); loadOptimizations(); }}>
                  <Button size="small" type="primary">적용</Button>
                </Popconfirm>
              )}
              <Popconfirm title="삭제?" onConfirm={async () => { await deleteJSON(`${API_BASE}/optimizations/${r.opt_id}`); loadOptimizations(); }}>
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </Space>
          )},
        ]}
      />
    </Spin>
  );

  return (
    <div>
      {renderOverviewCards()}
      <Segmented
        block
        value={view}
        onChange={(v) => setView(v as string)}
        options={[
          { label: '마트 카탈로그', value: 'catalog', icon: <AppstoreOutlined /> },
          { label: '스키마 변경', value: 'schema', icon: <BranchesOutlined /> },
          { label: 'Dimension 관리', value: 'dimensions', icon: <ApartmentOutlined /> },
          { label: '표준 지표', value: 'metrics', icon: <FundOutlined /> },
          { label: '데이터 흐름', value: 'flow', icon: <RocketOutlined /> },
          { label: '연결 최적화', value: 'optimization', icon: <ThunderboltOutlined /> },
        ]}
        style={{ marginBottom: 16 }}
      />
      {view === 'catalog' && renderCatalog()}
      {view === 'schema' && renderSchemaChanges()}
      {view === 'dimensions' && renderDimensions()}
      {view === 'metrics' && renderMetrics()}
      {view === 'flow' && renderFlowStages()}
      {view === 'optimization' && renderOptimization()}
    </div>
  );
};

export default DataMartOps;
