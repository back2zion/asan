/**
 * 데이터 조합 패널 (DPR-002: 드래그앤드롭 비주얼 조인 빌더)
 * - ReactFlow 기반 테이블 노드 + FK 엣지
 * - 조인 경로 자동 탐색
 * - SQL 생성 + 미리보기
 */

import React, { useState, useCallback, useMemo, useEffect } from 'react';
import {
  Row, Col, Card, Button, Space, Typography, Tag, List, Table as AntTable,
  Spin, Empty, Divider, App, Tooltip, Select,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, PlayCircleOutlined,
  CopyOutlined, DatabaseOutlined, ApiOutlined, CameraOutlined,
} from '@ant-design/icons';
import ReactFlow, {
  Background, Controls, MiniMap,
  useNodesState, useEdgesState,
} from 'reactflow';
import type { Node, Edge } from 'reactflow';
import 'reactflow/dist/style.css';
import { useQuery } from '@tanstack/react-query';
import { catalogComposeApi } from '../../services/catalogComposeApi';
import ComposeTableNode from './ComposeTableNode';

const { Text, Title } = Typography;

const nodeTypes = { tableNode: ComposeTableNode };

const TABLE_ALIASES: Record<string, string> = {
  person: 'p', visit_occurrence: 'vo', visit_detail: 'vd',
  condition_occurrence: 'co', condition_era: 'ce', drug_exposure: 'de',
  drug_era: 'dre', procedure_occurrence: 'po', measurement: 'm',
  observation: 'obs', observation_period: 'op', cost: 'c',
  payer_plan_period: 'pp', care_site: 'cs', provider: 'pv', location: 'loc',
};

const DataComposePanel: React.FC = () => {
  const { message } = App.useApp();

  // State
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Record<string, string[]>>({});
  const [generatedSql, setGeneratedSql] = useState<string | null>(null);
  const [previewResult, setPreviewResult] = useState<{ columns: string[]; rows: Record<string, unknown>[] } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [sqlLoading, setSqlLoading] = useState(false);
  const [joins, setJoins] = useState<Array<{ left_table: string; left_column: string; right_table: string; right_column: string; join_type: string }>>([]);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  // Fetch FK graph
  const { data: fkData } = useQuery({
    queryKey: ['fk-relationships'],
    queryFn: () => catalogComposeApi.getFkRelationships(),
    staleTime: 300_000,
  });

  const allTables = useMemo(() => fkData?.tables || [], [fkData]);
  const tableColumns = useMemo(() => fkData?.table_columns || {}, [fkData]);
  const availableTables = useMemo(
    () => allTables.filter((t: string) => !selectedTables.includes(t)),
    [allTables, selectedTables],
  );

  // Column toggle handler
  const handleColumnToggle = useCallback((tableName: string, columnName: string) => {
    setSelectedColumns((prev) => {
      const current = prev[tableName] || [];
      const updated = current.includes(columnName)
        ? current.filter((c) => c !== columnName)
        : [...current, columnName];
      return { ...prev, [tableName]: updated };
    });
  }, []);

  // Remove table handler
  const handleRemoveTable = useCallback((tableName: string) => {
    setSelectedTables((prev) => prev.filter((t) => t !== tableName));
    setSelectedColumns((prev) => {
      const next = { ...prev };
      delete next[tableName];
      return next;
    });
    setGeneratedSql(null);
    setPreviewResult(null);
  }, []);

  // Add table
  const handleAddTable = useCallback((tableName: string) => {
    if (selectedTables.includes(tableName)) return;
    setSelectedTables((prev) => [...prev, tableName]);
    // Auto-select PK columns
    const cols = (tableColumns[tableName] || []) as Array<{ name: string; pk?: boolean }>;
    const pks = cols.filter((c) => c.pk).map((c) => c.name);
    setSelectedColumns((prev) => ({
      ...prev,
      [tableName]: pks.length > 0 ? pks : cols.slice(0, 3).map((c) => c.name),
    }));
    setGeneratedSql(null);
    setPreviewResult(null);
  }, [selectedTables, tableColumns]);

  // Update ReactFlow nodes/edges when tables change
  useEffect(() => {
    const newNodes: Node[] = selectedTables.map((t, idx) => ({
      id: t,
      type: 'tableNode',
      position: { x: 50 + (idx % 3) * 270, y: 30 + Math.floor(idx / 3) * 300 },
      data: {
        tableName: t,
        columns: (tableColumns[t] || []) as Array<{ name: string; type: string; pk?: boolean }>,
        selectedColumns: selectedColumns[t] || [],
        onColumnToggle: handleColumnToggle,
        onRemove: handleRemoveTable,
      },
    }));
    setNodes(newNodes);
  }, [selectedTables, selectedColumns, tableColumns, handleColumnToggle, handleRemoveTable, setNodes]);

  // Auto-detect joins and update edges
  useEffect(() => {
    if (selectedTables.length < 2) {
      setEdges([]);
      setJoins([]);
      return;
    }

    catalogComposeApi.detectJoins(selectedTables).then((result) => {
      setJoins(result.joins || []);
      const newEdges: Edge[] = (result.joins || []).map((j: { left_table: string; left_column: string; right_table: string; right_column: string; join_type: string }, idx: number) => ({
        id: `edge-${idx}`,
        source: j.left_table,
        target: j.right_table,
        label: `${j.left_column} = ${j.right_column}`,
        animated: true,
        style: { stroke: '#005BAC', strokeWidth: 2 },
        labelStyle: { fontSize: 10, fill: '#666' },
        labelBgStyle: { fill: '#fff', fillOpacity: 0.9 },
      }));
      setEdges(newEdges);
    }).catch(() => {
      setEdges([]);
      setJoins([]);
    });
  }, [selectedTables, setEdges]);

  // Generate SQL
  const handleGenerateSql = async () => {
    if (selectedTables.length === 0) {
      message.warning('테이블을 1개 이상 선택하세요');
      return;
    }
    setSqlLoading(true);
    try {
      const result = await catalogComposeApi.generateSql({
        tables: selectedTables,
        selected_columns: selectedColumns,
        joins: joins.length > 0 ? joins : undefined,
        limit: 100,
      });
      setGeneratedSql(result.sql);
    } catch {
      message.error('SQL 생성에 실패했습니다');
    }
    setSqlLoading(false);
  };

  // Preview
  const handlePreview = async () => {
    if (!generatedSql) {
      await handleGenerateSql();
      return;
    }
    setPreviewLoading(true);
    try {
      const result = await catalogComposeApi.preview(generatedSql, 20);
      setPreviewResult(result);
    } catch (err: unknown) {
      const errMsg = err instanceof Error ? err.message : 'SQL 실행 오류';
      message.error(errMsg);
    }
    setPreviewLoading(false);
  };

  const handleCopy = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    message.success(`${label}이(가) 복사되었습니다`);
  };

  return (
    <div>
      <Row gutter={16}>
        {/* 좌측: 테이블 목록 */}
        <Col span={6}>
          <Card title={<><DatabaseOutlined /> 테이블 목록</>} size="small" style={{ height: '100%' }}>
            <Select
              showSearch
              placeholder="테이블 추가..."
              style={{ width: '100%', marginBottom: 12 }}
              onSelect={handleAddTable}
              value={undefined}
              options={availableTables.map((t: string) => ({ value: t, label: t }))}
              filterOption={(input, option) =>
                (option?.label as string)?.toLowerCase().includes(input.toLowerCase()) ?? false
              }
            />
            <List
              size="small"
              dataSource={selectedTables}
              renderItem={(t) => (
                <List.Item
                  actions={[
                    <Button key="del" type="text" size="small" danger icon={<DeleteOutlined />} onClick={() => handleRemoveTable(t)} />,
                  ]}
                >
                  <Space size={4}>
                    <Tag color="blue">{TABLE_ALIASES[t] || t.slice(0, 2)}</Tag>
                    <Text style={{ fontSize: 12 }}>{t}</Text>
                  </Space>
                </List.Item>
              )}
              locale={{ emptyText: '테이블을 추가하세요' }}
            />

            {/* 선택된 컬럼 요약 */}
            {selectedTables.length > 0 && (
              <>
                <Divider style={{ margin: '12px 0 8px' }} />
                <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 4 }}>
                  선택된 컬럼 ({Object.values(selectedColumns).flat().length}개)
                </Text>
                {selectedTables.map((t) => {
                  const cols = selectedColumns[t] || [];
                  if (cols.length === 0) return null;
                  return (
                    <div key={t} style={{ marginBottom: 4 }}>
                      <Text strong style={{ fontSize: 11 }}>{t}</Text>
                      <div>
                        {cols.map((c) => (
                          <Tag key={c} closable onClose={() => handleColumnToggle(t, c)} style={{ fontSize: 10, margin: '2px 2px' }}>
                            {c}
                          </Tag>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </>
            )}
          </Card>
        </Col>

        {/* 우측: ReactFlow 캔버스 + SQL */}
        <Col span={18}>
          <Card size="small" style={{ marginBottom: 12 }}>
            <div style={{ height: 350, border: '1px solid #f0f0f0', borderRadius: 6 }}>
              {selectedTables.length > 0 ? (
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  nodeTypes={nodeTypes}
                  fitView
                  fitViewOptions={{ padding: 0.2 }}
                  minZoom={0.3}
                  maxZoom={1.5}
                >
                  <Background />
                  <Controls />
                  <MiniMap style={{ height: 80 }} />
                </ReactFlow>
              ) : (
                <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
                  <Empty
                    description={
                      <span>
                        왼쪽에서 테이블을 추가하여<br />
                        비주얼 조인 빌더를 시작하세요
                      </span>
                    }
                  />
                </div>
              )}
            </div>
          </Card>

          {/* 생성된 SQL + 액션 */}
          <Card size="small">
            <Space style={{ marginBottom: 12 }}>
              <Button
                type="primary"
                icon={<ApiOutlined />}
                onClick={handleGenerateSql}
                loading={sqlLoading}
                disabled={selectedTables.length === 0}
              >
                SQL 생성
              </Button>
              <Button
                icon={<PlayCircleOutlined />}
                onClick={handlePreview}
                loading={previewLoading}
                disabled={!generatedSql}
              >
                미리보기
              </Button>
              {generatedSql && (
                <Button icon={<CopyOutlined />} onClick={() => handleCopy(generatedSql, 'SQL')}>
                  SQL 복사
                </Button>
              )}
            </Space>

            {generatedSql && (
              <pre style={{
                background: '#f5f5f5', padding: 12, borderRadius: 4,
                fontSize: 12, overflow: 'auto', maxHeight: 160,
                fontFamily: 'monospace',
              }}>
                {generatedSql}
              </pre>
            )}

            {/* 미리보기 결과 */}
            {previewResult && (
              <>
                <Divider style={{ margin: '12px 0' }} />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                  <Text strong>미리보기 결과 ({previewResult.rows?.length || 0}행)</Text>
                </div>
                <AntTable
                  dataSource={(previewResult.rows || []).map((r, i) => ({ ...r, _key: i }))}
                  columns={(previewResult.columns || []).map((col) => ({
                    title: col,
                    dataIndex: col,
                    key: col,
                    ellipsis: true,
                    width: 120,
                    render: (v: unknown) => (
                      <Text style={{ fontSize: 11 }}>
                        {v != null ? String(v) : <Text type="secondary">NULL</Text>}
                      </Text>
                    ),
                  }))}
                  rowKey="_key"
                  pagination={false}
                  size="small"
                  scroll={{ x: 'max-content', y: 200 }}
                  bordered
                />
              </>
            )}
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default DataComposePanel;
