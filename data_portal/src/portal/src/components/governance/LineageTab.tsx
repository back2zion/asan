/**
 * 데이터 리니지 탭 컴포넌트 - OMOP CDM DB 기반
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Tag, Space, Typography, Row, Col,
  Progress, Descriptions, List, Button, Tooltip, Spin,
} from 'antd';
import {
  DatabaseOutlined, TableOutlined, BranchesOutlined,
  CloseOutlined, CloudServerOutlined, FilterOutlined, InfoCircleOutlined,
  LoadingOutlined, BarChartOutlined,
} from '@ant-design/icons';
import LineageGraph, { type LineageNodeDetail } from '../lineage/LineageGraph';
import { governanceApi } from '../../services/api';
import { OMOP_TABLES } from './types';

const { Text } = Typography;

// 노드 타입별 아이콘 (사이드 패널용)
const NODE_TYPE_ICON: Record<string, React.ReactNode> = {
  source: <CloudServerOutlined />,
  bronze: <DatabaseOutlined />,
  silver: <FilterOutlined />,
  gold: <TableOutlined />,
  process: <BranchesOutlined />,
};
const NODE_TYPE_COLOR: Record<string, string> = {
  source: '#1890ff', bronze: '#CD7F32', silver: '#8c8c8c', gold: '#d4a017', process: '#52c41a',
};

interface NodeDetail {
  schedule: string;
  lastRun: string;
  rowCount: number;
  format: string;
  sla: string;
  owner: string;
  qualityScore: number;
  retention?: string;
  tableCount?: number;
}

interface UsageLineageData {
  table_name: string;
  query_count: number;
  related_tables: Record<string, number>;
  total_queries: number;
  analysis_period: { start: string; end: string } | null;
}

const LineageTab: React.FC = () => {
  const [selectedTable, setSelectedTable] = useState<string>('person');
  const [selectedNode, setSelectedNode] = useState<LineageNodeDetail | null>(null);
  const [lineageMeta, setLineageMeta] = useState<any>(null);
  const [detail, setDetail] = useState<NodeDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // 활용 기반 리니지 (AAR-001)
  const [usageData, setUsageData] = useState<UsageLineageData | null>(null);
  const [usageLoading, setUsageLoading] = useState(false);

  const handleNodeClick = useCallback((node: LineageNodeDetail | null, meta: any) => {
    setSelectedNode(node);
    setLineageMeta(meta);
  }, []);

  // 테이블 바뀌면 패널 닫기 + 활용 리니지 로드
  useEffect(() => {
    setSelectedNode(null);
    setDetail(null);
    // 활용 기반 리니지 로드
    let cancelled = false;
    const loadUsage = async () => {
      setUsageLoading(true);
      try {
        const resp = await governanceApi.getUsageLineage(selectedTable);
        if (!cancelled) setUsageData(resp);
      } catch {
        if (!cancelled) setUsageData(null);
      } finally {
        if (!cancelled) setUsageLoading(false);
      }
    };
    loadUsage();
    return () => { cancelled = true; };
  }, [selectedTable]);

  // 노드 선택 시 API 호출
  useEffect(() => {
    if (!selectedNode) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    const fetchDetail = async () => {
      setDetailLoading(true);
      try {
        const result = await governanceApi.getLineageDetail(selectedNode.id);
        if (!cancelled) setDetail(result);
      } catch {
        if (!cancelled) setDetail(null);
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    };
    fetchDetail();
    return () => { cancelled = true; };
  }, [selectedNode]);

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* 테이블 선택 */}
      <div>
        <Space wrap>
          <Text type="secondary">테이블 선택:</Text>
          {OMOP_TABLES.slice(0, 8).map((t) => (
            <Button
              key={t}
              size="small"
              type={selectedTable === t ? 'primary' : 'default'}
              onClick={() => setSelectedTable(t)}
            >
              {t}
            </Button>
          ))}
        </Space>
      </div>

      <Row gutter={12}>
        {/* ReactFlow 그래프 */}
        <Col xs={24} lg={selectedNode ? 16 : 24} style={{ transition: 'all 0.3s' }}>
          <Card size="small" title={<><BranchesOutlined /> {selectedTable} 데이터 리니지</>}
            extra={<Text type="secondary" style={{ fontSize: 12 }}>노드를 클릭하면 세부정보를 확인할 수 있습니다</Text>}
          >
            <div style={{ width: '100%', height: 500 }}>
              <LineageGraph
                tableName={selectedTable}
                physicalName={selectedTable}
                onNodeClick={handleNodeClick}
              />
            </div>
          </Card>
        </Col>

        {/* 세부정보 사이드 패널 */}
        {selectedNode && (
          <Col xs={24} lg={8}>
            <Card
              size="small"
              title={
                <Space>
                  <span style={{ color: NODE_TYPE_COLOR[selectedNode.nodeType] || '#666', fontSize: 18 }}>
                    {NODE_TYPE_ICON[selectedNode.nodeType] || <InfoCircleOutlined />}
                  </span>
                  <Text strong>{selectedNode.label}</Text>
                </Space>
              }
              extra={<Button type="text" size="small" icon={<CloseOutlined />} onClick={() => setSelectedNode(null)} />}
              style={{ height: '100%' }}
            >
              {detailLoading ? (
                <div style={{ textAlign: 'center', padding: 40 }}>
                  <Spin indicator={<LoadingOutlined style={{ fontSize: 24 }} spin />} />
                  <div style={{ marginTop: 8 }}><Text type="secondary">로딩 중...</Text></div>
                </div>
              ) : detail ? (
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  {/* 기본 정보 */}
                  <Descriptions column={1} size="small" bordered>
                    <Descriptions.Item label="레이어">
                      <Tag color={NODE_TYPE_COLOR[selectedNode.nodeType]}>{selectedNode.layer}</Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="설명">{selectedNode.description}</Descriptions.Item>
                    <Descriptions.Item label="데이터 포맷">{detail.format}</Descriptions.Item>
                    <Descriptions.Item label="스케줄">{detail.schedule}</Descriptions.Item>
                    <Descriptions.Item label="마지막 실행">
                      <Text code style={{ fontSize: 11 }}>{detail.lastRun}</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="레코드 수">{detail.rowCount?.toLocaleString()}</Descriptions.Item>
                    <Descriptions.Item label="SLA">
                      <Tag color="blue">{detail.sla}</Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="담당">{detail.owner}</Descriptions.Item>
                    {detail.retention && (
                      <Descriptions.Item label="보관 기간">{detail.retention}</Descriptions.Item>
                    )}
                    <Descriptions.Item label="품질 점수">
                      <Progress
                        percent={detail.qualityScore}
                        size="small"
                        status={detail.qualityScore >= 99 ? 'success' : 'normal'}
                        format={(p) => `${p}%`}
                      />
                    </Descriptions.Item>
                  </Descriptions>

                  {/* 원천 테이블 (source 노드) */}
                  {selectedNode.metadata?.tables && (
                    <Card size="small" title="원천 테이블" type="inner">
                      {selectedNode.metadata.tables.map((t: string, i: number) => (
                        <Tag key={i} style={{ marginBottom: 4 }}>{t}</Tag>
                      ))}
                    </Card>
                  )}

                  {/* 업/다운스트림 (API lineageMeta에서) */}
                  {lineageMeta && (
                    <>
                      {lineageMeta.upstream?.length > 0 && (
                        <Card size="small" title="Upstream (원천)" type="inner">
                          {lineageMeta.upstream.map((u: string, i: number) => (
                            <Tag key={i} color="orange" style={{ marginBottom: 4 }}>{u}</Tag>
                          ))}
                        </Card>
                      )}
                      {lineageMeta.downstream?.length > 0 && (
                        <Card size="small" title="Downstream (소비)" type="inner">
                          {lineageMeta.downstream.map((d: string, i: number) => (
                            <Tag key={i} color="green" style={{ marginBottom: 4 }}>{d}</Tag>
                          ))}
                        </Card>
                      )}
                      {lineageMeta.related_tables?.length > 0 && (
                        <Card size="small" title="연관 테이블" type="inner">
                          <List
                            size="small"
                            dataSource={lineageMeta.related_tables}
                            renderItem={(rt: any) => (
                              <List.Item style={{ padding: '4px 0' }}>
                                <Space>
                                  <Text code style={{ fontSize: 11 }}>{rt.name}</Text>
                                  <Text type="secondary" style={{ fontSize: 11 }}>{rt.business_name}</Text>
                                  <Tag style={{ fontSize: 10 }}>
                                    {rt.relationship === 'fk_patient' ? 'FK(person_id)' : '동일 도메인'}
                                  </Tag>
                                </Space>
                              </List.Item>
                            )}
                          />
                        </Card>
                      )}
                    </>
                  )}
                </Space>
              ) : (
                <Text type="secondary">상세 정보를 불러올 수 없습니다.</Text>
              )}
            </Card>
          </Col>
        )}
      </Row>

      {/* 활용 기반 관계 (AAR-001: Usage Lineage) */}
      <Card
        size="small"
        title={
          <Space>
            <BarChartOutlined style={{ color: '#13c2c2' }} />
            <Text strong>활용 기반 관계 (쿼리 로그 분석)</Text>
          </Space>
        }
        loading={usageLoading}
      >
        {usageData && usageData.query_count > 0 ? (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <Row gutter={16}>
              <Col span={8}>
                <Text type="secondary">테이블 조회 횟수</Text>
                <div><Text strong style={{ fontSize: 20, color: '#13c2c2' }}>{usageData.query_count}회</Text></div>
              </Col>
              <Col span={8}>
                <Text type="secondary">전체 쿼리 수</Text>
                <div><Text strong style={{ fontSize: 20 }}>{usageData.total_queries}건</Text></div>
              </Col>
              <Col span={8}>
                <Text type="secondary">분석 기간</Text>
                <div>
                  <Text style={{ fontSize: 12 }}>
                    {usageData.analysis_period
                      ? `${usageData.analysis_period.start?.slice(0, 10)} ~ ${usageData.analysis_period.end?.slice(0, 10)}`
                      : '-'}
                  </Text>
                </div>
              </Col>
            </Row>
            {Object.keys(usageData.related_tables).length > 0 && (
              <div>
                <Text type="secondary" style={{ fontSize: 12 }}>동시 조회된 테이블:</Text>
                <div style={{ marginTop: 4 }}>
                  {Object.entries(usageData.related_tables).map(([table, count]) => (
                    <Tag
                      key={table}
                      color="cyan"
                      style={{ marginBottom: 4, cursor: 'pointer' }}
                      onClick={() => setSelectedTable(table)}
                    >
                      {table} ({count}회)
                    </Tag>
                  ))}
                </div>
              </div>
            )}
          </Space>
        ) : (
          <Text type="secondary">
            {usageData?.query_count === 0
              ? `${selectedTable} 테이블에 대한 쿼리 이력이 없습니다.`
              : '쿼리 로그 데이터를 불러오는 중입니다...'}
          </Text>
        )}
      </Card>
    </Space>
  );
};

export default LineageTab;
