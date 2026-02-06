/**
 * 데이터 리니지 탭 컴포넌트
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Tag, Space, Typography, Row, Col,
  Progress, Descriptions, List, Button, Tooltip,
} from 'antd';
import {
  DatabaseOutlined, TableOutlined, BranchesOutlined,
  CloseOutlined, CloudServerOutlined, FilterOutlined, InfoCircleOutlined,
} from '@ant-design/icons';
import LineageGraph, { type LineageNodeDetail } from '../lineage/LineageGraph';
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

// 노드별 mock 상세 정보
const NODE_DETAIL_MOCK: Record<string, {
  schedule?: string; lastRun?: string; rowCount?: number; format?: string;
  sla?: string; owner?: string; qualityScore?: number; retention?: string;
}> = {
  source:  { schedule: '실시간 (CDC)', lastRun: '2026-02-06 09:15:32', rowCount: 2847523, format: 'Oracle → Debezium', sla: '< 5초', owner: '의료정보실', qualityScore: 99.2, retention: '영구' },
  cdc:     { schedule: '실시간', lastRun: '2026-02-06 09:15:33', rowCount: 2847523, format: 'Kafka Avro', sla: '< 10초', owner: '빅데이터연구센터', qualityScore: 99.8, retention: '7일 (토픽)' },
  bronze:  { schedule: '실시간 적재', lastRun: '2026-02-06 09:15:34', rowCount: 2847523, format: 'Delta Lake (Parquet)', sla: '< 30초', owner: '융합연구지원센터', qualityScore: 98.5, retention: '1년' },
  etl:     { schedule: '매일 02:00 KST', lastRun: '2026-02-06 02:00:00', rowCount: 2831045, format: 'Spark SQL', sla: '< 2시간', owner: '의공학연구소', qualityScore: 97.3, retention: '-' },
  silver:  { schedule: '매일 04:00 KST', lastRun: '2026-02-06 04:12:18', rowCount: 2831045, format: 'Delta Lake (Parquet)', sla: '< 4시간', owner: '융합연구지원센터', qualityScore: 99.1, retention: '3년' },
  gold:    { schedule: '매일 05:00 KST', lastRun: '2026-02-06 05:08:44', rowCount: 2831045, format: 'Delta Lake (Parquet)', sla: '< 6시간', owner: '임상의학연구소', qualityScore: 99.7, retention: '5년' },
};

const LineageTab: React.FC = () => {
  const [selectedTable, setSelectedTable] = useState<string>('person');
  const [selectedNode, setSelectedNode] = useState<LineageNodeDetail | null>(null);
  const [lineageMeta, setLineageMeta] = useState<any>(null);

  const handleNodeClick = useCallback((node: LineageNodeDetail | null, meta: any) => {
    setSelectedNode(node);
    setLineageMeta(meta);
  }, []);

  // 테이블 바뀌면 패널 닫기
  useEffect(() => { setSelectedNode(null); }, [selectedTable]);

  const detail = selectedNode ? NODE_DETAIL_MOCK[selectedNode.id] || NODE_DETAIL_MOCK.source : null;

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
            <div style={{ height: 500 }}>
              <LineageGraph
                tableName={selectedTable}
                physicalName={selectedTable}
                onNodeClick={handleNodeClick}
              />
            </div>
          </Card>
        </Col>

        {/* 세부정보 사이드 패널 */}
        {selectedNode && detail && (
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
                  <Descriptions.Item label="보관 기간">{detail.retention}</Descriptions.Item>
                  <Descriptions.Item label="품질 점수">
                    <Progress
                      percent={detail.qualityScore}
                      size="small"
                      status={detail.qualityScore! >= 99 ? 'success' : 'normal'}
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
                                  {rt.relationship === 'fk_patient' ? 'FK(PT_NO)' : '동일 도메인'}
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
            </Card>
          </Col>
        )}
      </Row>
    </Space>
  );
};

export default LineageTab;
