/**
 * 테이블 상세 모달 (DPR-001/DPR-002: 지능형 카탈로그, 재현성 확보)
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Table, Tag, Space, Typography, Row, Col,
  Button, Descriptions, Modal, Tabs, Badge, Divider, Avatar,
  Spin, Empty, Input, List, App, Tooltip, Card, Segmented, Select, Timeline, Progress,
} from 'antd';
import {
  TableOutlined, DatabaseOutlined, CopyOutlined,
  UserOutlined, TeamOutlined, CodeOutlined,
  BranchesOutlined, FileTextOutlined, ThunderboltOutlined, ApiOutlined,
  EyeOutlined, CommentOutlined, SendOutlined, CameraOutlined, DeleteOutlined,
  ExperimentOutlined, LinkOutlined, HistoryOutlined,
  CloseOutlined, CloudServerOutlined, FilterOutlined, LoadingOutlined,
} from '@ant-design/icons';
import type { TableInfo, ColumnInfo } from '../../services/api';
import { catalogExtApi } from '../../services/catalogExtApi';
import type { ColumnsType } from 'antd/es/table';
import LineageGraph, { type LineageNodeDetail } from '../lineage/LineageGraph';
import RelatedTables from './RelatedTables';
import { governanceApi } from '../../services/governanceApi';
import QualityInfo from './QualityInfo';
import ReactMarkdown from 'react-markdown';
import { sensitivityColors, sensitivityLabels, generateSqlCode, generatePythonCode, generateRCode, generateApiEndpoint } from './constants';

const { Title, Text } = Typography;

interface TableDetailModalProps {
  table: TableInfo | null;
  visible: boolean;
  activeTab: string;
  onTabChange: (key: string) => void;
  onClose: () => void;
  onCopyTableName: (name: string) => void;
  onCopyText: (text: string, label: string) => void;
}

const columnColumns: ColumnsType<ColumnInfo> = [
  {
    title: '컬럼명',
    key: 'name',
    width: 200,
    render: (_, record) => (
      <Space direction="vertical" size={0}>
        <Text strong>{record.business_name}</Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          {record.physical_name}
        </Text>
      </Space>
    ),
  },
  {
    title: '타입',
    dataIndex: 'data_type',
    key: 'data_type',
    width: 100,
    render: (type: string) => <Tag color="geekblue">{type}</Tag>,
  },
  {
    title: '설명',
    dataIndex: 'description',
    key: 'description',
    ellipsis: true,
  },
  {
    title: 'PK',
    dataIndex: 'is_pk',
    key: 'is_pk',
    width: 60,
    align: 'center',
    render: (isPk: boolean) => (isPk ? <Tag color="gold">PK</Tag> : '-'),
  },
  {
    title: '민감도',
    dataIndex: 'sensitivity',
    key: 'sensitivity',
    width: 80,
    render: (sensitivity: string) => (
      <Tag color={sensitivityColors[sensitivity] || 'default'}>
        {sensitivityLabels[sensitivity] || sensitivity}
      </Tag>
    ),
  },
];

const preStyle: React.CSSProperties = {
  background: '#f5f5f5',
  padding: 12,
  borderRadius: 4,
  fontSize: 12,
  overflow: 'auto',
};

const CODE_ENVS = [
  { label: 'SQL', value: 'sql' },
  { label: 'Python', value: 'python' },
  { label: 'R', value: 'r' },
  { label: 'REST API', value: 'api' },
  { label: 'JupyterLab', value: 'jupyter' },
];

const ImportCodeSection: React.FC<{
  table: TableInfo;
  onCopyText: (text: string, label: string) => void;
  jupyterCode: string;
}> = ({ table, onCopyText, jupyterCode }) => {
  const [env, setEnv] = React.useState('sql');
  const codeMap: Record<string, { code: string; label: string; icon: React.ReactNode }> = {
    sql: { code: generateSqlCode(table), label: 'SQL', icon: <DatabaseOutlined /> },
    python: { code: generatePythonCode(table), label: 'Python 코드', icon: <ThunderboltOutlined /> },
    r: { code: generateRCode(table), label: 'R 코드', icon: <ThunderboltOutlined /> },
    api: { code: generateApiEndpoint(table), label: 'API 정보', icon: <ApiOutlined /> },
    jupyter: { code: jupyterCode, label: 'Jupyter 코드', icon: <ExperimentOutlined /> },
  };
  const current = codeMap[env];

  return (
    <div style={{ padding: '12px 0' }}>
      <Segmented options={CODE_ENVS} value={env} onChange={(v) => setEnv(v as string)} style={{ marginBottom: 16 }} />
      <Card
        size="small"
        title={<Space>{current.icon}<Text strong>{current.label}</Text></Space>}
        extra={
          <Space>
            <Button size="small" icon={<CopyOutlined />} onClick={() => onCopyText(current.code, current.label)}>복사</Button>
            {env === 'jupyter' && (
              <Button size="small" type="primary" icon={<LinkOutlined />} onClick={() => window.open('http://localhost:18888/lab', '_blank')} style={{ background: '#FF6F00' }}>
                JupyterLab 열기
              </Button>
            )}
          </Space>
        }
      >
        <pre style={{ ...preStyle, margin: 0, maxHeight: 280 }}>{current.code}</pre>
      </Card>
    </div>
  );
};

const TableDetailModal: React.FC<TableDetailModalProps> = ({
  table,
  visible,
  activeTab,
  onTabChange,
  onClose,
  onCopyTableName,
  onCopyText,
}) => {
  const { message } = App.useApp();
  const [sampleData, setSampleData] = useState<{ columns: string[]; rows: Record<string, any>[] } | null>(null);
  const [sampleLoading, setSampleLoading] = useState(false);
  const [communityComments, setCommunityComments] = useState<{ id: string; author: string; content: string; created_at: string | null }[]>([]);
  const [commentsLoading, setCommentsLoading] = useState(false);
  const [newComment, setNewComment] = useState('');
  const [commentType, setCommentType] = useState<'comment' | 'question'>('comment');
  const [resolvedIds, setResolvedIds] = useState<Set<string>>(() => {
    try { return new Set(JSON.parse(localStorage.getItem('resolved_questions') || '[]')); } catch { return new Set(); }
  });
  const [snapshotName, setSnapshotName] = useState('');
  const [snapshotScope, setSnapshotScope] = useState<'private' | 'group' | 'public'>('private');
  const [snapshots, setSnapshots] = useState<any[]>([]);
  const [snapshotsLoading, setSnapshotsLoading] = useState(false);
  const [versions, setVersions] = useState<any[]>([]);
  const [versionsLoading, setVersionsLoading] = useState(false);
  const [versionCurrent, setVersionCurrent] = useState<{ row_count?: number; column_count?: number }>({});

  // 리니지 노드 클릭 상세
  const [lineageNode, setLineageNode] = useState<LineageNodeDetail | null>(null);
  const [lineageMeta, setLineageMeta] = useState<any>(null);
  const [lineageDetail, setLineageDetail] = useState<any>(null);
  const [lineageDetailLoading, setLineageDetailLoading] = useState(false);

  const handleLineageNodeClick = useCallback((node: LineageNodeDetail | null, meta: any) => {
    setLineageNode(node);
    setLineageMeta(meta);
  }, []);

  useEffect(() => {
    if (!lineageNode) { setLineageDetail(null); return; }
    let cancelled = false;
    (async () => {
      setLineageDetailLoading(true);
      try {
        const result = await governanceApi.getLineageDetail(lineageNode.id);
        if (!cancelled) setLineageDetail(result);
      } catch {
        if (!cancelled) setLineageDetail(null);
      } finally {
        if (!cancelled) setLineageDetailLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [lineageNode]);

  // 실제 Sample Data 로딩 (백엔드 API)
  useEffect(() => {
    if (activeTab === 'sample' && table && !sampleData) {
      setSampleLoading(true);
      catalogExtApi.getSampleData(table.physical_name, 10)
        .then((data) => {
          setSampleData({
            columns: data.columns || [],
            rows: data.rows || [],
          });
        })
        .catch(() => {
          // 백엔드 연결 안 될 때 fallback
          const cols = (table.columns || []).slice(0, 6).map((c) => c.physical_name);
          const fallbackRows = Array.from({ length: 5 }, (_, i) => {
            const row: Record<string, any> = {};
            cols.forEach((col) => {
              if (col.includes('_id')) row[col] = 1000 + i;
              else if (col.includes('date')) row[col] = `2024-0${(i % 9) + 1}-${10 + i}`;
              else row[col] = `sample_${i + 1}`;
            });
            return row;
          });
          setSampleData({ columns: cols, rows: fallbackRows });
        })
        .finally(() => setSampleLoading(false));
    }
  }, [activeTab, table]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { setSampleData(null); }, [table?.physical_name]);

  // 커뮤니티 댓글 로딩 (백엔드 API)
  const loadComments = useCallback(async () => {
    if (!table) return;
    setCommentsLoading(true);
    try {
      const data = await catalogExtApi.getComments(table.physical_name);
      setCommunityComments(data.comments || []);
    } catch {
      setCommunityComments([]);
    }
    setCommentsLoading(false);
  }, [table?.physical_name]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (table) loadComments();
  }, [table?.physical_name]); // eslint-disable-line react-hooks/exhaustive-deps

  // 스냅샷 로딩
  const loadSnapshots = useCallback(async () => {
    if (!table) return;
    setSnapshotsLoading(true);
    try {
      const data = await catalogExtApi.getSnapshots({ table_name: table.physical_name });
      setSnapshots(data.snapshots || []);
    } catch {
      setSnapshots([]);
    }
    setSnapshotsLoading(false);
  }, [table?.physical_name]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (activeTab === 'snapshot' && table) loadSnapshots();
  }, [activeTab, table?.physical_name]); // eslint-disable-line react-hooks/exhaustive-deps

  // 버전 이력 로딩
  useEffect(() => {
    if (activeTab === 'versions' && table) {
      setVersionsLoading(true);
      catalogExtApi.getTableVersions(table.physical_name)
        .then((data) => {
          setVersions(data.versions || []);
          setVersionCurrent(data.current || {});
        })
        .catch(() => setVersions([]))
        .finally(() => setVersionsLoading(false));
    }
  }, [activeTab, table?.physical_name]);

  const handleAddComment = async () => {
    if (!newComment.trim() || !table) return;
    const content = commentType === 'question' ? `[Q] ${newComment.trim()}` : newComment.trim();
    try {
      await catalogExtApi.createComment(table.physical_name, { content });
      setNewComment('');
      setCommentType('comment');
      loadComments();
    } catch {
      message.error('댓글 등록에 실패했습니다');
    }
  };

  const toggleResolved = (commentId: string) => {
    setResolvedIds((prev) => {
      const next = new Set(prev);
      if (next.has(commentId)) next.delete(commentId); else next.add(commentId);
      localStorage.setItem('resolved_questions', JSON.stringify([...next]));
      return next;
    });
  };

  const handleDeleteComment = async (commentId: string) => {
    if (!table) return;
    try {
      await catalogExtApi.deleteComment(table.physical_name, Number(commentId));
      loadComments();
    } catch {
      message.error('댓글 삭제에 실패했습니다');
    }
  };

  const handleCreateSnapshot = async () => {
    if (!snapshotName.trim() || !table) return;
    try {
      const cols = (table.columns || []).map(c => c.physical_name);
      await catalogExtApi.createSnapshot({
        name: snapshotName.trim(),
        table_name: table.physical_name,
        query_logic: generateSqlCode(table),
        columns: cols,
        share_scope: snapshotScope,
      });
      setSnapshotName('');
      setSnapshotScope('private');
      loadSnapshots();
      message.success('스냅샷이 저장되었습니다');
    } catch {
      message.error('스냅샷 저장에 실패했습니다');
    }
  };

  const formatTime = (isoStr: string | null) => {
    if (!isoStr) return '';
    const d = new Date(isoStr);
    const now = new Date();
    const diff = now.getTime() - d.getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return '방금';
    if (mins < 60) return `${mins}분 전`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}시간 전`;
    const days = Math.floor(hrs / 24);
    if (days < 30) return `${days}일 전`;
    return d.toLocaleDateString('ko-KR');
  };

  if (!table) return null;

  const markdownDescription = `## ${table.business_name}

**물리명**: \`${table.physical_name}\`
**도메인**: ${table.domain}

### 설명
${table.description || '설명 없음'}

### 데이터 구축 방법
- **소스**: OMOP CDM v5.4 표준 변환
- **적재 주기**: 일 1회 배치 (새벽 03:00)
- **변환 규칙**: Synthea → OMOP ETL 파이프라인

### 윤리 규정
- 개인식별정보(PHI) 컬럼은 비식별화 처리 후 제공
- IRB 승인 필요 데이터는 별도 접근 권한 신청
- 데이터 반출 시 정보보안팀 승인 필수
`;

  const jupyterCode = `# JupyterLab에서 실행 (http://localhost:18888/lab)
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://omopuser:omop@localhost:5436/omop_cdm")
df = pd.read_sql("SELECT * FROM ${table.physical_name} LIMIT 1000", engine)
df.head()`;

  return (
    <Modal
      title={
        <Space>
          <TableOutlined style={{ color: '#005BAC' }} />
          {table.business_name}
          <Tag color="blue">{table.domain}</Tag>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width={1000}
      footer={null}
    >
      <Descriptions bordered column={3} size="small" style={{ marginBottom: 16 }}>
        <Descriptions.Item label="물리명">
          <Space>
            <Text code>{table.physical_name}</Text>
            <Button
              type="text"
              size="small"
              icon={<CopyOutlined />}
              onClick={() => onCopyTableName(table.physical_name)}
            />
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="오너">
          <Space>
            <Avatar size="small" icon={<UserOutlined />} />
            <Text>데이터관리팀</Text>
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="활용 횟수">
          <Badge count={table.usage_count} showZero color="#005BAC" />
        </Descriptions.Item>
        <Descriptions.Item label="설명" span={3}>
          {table.description}
        </Descriptions.Item>
        <Descriptions.Item label="태그" span={2}>
          <Space wrap>
            {table.tags?.map((tag) => (
              <Tag key={tag}>{tag}</Tag>
            ))}
          </Space>
        </Descriptions.Item>
        <Descriptions.Item label="공유 범위">
          <Tag icon={<TeamOutlined />} color="green">전체 공개</Tag>
        </Descriptions.Item>
      </Descriptions>

      <Tabs
        activeKey={activeTab}
        onChange={onTabChange}
        items={[
          {
            key: 'columns',
            label: <><TableOutlined /> 컬럼 정보</>,
            children: (
              <Table
                dataSource={table.columns || []}
                columns={columnColumns}
                rowKey="physical_name"
                pagination={false}
                size="small"
                scroll={{ y: 280 }}
              />
            ),
          },
          {
            key: 'lineage',
            label: <><BranchesOutlined /> 데이터 계보</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                <Row gutter={12}>
                  <Col span={lineageNode ? 16 : 24} style={{ transition: 'all 0.3s' }}>
                    <Title level={5} style={{ marginBottom: 8 }}>Data Lineage
                      <Text type="secondary" style={{ fontSize: 12, fontWeight: 400, marginLeft: 8 }}>노드를 클릭하면 세부정보 확인</Text>
                    </Title>
                    <LineageGraph
                      key={`lineage-${table.physical_name}`}
                      tableName={table.business_name}
                      physicalName={table.physical_name}
                      height={350}
                      onNodeClick={handleLineageNodeClick}
                    />
                  </Col>
                  {lineageNode && (
                    <Col span={8}>
                      <Card
                        size="small"
                        title={<Space><Text strong>{lineageNode.label}</Text><Tag>{lineageNode.layer}</Tag></Space>}
                        extra={<Button type="text" size="small" icon={<CloseOutlined />} onClick={() => setLineageNode(null)} />}
                      >
                        {lineageDetailLoading ? (
                          <div style={{ textAlign: 'center', padding: 30 }}>
                            <Spin indicator={<LoadingOutlined style={{ fontSize: 20 }} spin />} />
                          </div>
                        ) : lineageDetail ? (
                          <Space direction="vertical" size="small" style={{ width: '100%' }}>
                            <Descriptions column={1} size="small" bordered>
                              <Descriptions.Item label="데이터 포맷">{lineageDetail.format}</Descriptions.Item>
                              <Descriptions.Item label="실행 스케줄">{lineageDetail.schedule}</Descriptions.Item>
                              <Descriptions.Item label="레코드 수">{lineageDetail.rowCount?.toLocaleString()}</Descriptions.Item>
                              <Descriptions.Item label="SLA"><Tag color="blue">{lineageDetail.sla}</Tag></Descriptions.Item>
                              <Descriptions.Item label="담당자">{lineageDetail.owner}</Descriptions.Item>
                              {lineageDetail.retention && (
                                <Descriptions.Item label="보관 기간">{lineageDetail.retention}</Descriptions.Item>
                              )}
                              <Descriptions.Item label="품질 점수">
                                <Progress percent={lineageDetail.qualityScore} size="small"
                                  status={lineageDetail.qualityScore >= 99 ? 'success' : 'normal'} />
                              </Descriptions.Item>
                            </Descriptions>
                            {lineageMeta?.upstream?.length > 0 && (
                              <div>
                                <Text type="secondary" style={{ fontSize: 11 }}>Upstream (원천)</Text>
                                <div>{lineageMeta.upstream.map((u: string, i: number) => <Tag key={i} color="orange" style={{ marginBottom: 2 }}>{u}</Tag>)}</div>
                              </div>
                            )}
                            {lineageMeta?.downstream?.length > 0 && (
                              <div>
                                <Text type="secondary" style={{ fontSize: 11 }}>Downstream (소비)</Text>
                                <div>{lineageMeta.downstream.map((d: string, i: number) => <Tag key={i} color="green" style={{ marginBottom: 2 }}>{d}</Tag>)}</div>
                              </div>
                            )}
                          </Space>
                        ) : (
                          <Text type="secondary">상세 정보를 불러올 수 없습니다.</Text>
                        )}
                      </Card>
                    </Col>
                  )}
                </Row>
                <Divider style={{ margin: '12px 0' }} />
                <Title level={5}>연관 테이블</Title>
                <RelatedTables tableName={table.physical_name} />
              </div>
            ),
          },
          {
            key: 'description',
            label: <><FileTextOutlined /> 상세 설명</>,
            children: (
              <div style={{ padding: '8px 0', maxHeight: 400, overflow: 'auto' }}>
                <ReactMarkdown
                  components={{
                    h2: ({ children }) => <Title level={4} style={{ marginTop: 0 }}>{children}</Title>,
                    h3: ({ children }) => <Title level={5}>{children}</Title>,
                    code: ({ children }) => <Text code>{children}</Text>,
                    p: ({ children }) => <p style={{ lineHeight: 1.8 }}>{children}</p>,
                  }}
                >
                  {markdownDescription}
                </ReactMarkdown>
              </div>
            ),
          },
          {
            key: 'sample',
            label: <><EyeOutlined /> 샘플 데이터</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 12 }}>
                  OMOP CDM 데이터베이스에서 조회한 실제 샘플 데이터입니다. (다운로드 불필요)
                </Text>
                <Spin spinning={sampleLoading}>
                  {sampleData && sampleData.rows.length > 0 ? (
                    <Table
                      dataSource={sampleData.rows.map((row, i) => ({ ...row, _key: i }))}
                      columns={sampleData.columns.map((col) => ({
                        title: col,
                        dataIndex: col,
                        key: col,
                        ellipsis: true,
                        width: 130,
                        render: (v: any) => <Text style={{ fontSize: 12 }}>{v != null ? String(v) : <Text type="secondary">NULL</Text>}</Text>,
                      }))}
                      rowKey="_key"
                      pagination={false}
                      size="small"
                      scroll={{ x: 'max-content' }}
                      bordered
                    />
                  ) : !sampleLoading ? (
                    <Empty description="샘플 데이터가 없습니다" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                  ) : null}
                </Spin>
              </div>
            ),
          },
          {
            key: 'code',
            label: <><CodeOutlined /> Import 코드</>,
            children: <ImportCodeSection table={table} onCopyText={onCopyText} jupyterCode={jupyterCode} />,
          },
          {
            key: 'quality',
            label: <><FileTextOutlined /> 품질 정보</>,
            children: <QualityInfo tableName={table.physical_name} />,
          },
          {
            key: 'snapshot',
            label: <><CameraOutlined /> 스냅샷/레시피</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 12 }}>
                  데이터셋 조건(쿼리 로직, 필터)을 스냅샷으로 저장하여 재현 가능하게 관리합니다. 각 스냅샷에는 고유 ID가 부여됩니다.
                </Text>
                <Space style={{ width: '100%', marginBottom: 16 }} direction="vertical" size={8}>
                  <Space.Compact style={{ width: '100%' }}>
                    <Input
                      placeholder="스냅샷 이름 (예: 당뇨 환자 코호트 2024)"
                      value={snapshotName}
                      onChange={(e) => setSnapshotName(e.target.value)}
                      onPressEnter={handleCreateSnapshot}
                      style={{ flex: 1 }}
                    />
                    <Select
                      value={snapshotScope}
                      onChange={(v) => setSnapshotScope(v)}
                      style={{ width: 120 }}
                      options={[
                        { value: 'private', label: '🔒 개인' },
                        { value: 'group', label: '👥 그룹' },
                        { value: 'public', label: '🌐 전체' },
                      ]}
                    />
                    <Button type="primary" icon={<CameraOutlined />} onClick={handleCreateSnapshot} style={{ background: '#006241' }}>
                      저장
                    </Button>
                  </Space.Compact>
                </Space>
                <Spin spinning={snapshotsLoading}>
                  <List
                    dataSource={snapshots}
                    renderItem={(item) => (
                      <List.Item
                        style={{ padding: '10px 0' }}
                        actions={[
                          <Tooltip title="쿼리 복사" key="copy">
                            <Button size="small" icon={<CopyOutlined />} onClick={() => onCopyText(item.query_logic || '', '스냅샷 쿼리')} />
                          </Tooltip>,
                        ]}
                      >
                        <List.Item.Meta
                          avatar={<CameraOutlined style={{ fontSize: 18, color: '#006241', marginTop: 4 }} />}
                          title={
                            <Space>
                              <Text strong>{item.name}</Text>
                              <Tag color="blue" style={{ fontSize: 10 }}>ID: {item.snapshot_id?.slice(0, 8)}...</Tag>
                              {item.share_scope === 'public' ? (
                                <Tag color="green" style={{ fontSize: 10 }}>🌐 전체 공유</Tag>
                              ) : item.share_scope === 'group' ? (
                                <Tag color="orange" style={{ fontSize: 10 }}>👥 그룹</Tag>
                              ) : (
                                <Tag style={{ fontSize: 10 }}>🔒 개인</Tag>
                              )}
                            </Space>
                          }
                          description={
                            <div>
                              <Text type="secondary" style={{ fontSize: 12 }}>
                                {item.creator} · {formatTime(item.created_at)}
                                {item.columns?.length > 0 && ` · ${item.columns.length}개 컬럼`}
                              </Text>
                              {item.query_logic && (
                                <pre style={{ ...preStyle, marginTop: 6, maxHeight: 60, fontSize: 11 }}>{item.query_logic}</pre>
                              )}
                            </div>
                          }
                        />
                      </List.Item>
                    )}
                    locale={{ emptyText: '저장된 스냅샷이 없습니다. 위에서 새 스냅샷을 저장하세요.' }}
                  />
                </Spin>
              </div>
            ),
          },
          {
            key: 'community',
            label: <><CommentOutlined /> Community</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                <div style={{ marginBottom: 16 }}>
                  <Space style={{ marginBottom: 8 }}>
                    <Segmented
                      size="small"
                      value={commentType}
                      onChange={(v) => setCommentType(v as 'comment' | 'question')}
                      options={[
                        { label: '💬 댓글', value: 'comment' },
                        { label: '❓ 질문', value: 'question' },
                      ]}
                    />
                  </Space>
                  <Space.Compact style={{ width: '100%' }}>
                    <Input
                      placeholder={commentType === 'question' ? '질문을 남기세요 (답변을 기다립니다)...' : '의견이나 팁을 공유하세요...'}
                      value={newComment}
                      onChange={(e) => setNewComment(e.target.value)}
                      onPressEnter={handleAddComment}
                    />
                    <Button type="primary" icon={<SendOutlined />} onClick={handleAddComment} style={{ background: '#006241' }}>
                      등록
                    </Button>
                  </Space.Compact>
                </div>
                <Spin spinning={commentsLoading}>
                  <List
                    dataSource={communityComments}
                    renderItem={(item) => {
                      const isQuestion = item.content.startsWith('[Q] ');
                      const displayContent = isQuestion ? item.content.slice(4) : item.content;
                      const isResolved = resolvedIds.has(item.id);
                      return (
                        <List.Item
                          style={{
                            padding: '8px 0',
                            borderLeft: isQuestion ? `3px solid ${isResolved ? '#52c41a' : '#faad14'}` : 'none',
                            paddingLeft: isQuestion ? 12 : 0,
                          }}
                          actions={[
                            ...(isQuestion ? [
                              <Button
                                key="resolve"
                                type="text"
                                size="small"
                                onClick={() => toggleResolved(item.id)}
                                style={{ color: isResolved ? '#52c41a' : '#faad14', fontSize: 11 }}
                              >
                                {isResolved ? '✓ 해결됨' : '미해결'}
                              </Button>,
                            ] : []),
                            <Button key="del" type="text" size="small" danger icon={<DeleteOutlined />} onClick={() => handleDeleteComment(item.id)} />,
                          ]}
                        >
                          <List.Item.Meta
                            avatar={<Avatar size="small" icon={<UserOutlined />} style={isQuestion ? { background: '#faad14' } : undefined} />}
                            title={
                              <Space>
                                {isQuestion && <Tag color={isResolved ? 'success' : 'warning'} style={{ fontSize: 10 }}>{isResolved ? '해결됨' : '질문'}</Tag>}
                                <Text strong style={{ fontSize: 13 }}>{item.author}</Text>
                                <Text type="secondary" style={{ fontSize: 11 }}>{formatTime(item.created_at)}</Text>
                              </Space>
                            }
                            description={<Text style={{ fontSize: 13 }}>{displayContent}</Text>}
                          />
                        </List.Item>
                      );
                    }}
                    locale={{ emptyText: '아직 댓글이 없습니다. 첫 번째 질문이나 의견을 남겨보세요!' }}
                  />
                </Spin>
              </div>
            ),
          },
          {
            key: 'versions',
            label: <><HistoryOutlined /> 버전 이력</>,
            children: (
              <div style={{ padding: '8px 0' }}>
                {versionCurrent.row_count != null && (
                  <Card size="small" style={{ marginBottom: 16, background: '#f6ffed', border: '1px solid #b7eb8f' }}>
                    <Space size={24}>
                      <div>
                        <Text type="secondary" style={{ fontSize: 11 }}>현재 행 수</Text>
                        <div style={{ fontWeight: 600 }}>{(versionCurrent.row_count || 0).toLocaleString()}</div>
                      </div>
                      <div>
                        <Text type="secondary" style={{ fontSize: 11 }}>현재 컬럼 수</Text>
                        <div style={{ fontWeight: 600 }}>{versionCurrent.column_count || 0}</div>
                      </div>
                      <div>
                        <Text type="secondary" style={{ fontSize: 11 }}>전체 버전</Text>
                        <div style={{ fontWeight: 600 }}>{versions.length}개</div>
                      </div>
                    </Space>
                  </Card>
                )}
                <Spin spinning={versionsLoading}>
                  {versions.length > 0 ? (
                    <Timeline
                      items={versions.map((v: any) => {
                        const typeConfig: Record<string, { color: string; label: string }> = {
                          schema_change: { color: '#1890ff', label: '스키마 변경' },
                          data_update: { color: '#52c41a', label: '데이터 갱신' },
                          quality_check: { color: '#faad14', label: '품질 검사' },
                        };
                        const cfg = typeConfig[v.type] || { color: '#d9d9d9', label: v.type };
                        return {
                          color: cfg.color,
                          children: (
                            <div style={{ marginBottom: 4 }}>
                              <Space style={{ marginBottom: 4 }}>
                                <Tag color={cfg.color} style={{ fontSize: 10 }}>{v.version}</Tag>
                                <Tag style={{ fontSize: 10 }}>{cfg.label}</Tag>
                                <Text type="secondary" style={{ fontSize: 11 }}>{v.date}</Text>
                              </Space>
                              <div>
                                <Text strong style={{ fontSize: 13 }}>{v.summary}</Text>
                              </div>
                              <Text type="secondary" style={{ fontSize: 11 }}>by {v.author}</Text>
                              {v.changes && v.changes.length > 0 && (
                                <ul style={{ margin: '4px 0 0 16px', padding: 0, fontSize: 11, color: '#666' }}>
                                  {v.changes.map((c: string, i: number) => (
                                    <li key={i}>{c}</li>
                                  ))}
                                </ul>
                              )}
                            </div>
                          ),
                        };
                      })}
                    />
                  ) : (
                    <Empty description="버전 이력이 없습니다." />
                  )}
                </Spin>
              </div>
            ),
          },
        ]}
      />
    </Modal>
  );
};

export default TableDetailModal;
