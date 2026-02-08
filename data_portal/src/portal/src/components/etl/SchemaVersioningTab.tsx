import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Alert, Spin, Empty, Select, Descriptions, Timeline, message,
} from 'antd';
import {
  DatabaseOutlined, SearchOutlined, HistoryOutlined,
  BranchesOutlined, FileSearchOutlined, WarningOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const SchemaVersioningTab: React.FC = () => {
  const [sources, setSources] = useState<any[]>([]);
  const [selectedSource, setSelectedSource] = useState<string>('omop-cdm');
  const [snapshots, setSnapshots] = useState<any[]>([]);
  const [selectedSnapshot, setSelectedSnapshot] = useState<any>(null);
  const [diffResult, setDiffResult] = useState<any>(null);
  const [impactResult, setImpactResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [discoverResult, setDiscoverResult] = useState<any>(null);

  const loadSources = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/sources`);
      setSources(data.sources || []);
    } catch { /* ignore */ }
  }, []);

  const loadSnapshots = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/schema/snapshots?source_id=${selectedSource}`);
      setSnapshots(data.snapshots || []);
    } catch { setSnapshots([]); }
  }, [selectedSource]);

  useEffect(() => { loadSources(); }, [loadSources]);
  useEffect(() => { loadSnapshots(); }, [loadSnapshots]);

  const handleDiscover = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/schema/discover`, { source_id: selectedSource });
      setDiscoverResult(data);
      message.success(`${data.total_tables} 테이블 탐색 완료`);
    } catch { message.error('스키마 탐색 실패'); }
    finally { setLoading(false); }
  };

  const handleSnapshot = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/schema/snapshot`, { source_id: selectedSource });
      message.success(data.message);
      loadSnapshots();
    } catch { message.error('스냅샷 저장 실패'); }
    finally { setLoading(false); }
  };

  const handleDetectChanges = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/schema/detect-changes/${selectedSource}`, {});
      setDiffResult(data);
      if (data.has_changes) {
        message.warning(`${data.total_changes}건 변경 감지됨`);
        const impact = await postJSON(`${API_BASE}/schema/impact-analysis`, {
          source_id: selectedSource, diffs: data.diffs,
        });
        setImpactResult(impact);
      } else {
        message.success('변경 없음');
        setImpactResult(null);
      }
    } catch (e: any) {
      message.error(e.message?.includes('404') ? '이전 스냅샷이 없습니다. 먼저 스냅샷을 저장하세요.' : '변경 감지 실패');
    }
    finally { setLoading(false); }
  };

  const handleViewSnapshot = async (snapId: string) => {
    try {
      const data = await fetchJSON(`${API_BASE}/schema/snapshots/${snapId}`);
      setSelectedSnapshot(data);
    } catch { message.error('스냅샷 조회 실패'); }
  };

  const handleCompare = async (snapA: string, snapB: string) => {
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/schema/compare`, { snapshot_id_a: snapA, snapshot_id_b: snapB });
      setDiffResult({ ...data, has_changes: data.total_changes > 0 });
      if (data.total_changes > 0) {
        const impact = await postJSON(`${API_BASE}/schema/impact-analysis`, {
          source_id: selectedSource, diffs: data.diffs,
        });
        setImpactResult(impact);
      }
    } catch { message.error('비교 실패'); }
    finally { setLoading(false); }
  };

  const handleRollback = async (snapId: string) => {
    try {
      const data = await postJSON(`${API_BASE}/schema/rollback/${snapId}`, {});
      message.success(data.message);
      loadSnapshots();
    } catch { message.error('롤백 실패'); }
  };

  const diffTypeTag = (type: string) => {
    switch (type) {
      case 'table_added': return <Tag color="green">테이블 추가</Tag>;
      case 'table_removed': return <Tag color="red">테이블 삭제</Tag>;
      case 'column_added': return <Tag color="green">컬럼 추가</Tag>;
      case 'column_removed': return <Tag color="red">컬럼 삭제</Tag>;
      case 'column_type_changed': return <Tag color="orange">타입 변경</Tag>;
      default: return <Tag>{type}</Tag>;
    }
  };

  const riskBadge = (risk: string) => {
    switch (risk) {
      case 'high': return <Tag color="red">HIGH</Tag>;
      case 'medium': return <Tag color="orange">MEDIUM</Tag>;
      case 'low': return <Tag color="green">LOW</Tag>;
      default: return <Tag>{risk}</Tag>;
    }
  };

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} md={8}>
          <Card size="small" title="소스 선택">
            <Select value={selectedSource} onChange={setSelectedSource} style={{ width: '100%' }}
              options={sources.map(s => ({ value: s.id, label: s.name }))} />
            <Space style={{ marginTop: 12 }} wrap>
              <Button icon={<SearchOutlined />} onClick={handleDiscover} loading={loading} size="small">스키마 탐색</Button>
              <Button icon={<HistoryOutlined />} onClick={handleSnapshot} loading={loading} size="small" type="primary">스냅샷 저장</Button>
              <Button icon={<BranchesOutlined />} onClick={handleDetectChanges} loading={loading} size="small">변경 감지</Button>
            </Space>
          </Card>
        </Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="총 스냅샷" value={snapshots.length} prefix={<HistoryOutlined />} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="추적 소스" value={new Set(snapshots.map(s => s.source_id)).size} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="변경 건수" value={diffResult?.total_changes || 0} prefix={<BranchesOutlined />}
          valueStyle={{ color: (diffResult?.total_changes || 0) > 0 ? '#faad14' : '#3f8600' }} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="고위험" value={impactResult?.summary?.risk_counts?.high || 0} prefix={<WarningOutlined />}
          valueStyle={{ color: (impactResult?.summary?.risk_counts?.high || 0) > 0 ? '#cf1322' : '#3f8600' }} /></Card></Col>
      </Row>

      <Row gutter={16}>
        <Col xs={24} md={10}>
          <Card size="small" title={<><HistoryOutlined /> 스냅샷 타임라인</>} style={{ maxHeight: 600, overflow: 'auto' }}>
            {snapshots.length > 0 ? (
              <Timeline
                items={[...snapshots].reverse().map(snap => ({
                  color: snap.is_baseline ? 'green' : 'blue',
                  children: (
                    <div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Text strong>{snap.label} {snap.is_baseline && <Tag color="green" style={{ marginLeft: 4 }}>기준선</Tag>}</Text>
                        <Text type="secondary" style={{ fontSize: 11 }}>{dayjs(snap.created_at).format('MM-DD HH:mm')}</Text>
                      </div>
                      <div><Text type="secondary" style={{ fontSize: 12 }}>{snap.total_tables} 테이블, {snap.total_columns} 컬럼</Text></div>
                      <Space size="small" style={{ marginTop: 4 }}>
                        <Button size="small" onClick={() => handleViewSnapshot(snap.id)}>상세</Button>
                        {snapshots.length >= 2 && (
                          <Button size="small" onClick={() => {
                            const prev = snapshots.filter(s => s.version < snap.version).pop();
                            if (prev) handleCompare(prev.id, snap.id);
                            else message.info('이전 스냅샷이 없습니다');
                          }}>이전과 비교</Button>
                        )}
                        <Button size="small" onClick={() => handleRollback(snap.id)}>기준선 설정</Button>
                      </Space>
                    </div>
                  ),
                }))}
              />
            ) : <Empty description="저장된 스냅샷이 없습니다" />}
          </Card>

          {discoverResult && (
            <Card size="small" title={<><FileSearchOutlined /> 탐색 결과</>} style={{ marginTop: 16 }}>
              <Descriptions size="small" column={2}>
                <Descriptions.Item label="테이블">{discoverResult.total_tables}</Descriptions.Item>
                <Descriptions.Item label="컬럼">{discoverResult.total_columns}</Descriptions.Item>
                <Descriptions.Item label="총 행수">{discoverResult.total_rows?.toLocaleString()}</Descriptions.Item>
                <Descriptions.Item label="탐색 시간">{dayjs(discoverResult.discovered_at).format('HH:mm:ss')}</Descriptions.Item>
              </Descriptions>
              <Table
                dataSource={(discoverResult.tables || []).map((t: any) => ({ ...t, key: t.table_name }))}
                columns={[
                  { title: '테이블', dataIndex: 'table_name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                  { title: '컬럼', dataIndex: 'column_count', key: 'cols', width: 60 },
                  { title: '행수', dataIndex: 'row_count', key: 'rows', width: 100, render: (v: number) => v.toLocaleString() },
                  { title: '크기', dataIndex: 'size_mb', key: 'size', width: 80, render: (v: number) => `${v} MB` },
                ]}
                size="small" pagination={false} style={{ marginTop: 12 }}
              />
            </Card>
          )}
        </Col>

        <Col xs={24} md={14}>
          {diffResult ? (
            <div>
              <Card size="small" title={<><BranchesOutlined /> 변경 사항 ({diffResult.total_changes}건)</>} style={{ marginBottom: 16 }}>
                {diffResult.diffs?.length > 0 ? (
                  <Table
                    dataSource={diffResult.diffs.map((d: any, i: number) => ({ ...d, key: i }))}
                    columns={[
                      { title: '유형', dataIndex: 'type', key: 'type', width: 120, render: diffTypeTag },
                      { title: '테이블', dataIndex: 'table', key: 'table', width: 160, render: (v: string) => <Text code>{v}</Text> },
                      { title: '컬럼', dataIndex: 'column', key: 'col', width: 140, render: (v: string) => v ? <Text code>{v}</Text> : '-' },
                      { title: '상세', dataIndex: 'detail', key: 'detail' },
                    ]}
                    size="small" pagination={false}
                  />
                ) : <Alert type="success" message="변경 사항 없음" showIcon />}
              </Card>

              {impactResult && impactResult.impacts?.length > 0 && (
                <Card size="small" title={<><WarningOutlined /> 영향도 분석</>}>
                  <Row gutter={16} style={{ marginBottom: 12 }}>
                    <Col span={8}><Statistic title="HIGH" value={impactResult.summary.risk_counts.high} valueStyle={{ color: '#cf1322' }} /></Col>
                    <Col span={8}><Statistic title="MEDIUM" value={impactResult.summary.risk_counts.medium} valueStyle={{ color: '#d48806' }} /></Col>
                    <Col span={8}><Statistic title="LOW" value={impactResult.summary.risk_counts.low} valueStyle={{ color: '#3f8600' }} /></Col>
                  </Row>
                  {impactResult.summary.affected_mapping_tables.length > 0 && (
                    <div style={{ marginBottom: 12 }}>
                      <Text strong>영향 받는 매핑 테이블: </Text>
                      {impactResult.summary.affected_mapping_tables.map((t: string) => <Tag key={t} color="blue">{t}</Tag>)}
                    </div>
                  )}
                  <Table
                    dataSource={impactResult.impacts.map((imp: any, i: number) => ({ ...imp, key: i }))}
                    columns={[
                      { title: '위험도', dataIndex: 'risk', key: 'risk', width: 90, render: riskBadge },
                      { title: '변경', key: 'diff', width: 200, render: (_: any, r: any) => (
                        <span>{diffTypeTag(r.diff.type)} <Text code>{r.diff.table}{r.diff.column ? '.' + r.diff.column : ''}</Text></span>
                      )},
                      { title: '영향 매핑', key: 'maps', width: 140, render: (_: any, r: any) => r.affected_mappings.map((m: string) => <Tag key={m}>{m}</Tag>) },
                      { title: '권장 조치', key: 'rec', render: (_: any, r: any) => (
                        <ul style={{ margin: 0, paddingLeft: 16, fontSize: 12 }}>{r.recommendations.map((rec: string, i: number) => <li key={i}>{rec}</li>)}</ul>
                      )},
                    ]}
                    size="small" pagination={false}
                  />
                </Card>
              )}
            </div>
          ) : (
            <Card size="small" style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Empty description="'변경 감지' 버튼을 클릭하거나 스냅샷을 비교하세요" />
            </Card>
          )}

          {selectedSnapshot && (
            <Card size="small" title={`스냅샷 상세: ${selectedSnapshot.label}`} style={{ marginTop: 16 }}
              extra={<Button size="small" onClick={() => setSelectedSnapshot(null)}>닫기</Button>}>
              <Table
                dataSource={(selectedSnapshot.tables || []).map((t: any) => ({ ...t, key: t.table_name }))}
                columns={[
                  { title: '테이블', dataIndex: 'table_name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                  { title: '컬럼수', dataIndex: 'column_count', key: 'cols', width: 70 },
                  { title: '행수', dataIndex: 'row_count', key: 'rows', width: 100, render: (v: number) => v.toLocaleString() },
                  { title: '크기(MB)', dataIndex: 'size_mb', key: 'size', width: 80 },
                ]}
                size="small" pagination={false}
                expandable={{
                  expandedRowRender: (record: any) => (
                    <Table
                      dataSource={record.columns.map((c: any) => ({ ...c, key: c.name }))}
                      columns={[
                        { title: '컬럼', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                        { title: '타입', dataIndex: 'type', key: 'type' },
                        { title: 'Nullable', dataIndex: 'nullable', key: 'null', width: 80, render: (v: boolean) => v ? 'YES' : 'NO' },
                        { title: 'PK', dataIndex: 'is_pk', key: 'pk', width: 60, render: (v: boolean) => v ? <Tag color="gold">PK</Tag> : '-' },
                      ]}
                      size="small" pagination={false}
                    />
                  ),
                }}
              />
            </Card>
          )}
        </Col>
      </Row>
    </div>
  );
};

export default SchemaVersioningTab;
