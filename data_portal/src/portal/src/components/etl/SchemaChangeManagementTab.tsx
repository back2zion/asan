import React, { useState, useEffect, useCallback } from 'react';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Badge, Spin, Empty, Timeline, Drawer, Descriptions, Switch,
} from 'antd';
import {
  FileSearchOutlined, CameraOutlined, HistoryOutlined,
  ThunderboltOutlined, SettingOutlined, WarningOutlined,
  SyncOutlined, PauseCircleOutlined, StopOutlined, AuditOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { fetchPost, fetchPut } from '../../services/apiUtils';

const { Text } = Typography;

const SM_BASE = '/api/v1/schema-monitor';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetchPost(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function putJSON(url: string, body: any) {
  const res = await fetchPut(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const SchemaChangeManagementTab: React.FC = () => {
  const { message } = App.useApp();
  const [diffResult, setDiffResult] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [cdcStatus, setCdcStatus] = useState<any>(null);
  const [policies, setPolicies] = useState<any[]>([]);
  const [policySummary, setPolicySummary] = useState<any>(null);
  const [auditStats, setAuditStats] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [detailDrawer, setDetailDrawer] = useState<any>(null);

  const loadHistory = useCallback(async () => {
    try {
      const data = await fetchJSON(`${SM_BASE}/history`);
      setHistory(Array.isArray(data) ? data : []);
    } catch { setHistory([]); }
  }, []);

  const loadCDCStatus = useCallback(async () => {
    try {
      const data = await fetchJSON(`${SM_BASE}/cdc/status`);
      setCdcStatus(data);
    } catch { /* ignore */ }
  }, []);

  const loadPolicies = useCallback(async () => {
    try {
      const data = await fetchJSON(`${SM_BASE}/policies`);
      setPolicies(data.policies || []);
      setPolicySummary(data.summary || null);
    } catch { /* ignore */ }
  }, []);

  const loadAudit = useCallback(async () => {
    try {
      const data = await fetchJSON(`${SM_BASE}/policies/audit`);
      setAuditStats(data);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadHistory(); loadCDCStatus(); loadPolicies(); loadAudit(); }, [loadHistory, loadCDCStatus, loadPolicies, loadAudit]);

  const handleSnapshot = async () => {
    setLoading(true);
    try {
      const data = await postJSON(`${SM_BASE}/snapshot`, {});
      message.success(`v${data.version} 스냅샷 캡처 완료 (${data.changes?.total_changes || 0}건 변경)`);
      setDiffResult(data);
      loadHistory();
    } catch { message.error('스냅샷 캡처 실패'); }
    finally { setLoading(false); }
  };

  const handleDiff = async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${SM_BASE}/diff`);
      setDiffResult(data);
      if (data.total_changes > 0) message.warning(`${data.total_changes}건 변경 감지`);
      else message.success('변경 없음');
    } catch { message.error('변경 감지 실패'); }
    finally { setLoading(false); }
  };

  const handleViewDetail = async (logId: number) => {
    try {
      const data = await fetchJSON(`${SM_BASE}/history/${logId}`);
      setDetailDrawer(data);
    } catch { message.error('상세 조회 실패'); }
  };

  const handleSwitchMode = async (tableName: string, mode: string) => {
    try {
      await postJSON(`${SM_BASE}/cdc/switch-mode`, { table_name: tableName, mode });
      message.success(`${tableName} → ${mode} 전환`);
      loadCDCStatus();
    } catch { message.error('모드 전환 실패'); }
  };

  const handleResetWatermark = async (tableName: string) => {
    try {
      const data = await postJSON(`${SM_BASE}/cdc/reset-watermark`, { table_name: tableName });
      message.success(data.message);
      loadCDCStatus();
    } catch { message.error('워터마크 리셋 실패'); }
  };

  const handleUpdatePolicy = async (key: string, policyValue: any) => {
    try {
      await putJSON(`${SM_BASE}/policies/${key}`, { policy_value: policyValue });
      message.success(`정책 '${key}' 수정됨`);
      loadPolicies();
    } catch { message.error('정책 수정 실패'); }
  };

  const riskTag = (risk: string) => {
    switch (risk) {
      case 'high': return <Tag color="red">HIGH</Tag>;
      case 'medium': return <Tag color="orange">MEDIUM</Tag>;
      case 'low': return <Tag color="green">LOW</Tag>;
      default: return <Tag>{risk}</Tag>;
    }
  };

  const diffTypeTag = (type: string) => {
    switch (type) {
      case 'table_added': return <Tag color="green">테이블 추가</Tag>;
      case 'table_removed': return <Tag color="red">테이블 삭제</Tag>;
      case 'column_added': return <Tag color="green">컬럼 추가</Tag>;
      case 'column_removed': return <Tag color="red">컬럼 삭제</Tag>;
      case 'column_type_changed': return <Tag color="orange">타입 변경</Tag>;
      case 'column_nullable_changed': return <Tag color="blue">Nullable 변경</Tag>;
      default: return <Tag>{type}</Tag>;
    }
  };

  const modeIcon = (mode: string) => {
    switch (mode) {
      case 'streaming': return <SyncOutlined spin style={{ color: '#52c41a' }} />;
      case 'batch': return <PauseCircleOutlined style={{ color: '#faad14' }} />;
      case 'disabled': return <StopOutlined style={{ color: '#ff4d4f' }} />;
      default: return null;
    }
  };

  return (
    <div>
      {/* Section A: 스키마 변경 탐지 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} md={8}>
          <Card size="small" title={<><FileSearchOutlined /> 스키마 변경 탐지</>}>
            <Space>
              <Button icon={<CameraOutlined />} type="primary" onClick={handleSnapshot} loading={loading} size="small">스냅샷 캡처</Button>
              <Button icon={<FileSearchOutlined />} onClick={handleDiff} loading={loading} size="small">변경 감지(Dry-run)</Button>
            </Space>
          </Card>
        </Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="스냅샷 이력" value={history.length} prefix={<HistoryOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="변경 건수" value={diffResult?.total_changes || diffResult?.changes?.total_changes || 0}
          valueStyle={{ color: (diffResult?.total_changes || 0) > 0 ? '#faad14' : '#3f8600' }} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="고위험" value={diffResult?.summary?.high_risk || diffResult?.changes?.high_risk || 0}
          prefix={<WarningOutlined />} valueStyle={{ color: (diffResult?.summary?.high_risk || 0) > 0 ? '#cf1322' : '#3f8600' }} /></Card></Col>
        <Col xs={24} md={4}><Card size="small"><Statistic title="자동 반영 가능" value={diffResult?.summary?.auto_applicable || diffResult?.changes?.auto_applicable || 0}
          prefix={<ThunderboltOutlined />} /></Card></Col>
      </Row>

      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col xs={24} md={10}>
          <Card size="small" title={<><HistoryOutlined /> 변경 이력</>} style={{ maxHeight: 400, overflow: 'auto' }}>
            {history.length > 0 ? (
              <Timeline
                items={history.map(h => ({
                  color: h.summary?.total_changes > 0 ? 'red' : 'green',
                  children: (
                    <div>
                      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <Text strong>v{h.version} {h.label && <Text type="secondary">({h.label})</Text>}</Text>
                        <Text type="secondary" style={{ fontSize: 11 }}>{dayjs(h.created_at).format('MM-DD HH:mm')}</Text>
                      </div>
                      {h.summary && (
                        <div style={{ margin: '4px 0' }}>
                          <Text type="secondary" style={{ fontSize: 12 }}>{h.summary.total_changes}건 변경</Text>
                          {h.summary.total_changes > 0 && (
                            <span style={{ marginLeft: 6 }}>
                              {h.summary.high_risk > 0 && <Tag color="red" style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>HIGH {h.summary.high_risk}</Tag>}
                              {h.summary.medium_risk > 0 && <Tag color="orange" style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>MEDIUM {h.summary.medium_risk}</Tag>}
                              {h.summary.low_risk > 0 && <Tag color="green" style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>LOW {h.summary.low_risk}</Tag>}
                            </span>
                          )}
                        </div>
                      )}
                      <Button size="small" onClick={() => handleViewDetail(h.id)}>상세</Button>
                    </div>
                  ),
                }))}
              />
            ) : <Empty description="이력 없음" />}
          </Card>
        </Col>
        <Col xs={24} md={14}>
          {(Array.isArray(diffResult?.changes) && diffResult.changes.length > 0) || (Array.isArray(diffResult?.detected_changes) && diffResult.detected_changes.length > 0) ? (
            <Card size="small" title="변경 상세">
              <Table
                dataSource={(Array.isArray(diffResult.changes) ? diffResult.changes : diffResult.detected_changes || []).map((d: any, i: number) => ({ ...d, key: i }))}
                columns={[
                  { title: '유형', dataIndex: 'type', key: 'type', width: 130, render: diffTypeTag },
                  { title: '테이블', dataIndex: 'table', key: 'table', width: 150, render: (v: string) => <Text code>{v}</Text> },
                  { title: '컬럼', dataIndex: 'column', key: 'col', width: 130, render: (v: string) => v ? <Text code>{v}</Text> : '-' },
                  { title: '위험도', dataIndex: 'risk_level', key: 'risk', width: 90, render: riskTag },
                  { title: '자동', dataIndex: 'auto_applicable', key: 'auto', width: 60, render: (v: boolean) => v ? <Tag color="green">Y</Tag> : <Tag>N</Tag> },
                  { title: '상세', dataIndex: 'detail', key: 'detail' },
                ]}
                size="small" pagination={false}
              />
            </Card>
          ) : (
            <Card size="small" style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Empty description="'스냅샷 캡처' 또는 '변경 감지'를 실행하세요" />
            </Card>
          )}
        </Col>
      </Row>

      {/* Section B: CDC 스트리밍 관리 */}
      <Card size="small" title={<><ThunderboltOutlined /> CDC 스트리밍 관리</>} style={{ marginBottom: 16 }}>
        {cdcStatus ? (
          <>
            <Row gutter={16} style={{ marginBottom: 12 }}>
              <Col span={4}><Statistic title="총 테이블" value={cdcStatus.summary?.total_tables || 0} /></Col>
              <Col span={4}><Statistic title="Streaming" value={cdcStatus.summary?.streaming || 0} valueStyle={{ color: '#52c41a' }} /></Col>
              <Col span={4}><Statistic title="Batch" value={cdcStatus.summary?.batch || 0} valueStyle={{ color: '#faad14' }} /></Col>
              <Col span={4}><Statistic title="Disabled" value={cdcStatus.summary?.disabled || 0} valueStyle={{ color: '#ff4d4f' }} /></Col>
              <Col span={8}><Statistic title="Pending Rows" value={cdcStatus.summary?.total_pending_rows?.toLocaleString() || 0} /></Col>
            </Row>
            <Table
              dataSource={(cdcStatus.tables || []).map((t: any) => ({ ...t, key: t.table_name }))}
              columns={[
                { title: '테이블', dataIndex: 'table_name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                { title: '모드', dataIndex: 'mode', key: 'mode', width: 120, render: (v: string) => <Space>{modeIcon(v)}<Tag color={v === 'streaming' ? 'green' : v === 'batch' ? 'orange' : 'default'}>{v}</Tag></Space> },
                { title: '행수', dataIndex: 'row_count', key: 'rows', width: 100, render: (v: number) => v?.toLocaleString() },
                { title: 'Pending', dataIndex: 'pending_rows', key: 'pending', width: 100, render: (v: number) => v > 0 ? <Text type="warning">{v.toLocaleString()}</Text> : '0' },
                { title: 'Lag(분)', dataIndex: 'lag_minutes', key: 'lag', width: 80, render: (v: number | null) => v !== null ? (v > 60 ? <Text type="danger">{v}</Text> : v) : '-' },
                { title: '동기화', dataIndex: 'last_sync', key: 'sync', width: 130, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
                { title: '작업', key: 'action', width: 280, render: (_: any, r: any) => (
                  <Space size="small">
                    <Button size="small" onClick={() => handleSwitchMode(r.table_name, 'streaming')} disabled={r.mode === 'streaming'}>Streaming</Button>
                    <Button size="small" onClick={() => handleSwitchMode(r.table_name, 'batch')} disabled={r.mode === 'batch'}>Batch</Button>
                    <Button size="small" onClick={() => handleSwitchMode(r.table_name, 'disabled')} disabled={r.mode === 'disabled'} danger>Off</Button>
                    <Button size="small" onClick={() => handleResetWatermark(r.table_name)}>Reset</Button>
                  </Space>
                )},
              ]}
              size="small" pagination={false}
            />
          </>
        ) : <Spin />}
      </Card>

      {/* Section C: 자동 반영 정책 */}
      <Row gutter={16}>
        <Col xs={24} md={16}>
          <Card size="small" title={<><SettingOutlined /> 자동 반영 정책</>}>
            {policies.length > 0 ? (
              <Table
                dataSource={policies.map(p => ({ ...p, key: p.key }))}
                columns={[
                  { title: '정책', dataIndex: 'key', key: 'key', width: 180, render: (v: string) => <Text code>{v}</Text> },
                  { title: '설명', dataIndex: 'description', key: 'desc' },
                  { title: '동작', key: 'action', width: 100, render: (_: any, r: any) => {
                    const action = r.value?.action;
                    return action ? <Tag color={action === 'auto' ? 'green' : 'orange'}>{action}</Tag> : <Tag>config</Tag>;
                  }},
                  { title: '자동/수동', key: 'toggle', width: 100, render: (_: any, r: any) => {
                    const action = r.value?.action;
                    if (!action) return '-';
                    return (
                      <Switch size="small" checked={action === 'auto'}
                        onChange={(checked) => handleUpdatePolicy(r.key, { ...r.value, action: checked ? 'auto' : 'manual' })} />
                    );
                  }},
                  { title: '수정일', dataIndex: 'updated_at', key: 'updated', width: 120, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
                ]}
                size="small" pagination={false}
              />
            ) : <Empty description="정책 없음" />}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title={<><AuditOutlined /> 감사 통계</>}>
            {auditStats ? (
              <Descriptions size="small" column={1}>
                <Descriptions.Item label="총 변경">{auditStats.total_changes}</Descriptions.Item>
                <Descriptions.Item label="자동 반영">{auditStats.auto_applied}</Descriptions.Item>
                <Descriptions.Item label="수동 필요">{auditStats.manual_required}</Descriptions.Item>
                <Descriptions.Item label="고위험">{auditStats.high_risk}</Descriptions.Item>
                <Descriptions.Item label="스캔 이력">{auditStats.audit_period?.entries_scanned || 0}건</Descriptions.Item>
              </Descriptions>
            ) : <Empty description="감사 데이터 없음" />}
            {policySummary && (
              <Row gutter={8} style={{ marginTop: 12 }}>
                <Col span={8}><Statistic title="자동" value={policySummary.auto} valueStyle={{ color: '#52c41a', fontSize: 18 }} /></Col>
                <Col span={8}><Statistic title="수동" value={policySummary.manual} valueStyle={{ color: '#faad14', fontSize: 18 }} /></Col>
                <Col span={8}><Statistic title="설정" value={policySummary.config} valueStyle={{ fontSize: 18 }} /></Col>
              </Row>
            )}
          </Card>
        </Col>
      </Row>

      {/* Detail Drawer */}
      <Drawer title={`스냅샷 상세: v${detailDrawer?.version}`} open={!!detailDrawer} onClose={() => setDetailDrawer(null)} width={700}>
        {detailDrawer && (
          <>
            <Descriptions size="small" column={2} style={{ marginBottom: 16 }}>
              <Descriptions.Item label="버전">v{detailDrawer.version}</Descriptions.Item>
              <Descriptions.Item label="라벨">{detailDrawer.label}</Descriptions.Item>
              <Descriptions.Item label="테이블">{detailDrawer.total_tables}</Descriptions.Item>
              <Descriptions.Item label="컬럼">{detailDrawer.total_columns}</Descriptions.Item>
              <Descriptions.Item label="생성일">{dayjs(detailDrawer.created_at).format('YYYY-MM-DD HH:mm')}</Descriptions.Item>
            </Descriptions>
            {detailDrawer.changes?.length > 0 && (
              <Card size="small" title={`변경 사항 (${detailDrawer.changes.length}건)`} style={{ marginBottom: 16 }}>
                <Table
                  dataSource={detailDrawer.changes.map((c: any, i: number) => ({ ...c, key: i }))}
                  columns={[
                    { title: '유형', dataIndex: 'type', key: 'type', width: 120, render: diffTypeTag },
                    { title: '테이블', dataIndex: 'table', key: 'table', width: 140, render: (v: string) => <Text code>{v}</Text> },
                    { title: '컬럼', dataIndex: 'column', key: 'col', width: 120 },
                    { title: '위험도', dataIndex: 'risk_level', key: 'risk', width: 80, render: riskTag },
                    { title: '상세', dataIndex: 'detail', key: 'detail' },
                  ]}
                  size="small" pagination={false}
                />
              </Card>
            )}
            {detailDrawer.tables?.length > 0 && (
              <Card size="small" title={`테이블 목록 (${detailDrawer.tables.length}개)`}>
                <Table
                  dataSource={detailDrawer.tables.map((t: any) => ({ ...t, key: t.table_name }))}
                  columns={[
                    { title: '테이블', dataIndex: 'table_name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                    { title: '컬럼수', dataIndex: 'column_count', key: 'cols', width: 80 },
                    { title: '행수', dataIndex: 'row_count', key: 'rows', width: 100, render: (v: number) => v?.toLocaleString() },
                  ]}
                  size="small" pagination={false}
                  expandable={{
                    expandedRowRender: (record: any) => (
                      <Table
                        dataSource={(record.columns || []).map((c: any) => ({ ...c, key: c.name }))}
                        columns={[
                          { title: '컬럼', dataIndex: 'name', key: 'name', render: (v: string) => <Text code>{v}</Text> },
                          { title: '타입', dataIndex: 'data_type', key: 'type' },
                          { title: 'Nullable', dataIndex: 'is_nullable', key: 'null', width: 80 },
                        ]}
                        size="small" pagination={false}
                      />
                    ),
                  }}
                />
              </Card>
            )}
          </>
        )}
      </Drawer>
    </div>
  );
};

export default SchemaChangeManagementTab;
