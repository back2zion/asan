import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Select, Empty, Popconfirm, Descriptions, message,
} from 'antd';
import {
  BookOutlined, PlusOutlined, DeleteOutlined, ThunderboltOutlined,
  CheckCircleOutlined, WarningOutlined, RocketOutlined,
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
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const IngestionTemplatesTab: React.FC = () => {
  const [templates, setTemplates] = useState<any[]>([]);
  const [sources, setSources] = useState<any[]>([]);
  const [targetTables, setTargetTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [genSource, setGenSource] = useState<string>('');
  const [genTarget, setGenTarget] = useState<string>('');
  const [validationResult, setValidationResult] = useState<any>(null);

  const loadTemplates = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/templates`);
      setTemplates(data.templates || []);
    } catch { /* ignore */ }
  }, []);

  const loadSources = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/sources`);
      setSources(data.sources || []);
    } catch { /* ignore */ }
  }, []);

  const loadTargetTables = useCallback(async () => {
    try {
      const data = await fetchJSON(`${API_BASE}/mapping/target-tables`);
      setTargetTables(data.tables || []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadTemplates(); loadSources(); loadTargetTables(); }, [loadTemplates, loadSources, loadTargetTables]);

  const handleGenerate = async () => {
    if (!genSource || !genTarget) { message.warning('소스와 타겟을 선택하세요'); return; }
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/templates/generate`, { source_id: genSource, target_table: genTarget });
      message.success(data.message);
      loadTemplates();
    } catch { message.error('템플릿 자동 생성 실패'); }
    finally { setLoading(false); }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteJSON(`${API_BASE}/templates/${id}`);
      message.success('템플릿 삭제됨');
      loadTemplates();
    } catch { message.error('삭제 실패'); }
  };

  const handleValidate = async () => {
    if (!genSource) { message.warning('소스를 선택하세요'); return; }
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/terminology/validate`, { source_id: genSource });
      setValidationResult(data);
      message.success(`${data.matched?.length || 0}개 매칭, ${data.unmatched?.length || 0}개 미매칭`);
    } catch { message.error('검증 실패'); }
    finally { setLoading(false); }
  };

  const builtinCount = templates.filter(t => t.is_builtin).length;
  const autoCount = templates.filter(t => t.auto_generated).length;

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} md={10}>
          <Card size="small" title="수집 템플릿 자동 생성">
            <Space direction="vertical" style={{ width: '100%' }} size="small">
              <Select value={genSource || undefined} onChange={setGenSource} placeholder="소스 선택" style={{ width: '100%' }}
                options={sources.map(s => ({ value: s.id, label: s.name }))} />
              <Select value={genTarget || undefined} onChange={setGenTarget} placeholder="타겟 테이블 선택" style={{ width: '100%' }}
                options={targetTables.map(t => ({ value: t, label: t }))} />
              <Space>
                <Button icon={<RocketOutlined />} type="primary" onClick={handleGenerate} loading={loading} size="small">자동 생성</Button>
                <Button icon={<CheckCircleOutlined />} onClick={handleValidate} loading={loading} size="small">용어 검증</Button>
              </Space>
            </Space>
          </Card>
        </Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="총 템플릿" value={templates.length} prefix={<BookOutlined />} /></Card></Col>
        <Col xs={8} md={4}><Card size="small"><Statistic title="Built-in" value={builtinCount} /></Card></Col>
        <Col xs={8} md={6}><Card size="small"><Statistic title="자동 생성" value={autoCount} prefix={<ThunderboltOutlined />} /></Card></Col>
      </Row>

      <Card size="small" title={<><BookOutlined /> 수집 템플릿 목록</>}>
        {templates.length > 0 ? (
          <Table
            dataSource={templates.map(t => ({ ...t, key: t.id }))}
            columns={[
              { title: '이름', dataIndex: 'name', key: 'name', render: (v: string, r: any) => (
                <span><Text strong>{v}</Text>{r.auto_generated && <Tag color="purple" style={{ marginLeft: 4 }}>자동</Tag>}{r.is_builtin && <Tag color="cyan" style={{ marginLeft: 4 }}>내장</Tag>}</span>
              )},
              { title: '소스 유형', key: 'source', width: 130, render: (_: any, r: any) => <Tag>{r.source_type}/{r.source_subtype}</Tag> },
              { title: '타겟 테이블', dataIndex: 'target_table', key: 'target', width: 160, render: (v: string) => <Text code>{v}</Text> },
              { title: '매핑수', dataIndex: 'mappings', key: 'mappings', width: 80, render: (v: any[]) => v?.length || 0 },
              { title: '커버리지', dataIndex: 'mapping_coverage', key: 'cov', width: 100, render: (v: number) => v ? (
                <Tag color={v >= 80 ? 'green' : v >= 50 ? 'orange' : 'red'}>{v}%</Tag>
              ) : '-' },
              { title: '태그', dataIndex: 'tags', key: 'tags', render: (v: string[]) => v?.map(t => <Tag key={t}>{t}</Tag>) || '-' },
              { title: '생성일', dataIndex: 'created_at', key: 'created', width: 120, render: (v: string) => v ? dayjs(v).format('MM-DD HH:mm') : '-' },
              { title: '', key: 'action', width: 70, render: (_: any, r: any) => !r.is_builtin ? (
                <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(r.id)}>
                  <Button size="small" icon={<DeleteOutlined />} danger />
                </Popconfirm>
              ) : null },
            ]}
            size="small" pagination={false}
          />
        ) : <Empty description="템플릿이 없습니다" />}
      </Card>

      {validationResult && (
        <Card size="small" title={<><CheckCircleOutlined /> 표준 용어 검증 결과</>} style={{ marginTop: 16 }}>
          <Row gutter={16} style={{ marginBottom: 12 }}>
            <Col span={8}><Statistic title="매칭" value={validationResult.matched?.length || 0} valueStyle={{ color: '#3f8600' }} /></Col>
            <Col span={8}><Statistic title="미매칭" value={validationResult.unmatched?.length || 0} valueStyle={{ color: '#cf1322' }} /></Col>
            <Col span={8}><Statistic title="매칭률" value={validationResult.match_rate ? `${(validationResult.match_rate * 100).toFixed(1)}%` : '-'} /></Col>
          </Row>
          {validationResult.matched?.length > 0 && (
            <Table
              dataSource={validationResult.matched.map((m: any, i: number) => ({ ...m, key: i }))}
              columns={[
                { title: '컬럼', dataIndex: 'column', key: 'col', render: (v: string) => <Text code>{v}</Text> },
                { title: '표준', dataIndex: 'standard', key: 'std', width: 100, render: (v: string) => <Tag color="blue">{v}</Tag> },
                { title: '코드', dataIndex: 'code', key: 'code', width: 140 },
                { title: '도메인', dataIndex: 'domain', key: 'domain', width: 120 },
                { title: '설명', dataIndex: 'description', key: 'desc' },
              ]}
              size="small" pagination={false}
            />
          )}
          {validationResult.unmatched?.length > 0 && (
            <div style={{ marginTop: 12 }}>
              <Text type="secondary">미매칭 컬럼: </Text>
              {validationResult.unmatched.map((u: any, i: number) => (
                <Tag key={i} color="red" icon={<WarningOutlined />}>{typeof u === 'string' ? u : u.column}</Tag>
              ))}
            </div>
          )}
        </Card>
      )}
    </div>
  );
};

export default IngestionTemplatesTab;
