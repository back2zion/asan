import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Typography, Descriptions, InputNumber, Switch,
  Spin, Alert, message,
} from 'antd';
import { ThunderboltOutlined } from '@ant-design/icons';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function putJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const ParallelLoadingTab: React.FC = () => {
  const [config, setConfig] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/parallel-config`);
      setConfig(data);
    } catch { message.error('병렬 설정 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadConfig(); }, [loadConfig]);

  const updateTable = async (tableName: string, field: string, value: any) => {
    if (!config) return;
    const tbl = config.tables[tableName];
    const updated = { ...tbl, [field]: value };
    try {
      await putJSON(`${API_BASE}/parallel-config/${tableName}`, {
        workers: updated.workers,
        batch_size: updated.batch_size,
        enabled: updated.enabled,
      });
      setConfig((prev: any) => ({
        ...prev,
        tables: { ...prev.tables, [tableName]: updated },
      }));
      message.success(`${tableName} 설정 업데이트됨`);
    } catch { message.error('설정 업데이트 실패'); }
  };

  if (loading || !config) return <Spin style={{ display: 'block', margin: '60px auto' }} />;

  const tables = Object.entries(config.tables || {}) as [string, any][];

  return (
    <div>
      <Card size="small" title="전역 병렬 설정" style={{ marginBottom: 16 }}>
        <Descriptions column={4} size="small">
          <Descriptions.Item label="최대 Worker">{config.global?.max_workers}</Descriptions.Item>
          <Descriptions.Item label="기본 배치 크기">{config.global?.batch_size?.toLocaleString()}</Descriptions.Item>
          <Descriptions.Item label="청크 전략">{config.global?.chunk_strategy}</Descriptions.Item>
          <Descriptions.Item label="활성화"><Tag color={config.global?.enabled ? 'green' : 'default'}>{config.global?.enabled ? 'ON' : 'OFF'}</Tag></Descriptions.Item>
        </Descriptions>
      </Card>

      <Card size="small" title={<><ThunderboltOutlined /> 테이블별 병렬 적재 설정</>}>
        <Table
          dataSource={tables.map(([name, cfg]) => ({ key: name, name, ...cfg }))}
          columns={[
            { title: '테이블', dataIndex: 'name', key: 'name', width: 200, render: (v: string) => <Text strong code>{v}</Text> },
            { title: '설명', dataIndex: 'description', key: 'desc', width: 220 },
            {
              title: 'Workers', key: 'workers', width: 120,
              render: (_: any, r: any) => (
                <InputNumber size="small" min={1} max={16} value={r.workers}
                  onChange={(v) => v && updateTable(r.name, 'workers', v)} />
              ),
            },
            {
              title: '배치 크기', key: 'batch', width: 140,
              render: (_: any, r: any) => (
                <InputNumber size="small" min={1000} max={200000} step={5000}
                  value={r.batch_size} onChange={(v) => v && updateTable(r.name, 'batch_size', v)}
                  formatter={(v) => `${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')} />
              ),
            },
            { title: '파티션 키', dataIndex: 'partition_key', key: 'pk', width: 180, render: (v: string) => <Text code>{v}</Text> },
            { title: '전략', dataIndex: 'strategy', key: 'strategy', width: 100, render: (v: string) => <Tag>{v}</Tag> },
            {
              title: '활성화', key: 'enabled', width: 80,
              render: (_: any, r: any) => <Switch size="small" checked={r.enabled} onChange={(v) => updateTable(r.name, 'enabled', v)} />,
            },
          ]}
          pagination={false}
          size="small"
        />
      </Card>

      <Alert style={{ marginTop: 16 }} type="info" showIcon
        message="병렬 적재 전략"
        description="ID Range 파티셔닝: PK 컬럼의 MIN/MAX를 Worker 수로 분할하여 각 Worker가 독립 범위를 병렬 INSERT 합니다. measurement (36.6M rows, 8 workers) 기준 약 4.6M rows/worker로 분할됩니다." />
    </div>
  );
};

export default ParallelLoadingTab;
