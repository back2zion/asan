import React, { useState, useEffect } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Progress, Select, Input, Empty, message,
} from 'antd';
import {
  SwapOutlined, ThunderboltOutlined, CodeOutlined, CopyOutlined,
} from '@ant-design/icons';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

const { Text } = Typography;
const { TextArea } = Input;

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

const MappingGeneratorTab: React.FC = () => {
  const [targetTables, setTargetTables] = useState<any[]>([]);
  const [selectedTarget, setSelectedTarget] = useState<string>('person');
  const [sourceTable, setSourceTable] = useState<string>('');
  const [sourceColumns, setSourceColumns] = useState<string>('');
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchJSON(`${API_BASE}/mapping/target-tables`).then(d => setTargetTables(d.tables || [])).catch(() => {});
  }, []);

  const generateMapping = async () => {
    if (!sourceTable.trim() || !sourceColumns.trim()) {
      message.warning('소스 테이블명과 컨럼 목록을 입력하세요');
      return;
    }
    const cols = sourceColumns.split(/[,\n]+/).map(c => c.trim()).filter(Boolean);
    setLoading(true);
    try {
      const data = await postJSON(`${API_BASE}/mapping/generate`, {
        source_table: sourceTable.trim(),
        source_columns: cols,
        target_table: selectedTarget,
      });
      setResult(data);
    } catch { message.error('매핑 생성 실패'); }
    finally { setLoading(false); }
  };

  const confidenceColor = (c: number) => c >= 0.9 ? 'green' : c >= 0.7 ? 'blue' : c >= 0.5 ? 'orange' : 'red';

  const mappingColumns = [
    { title: '소스 컨럼', dataIndex: 'source_column', key: 'src', render: (v: string) => <Text code>{v}</Text> },
    { title: '타겟 컨럼', dataIndex: 'target_column', key: 'tgt', render: (v: string) => <Text strong code>{v}</Text> },
    {
      title: '신뢰도', dataIndex: 'confidence', key: 'conf', width: 120,
      render: (v: number) => <Progress percent={Math.round(v * 100)} size="small" strokeColor={confidenceColor(v)} />,
    },
    { title: '매칭 방법', dataIndex: 'method', key: 'method', width: 120, render: (v: string) => <Tag>{v}</Tag> },
  ];

  const presets: Record<string, { table: string; columns: string }> = {
    ehr_patient: { table: 'ehr_patients', columns: 'patient_id, sex, birth_year, pat_id, sex_cd' },
    ehr_visit: { table: 'ehr_encounters', columns: 'encounter_id, patient_id, admission_date, discharge_date, visit_type' },
    lab_result: { table: 'lab_results', columns: 'patient_id, test_code, test_date, result_value, unit, lab_code' },
  };

  return (
    <div>
      <Row gutter={16}>
        <Col xs={24} md={10}>
          <Card size="small" title={<><SwapOutlined /> 소스 → 타겟 매핑 정의</>}>
            <div style={{ marginBottom: 12 }}>
              <Text strong>예시 프리셋:</Text>
              <div style={{ marginTop: 4 }}>
                <Space wrap>
                  {Object.entries(presets).map(([key, val]) => (
                    <Button size="small" key={key} onClick={() => { setSourceTable(val.table); setSourceColumns(val.columns); }}>
                      {val.table}
                    </Button>
                  ))}
                </Space>
              </div>
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text>소스 테이블명</Text>
              <Input value={sourceTable} onChange={e => setSourceTable(e.target.value)} placeholder="예: ehr_patients" style={{ marginTop: 4 }} />
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text>소스 컨럼 (쉼표 또는 줄바꿈 구분)</Text>
              <TextArea rows={4} value={sourceColumns} onChange={e => setSourceColumns(e.target.value)}
                placeholder="patient_id, sex, birth_year, ..." style={{ marginTop: 4 }} />
            </div>

            <div style={{ marginBottom: 12 }}>
              <Text>타겟 테이블 (OMOP CDM)</Text>
              <Select value={selectedTarget} onChange={setSelectedTarget} style={{ width: '100%', marginTop: 4 }}
                options={targetTables.map(t => ({ value: t.name, label: `${t.name} (${t.column_count} cols)` }))} />
            </div>

            <Button type="primary" icon={<ThunderboltOutlined />} onClick={generateMapping} loading={loading} block>
              매핑 자동 생성
            </Button>
          </Card>
        </Col>

        <Col xs={24} md={14}>
          {result ? (
            <div>
              <Card size="small" title="매핑 결과" style={{ marginBottom: 16 }}
                extra={<Tag color={result.stats.coverage_percent >= 80 ? 'green' : result.stats.coverage_percent >= 50 ? 'orange' : 'red'}>
                  커버리지 {result.stats.coverage_percent}%
                </Tag>}>
                <Row gutter={16} style={{ marginBottom: 16 }}>
                  <Col span={8}><Statistic title="소스 컨럼" value={result.stats.total_source_columns} /></Col>
                  <Col span={8}><Statistic title="매칭됨" value={result.stats.matched_columns} valueStyle={{ color: '#52c41a' }} /></Col>
                  <Col span={8}><Statistic title="미매칭" value={result.unmapped_source.length} valueStyle={{ color: result.unmapped_source.length > 0 ? '#faad14' : undefined }} /></Col>
                </Row>
                <Table dataSource={result.mappings} columns={mappingColumns} rowKey="source_column" size="small" pagination={false} />
                {result.unmapped_source.length > 0 && (
                  <div style={{ marginTop: 12 }}>
                    <Text type="warning">미매칭 소스 컨럼: </Text>
                    {result.unmapped_source.map((c: string) => <Tag key={c} color="orange">{c}</Tag>)}
                  </div>
                )}
              </Card>

              <Card size="small" title={<><CodeOutlined /> 자동 생성된 ETL 코드</>}
                extra={<Button size="small" icon={<CopyOutlined />} onClick={() => { navigator.clipboard.writeText(result.generated_etl_code); message.success('클립보드에 복사됨'); }}>복사</Button>}>
                <SyntaxHighlighter language="python" style={oneDark} customStyle={{ maxHeight: 400, fontSize: 12 }}>
                  {result.generated_etl_code}
                </SyntaxHighlighter>
              </Card>
            </div>
          ) : (
            <Card size="small" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Empty description="좌측에서 소스 컨럼을 입력하고 매핑을 생성하세요" />
            </Card>
          )}
        </Col>
      </Row>
    </div>
  );
};

export default MappingGeneratorTab;
