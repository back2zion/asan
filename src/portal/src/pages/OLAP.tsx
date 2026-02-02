import React, { useState, useCallback } from 'react';
import { Card, Button, Typography, Spin, Table, Alert, Space, Row, Col, Empty } from 'antd';
import { RocketOutlined, TableOutlined, ConsoleSqlOutlined } from '@ant-design/icons';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';

const { Title, Paragraph, Text } = Typography;

const initialQuery = `
-- OLAP 쿼리 예시:
-- 성별/연령대별 평균 입원일수를 계산합니다.
WITH patient_age_band AS (
  SELECT
    patient_id,
    gender,
    CAST(EXTRACT(YEAR FROM AGE(birth_date)) / 10 AS INTEGER) * 10 AS age_band
  FROM "환자 기본정보 마트 (Patient_Master)"
),
admission_duration AS (
  SELECT
    patient_id,
    DATE_DIFF('day', admission_date, discharge_date) AS duration_days
  FROM "입퇴원 이력 테이블 (mock)"
)
SELECT
  p.gender,
  p.age_band,
  AVG(a.duration_days) AS avg_stay_days,
  COUNT(p.patient_id) AS patient_count
FROM patient_age_band p
JOIN admission_duration a
  ON p.patient_id = a.patient_id
GROUP BY
  p.gender,
  p.age_band
ORDER BY
  p.age_band,
  p.gender;
`.trim();

const mockQueryResult = {
  columns: [
    { title: '성별 (gender)', dataIndex: 'gender', key: 'gender', render: (g:string) => g === 'M' ? '남성' : '여성' },
    { title: '연령대 (age_band)', dataIndex: 'age_band', key: 'age_band', render: (age:number) => `${age}대` },
    { title: '평균 입원일수 (avg_stay_days)', dataIndex: 'avg_stay_days', key: 'avg_stay_days', render: (days:number) => days.toFixed(1) },
    { title: '환자 수 (patient_count)', dataIndex: 'patient_count', key: 'patient_count' },
  ],
  data: [
    { gender: 'F', age_band: 20, avg_stay_days: 5.2, patient_count: 120 },
    { gender: 'M', age_band: 20, avg_stay_days: 6.1, patient_count: 110 },
    { gender: 'F', age_band: 30, avg_stay_days: 7.8, patient_count: 230 },
    { gender: 'M', age_band: 30, avg_stay_days: 8.5, patient_count: 250 },
    { gender: 'F', age_band: 40, avg_stay_days: 9.1, patient_count: 310 },
    { gender: 'M', age_band: 40, avg_stay_days: 10.2, patient_count: 300 },
    { gender: 'F', age_band: 50, avg_stay_days: 11.5, patient_count: 450 },
    { gender: 'M', age_band: 50, avg_stay_days: 12.3, patient_count: 480 },
  ],
};


const OLAP: React.FC = () => {
  const [query, setQuery] = useState<string>(initialQuery);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [results, setResults] = useState<any>(null);

  const onQueryChange = useCallback((value: string) => {
    setQuery(value);
  }, []);

  const handleRunQuery = () => {
    setLoading(true);
    setError(null);
    setResults(null);

    // Simulate API call to a query engine like DuckDB or Trino
    setTimeout(() => {
      if (query.toLowerCase().includes('select') && query.toLowerCase().includes('group by')) {
        setResults(mockQueryResult);
      } else {
        setError('쿼리 실행에 실패했습니다. 현재는 예시 쿼리만 실행 가능합니다.');
      }
      setLoading(false);
    }, 1500);
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Title level={3} style={{ margin: 0 }}>
          <ConsoleSqlOutlined style={{ marginRight: 8 }} />
          OLAP 직접 분석
        </Title>
        <Paragraph type="secondary">
          SFR-002: 데이터 레이크하우스에 직접 SQL 쿼리를 실행하여 OLAP 분석을 수행합니다.
        </Paragraph>
        <Alert
          message="DuckDB 기반 고속 분석 엔진"
          description="이 화면에서는 내장된 고속 OLAP 엔진(DuckDB)을 사용하여 대화형으로 데이터를 탐색하고 집계할 수 있습니다. BI 툴에서 제공하는 정형화된 대시보드를 넘어, 연구자가 가설을 검증하기 위한 Ad-hoc 쿼리를 직접 실행할 수 있는 전문가용 도구입니다."
          type="info"
          showIcon
        />
      </Card>
      
      <Row gutter={[16, 16]}>
        <Col xs={24} xl={10}>
          <Card 
            title="SQL 편집기" 
            extra={<Button type="primary" icon={<RocketOutlined />} onClick={handleRunQuery} loading={loading}>쿼리 실행</Button>}
            style={{height: '100%'}}
          >
            <CodeMirror
              value={query}
              height="400px"
              extensions={[sql()]}
              onChange={onQueryChange}
              style={{ border: '1px solid #f0f0f0' }}
            />
            <Paragraph style={{marginTop: 16}}>
                <Text strong>사용 가능한 테이블:</Text>
                <ul>
                    <li><Text code>"환자 기본정보 마트 (Patient_Master)"</Text></li>
                    <li><Text code>"처방 및 투약정보 마트 (Prescription_Medication)"</Text></li>
                    <li><Text code>"입퇴원 이력 테이블 (mock)"</Text></li>
                </ul>
            </Paragraph>
          </Card>
        </Col>

        <Col xs={24} xl={14}>
          <Card title="쿼리 결과" style={{height: '100%'}}>
            {loading && (
              <div style={{ textAlign: 'center', padding: '50px 0' }}>
                <Spin size="large" />
                <p>쿼리를 실행 중입니다...</p>
              </div>
            )}
            {error && <Alert message={error} type="error" showIcon />}
            {!loading && !error && !results && (
                <Empty description="쿼리를 실행하여 결과를 확인하세요." />
            )}
            {results && (
              <Table
                columns={results.columns}
                dataSource={results.data}
                size="small"
                scroll={{ x: 'max-content' }}
                pagination={{ pageSize: 10 }}
              />
            )}
          </Card>
        </Col>
      </Row>
    </Space>
  );
};

export default OLAP;