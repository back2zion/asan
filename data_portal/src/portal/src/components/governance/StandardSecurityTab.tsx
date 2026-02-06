/**
 * 표준/보안 탭 컴포넌트
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Spin, Progress, Badge, Button,
} from 'antd';
import {
  CheckCircleOutlined, LockOutlined, SafetyCertificateOutlined, ReloadOutlined,
} from '@ant-design/icons';
import { executeSQL } from './helpers';
import { OMOP_TABLES, type SecurityClassification } from './types';

const { Text } = Typography;

const SENSITIVITY_MOCK: SecurityClassification[] = [
  { level: '극비', color: 'red', count: 12, columns: ['person.person_id', 'person.year_of_birth', 'person.gender_concept_id', 'death.death_date', 'note.note_text'] },
  { level: '민감', color: 'orange', count: 28, columns: ['condition_occurrence.condition_concept_id', 'drug_exposure.drug_concept_id', 'measurement.value_as_number', 'observation.value_as_string'] },
  { level: '일반', color: 'green', count: 185, columns: ['concept.concept_name', 'care_site.care_site_name', 'provider.specialty_concept_id', 'location.city'] },
];

const RBAC_MOCK = [
  { role: '데이터 관리자', access: '전체 접근', tables: 'ALL', level: 'Row/Column/Cell' },
  { role: '연구자', access: '비식별 데이터', tables: '주요 CDM 테이블', level: 'Row/Column' },
  { role: '임상의', access: '담당 환자', tables: 'person, visit, condition, drug', level: 'Row' },
  { role: '분석가', access: '집계 데이터', tables: '집계 뷰', level: 'Table' },
];

const StandardSecurityTab: React.FC = () => {
  const [cdmCheck, setCdmCheck] = useState<{ table: string; exists: boolean }[]>([]);
  const [loading, setLoading] = useState(false);

  const checkCDMTables = useCallback(async () => {
    setLoading(true);
    try {
      const sql = `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`;
      const result = await executeSQL(sql);
      const existing = new Set(result.results.map((r) => String(r[0]).toLowerCase()));
      setCdmCheck(OMOP_TABLES.map((t) => ({ table: t, exists: existing.has(t) })));
    } catch {
      setCdmCheck(OMOP_TABLES.map((t) => ({ table: t, exists: false })));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { checkCDMTables(); }, [checkCDMTables]);

  const existsCount = cdmCheck.filter((c) => c.exists).length;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Row gutter={16}>
        {/* 민감도 분류 현황 */}
        <Col xs={24} lg={12}>
          <Card title={<><LockOutlined /> 민감도 분류 현황</>} size="small">
            <Row gutter={12} style={{ marginBottom: 16 }}>
              {SENSITIVITY_MOCK.map((s) => (
                <Col span={8} key={s.level}>
                  <Card size="small">
                    <Statistic
                      title={<Tag color={s.color}>{s.level}</Tag>}
                      value={s.count}
                      suffix="컬럼"
                    />
                  </Card>
                </Col>
              ))}
            </Row>
            <Table
              columns={[
                { title: '등급', dataIndex: 'level', key: 'level', render: (v: string, r: SecurityClassification) => <Tag color={r.color}>{v}</Tag> },
                { title: '컬럼 수', dataIndex: 'count', key: 'count' },
                { title: '주요 컬럼', dataIndex: 'columns', key: 'columns', render: (v: string[]) => v.slice(0, 3).map((c) => <Tag key={c} style={{ marginBottom: 2 }}>{c}</Tag>) },
              ]}
              dataSource={SENSITIVITY_MOCK}
              rowKey="level"
              size="small"
              pagination={false}
            />
            <Text type="secondary" style={{ fontSize: 12, marginTop: 8, display: 'block' }}>
              * mock 데이터 — 향후 실제 민감도 분류 API 연동 예정
            </Text>
          </Card>
        </Col>

        {/* RBAC 현황 */}
        <Col xs={24} lg={12}>
          <Card title={<><SafetyCertificateOutlined /> 접근 권한 현황 (RBAC)</>} size="small">
            <Table
              columns={[
                { title: '역할', dataIndex: 'role', key: 'role', render: (v: string) => <Text strong>{v}</Text> },
                { title: '접근 범위', dataIndex: 'access', key: 'access' },
                { title: '대상 테이블', dataIndex: 'tables', key: 'tables' },
                { title: '보안 레벨', dataIndex: 'level', key: 'level', render: (v: string) => <Tag color="blue">{v}</Tag> },
              ]}
              dataSource={RBAC_MOCK}
              rowKey="role"
              size="small"
              pagination={false}
            />
            <Text type="secondary" style={{ fontSize: 12, marginTop: 8, display: 'block' }}>
              * mock 데이터 — 향후 실제 RBAC 시스템 연동 예정
            </Text>
          </Card>
        </Col>
      </Row>

      {/* OMOP CDM 표준 준수 */}
      <Card
        title={<><CheckCircleOutlined /> OMOP CDM v5.4 표준 테이블 체크리스트</>}
        extra={
          <Space>
            <Badge status={existsCount === OMOP_TABLES.length ? 'success' : 'warning'} />
            <Text>{existsCount} / {OMOP_TABLES.length} 존재</Text>
            <Button icon={<ReloadOutlined />} size="small" onClick={checkCDMTables} loading={loading}>확인</Button>
          </Space>
        }
      >
        <Spin spinning={loading}>
          <Progress
            percent={Math.round((existsCount / OMOP_TABLES.length) * 100)}
            status={existsCount === OMOP_TABLES.length ? 'success' : 'active'}
            style={{ marginBottom: 16 }}
          />
          <Row gutter={[8, 8]}>
            {cdmCheck.map((c) => (
              <Col key={c.table} xs={12} sm={8} md={6}>
                <Badge
                  status={c.exists ? 'success' : 'error'}
                  text={
                    <Text
                      style={{ fontFamily: 'monospace', fontSize: 12 }}
                      type={c.exists ? undefined : 'danger'}
                    >
                      {c.table}
                    </Text>
                  }
                />
              </Col>
            ))}
          </Row>
        </Spin>
      </Card>
    </Space>
  );
};

export default StandardSecurityTab;
