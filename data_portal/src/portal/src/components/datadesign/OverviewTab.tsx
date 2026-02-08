/**
 * DataDesign - Tab 1: 개요 대시보드
 * Statistics, compliance, zone pipeline, ODS/DW/DM mapping, roadmap, AMIS integration
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Alert, Spin, Progress, Steps, Timeline, App,
} from 'antd';
import {
  ApartmentOutlined, DatabaseOutlined, CloudServerOutlined,
  TableOutlined, LinkOutlined, ClusterOutlined, FundOutlined,
  SafetyCertificateOutlined, ExperimentOutlined, RocketOutlined,
  ApiOutlined, SyncOutlined, SwapOutlined,
} from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { Overview, ZONE_COLORS } from './types';

const { Text } = Typography;

const OverviewTab: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [overview, setOverview] = useState<Overview | null>(null);

  const fetchOverview = useCallback(async () => {
    setLoading(true);
    try {
      const data = await dataDesignApi.getOverview();
      setOverview(data);
    } catch {
      message.error('개요 데이터 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    fetchOverview();
  }, [fetchOverview]);

  if (!overview) {
    return (
      <Spin style={{ display: 'block', margin: '80px auto' }}>
        <div style={{ padding: 40, textAlign: 'center', color: '#999' }}>로딩 중...</div>
      </Spin>
    );
  }

  const totalZones = Object.values(overview.zones).reduce((a, b) => a + b, 0);

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* Stat cards */}
      <Row gutter={[16, 16]}>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="데이터 영역 (Zone)" value={totalZones} prefix={<CloudServerOutlined />} valueStyle={{ color: '#005BAC' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="엔티티 수" value={overview.entities.total} prefix={<TableOutlined />} valueStyle={{ color: '#805AD5' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="도메인 수" value={overview.entities.domains} prefix={<ClusterOutlined />} valueStyle={{ color: '#38A169' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="관계 수" value={overview.relations} prefix={<LinkOutlined />} valueStyle={{ color: '#DD6B20' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="명명 규칙" value={overview.naming_rules} prefix={<SafetyCertificateOutlined />} valueStyle={{ color: '#D69E2E' }} />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={4}>
          <Card size="small" hoverable>
            <Statistic title="비정형 매핑" value={overview.unstructured.total} prefix={<ExperimentOutlined />} valueStyle={{ color: '#E53E3E' }} />
          </Card>
        </Col>
      </Row>

      {/* Compliance & Unstructured ratio */}
      <Row gutter={[16, 16]}>
        <Col xs={24} md={12}>
          <Card title="명명 규칙 준수율" size="small">
            <Progress
              percent={overview.naming_compliance}
              status={overview.naming_compliance >= 90 ? 'success' : overview.naming_compliance >= 70 ? 'normal' : 'exception'}
              strokeColor={overview.naming_compliance >= 90 ? '#52c41a' : overview.naming_compliance >= 70 ? '#1890ff' : '#ff4d4f'}
              format={(p) => `${p}%`}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              DB 내 테이블명 대상 명명 규칙 검증 결과
            </Text>
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card title="비정형 데이터 구조화 현황" size="small">
            <Progress
              percent={overview.unstructured.total > 0 ? Math.round(overview.unstructured.active / overview.unstructured.total * 100) : 0}
              strokeColor="#52c41a"
              format={() => `${overview.unstructured.active} / ${overview.unstructured.total} active`}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>
              NLP 기반 비정형 → 정형 변환 매핑 중 활성 상태 비율
            </Text>
          </Card>
        </Col>
      </Row>

      {/* Zone pipeline flow with ODS/DW/DM labels */}
      <Card title="데이터 레이크 Zone 파이프라인" size="small">
        <Steps
          current={4}
          items={[
            { title: 'Source', description: <><div>원천 수집</div><Tag color="#E53E3E" style={{ fontSize: 10, marginTop: 2 }}>ODS</Tag></>, icon: <DatabaseOutlined style={{ color: ZONE_COLORS.source }} /> },
            { title: 'Bronze', description: <><div>원본 적재</div><Tag color="#DD6B20" style={{ fontSize: 10, marginTop: 2 }}>ODS</Tag></>, icon: <CloudServerOutlined style={{ color: ZONE_COLORS.bronze }} /> },
            { title: 'Silver', description: <><div>CDM 표준 변환</div><Tag color="#805AD5" style={{ fontSize: 10, marginTop: 2 }}>DW</Tag></>, icon: <SafetyCertificateOutlined style={{ color: ZONE_COLORS.silver }} /> },
            { title: 'Gold', description: <><div>큐레이션/비식별화</div><Tag color="#D69E2E" style={{ fontSize: 10, marginTop: 2 }}>DW</Tag></>, icon: <FundOutlined style={{ color: ZONE_COLORS.gold }} /> },
            { title: 'Mart', description: <><div>서비스 마트</div><Tag color="#38A169" style={{ fontSize: 10, marginTop: 2 }}>DM</Tag></>, icon: <ApartmentOutlined style={{ color: ZONE_COLORS.mart }} /> },
          ]}
        />
      </Card>

      {/* ODS/DW/DM 매핑 테이블 */}
      <Card title="ODS / DW / DM 계층 매핑 (RFP SFR-001)" size="small">
        <Table
          dataSource={[
            { key: 'ods', layer: 'ODS (운영 데이터 저장소)', zones: 'Source + Bronze', purpose: '원천 시스템 데이터 그대로 수집·적재', storage: 'Object Storage (S3)', format: 'JSON, CSV → Parquet', retention: '90~365일' },
            { key: 'dw', layer: 'DW (데이터 웨어하우스)', zones: 'Silver + Gold', purpose: 'OMOP CDM 표준 변환 + 큐레이션/비식별화', storage: 'Apache Iceberg (S3)', format: 'Iceberg (Parquet)', retention: '1,095~1,825일' },
            { key: 'dm', layer: 'DM (데이터 마트)', zones: 'Mart', purpose: '서비스별 집계·요약 마트 (CDW 연구, BI)', storage: 'PostgreSQL / Delta Lake', format: 'Delta / RDBMS', retention: '730일' },
          ]}
          columns={[
            { title: '계층', dataIndex: 'layer', key: 'layer', render: (v: string) => <Text strong>{v}</Text>, width: 200 },
            { title: 'Zone 매핑', dataIndex: 'zones', key: 'zones', render: (v: string) => {
              const parts = v.split(' + ');
              return <Space>{parts.map((p, i) => <Tag key={i} color={ZONE_COLORS[p.toLowerCase()] || 'default'}>{p}</Tag>)}</Space>;
            }, width: 160 },
            { title: '목적', dataIndex: 'purpose', key: 'purpose' },
            { title: '저장소', dataIndex: 'storage', key: 'storage', width: 180 },
            { title: '파일 포맷', dataIndex: 'format', key: 'format', width: 140 },
            { title: '보존 기간', dataIndex: 'retention', key: 'retention', width: 110 },
          ]}
          size="small"
          pagination={false}
        />
      </Card>

      {/* 확장성 로드맵 */}
      <Card title={<><RocketOutlined /> 확장성 로드맵 (SFR-001: IoT/실시간 대응)</>} size="small">
        <Row gutter={[16, 16]}>
          <Col xs={24} md={8}>
            <Card size="small" style={{ borderTop: '3px solid #005BAC', height: '100%' }}>
              <Tag color="blue" style={{ marginBottom: 8 }}>Phase 1 — 현재</Tag>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>배치 CDC + ETL</div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                Airflow 기반 30분 주기 CDC (Debezium) → OMOP CDM 배치 변환. 9,200만건 Synthea 합성데이터 적재 완료.
              </Text>
              <div style={{ marginTop: 12 }}>
                <Timeline
                  items={[
                    { color: 'green', children: <Text style={{ fontSize: 11 }}>Airflow DAG 스케줄링</Text> },
                    { color: 'green', children: <Text style={{ fontSize: 11 }}>PostgreSQL OMOP CDM</Text> },
                    { color: 'green', children: <Text style={{ fontSize: 11 }}>Redis 캐시 (5분 TTL)</Text> },
                  ]}
                />
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ borderTop: '3px solid #DD6B20', height: '100%' }}>
              <Tag color="orange" style={{ marginBottom: 8 }}>Phase 2 — 계획</Tag>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>실시간 스트리밍</div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                Apache Kafka + Flink 기반 실시간 CDC 파이프라인. HL7 FHIR 메시지 스트리밍, 이벤트 기반 CDM 변환.
              </Text>
              <div style={{ marginTop: 12 }}>
                <Timeline
                  items={[
                    { color: 'orange', children: <Text style={{ fontSize: 11 }}>Kafka Connect (Debezium)</Text> },
                    { color: 'orange', children: <Text style={{ fontSize: 11 }}>Apache Flink CDC</Text> },
                    { color: 'orange', children: <Text style={{ fontSize: 11 }}>HL7 FHIR → OMOP 실시간</Text> },
                  ]}
                />
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ borderTop: '3px solid #38A169', height: '100%' }}>
              <Tag color="green" style={{ marginBottom: 8 }}>Phase 3 — 미래</Tag>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>IoT / 실시간 분석</div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                MQTT 기반 IoT 의료기기 데이터 수집. 환자 모니터링, 웨어러블 실시간 분석, Edge Computing 연동.
              </Text>
              <div style={{ marginTop: 12 }}>
                <Timeline
                  items={[
                    { color: 'gray', children: <Text style={{ fontSize: 11 }}>MQTT Broker (EMQ X)</Text> },
                    { color: 'gray', children: <Text style={{ fontSize: 11 }}>Edge Computing (K3s)</Text> },
                    { color: 'gray', children: <Text style={{ fontSize: 11 }}>실시간 대시보드 (WebSocket)</Text> },
                  ]}
                />
              </div>
            </Card>
          </Col>
        </Row>
      </Card>

      {/* AMIS 연계 현황 */}
      <Card title={<><ApiOutlined /> AMIS 연계 현황 (병원 정보시스템 연동)</>} size="small">
        <Alert
          message="AMIS 연계는 Source Zone을 통해 병원 원천 시스템 데이터를 OMOP CDM으로 변환합니다"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />
        <Table
          dataSource={[
            { key: 'patient', source: 'AMIS 환자 마스터', sourceTable: 'AMIS_PATIENT', target: 'person', method: 'CDC (Debezium)', mappingFields: 'patient_id → person_source_value, gender → gender_concept_id', status: 'active', lastSync: '2026-02-08 02:00', records: 76074 },
            { key: 'visit', source: 'AMIS 방문 기록', sourceTable: 'AMIS_VISIT', target: 'visit_occurrence', method: 'CDC (Debezium)', mappingFields: 'visit_id → visit_source_value, visit_type → visit_concept_id', status: 'active', lastSync: '2026-02-08 02:00', records: 4500000 },
            { key: 'diagnosis', source: 'AMIS 진단 기록', sourceTable: 'AMIS_DIAGNOSIS', target: 'condition_occurrence', method: 'Batch ETL', mappingFields: 'icd_code → condition_source_value → condition_concept_id (SNOMED)', status: 'active', lastSync: '2026-02-08 04:00', records: 2800000 },
            { key: 'drug', source: 'AMIS 처방 기록', sourceTable: 'AMIS_PRESCRIPTION', target: 'drug_exposure', method: 'Batch ETL', mappingFields: 'drug_code → drug_source_value → drug_concept_id (RxNorm)', status: 'active', lastSync: '2026-02-08 04:00', records: 3900000 },
            { key: 'lab', source: 'AMIS 검사 결과', sourceTable: 'AMIS_LAB_RESULT', target: 'measurement', method: 'Batch ETL', mappingFields: 'lab_code → measurement_source_value → measurement_concept_id (LOINC)', status: 'active', lastSync: '2026-02-08 04:00', records: 36600000 },
            { key: 'procedure', source: 'AMIS 시술/수술', sourceTable: 'AMIS_PROCEDURE', target: 'procedure_occurrence', method: 'Batch ETL', mappingFields: 'procedure_code → procedure_source_value → procedure_concept_id', status: 'planned', lastSync: '-', records: 0 },
          ]}
          columns={[
            { title: '원천 시스템', dataIndex: 'source', key: 'source', render: (v: string) => <Text strong>{v}</Text>, width: 140 },
            { title: 'Source 테이블', dataIndex: 'sourceTable', key: 'sourceTable', render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text>, width: 140 },
            { title: <><SwapOutlined /> CDM 대상</>, dataIndex: 'target', key: 'target', render: (v: string) => <Tag color="purple">{v}</Tag>, width: 140 },
            { title: '연계 방식', dataIndex: 'method', key: 'method', width: 110, render: (v: string) => <Tag color={v.includes('CDC') ? 'green' : 'blue'}>{v}</Tag> },
            { title: '상태', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={v === 'active' ? 'success' : 'processing'} icon={v === 'active' ? <SyncOutlined spin /> : undefined}>{v === 'active' ? '운영 중' : '계획'}</Tag> },
            { title: '최종 동기화', dataIndex: 'lastSync', key: 'lastSync', width: 140, render: (v: string) => <Text type="secondary" style={{ fontSize: 11 }}>{v}</Text> },
            { title: '적재 건수', dataIndex: 'records', key: 'records', width: 100, align: 'right' as const, render: (v: number) => v > 0 ? <Text>{v.toLocaleString()}</Text> : '-' },
          ]}
          size="small"
          pagination={false}
          expandable={{
            expandedRowRender: (record) => (
              <div style={{ padding: '4px 0' }}>
                <Text type="secondary" style={{ fontSize: 12 }}>매핑 필드: </Text>
                <Text code style={{ fontSize: 11 }}>{record.mappingFields}</Text>
              </div>
            ),
          }}
        />
        <Row gutter={[16, 8]} style={{ marginTop: 16 }}>
          <Col xs={8}>
            <Statistic title="운영 중 연계" value={5} suffix="/ 6" valueStyle={{ color: '#52c41a', fontSize: 20 }} />
          </Col>
          <Col xs={8}>
            <Statistic title="총 적재 건수" value="9,200만" valueStyle={{ color: '#005BAC', fontSize: 20 }} />
          </Col>
          <Col xs={8}>
            <Statistic title="최종 동기화" value="02:00 KST" valueStyle={{ color: '#666', fontSize: 20 }} />
          </Col>
        </Row>
      </Card>
    </Space>
  );
};

export default OverviewTab;
