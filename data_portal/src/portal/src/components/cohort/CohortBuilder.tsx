import React, { useState } from 'react';
import {
  Card, Row, Col, Select, InputNumber, DatePicker, Radio, Button, Tag, Space,
  Tabs, Statistic, Typography, Spin, Alert, Modal, Table, App,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, PlayCircleOutlined,
  TeamOutlined, ExperimentOutlined,
} from '@ant-design/icons';
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import CONSORTFlow from './CONSORTFlow';
import VennDiagram from './VennDiagram';
import {
  cohortApi,
  type Criterion,
  type FlowStep,
  type FlowStepResult,
  type SetOperationResponse,
  type SummaryStatsResponse,
  type PatientRow,
} from '../../services/cohortApi';

const { Text, Title } = Typography;

const CRITERION_TYPES = [
  { value: 'age_range', label: '연령 범위' },
  { value: 'gender', label: '성별' },
  { value: 'condition', label: '진단 (SNOMED)' },
  { value: 'drug', label: '약물' },
  { value: 'procedure', label: '시술' },
  { value: 'measurement', label: '검사' },
  { value: 'visit_type', label: '방문 유형' },
];

const VISIT_TYPES = [
  { value: 9201, label: '입원 (9201)' },
  { value: 9202, label: '외래 (9202)' },
  { value: 9203, label: '응급 (9203)' },
];

const COMMON_CONDITIONS = [
  { code: '44054006', label: '당뇨 (44054006)' },
  { code: '38341003', label: '고혈압 (38341003)' },
];

const PIE_COLORS = ['#1890ff', '#ff6b81', '#52c41a', '#fa8c16'];

const CohortBuilder: React.FC = () => {
  const { message } = App.useApp();
  // ── Criterion form state ───────────────────────────────
  const [criterionType, setCriterionType] = useState<string>('condition');
  const [stepType, setStepType] = useState<'inclusion' | 'exclusion'>('inclusion');
  const [formLabel, setFormLabel] = useState('');

  // type-specific fields
  const [minAge, setMinAge] = useState(0);
  const [maxAge, setMaxAge] = useState(100);
  const [gender, setGender] = useState<'M' | 'F'>('M');
  const [conceptCode, setConceptCode] = useState('');
  const [dateFrom, setDateFrom] = useState<string | undefined>();
  const [dateTo, setDateTo] = useState<string | undefined>();
  const [valueMin, setValueMin] = useState<number | undefined>();
  const [valueMax, setValueMax] = useState<number | undefined>();
  const [visitConceptId, setVisitConceptId] = useState<9201 | 9202 | 9203>(9202);

  // ── Steps ──────────────────────────────────────────────
  const [steps, setSteps] = useState<FlowStep[]>([]);

  // ── Results ────────────────────────────────────────────
  const [flowLoading, setFlowLoading] = useState(false);
  const [flowResult, setFlowResult] = useState<{ total: number; steps: FlowStepResult[] } | null>(null);
  const [flowError, setFlowError] = useState<string | null>(null);

  // ── Set operation ──────────────────────────────────────
  const [setOpLoading, setSetOpLoading] = useState(false);
  const [setOpResult, setSetOpResult] = useState<SetOperationResponse | null>(null);
  const [setOpOperation, setSetOpOperation] = useState<'intersection' | 'union' | 'difference'>('intersection');

  // ── Summary stats ──────────────────────────────────────
  const [statsLoading, setStatsLoading] = useState(false);
  const [stats, setStats] = useState<SummaryStatsResponse | null>(null);

  // ── Drill-down modal ───────────────────────────────────
  const [drillModalOpen, setDrillModalOpen] = useState(false);
  const [drillLoading, setDrillLoading] = useState(false);
  const [drillPatients, setDrillPatients] = useState<PatientRow[]>([]);
  const [drillStepLabel, setDrillStepLabel] = useState('');

  // ── Build criterion from form ──────────────────────────
  const buildCriterion = (): Criterion | null => {
    const label = formLabel.trim();
    switch (criterionType) {
      case 'age_range':
        return { type: 'age_range', label: label || '연령', min_age: minAge, max_age: maxAge };
      case 'gender':
        return { type: 'gender', label: label || (gender === 'M' ? '남성' : '여성'), gender };
      case 'condition':
        if (!conceptCode) { message.warning('진단 코드를 입력하세요'); return null; }
        return { type: 'condition', label: label || `진단 ${conceptCode}`, concept_code: conceptCode, date_from: dateFrom, date_to: dateTo };
      case 'drug':
        if (!conceptCode) { message.warning('약물 코드를 입력하세요'); return null; }
        return { type: 'drug', label: label || `약물 ${conceptCode}`, concept_code: conceptCode, date_from: dateFrom, date_to: dateTo };
      case 'procedure':
        if (!conceptCode) { message.warning('시술 코드를 입력하세요'); return null; }
        return { type: 'procedure', label: label || `시술 ${conceptCode}`, concept_code: conceptCode, date_from: dateFrom, date_to: dateTo };
      case 'measurement':
        if (!conceptCode) { message.warning('검사 코드를 입력하세요'); return null; }
        return { type: 'measurement', label: label || `검사 ${conceptCode}`, concept_code: conceptCode, value_min: valueMin, value_max: valueMax, date_from: dateFrom, date_to: dateTo };
      case 'visit_type':
        return { type: 'visit_type', label: label || `방문 ${visitConceptId}`, visit_concept_id: visitConceptId, date_from: dateFrom, date_to: dateTo };
      default:
        return null;
    }
  };

  const addStep = () => {
    const criterion = buildCriterion();
    if (!criterion) return;
    setSteps(prev => [...prev, { step_type: stepType, criterion, label: criterion.label }]);
    setConceptCode('');
    setFormLabel('');
  };

  const removeStep = (idx: number) => {
    setSteps(prev => prev.filter((_, i) => i !== idx));
  };

  // ── Execute flow ───────────────────────────────────────
  const executeFlow = async () => {
    if (steps.length === 0) { message.warning('단계를 1개 이상 추가하세요'); return; }
    setFlowLoading(true);
    setFlowError(null);
    try {
      const res = await cohortApi.executeFlow(steps);
      setFlowResult({ total: res.total_population, steps: res.steps });

      // Auto-fetch summary stats
      const criteria = steps.filter(s => s.step_type === 'inclusion').map(s => s.criterion);
      if (criteria.length > 0) {
        setStatsLoading(true);
        try {
          const s = await cohortApi.summaryStats(criteria);
          setStats(s);
        } catch { /* ignore stats error */ }
        setStatsLoading(false);
      }
    } catch (err: any) {
      setFlowError(err?.response?.data?.detail || '분석 실행 중 오류 발생');
    } finally {
      setFlowLoading(false);
    }
  };

  // ── Set operation ──────────────────────────────────────
  const executeSetOperation = async () => {
    const inclusionSteps = steps.filter(s => s.step_type === 'inclusion');
    const exclusionSteps = steps.filter(s => s.step_type === 'exclusion');
    if (inclusionSteps.length === 0 || exclusionSteps.length === 0) {
      message.warning('집합 연산에는 선정/제외 조건이 각각 1개 이상 필요합니다');
      return;
    }
    setSetOpLoading(true);
    try {
      const res = await cohortApi.setOperation(
        inclusionSteps.map(s => s.criterion),
        exclusionSteps.map(s => s.criterion),
        setOpOperation,
      );
      setSetOpResult(res);
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '집합 연산 오류');
    } finally {
      setSetOpLoading(false);
    }
  };

  // ── Drill-down ─────────────────────────────────────────
  const handleDrillDown = async (stepIndex: number) => {
    const criteria = steps
      .slice(0, stepIndex + 1)
      .filter(s => s.step_type === 'inclusion')
      .map(s => s.criterion);
    if (criteria.length === 0) return;
    setDrillStepLabel(steps[stepIndex].label || `단계 ${stepIndex + 1}`);
    setDrillModalOpen(true);
    setDrillLoading(true);
    try {
      const res = await cohortApi.drillDown(criteria, 50, 0);
      setDrillPatients(res.patients);
    } catch { setDrillPatients([]); }
    setDrillLoading(false);
  };

  // ── Render ─────────────────────────────────────────────
  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Row gutter={16}>
        {/* ── Left Panel: Criterion form ── */}
        <Col span={8}>
          <Card title={<><ExperimentOutlined /> 조건 추가</>} size="small">
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <div>
                <Text strong style={{ fontSize: 13 }}>조건 유형</Text>
                <Select
                  style={{ width: '100%', marginTop: 4 }}
                  value={criterionType}
                  onChange={setCriterionType}
                  options={CRITERION_TYPES}
                />
              </div>

              {/* Dynamic fields */}
              {criterionType === 'age_range' && (
                <Space>
                  <InputNumber min={0} max={150} value={minAge} onChange={v => setMinAge(v ?? 0)} addonBefore="최소" />
                  <InputNumber min={0} max={150} value={maxAge} onChange={v => setMaxAge(v ?? 100)} addonBefore="최대" />
                </Space>
              )}

              {criterionType === 'gender' && (
                <Radio.Group value={gender} onChange={e => setGender(e.target.value)}>
                  <Radio value="M">남성</Radio>
                  <Radio value="F">여성</Radio>
                </Radio.Group>
              )}

              {['condition', 'drug', 'procedure', 'measurement'].includes(criterionType) && (
                <div>
                  <Text strong style={{ fontSize: 13 }}>코드 (SNOMED/source_value)</Text>
                  <Select
                    style={{ width: '100%', marginTop: 4 }}
                    showSearch
                    allowClear
                    value={conceptCode || undefined}
                    onChange={(v) => setConceptCode(v || '')}
                    placeholder="코드 입력 또는 선택"
                    options={COMMON_CONDITIONS.map(c => ({ value: c.code, label: c.label }))}
                    filterOption={(input, option) =>
                      (option?.label ?? '').toLowerCase().includes(input.toLowerCase())
                    }
                  />
                </div>
              )}

              {criterionType === 'measurement' && (
                <Space>
                  <InputNumber placeholder="최소값" value={valueMin} onChange={v => setValueMin(v ?? undefined)} />
                  <InputNumber placeholder="최대값" value={valueMax} onChange={v => setValueMax(v ?? undefined)} />
                </Space>
              )}

              {criterionType === 'visit_type' && (
                <Select
                  style={{ width: '100%' }}
                  value={visitConceptId}
                  onChange={setVisitConceptId}
                  options={VISIT_TYPES}
                />
              )}

              {['condition', 'drug', 'procedure', 'measurement', 'visit_type'].includes(criterionType) && (
                <Space>
                  <DatePicker
                    placeholder="시작일"
                    onChange={(_, ds) => setDateFrom(typeof ds === 'string' && ds ? ds : undefined)}
                    style={{ width: 130 }}
                  />
                  <DatePicker
                    placeholder="종료일"
                    onChange={(_, ds) => setDateTo(typeof ds === 'string' && ds ? ds : undefined)}
                    style={{ width: 130 }}
                  />
                </Space>
              )}

              <div>
                <Text strong style={{ fontSize: 13 }}>라벨 (선택)</Text>
                <input
                  type="text"
                  value={formLabel}
                  onChange={e => setFormLabel(e.target.value)}
                  placeholder="단계 설명"
                  style={{ width: '100%', padding: '4px 8px', borderRadius: 4, border: '1px solid #d9d9d9', marginTop: 4 }}
                />
              </div>

              <Space>
                <Radio.Group value={stepType} onChange={e => setStepType(e.target.value)} buttonStyle="solid" size="small">
                  <Radio.Button value="inclusion">선정</Radio.Button>
                  <Radio.Button value="exclusion">제외</Radio.Button>
                </Radio.Group>
                <Button type="primary" icon={<PlusOutlined />} onClick={addStep} size="small">
                  단계 추가
                </Button>
              </Space>
            </Space>
          </Card>

          {/* Steps list */}
          <Card
            title={<><TeamOutlined /> 설계 단계 ({steps.length}개)</>}
            size="small"
            style={{ marginTop: 16 }}
            extra={
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={executeFlow}
                loading={flowLoading}
                disabled={steps.length === 0}
                size="small"
              >
                분석 실행
              </Button>
            }
          >
            {steps.length === 0 ? (
              <Text type="secondary">조건을 추가하여 코호트를 설계하세요.</Text>
            ) : (
              <Space direction="vertical" style={{ width: '100%' }}>
                {steps.map((s, idx) => (
                  <div key={idx} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <Tag color={s.step_type === 'inclusion' ? 'blue' : 'orange'}>
                      {s.step_type === 'inclusion' ? '선정' : '제외'}
                    </Tag>
                    <Text style={{ flex: 1, fontSize: 13 }}>{s.label || s.criterion.label}</Text>
                    <Button
                      type="text"
                      danger
                      icon={<DeleteOutlined />}
                      size="small"
                      onClick={() => removeStep(idx)}
                    />
                  </div>
                ))}
              </Space>
            )}
          </Card>
        </Col>

        {/* ── Right Panel: Results ── */}
        <Col span={16}>
          {flowError && (
            <Alert message="오류" description={flowError} type="error" showIcon closable onClose={() => setFlowError(null)} style={{ marginBottom: 16 }} />
          )}

          <Card>
            <Tabs
              defaultActiveKey="consort"
              items={[
                {
                  key: 'consort',
                  label: 'CONSORT 흐름도',
                  children: flowLoading ? (
                    <div style={{ textAlign: 'center', padding: 60 }}><Spin size="large" /><div style={{ marginTop: 16 }}>분석 중...</div></div>
                  ) : flowResult ? (
                    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                      <Row gutter={16}>
                        <Col span={8}>
                          <Statistic title="전체 모수" value={flowResult.total} suffix="명" valueStyle={{ color: '#52c41a' }} />
                        </Col>
                        <Col span={8}>
                          <Statistic
                            title="최종 잔여"
                            value={flowResult.steps.length > 0 ? flowResult.steps[flowResult.steps.length - 1].remaining_count : flowResult.total}
                            suffix="명"
                            valueStyle={{ color: '#1890ff' }}
                          />
                        </Col>
                        <Col span={8}>
                          <Statistic
                            title="총 탈락"
                            value={flowResult.total - (flowResult.steps.length > 0 ? flowResult.steps[flowResult.steps.length - 1].remaining_count : flowResult.total)}
                            suffix="명"
                            valueStyle={{ color: '#ff4d4f' }}
                          />
                        </Col>
                      </Row>
                      <CONSORTFlow
                        totalPopulation={flowResult.total}
                        steps={flowResult.steps}
                        onNodeClick={handleDrillDown}
                      />
                    </Space>
                  ) : (
                    <div style={{ textAlign: 'center', padding: 60 }}>
                      <Text type="secondary">좌측에서 조건을 추가하고 "분석 실행"을 클릭하세요.</Text>
                    </div>
                  ),
                },
                {
                  key: 'set-op',
                  label: '집합 연산',
                  children: (
                    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                      <Row gutter={16} align="middle">
                        <Col>
                          <Text strong>Group A: 선정 조건</Text>
                        </Col>
                        <Col>
                          <Select
                            value={setOpOperation}
                            onChange={setSetOpOperation}
                            style={{ width: 140 }}
                            options={[
                              { value: 'intersection', label: '교집합 (A∩B)' },
                              { value: 'union', label: '합집합 (A∪B)' },
                              { value: 'difference', label: '차집합 (A-B)' },
                            ]}
                          />
                        </Col>
                        <Col>
                          <Text strong>Group B: 제외 조건</Text>
                        </Col>
                        <Col>
                          <Button type="primary" onClick={executeSetOperation} loading={setOpLoading} size="small">실행</Button>
                        </Col>
                      </Row>

                      {setOpResult && (
                        <>
                          <div style={{ display: 'flex', justifyContent: 'center' }}>
                            <VennDiagram
                              countA={setOpResult.count_a}
                              countB={setOpResult.count_b}
                              countOverlap={setOpResult.count_overlap}
                              labelA="Group A (선정)"
                              labelB="Group B (제외)"
                            />
                          </div>
                          <Row gutter={16}>
                            <Col span={8}><Statistic title="A 전체" value={setOpResult.count_a} suffix="명" /></Col>
                            <Col span={8}><Statistic title="B 전체" value={setOpResult.count_b} suffix="명" /></Col>
                            <Col span={8}>
                              <Statistic
                                title={`결과 (${setOpResult.operation})`}
                                value={setOpResult.count_result}
                                suffix="명"
                                valueStyle={{ color: '#1890ff', fontWeight: 'bold' }}
                              />
                            </Col>
                          </Row>
                        </>
                      )}
                    </Space>
                  ),
                },
              ]}
            />
          </Card>

          {/* Summary stats */}
          {(statsLoading || stats) && (
            <Card title="코호트 요약 통계" style={{ marginTop: 16 }}>
              {statsLoading ? (
                <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>
              ) : stats ? (
                <Row gutter={16}>
                  <Col span={12}>
                    <Title level={5}>성별 분포</Title>
                    <ResponsiveContainer width="100%" height={200}>
                      <PieChart>
                        <Pie
                          data={stats.gender_dist.map(g => ({ name: g.gender === 'M' ? '남성' : '여성', value: g.count }))}
                          cx="50%" cy="50%" outerRadius={70}
                          dataKey="value" label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                        >
                          {stats.gender_dist.map((_, i) => (
                            <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                          ))}
                        </Pie>
                        <Tooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </Col>
                  <Col span={12}>
                    <Title level={5}>연령대 분포</Title>
                    <ResponsiveContainer width="100%" height={200}>
                      <BarChart data={stats.age_dist}>
                        <XAxis dataKey="age_group" />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="count" fill="#1890ff" />
                      </BarChart>
                    </ResponsiveContainer>
                  </Col>
                </Row>
              ) : null}
            </Card>
          )}
        </Col>
      </Row>

      {/* Drill-down modal */}
      <Modal
        title={`환자 목록 — ${drillStepLabel}`}
        open={drillModalOpen}
        onCancel={() => setDrillModalOpen(false)}
        footer={null}
        width={700}
      >
        <Table
          loading={drillLoading}
          dataSource={drillPatients}
          rowKey="person_id"
          size="small"
          pagination={{ pageSize: 10 }}
          columns={[
            { title: 'Person ID', dataIndex: 'person_id', key: 'person_id' },
            { title: '성별', dataIndex: 'gender', key: 'gender', render: (v: string) => <Tag color={v === 'M' ? 'blue' : 'pink'}>{v === 'M' ? '남성' : '여성'}</Tag> },
            { title: '출생연도', dataIndex: 'birth_year', key: 'birth_year' },
            { title: '나이', dataIndex: 'age', key: 'age', render: (v: number) => `${v}세` },
          ]}
        />
      </Modal>
    </Space>
  );
};

export default CohortBuilder;
