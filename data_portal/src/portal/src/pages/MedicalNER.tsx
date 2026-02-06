/**
 * 의료 NER 데모 페이지
 * PRD AAR-001 §2 기반 - BioClinicalBERT, CodeMapper, AutoTagger
 * 프론트엔드 전용 Mock NER 처리
 */

import React, { useState, useCallback } from 'react';
import {
  Card, Typography, Space, Row, Col, Button, Input, Tag, Table, Statistic, Alert,
} from 'antd';
import {
  ExperimentOutlined, ThunderboltOutlined, FileSearchOutlined,
  MedicineBoxOutlined, CheckCircleOutlined,
} from '@ant-design/icons';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

// ── 엔티티 타입 정의 ──
interface NEREntity {
  text: string;
  type: 'condition' | 'drug' | 'measurement' | 'procedure' | 'person';
  start: number;
  end: number;
  omopConcept: string;
  standardCode: string;
  codeSystem: string;
  confidence: number;
}

// ── 엔티티 색상 맵 ──
const ENTITY_COLORS: Record<string, { bg: string; border: string; text: string; label: string }> = {
  condition: { bg: '#fff1f0', border: '#ffa39e', text: '#cf1322', label: '진단' },
  drug: { bg: '#e6f7ff', border: '#91d5ff', text: '#096dd9', label: '약물' },
  measurement: { bg: '#f6ffed', border: '#b7eb8f', text: '#389e0d', label: '검사' },
  procedure: { bg: '#f9f0ff', border: '#d3adf7', text: '#722ed1', label: '시술' },
  person: { bg: '#fff7e6', border: '#ffd591', text: '#d46b08', label: '인물' },
};

// ── 사전 정의 NER 패턴 ──
interface NERPattern {
  pattern: RegExp;
  type: NEREntity['type'];
  omopConcept: string;
  standardCode: string;
  codeSystem: string;
  confidence: number;
}

const NER_PATTERNS: NERPattern[] = [
  // 진단 (Conditions)
  { pattern: /2형 당뇨병/g, type: 'condition', omopConcept: 'Type 2 diabetes mellitus', standardCode: 'E11.9', codeSystem: 'ICD-10', confidence: 0.96 },
  { pattern: /제2형 당뇨병/g, type: 'condition', omopConcept: 'Type 2 diabetes mellitus', standardCode: 'E11.9', codeSystem: 'ICD-10', confidence: 0.96 },
  { pattern: /고혈압/g, type: 'condition', omopConcept: 'Essential hypertension', standardCode: 'I10', codeSystem: 'ICD-10', confidence: 0.94 },
  { pattern: /Cardiomegaly/gi, type: 'condition', omopConcept: 'Cardiomegaly', standardCode: 'I51.7', codeSystem: 'ICD-10', confidence: 0.91 },
  { pattern: /심부전/g, type: 'condition', omopConcept: 'Heart failure', standardCode: 'I50.9', codeSystem: 'ICD-10', confidence: 0.93 },
  { pattern: /폐렴/g, type: 'condition', omopConcept: 'Pneumonia', standardCode: 'J18.9', codeSystem: 'ICD-10', confidence: 0.95 },
  { pattern: /급성 심근경색/g, type: 'condition', omopConcept: 'Acute myocardial infarction', standardCode: 'I21.9', codeSystem: 'ICD-10', confidence: 0.97 },
  { pattern: /만성 신장질환/g, type: 'condition', omopConcept: 'Chronic kidney disease', standardCode: 'N18.9', codeSystem: 'ICD-10', confidence: 0.94 },
  { pattern: /관상동맥질환/g, type: 'condition', omopConcept: 'Coronary artery disease', standardCode: 'I25.10', codeSystem: 'ICD-10', confidence: 0.93 },
  { pattern: /협심증/g, type: 'condition', omopConcept: 'Angina pectoris', standardCode: 'I20.9', codeSystem: 'ICD-10', confidence: 0.92 },

  // 약물 (Drugs)
  { pattern: /Metformin\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Metformin', standardCode: '6809', codeSystem: 'RxNorm', confidence: 0.98 },
  { pattern: /Aspirin\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Aspirin', standardCode: '1191', codeSystem: 'RxNorm', confidence: 0.97 },
  { pattern: /Glimepiride\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Glimepiride', standardCode: '25789', codeSystem: 'RxNorm', confidence: 0.95 },
  { pattern: /Atorvastatin\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Atorvastatin', standardCode: '83367', codeSystem: 'RxNorm', confidence: 0.96 },
  { pattern: /Amlodipine\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Amlodipine', standardCode: '17767', codeSystem: 'RxNorm', confidence: 0.95 },
  { pattern: /Losartan\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Losartan', standardCode: '52175', codeSystem: 'RxNorm', confidence: 0.94 },
  { pattern: /Clopidogrel\s*\d*\s*mg/gi, type: 'drug', omopConcept: 'Clopidogrel', standardCode: '32968', codeSystem: 'RxNorm', confidence: 0.96 },
  { pattern: /Nitroglycerin/gi, type: 'drug', omopConcept: 'Nitroglycerin', standardCode: '7832', codeSystem: 'RxNorm', confidence: 0.93 },

  // 검사 (Measurements)
  { pattern: /HbA1c\s*[\d.]+%?/g, type: 'measurement', omopConcept: 'Hemoglobin A1c', standardCode: '4548-4', codeSystem: 'LOINC', confidence: 0.97 },
  { pattern: /LDL\s*\d+\s*mg\/dL/g, type: 'measurement', omopConcept: 'LDL Cholesterol', standardCode: '2089-1', codeSystem: 'LOINC', confidence: 0.95 },
  { pattern: /eGFR\s*\d+/g, type: 'measurement', omopConcept: 'Glomerular filtration rate', standardCode: '48642-3', codeSystem: 'LOINC', confidence: 0.93 },
  { pattern: /BNP\s*\d+\s*pg\/mL/g, type: 'measurement', omopConcept: 'Brain natriuretic peptide', standardCode: '30934-4', codeSystem: 'LOINC', confidence: 0.94 },
  { pattern: /Troponin[- ]?I\s*[\d.]+\s*ng\/mL/gi, type: 'measurement', omopConcept: 'Troponin I', standardCode: '10839-9', codeSystem: 'LOINC', confidence: 0.96 },
  { pattern: /CRP\s*[\d.]+\s*mg\/L/g, type: 'measurement', omopConcept: 'C-reactive protein', standardCode: '1988-5', codeSystem: 'LOINC', confidence: 0.94 },
  { pattern: /Creatinine\s*[\d.]+\s*mg\/dL/gi, type: 'measurement', omopConcept: 'Creatinine', standardCode: '2160-0', codeSystem: 'LOINC', confidence: 0.95 },
  { pattern: /WBC\s*[\d.]+/g, type: 'measurement', omopConcept: 'White blood cell count', standardCode: '6690-2', codeSystem: 'LOINC', confidence: 0.93 },

  // 시술 (Procedures)
  { pattern: /심초음파/g, type: 'procedure', omopConcept: 'Echocardiography', standardCode: '40701008', codeSystem: 'SNOMED CT', confidence: 0.94 },
  { pattern: /흉부\s*X-?ray/g, type: 'procedure', omopConcept: 'Chest X-ray', standardCode: '399208008', codeSystem: 'SNOMED CT', confidence: 0.96 },
  { pattern: /관상동맥 조영술/g, type: 'procedure', omopConcept: 'Coronary angiography', standardCode: '33367005', codeSystem: 'SNOMED CT', confidence: 0.95 },
  { pattern: /스텐트 삽입술/g, type: 'procedure', omopConcept: 'Coronary stent insertion', standardCode: '36969009', codeSystem: 'SNOMED CT', confidence: 0.93 },
  { pattern: /CT\s*(촬영|스캔)/g, type: 'procedure', omopConcept: 'CT scan', standardCode: '77477000', codeSystem: 'SNOMED CT', confidence: 0.92 },

  // 인물 (PII)
  { pattern: /[가-힣]{2,4}(?=\s*환자|\s*씨|\(|,\s*만?\s*\d)/g, type: 'person', omopConcept: 'Person name (PII)', standardCode: '-', codeSystem: 'PII', confidence: 0.88 },
];

// ── 예시 텍스트 ──
const SAMPLE_TEXTS = [
  {
    label: '당뇨 진료기록',
    text: '55세 남성 환자 홍길동, 2형 당뇨병 진단. HbA1c 7.8%, LDL 145mg/dL, eGFR 68. Metformin 500mg bid 처방, Glimepiride 2mg qd 추가. 고혈압 동반되어 Amlodipine 5mg qd 병용.',
  },
  {
    label: '영상의학 소견',
    text: '흉부 X-ray 소견: Cardiomegaly 의심. 심초음파 추가 검사 필요. BNP 450 pg/mL 상승 소견. 심부전 가능성 높음. Losartan 50mg qd, Aspirin 100mg qd 처방.',
  },
  {
    label: '심장내과 경과기록',
    text: '62세 남성 김철수, 급성 심근경색 진단. Troponin-I 2.8 ng/mL 상승. 관상동맥 조영술 시행, 좌전하행지 90% 협착 확인. 스텐트 삽입술 시행. Clopidogrel 75mg qd, Atorvastatin 40mg qd 처방. Creatinine 1.4 mg/dL.',
  },
  {
    label: '혈액검사 결과',
    text: '박영희 환자, 만성 신장질환 경과 관찰. Creatinine 2.1 mg/dL, eGFR 32, CRP 3.5 mg/L, WBC 8.2. HbA1c 8.1%, LDL 162mg/dL. 관상동맥질환 및 협심증 병력. Nitroglycerin SL 처방.',
  },
];

// ── Mock NER 처리 로직 ──
function runMockNER(text: string): NEREntity[] {
  const entities: NEREntity[] = [];
  const usedRanges: { start: number; end: number }[] = [];

  for (const p of NER_PATTERNS) {
    // Reset regex lastIndex
    p.pattern.lastIndex = 0;
    let match;
    while ((match = p.pattern.exec(text)) !== null) {
      const start = match.index;
      const end = match.index + match[0].length;

      // Skip overlapping matches
      const overlaps = usedRanges.some(
        (r) => (start >= r.start && start < r.end) || (end > r.start && end <= r.end)
      );
      if (overlaps) continue;

      usedRanges.push({ start, end });
      entities.push({
        text: match[0],
        type: p.type,
        start,
        end,
        omopConcept: p.omopConcept,
        standardCode: p.standardCode,
        codeSystem: p.codeSystem,
        confidence: p.confidence + (Math.random() * 0.04 - 0.02), // slight variance
      });
    }
  }

  return entities.sort((a, b) => a.start - b.start);
}

// ── 하이라이트 렌더러 ──
function renderHighlightedText(text: string, entities: NEREntity[]): React.ReactNode {
  if (entities.length === 0) return <span>{text}</span>;

  const parts: React.ReactNode[] = [];
  let lastIndex = 0;

  entities.forEach((entity, i) => {
    if (entity.start > lastIndex) {
      parts.push(<span key={`t-${i}`}>{text.slice(lastIndex, entity.start)}</span>);
    }
    const color = ENTITY_COLORS[entity.type];
    parts.push(
      <span
        key={`e-${i}`}
        style={{
          background: color.bg,
          border: `1px solid ${color.border}`,
          color: color.text,
          fontWeight: 600,
          padding: '1px 4px',
          borderRadius: 3,
          cursor: 'pointer',
        }}
        title={`${color.label}: ${entity.omopConcept} (${entity.codeSystem}: ${entity.standardCode})`}
      >
        {text.slice(entity.start, entity.end)}
        <sup style={{ fontSize: 9, marginLeft: 2, opacity: 0.7 }}>{color.label}</sup>
      </span>
    );
    lastIndex = entity.end;
  });

  if (lastIndex < text.length) {
    parts.push(<span key="end">{text.slice(lastIndex)}</span>);
  }

  return parts;
}

// ═══════════════════════════════════
// 메인 컴포넌트
// ═══════════════════════════════════
const MedicalNER: React.FC = () => {
  const [inputText, setInputText] = useState('');
  const [entities, setEntities] = useState<NEREntity[]>([]);
  const [analyzed, setAnalyzed] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [processTime, setProcessTime] = useState(0);

  const handleAnalyze = useCallback(() => {
    if (!inputText.trim()) return;
    setProcessing(true);
    setAnalyzed(false);

    // Simulate realistic model inference delay (2~3초)
    const start = performance.now();
    const delay = 2000 + Math.random() * 1000;
    setTimeout(() => {
      const result = runMockNER(inputText);
      const elapsed = performance.now() - start;
      setEntities(result);
      setProcessTime(Math.round(elapsed));
      setAnalyzed(true);
      setProcessing(false);
    }, delay);
  }, [inputText]);

  const handleSample = useCallback((text: string) => {
    setInputText(text);
    setAnalyzed(false);
    setEntities([]);
  }, []);

  const mappedCount = entities.filter((e) => e.standardCode !== '-').length;
  const mappingRate = entities.length > 0 ? Math.round((mappedCount / entities.length) * 100) : 0;

  // Group by code system for CodeMapper section
  const codeSystemGroups = entities.reduce<Record<string, NEREntity[]>>((acc, e) => {
    if (e.codeSystem !== 'PII') {
      if (!acc[e.codeSystem]) acc[e.codeSystem] = [];
      acc[e.codeSystem].push(e);
    }
    return acc;
  }, {});

  const entityColumns = [
    { title: '텍스트', dataIndex: 'text', key: 'text', render: (v: string, r: NEREntity) => {
      const c = ENTITY_COLORS[r.type];
      return <Text strong style={{ color: c.text }}>{v}</Text>;
    }},
    { title: '유형', dataIndex: 'type', key: 'type', render: (v: string) => {
      const c = ENTITY_COLORS[v];
      return <Tag color={c.border} style={{ color: c.text, borderColor: c.border }}>{c.label}</Tag>;
    }},
    { title: 'OMOP Concept', dataIndex: 'omopConcept', key: 'omopConcept', render: (v: string) => <Text style={{ fontSize: 12 }}>{v}</Text> },
    { title: '표준코드', dataIndex: 'standardCode', key: 'standardCode', render: (v: string, r: NEREntity) =>
      <Tag color={v === '-' ? 'default' : 'blue'}>{r.codeSystem}: {v}</Tag>
    },
    { title: '신뢰도', dataIndex: 'confidence', key: 'confidence', render: (v: number) => {
      const pct = Math.round(v * 100);
      return <Tag color={pct >= 95 ? 'green' : pct >= 90 ? 'blue' : 'orange'}>{pct}%</Tag>;
    }},
  ];

  return (
    <div>
      {/* 헤더 */}
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <ExperimentOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              비정형 데이터 구조화 (Medical NER)
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              의무기록 텍스트에서 의료 개체명을 추출하고 표준 코드에 매핑합니다
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Row gutter={16}>
        {/* 입력 영역 */}
        <Col xs={24} lg={10}>
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Card title={<><FileSearchOutlined /> 의무기록 텍스트 입력</>} size="small">
              <TextArea
                rows={8}
                value={inputText}
                onChange={(e) => { setInputText(e.target.value); setAnalyzed(false); }}
                placeholder="의무기록 텍스트를 입력하거나 아래 예시를 선택하세요..."
                style={{ fontFamily: 'monospace', fontSize: 13 }}
              />
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                onClick={handleAnalyze}
                loading={processing}
                disabled={!inputText.trim()}
                style={{ marginTop: 12, width: '100%' }}
                size="large"
              >
                NER 분석 실행
              </Button>
            </Card>

            <Card title="예시 텍스트" size="small">
              <Space direction="vertical" style={{ width: '100%' }}>
                {SAMPLE_TEXTS.map((s, i) => (
                  <Button
                    key={i}
                    block
                    size="small"
                    onClick={() => handleSample(s.text)}
                    style={{ textAlign: 'left', height: 'auto', whiteSpace: 'normal', padding: '8px 12px' }}
                  >
                    <Text strong style={{ fontSize: 12 }}>{s.label}</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: 11 }}>{s.text.slice(0, 60)}...</Text>
                  </Button>
                ))}
              </Space>
            </Card>

            {/* Legend */}
            <Card size="small" title="엔티티 범례">
              <Space wrap>
                {Object.entries(ENTITY_COLORS).map(([key, c]) => (
                  <Tag key={key} style={{ background: c.bg, color: c.text, border: `1px solid ${c.border}` }}>
                    {c.label}
                  </Tag>
                ))}
              </Space>
            </Card>
          </Space>
        </Col>

        {/* 결과 영역 */}
        <Col xs={24} lg={14}>
          {!analyzed && !processing && (
            <Card style={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ textAlign: 'center', color: '#999' }}>
                <ExperimentOutlined style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }} />
                <br />
                <Text type="secondary">텍스트를 입력하고 "NER 분석 실행" 버튼을 클릭하세요</Text>
              </div>
            </Card>
          )}

          {processing && (
            <Card style={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ textAlign: 'center' }}>
                <MedicineBoxOutlined spin style={{ fontSize: 48, color: '#005BAC', marginBottom: 16 }} />
                <br />
                <Text>BioClinicalBERT 모델 분석 중...</Text>
              </div>
            </Card>
          )}

          {analyzed && (
            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              {/* 통계 요약 */}
              <Row gutter={12}>
                <Col span={8}>
                  <Card size="small">
                    <Statistic title="총 엔티티" value={entities.length} suffix="개" prefix={<ExperimentOutlined />} />
                  </Card>
                </Col>
                <Col span={8}>
                  <Card size="small">
                    <Statistic
                      title="매핑 성공률"
                      value={mappingRate}
                      suffix="%"
                      prefix={<CheckCircleOutlined />}
                      valueStyle={{ color: '#3f8600' }}
                    />
                  </Card>
                </Col>
                <Col span={8}>
                  <Card size="small">
                    <Statistic title="처리 시간" value={processTime} suffix="ms" prefix={<ThunderboltOutlined />} />
                  </Card>
                </Col>
              </Row>

              {/* 하이라이트 원문 */}
              <Card title="개체명 인식 결과" size="small">
                <div style={{
                  fontFamily: 'monospace',
                  fontSize: 14,
                  lineHeight: 2.2,
                  padding: 12,
                  background: '#fafafa',
                  borderRadius: 6,
                  border: '1px solid #f0f0f0',
                }}>
                  {renderHighlightedText(inputText, entities)}
                </div>
              </Card>

              {/* 추출 엔티티 테이블 */}
              <Card title="추출된 엔티티 목록" size="small">
                <Table
                  columns={entityColumns}
                  dataSource={entities.map((e, i) => ({ ...e, key: i }))}
                  size="small"
                  pagination={false}
                  scroll={{ y: 300 }}
                />
              </Card>

              {/* CodeMapper 결과 */}
              <Card title={<><MedicineBoxOutlined /> CodeMapper — 표준 코드 매핑</>} size="small">
                {entities.some((e) => e.type === 'person') && (
                  <Alert
                    message="PII 탐지"
                    description="개인정보(인물명)가 탐지되었습니다. 비식별화 처리가 필요합니다."
                    type="warning"
                    showIcon
                    style={{ marginBottom: 12 }}
                  />
                )}
                <Row gutter={12}>
                  {Object.entries(codeSystemGroups).map(([system, ents]) => (
                    <Col xs={24} sm={12} md={6} key={system}>
                      <Card
                        size="small"
                        title={<Tag color="blue">{system}</Tag>}
                        style={{ marginBottom: 8 }}
                      >
                        {ents.map((e, i) => (
                          <div key={i} style={{ marginBottom: 4 }}>
                            <Text style={{ fontSize: 12 }}>{e.text}</Text>
                            <br />
                            <Text code style={{ fontSize: 11 }}>{e.standardCode}</Text>
                          </div>
                        ))}
                      </Card>
                    </Col>
                  ))}
                </Row>
              </Card>
            </Space>
          )}
        </Col>
      </Row>
    </div>
  );
};

export default MedicalNER;
