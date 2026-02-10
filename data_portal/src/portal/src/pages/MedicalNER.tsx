/**
 * 비정형 데이터 구조화 페이지
 * PRD AAR-001 §2 — BioClinicalBERT NER + 임상노트 구조화 + DICOM 파싱
 * 탭: NER 분석 | 임상노트 구조화 | DICOM 파싱 | 처리 이력
 */

import React, { useState, useCallback, useEffect } from 'react';
import {
  Card, Typography, Space, Row, Col, Button, Input, Tag, Table, Statistic, Alert, Badge,
  Tabs, Upload, Select, Descriptions, Spin, App,
} from 'antd';
import type { MessageInstance } from 'antd/es/message/interface';
import {
  ExperimentOutlined, ThunderboltOutlined, FileSearchOutlined,
  MedicineBoxOutlined, CheckCircleOutlined, ApiOutlined,
  FileTextOutlined, CloudUploadOutlined, HistoryOutlined, DatabaseOutlined,
  InboxOutlined,
} from '@ant-design/icons';
import { fetchPost, getCsrfToken } from '../services/apiUtils';
import {
  NEREntity, TextProcessResult, DicomResult, JobRecord, PipelineStats,
  ENTITY_COLORS, SAMPLE_TEXTS, SECTION_LABELS, SOURCE_TYPES, DICOM_META_LABELS,
  STATUS_COLORS, TYPE_LABELS,
  NER_API_BASE, UNSTRUCT_API_BASE,
  callNERApi, checkNERHealth, renderHighlightedText,
} from '../components/ner/NERHelpers';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const { Dragger } = Upload;

// ══════════════════════════════════════════
// Tab 1: NER 분석 (기존 기능)
// ══════════════════════════════════════════
const NERAnalysisTab: React.FC = () => {
  const [inputText, setInputText] = useState('');
  const [entities, setEntities] = useState<NEREntity[]>([]);
  const [analyzed, setAnalyzed] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [processTime, setProcessTime] = useState(0);
  const [usedModel, setUsedModel] = useState('');
  const [apiError, setApiError] = useState<string | null>(null);

  const handleAnalyze = useCallback(async () => {
    if (!inputText.trim()) return;
    setProcessing(true);
    setAnalyzed(false);
    setApiError(null);

    const start = performance.now();
    const [apiResult] = await Promise.all([
      callNERApi(inputText),
      new Promise((r) => setTimeout(r, 500)),
    ]);
    const elapsed = Math.round(performance.now() - start);

    if (apiResult) {
      setEntities(apiResult.entities);
      setProcessTime(elapsed);
      setUsedModel(apiResult.model);
      setAnalyzed(true);
    } else {
      setEntities([]);
      setProcessTime(elapsed);
      setUsedModel('');
      setApiError('NER 백엔드 서비스에 연결할 수 없습니다. GPU 서버 및 SSH 터널 상태를 확인해주세요.');
    }
    setProcessing(false);
  }, [inputText]);

  const handleSample = useCallback((text: string) => {
    setInputText(text);
    setAnalyzed(false);
    setEntities([]);
  }, []);

  const mappedCount = entities.filter((e) => e.standardCode !== '-').length;
  const mappingRate = entities.length > 0 ? Math.round((mappedCount / entities.length) * 100) : 0;

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
    <Row gutter={16}>
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

      <Col xs={24} lg={14}>
        {apiError && (
          <Alert message="NER 서비스 오류" description={apiError} type="error" showIcon closable onClose={() => setApiError(null)} style={{ marginBottom: 16 }} />
        )}

        {!analyzed && !processing && !apiError && (
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
            {usedModel && <Alert message={`모델: ${usedModel}`} type="success" showIcon style={{ marginBottom: 0 }} />}

            <Row gutter={12}>
              <Col span={8}>
                <Card size="small"><Statistic title="총 엔티티" value={entities.length} suffix="개" prefix={<ExperimentOutlined />} /></Card>
              </Col>
              <Col span={8}>
                <Card size="small"><Statistic title="매핑 성공률" value={mappingRate} suffix="%" prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} /></Card>
              </Col>
              <Col span={8}>
                <Card size="small"><Statistic title="처리 시간" value={processTime} suffix="ms" prefix={<ThunderboltOutlined />} /></Card>
              </Col>
            </Row>

            <Card title="개체명 인식 결과" size="small">
              <div style={{ fontFamily: 'monospace', fontSize: 14, lineHeight: 2.2, padding: 12, background: '#fafafa', borderRadius: 6, border: '1px solid #f0f0f0' }}>
                {renderHighlightedText(inputText, entities)}
              </div>
            </Card>

            <Card title="추출된 엔티티 목록" size="small">
              <Table columns={entityColumns} dataSource={entities.map((e, i) => ({ ...e, key: i }))} size="small" pagination={false} scroll={{ y: 300 }} />
            </Card>

            <Card title={<><MedicineBoxOutlined /> CodeMapper — 표준 코드 매핑</>} size="small">
              {entities.some((e) => e.type === 'person') && (
                <Alert message="PII 탐지" description="개인정보(인물명)가 탐지되었습니다. 비식별화 처리가 필요합니다." type="warning" showIcon style={{ marginBottom: 12 }} />
              )}
              <Row gutter={12}>
                {Object.entries(codeSystemGroups).map(([system, ents]) => (
                  <Col xs={24} sm={12} md={6} key={system}>
                    <Card size="small" title={<Tag color="blue">{system}</Tag>} style={{ marginBottom: 8 }}>
                      {ents.map((e, i) => (
                        <div key={i} style={{ marginBottom: 4 }}>
                          <Text style={{ fontSize: 12 }}>{e.text}</Text><br />
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
  );
};


// ══════════════════════════════════════════
// Tab 2: 임상노트 구조화
// ══════════════════════════════════════════
const ClinicalNoteTab: React.FC<{ messageApi: MessageInstance }> = ({ messageApi }) => {
  const [text, setText] = useState('');
  const [sourceType, setSourceType] = useState('경과기록');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<TextProcessResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleProcess = useCallback(async () => {
    if (!text.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const resp = await fetchPost(`${UNSTRUCT_API_BASE}/process/text`, {
        text,
        source_type: sourceType,
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: '처리 실패' }));
        throw new Error(err.detail || '처리 실패');
      }
      const data: TextProcessResult = await resp.json();
      setResult(data);
      messageApi.success(`구조화 완료 — ${data.entities.length}개 엔티티, OMOP ${data.omop.note_nlp_count}건 적재`);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : '처리 실패';
      setError(msg);
    } finally {
      setLoading(false);
    }
  }, [text, sourceType, messageApi]);

  return (
    <Row gutter={16}>
      <Col xs={24} lg={10}>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Card title={<><FileTextOutlined /> 임상노트 입력</>} size="small">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Select value={sourceType} onChange={setSourceType} style={{ width: '100%' }}>
                {SOURCE_TYPES.map(t => <Select.Option key={t} value={t}>{t}</Select.Option>)}
              </Select>
              <TextArea
                rows={10}
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="임상노트 텍스트를 입력하세요..."
                style={{ fontFamily: 'monospace', fontSize: 13 }}
              />
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                onClick={handleProcess}
                loading={loading}
                disabled={!text.trim()}
                block
                size="large"
              >
                구조화 실행 (LLM + NER + OMOP 적재)
              </Button>
            </Space>
          </Card>
          <Card title="예시 텍스트" size="small">
            <Space direction="vertical" style={{ width: '100%' }}>
              {SAMPLE_TEXTS.map((s, i) => (
                <Button key={i} block size="small" onClick={() => setText(s.text)} style={{ textAlign: 'left', height: 'auto', whiteSpace: 'normal', padding: '8px 12px' }}>
                  <Text strong style={{ fontSize: 12 }}>{s.label}</Text><br />
                  <Text type="secondary" style={{ fontSize: 11 }}>{s.text.slice(0, 60)}...</Text>
                </Button>
              ))}
            </Space>
          </Card>
        </Space>
      </Col>
      <Col xs={24} lg={14}>
        {error && <Alert message="처리 오류" description={error} type="error" showIcon closable onClose={() => setError(null)} style={{ marginBottom: 16 }} />}

        {loading && (
          <Card style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin size="large" tip="LLM 섹션 분리 + NER 분석 + OMOP 적재 중..."><div style={{ minWidth: 300, minHeight: 80 }} /></Spin>
          </Card>
        )}

        {!result && !loading && !error && (
          <Card style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div style={{ textAlign: 'center', color: '#999' }}>
              <FileTextOutlined style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }} />
              <br />
              <Text type="secondary">임상노트를 입력하고 "구조화 실행" 버튼을 클릭하세요</Text>
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>LLM 섹션 분리 → NER 엔티티 추출 → OMOP note/note_nlp 적재</Text>
            </div>
          </Card>
        )}

        {result && (
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Row gutter={12}>
              <Col span={6}><Card size="small"><Statistic title="Job ID" value={result.job_id} prefix={<DatabaseOutlined />} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="엔티티" value={result.entities.length} suffix="개" prefix={<ExperimentOutlined />} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="OMOP 적재" value={result.omop.note_nlp_count} suffix="건" valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="처리시간" value={result.processing_time_ms} suffix="ms" prefix={<ThunderboltOutlined />} /></Card></Col>
            </Row>

            <Card title="섹션 분리 결과 (DS LLM)" size="small">
              <Descriptions column={1} bordered size="small">
                {Object.entries(result.sections).map(([key, val]) => (
                  <Descriptions.Item key={key} label={SECTION_LABELS[key] || key}>
                    {val || <Text type="secondary">-</Text>}
                  </Descriptions.Item>
                ))}
              </Descriptions>
            </Card>

            <Card title="NER 엔티티 (BioClinicalBERT)" size="small">
              <div style={{ fontFamily: 'monospace', fontSize: 14, lineHeight: 2.2, padding: 12, background: '#fafafa', borderRadius: 6, border: '1px solid #f0f0f0', marginBottom: 12 }}>
                {renderHighlightedText(text, result.entities as NEREntity[])}
              </div>
              <Table
                columns={[
                  { title: '텍스트', dataIndex: 'text', key: 'text' },
                  { title: '유형', dataIndex: 'type', key: 'type', render: (v: string) => { const c = ENTITY_COLORS[v]; return c ? <Tag style={{ background: c.bg, color: c.text }}>{c.label}</Tag> : v; }},
                  { title: 'OMOP', dataIndex: 'omopConcept', key: 'omop', render: (v: string) => <Text style={{ fontSize: 12 }}>{v}</Text> },
                  { title: '코드', dataIndex: 'standardCode', key: 'code', render: (v: string, r: { codeSystem?: string }) => <Tag color="blue">{r.codeSystem}: {v}</Tag> },
                ]}
                dataSource={(result.entities || []).map((e, i) => ({ ...e, key: i }))}
                size="small"
                pagination={false}
                scroll={{ y: 200 }}
              />
            </Card>

            <Alert
              message="OMOP CDM 적재 완료"
              description={`note 테이블: note_id=${result.omop.note_id} | note_nlp 테이블: ${result.omop.note_nlp_count}건 | S3: ${result.s3_key}`}
              type="success"
              showIcon
            />
          </Space>
        )}
      </Col>
    </Row>
  );
};


// ══════════════════════════════════════════
// Tab 3: DICOM 파싱
// ══════════════════════════════════════════
const DicomTab: React.FC<{ messageApi: MessageInstance }> = ({ messageApi }) => {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<DicomResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleUpload = useCallback(async (file: File) => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const formData = new FormData();
      formData.append('file', file);
      const csrfToken = getCsrfToken();
      const headers: Record<string, string> = {};
      if (csrfToken) headers['X-CSRF-Token'] = csrfToken;

      const resp = await fetch(`${UNSTRUCT_API_BASE}/process/dicom`, {
        method: 'POST',
        headers,
        body: formData,
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: 'DICOM 처리 실패' }));
        throw new Error(err.detail || 'DICOM 처리 실패');
      }
      const data: DicomResult = await resp.json();
      setResult(data);
      messageApi.success(`DICOM 메타 추출 완료 — imaging_study_id: ${data.omop.imaging_study_id}`);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'DICOM 처리 실패';
      setError(msg);
    } finally {
      setLoading(false);
    }
    return false; // prevent default upload
  }, [messageApi]);

  return (
    <Row gutter={16}>
      <Col xs={24} lg={10}>
        <Card title={<><CloudUploadOutlined /> DICOM 파일 업로드</>} size="small">
          <Dragger
            accept=".dcm,.dicom,.DCM"
            showUploadList={false}
            beforeUpload={(file) => { handleUpload(file); return false; }}
            disabled={loading}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">DICOM 파일을 드래그하거나 클릭하여 업로드</p>
            <p className="ant-upload-hint">.dcm, .dicom 파일 지원</p>
          </Dragger>
          <div style={{ marginTop: 16, padding: 12, background: '#f6f8fa', borderRadius: 6 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              파이프라인: DICOM 업로드 → pydicom 메타 추출 → MinIO 저장 → OMOP imaging_study 적재
            </Text>
          </div>
        </Card>
      </Col>
      <Col xs={24} lg={14}>
        {error && <Alert message="DICOM 오류" description={error} type="error" showIcon closable onClose={() => setError(null)} style={{ marginBottom: 16 }} />}

        {loading && (
          <Card style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin size="large" tip="DICOM 메타데이터 추출 중..."><div style={{ minWidth: 300, minHeight: 80 }} /></Spin>
          </Card>
        )}

        {!result && !loading && !error && (
          <Card style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div style={{ textAlign: 'center', color: '#999' }}>
              <CloudUploadOutlined style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }} />
              <br />
              <Text type="secondary">DICOM 파일을 업로드하면 메타데이터를 추출하고 OMOP에 적재합니다</Text>
            </div>
          </Card>
        )}

        {result && (
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Row gutter={12}>
              <Col span={6}><Card size="small"><Statistic title="Job ID" value={result.job_id} prefix={<DatabaseOutlined />} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="파일명" value={result.filename} valueStyle={{ fontSize: 14 }} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="OMOP ID" value={result.omop.imaging_study_id} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} /></Card></Col>
              <Col span={6}><Card size="small"><Statistic title="처리시간" value={result.processing_time_ms} suffix="ms" prefix={<ThunderboltOutlined />} /></Card></Col>
            </Row>

            <Card title="DICOM 메타데이터" size="small">
              <Descriptions column={2} bordered size="small">
                {Object.entries(result.dicom_meta).map(([key, val]) => (
                  <Descriptions.Item key={key} label={DICOM_META_LABELS[key] || key}>
                    {val !== null && val !== '' ? String(val) : <Text type="secondary">-</Text>}
                  </Descriptions.Item>
                ))}
              </Descriptions>
            </Card>

            <Alert
              message="OMOP CDM 적재 완료"
              description={`imaging_study 테이블: id=${result.omop.imaging_study_id} | S3: ${result.s3_key}`}
              type="success"
              showIcon
            />
          </Space>
        )}
      </Col>
    </Row>
  );
};


// ══════════════════════════════════════════
// Tab 4: 처리 이력
// ══════════════════════════════════════════
const JobHistoryTab: React.FC = () => {
  const [jobs, setJobs] = useState<JobRecord[]>([]);
  const [stats, setStats] = useState<PipelineStats | null>(null);
  const [loading, setLoading] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [jobsResp, statsResp] = await Promise.all([
        fetch(`${UNSTRUCT_API_BASE}/jobs?limit=50`),
        fetch(`${UNSTRUCT_API_BASE}/stats`),
      ]);
      if (jobsResp.ok) setJobs(await jobsResp.json());
      if (statsResp.ok) setStats(await statsResp.json());
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  const columns = [
    { title: 'ID', dataIndex: 'job_id', key: 'id', width: 60 },
    { title: '유형', dataIndex: 'job_type', key: 'type', width: 80, render: (v: string) => <Tag>{TYPE_LABELS[v] || v}</Tag> },
    { title: '소스', dataIndex: 'source_type', key: 'source', width: 100 },
    { title: '상태', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={STATUS_COLORS[v] || 'default'}>{v}</Tag> },
    { title: '입력 요약', dataIndex: 'input_summary', key: 'summary', ellipsis: true, render: (v: string | null) => <Text style={{ fontSize: 12 }}>{v ? v.slice(0, 80) : '-'}</Text> },
    { title: '추출', dataIndex: 'result_count', key: 'result', width: 60 },
    { title: 'OMOP', dataIndex: 'omop_records_created', key: 'omop', width: 60, render: (v: number) => <Text strong style={{ color: v > 0 ? '#3f8600' : '#999' }}>{v}</Text> },
    { title: '시간(ms)', dataIndex: 'processing_time_ms', key: 'time', width: 80 },
    { title: '생성일시', dataIndex: 'created_at', key: 'created', width: 160, render: (v: string | null) => v ? new Date(v).toLocaleString('ko-KR') : '-' },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {stats && (
        <Row gutter={12}>
          <Col span={4}><Card size="small"><Statistic title="전체 작업" value={stats.total_jobs} /></Card></Col>
          <Col span={4}><Card size="small"><Statistic title="텍스트" value={stats.by_type.text || 0} /></Card></Col>
          <Col span={4}><Card size="small"><Statistic title="DICOM" value={stats.by_type.dicom || 0} /></Card></Col>
          <Col span={4}><Card size="small"><Statistic title="완료" value={stats.by_status.completed || 0} valueStyle={{ color: '#3f8600' }} /></Card></Col>
          <Col span={4}><Card size="small"><Statistic title="OMOP note" value={stats.omop_records.note || 0} /></Card></Col>
          <Col span={4}><Card size="small"><Statistic title="OMOP imaging" value={stats.omop_records.imaging_study || 0} /></Card></Col>
        </Row>
      )}

      <Card
        title={<><HistoryOutlined /> 처리 이력</>}
        size="small"
        extra={<Button size="small" onClick={loadData} loading={loading}>새로고침</Button>}
      >
        <Table
          columns={columns}
          dataSource={jobs.map(j => ({ ...j, key: j.job_id }))}
          size="small"
          pagination={{ pageSize: 20, showSizeChanger: false }}
          loading={loading}
          scroll={{ x: 900 }}
        />
      </Card>
    </Space>
  );
};


// ═══════════════════════════════════════════
// 메인 컴포넌트
// ═══════════════════════════════════════════
const MedicalNER: React.FC = () => {
  const { message: messageApi } = App.useApp();
  const [backendConnected, setBackendConnected] = useState<boolean | null>(null);
  const [gpuDevice, setGpuDevice] = useState('');

  useEffect(() => {
    checkNERHealth().then(({ healthy, device }) => {
      setBackendConnected(healthy);
      if (device) setGpuDevice(device);
    });
    const interval = setInterval(() => {
      checkNERHealth().then(({ healthy, device }) => {
        setBackendConnected(healthy);
        if (device) setGpuDevice(device);
      });
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div>
      {/* 헤더 */}
      <Card style={{ marginBottom: 16 }}>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <ExperimentOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              비정형 데이터 구조화
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              의무기록 텍스트 NER, 임상노트 구조화(LLM+NER→OMOP), DICOM 메타 추출
            </Paragraph>
          </Col>
          <Col>
            <Space direction="vertical" align="end" size={4}>
              <Badge
                status={backendConnected === null ? 'default' : backendConnected ? 'success' : 'warning'}
                text={
                  <Text style={{ fontSize: 12 }}>
                    <ApiOutlined style={{ marginRight: 4 }} />
                    {backendConnected === null ? 'GPU 연결 확인 중...' : backendConnected ? 'BioClinicalBERT GPU 연결됨' : 'GPU 미연결 (SSH 터널 확인 필요)'}
                  </Text>
                }
              />
              {gpuDevice && backendConnected && (
                <Text type="secondary" style={{ fontSize: 11 }}>{gpuDevice}</Text>
              )}
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 탭 */}
      <Tabs
        defaultActiveKey="ner"
        type="card"
        items={[
          {
            key: 'ner',
            label: <><ExperimentOutlined /> NER 분석</>,
            children: <NERAnalysisTab />,
          },
          {
            key: 'clinical',
            label: <><FileTextOutlined /> 임상노트 구조화</>,
            children: <ClinicalNoteTab messageApi={messageApi} />,
          },
          {
            key: 'dicom',
            label: <><CloudUploadOutlined /> DICOM 파싱</>,
            children: <DicomTab messageApi={messageApi} />,
          },
          {
            key: 'history',
            label: <><HistoryOutlined /> 처리 이력</>,
            children: <JobHistoryTab />,
          },
        ]}
      />
    </div>
  );
};

export default MedicalNER;
