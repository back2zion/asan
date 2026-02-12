import React, { useState, useEffect, useRef } from 'react';
import { Card, Input, Button, Typography, Space, Row, Col, Alert, Divider, Tag, Spin, Select, App, Steps, Collapse } from 'antd';
import { SendOutlined, ClearOutlined, CopyOutlined, UserOutlined, CheckCircleOutlined, LoadingOutlined, DatabaseOutlined, CodeOutlined, TableOutlined, BulbOutlined } from '@ant-design/icons';
import { apiClient } from '../../services/apiUtils';
import ImageCell from '../common/ImageCell';
import ResultChart from '../common/ResultChart';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;

interface EnhancementResult {
  original_question: string;
  enhanced_question: string;
  enhancements_applied: string[];
  enhancement_confidence: number;
  sql: string;
  sql_explanation: string;
  sql_confidence: number;
  execution_result?: {
    results: any[];
    row_count: number;
    columns: string[];
    execution_time_ms: number;
    natural_language_explanation: string;
    error?: string;
  };
}

const Text2SQLTab: React.FC = () => {
  const { message } = App.useApp();
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<EnhancementResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingStep, setLoadingStep] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [userRole, setUserRole] = useState('clinician');
  const [showAllRows, setShowAllRows] = useState(false);
  const stepTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  // 마운트 시 이전 캐시 강제 삭제
  useEffect(() => {
    try { sessionStorage.removeItem('text2sql_question'); sessionStorage.removeItem('text2sql_result'); } catch {}
    return () => { if (stepTimer.current) clearInterval(stepTimer.current); };
  }, []);

  const handleEnhance = async () => {
    if (!question.trim()) return;
    setLoading(true);
    setLoadingStep(0);
    setError(null);
    setShowAllRows(false);

    // 단계별 진행 시뮬레이션 (실제 API 호출은 한 번)
    stepTimer.current = setInterval(() => {
      setLoadingStep(prev => (prev < 2 ? prev + 1 : prev));
    }, 1800);

    try {
      const response = await apiClient.post('/text2sql/enhanced-generate', {
        question: question.trim(),
        enhancement_type: 'medical',
        include_explanation: true,
        auto_execute: true
      });
      setLoadingStep(3); // 완료
      setResult(response.data);
    } catch (err) {
      setError('질의 처리 중 오류가 발생했습니다.');
    } finally {
      if (stepTimer.current) clearInterval(stepTimer.current);
      stepTimer.current = null;
      setLoading(false);
    }
  };

  const handleClear = () => {
    setQuestion('');
    setResult(null);
    setError(null);
    setShowAllRows(false);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return 'success';
    if (confidence >= 0.6) return 'warning';
    return 'error';
  };

  const exampleQuestionsByRole: Record<string, string[]> = {
    researcher: [
      "폐렴 소견 흉부 X-ray 보여줘",
      "고혈압 진단 환자 중 동시에 다른 진단을 보유한 환자의 동반질환 분포",
      "50세 이상 남성 환자 중 흉부 X-ray 촬영 이력이 있는 환자 목록과 소견",
      "2010년 이후 고혈압 환자의 연도별 외래 방문 건수 추이",
    ],
    clinician: [
      "급성 심근경색 환자의 연령별 분포",
      "고혈압 환자이면서 약물 처방 이력이 있는 환자 목록과 처방 약물 정보",
      "입원 환자 중 재원일수가 7일 이상인 환자의 진단 기록",
      "응급실 방문 후 입원으로 전환된 환자 수와 주요 진단명",
    ],
    analyst: [
      "연도별 외래, 입원, 응급 방문 건수 추이 비교",
      "성별, 연령대별 환자 수 분포 현황",
      "진단 코드별 환자 수 상위 10개 질환과 환자 비율",
      "환자당 평균 방문 횟수와 평균 진단 건수 통계",
    ],
    admin: [
      "OMOP CDM 주요 테이블별 전체 레코드 수 현황",
      "imaging_study 테이블의 전체 레코드 수와 소견 유형별 분포",
      "person 테이블의 성별 분포와 출생연도 범위",
      "방문 기록의 최초 및 최종 일자와 연간 데이터 건수 추이",
    ],
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* User Role Select */}
      <Card>
        <Space align="center">
          <UserOutlined style={{ color: '#006241', fontSize: '18px' }} />
          <Text strong style={{ color: '#333', fontSize: '16px' }}>사용자 역할:</Text>
          <Select
            value={userRole}
            onChange={setUserRole}
            style={{ width: 180 }}
            size="large"
            options={[
              { value: 'researcher', label: '연구자' },
              { value: 'clinician', label: '임상의' },
              { value: 'analyst', label: '데이터 분석가' },
              { value: 'admin', label: '시스템 관리자' }
            ]}
          />
        </Space>
      </Card>

      {/* Query Interface */}
      <Card title="자연어 → SQL 변환 (Text2SQL)" extra={
        <Space>
          <Button icon={<ClearOutlined />} onClick={handleClear}>초기화</Button>
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={handleEnhance}
            loading={loading}
            disabled={!question.trim()}
          >
            질의 실행
          </Button>
        </Space>
      }>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <TextArea
            placeholder="의료 질의를 자연어로 입력하세요... (예: 홍길동 환자의 당뇨 경과기록과 TG, 콜레스테롤 검사결과 보여주세요)"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            rows={3}
            showCount
            maxLength={1000}
            size="large"
            style={{ borderRadius: '6px', border: '1px solid #d1ecf1', fontSize: '16px', backgroundColor: '#ffffff' }}
          />
          <div>
            <Text strong style={{ color: '#333', fontSize: '16px' }}>예시 질의:</Text>
            <div style={{ marginTop: 12 }}>
              {(exampleQuestionsByRole[userRole] || []).map((text, index) => (
                <Tag
                  key={`${userRole}-${index}`}
                  style={{
                    margin: '4px 6px 4px 0', cursor: 'pointer', borderRadius: '6px',
                    padding: '8px 14px', border: '1px solid #006241', color: '#006241',
                    backgroundColor: '#f0f8f3', fontWeight: '400', fontSize: '15px',
                    lineHeight: '1.4', maxWidth: '100%', whiteSpace: 'normal' as const,
                    height: 'auto', transition: 'all 0.2s ease',
                  }}
                  onClick={() => setQuestion(text)}
                >
                  {text}
                </Tag>
              ))}
            </div>
          </div>
        </Space>
      </Card>

      {error && (
        <Alert message="오류 발생" description={error} type="error" showIcon closable onClose={() => setError(null)} />
      )}

      {/* 단계별 진행 표시 (로딩 중) */}
      {loading && (
        <Card>
          <Steps
            current={loadingStep}
            size="small"
            items={[
              {
                title: 'IT메타 기반 비즈메타 생성',
                description: loadingStep === 0 ? '스키마 분석 중...' : '분석 완료',
                icon: loadingStep === 0 ? <LoadingOutlined /> : <CheckCircleOutlined style={{ color: '#52c41a' }} />,
              },
              {
                title: 'SQL 생성',
                description: loadingStep === 1 ? '쿼리 생성 중...' : loadingStep > 1 ? '생성 완료' : '',
                icon: loadingStep === 1 ? <LoadingOutlined /> : loadingStep > 1 ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CodeOutlined />,
              },
              {
                title: '데이터 조회',
                description: loadingStep === 2 ? '실행 중...' : loadingStep > 2 ? '조회 완료' : '',
                icon: loadingStep === 2 ? <LoadingOutlined /> : loadingStep > 2 ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <TableOutlined />,
              },
            ]}
            style={{ padding: '24px 0' }}
          />
        </Card>
      )}

      {/* 결과 표시 */}
      {result && !loading && (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          {/* Step 1: 사고 과정 */}
          <Collapse
            size="small"
            items={[{
              key: 'thinking',
              label: (
                <Space>
                  <BulbOutlined style={{ color: '#faad14' }} />
                  <Text strong>사고 과정 보기</Text>
                  <Tag color="green">IT메타 기반 비즈메타 생성</Tag>
                </Space>
              ),
              children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                  {/* IT메타 — 스키마 분석 */}
                  <div>
                    <Text strong style={{ color: '#006241' }}>[IT메타 — 스키마 분석]</Text>
                    <div style={{ marginTop: 4, paddingLeft: 8 }}>
                      <Text type="secondary" style={{ fontSize: 15 }}>
                        {result.enhancements_applied && result.enhancements_applied.length > 0
                          ? result.enhancements_applied.map((e, i) => <div key={i}>• {e}</div>)
                          : <div>• 질의 의도 분석 완료</div>
                        }
                      </Text>
                    </div>
                  </div>
                  <Divider style={{ margin: '8px 0' }} />
                  {/* 비즈메타 — 업무 의미 */}
                  <div>
                    <Text strong style={{ color: '#006241' }}>[비즈메타 — 업무 의미 생성]</Text>
                    <div style={{ marginTop: 4, paddingLeft: 8 }}>
                      <Text type="secondary" style={{ fontSize: 15 }}>
                        • 원본: {result.original_question}
                      </Text>
                      <br />
                      <Text style={{ fontSize: 15, color: '#52c41a' }}>
                        • 강화: {result.enhanced_question}
                      </Text>
                    </div>
                  </div>
                  <div style={{ marginTop: 8 }}>
                    <Space>
                      <Tag color={getConfidenceColor(result.enhancement_confidence)}>
                        분석 신뢰도: {(result.enhancement_confidence * 100).toFixed(0)}%
                      </Tag>
                      <Tag color={getConfidenceColor(result.sql_confidence)}>
                        SQL 신뢰도: {(result.sql_confidence * 100).toFixed(0)}%
                      </Tag>
                    </Space>
                  </div>
                </Space>
              ),
            }]}
          />

          {/* Step 2: SQL 생성 */}
          <Card
            size="small"
            title={<Space><CodeOutlined style={{ color: '#1890ff' }} /><Text strong>SQL 생성</Text></Space>}
            extra={<Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyToClipboard(result.sql)}>복사</Button>}
          >
            <Card type="inner" style={{ background: '#f0f8ff', border: '1px solid #91d5ff' }}>
              <Text code style={{ whiteSpace: 'pre-wrap', fontSize: '14px' }}>{result.sql}</Text>
            </Card>
          </Card>

          {/* Step 3: 데이터 조회 */}
          {result.execution_result && (
            <Card
              size="small"
              title={
                <Space>
                  <TableOutlined style={{ color: '#52c41a' }} />
                  <Text strong>데이터 조회</Text>
                  <Tag color="success">{result.execution_result.row_count}건 조회 완료</Tag>
                  <Text type="secondary" style={{ fontSize: 14 }}>
                    {result.execution_result.execution_time_ms?.toFixed(0)}ms
                  </Text>
                </Space>
              }
            >
              {result.execution_result.error ? (
                <Alert message="SQL 실행 오류" description={result.execution_result.error} type="error" showIcon />
              ) : (
                <Space direction="vertical" style={{ width: '100%' }}>
                  {/* LLM 설명 완전 무시 — 실제 데이터에서 직접 생성 (할루시네이션 원천 차단) */}
                  {(() => {
                    const cols = result.execution_result!.columns || [];
                    const rows = result.execution_result!.results || [];
                    const fmt = (v: any) => {
                      if (v == null) return '없음';
                      if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(1);
                      return String(v);
                    };
                    let txt: string;
                    if (rows.length === 0) {
                      txt = '조회 결과가 없습니다.';
                    } else if (rows.length === 1 && cols.length === 1) {
                      txt = `「${result.original_question}」 조회 결과: ${fmt(rows[0][0])}`;
                    } else if (rows.length === 1 && cols.length <= 5) {
                      const parts = cols.map((c, i) => `${c}=${fmt(rows[0][i])}`);
                      txt = `「${result.original_question}」 조회 결과: ${parts.join(', ')}`;
                    } else {
                      txt = `「${result.original_question}」 ${rows.length}건 조회 완료`;
                    }
                    return <Alert message={txt} type="success" showIcon style={{ marginBottom: 8 }} />;
                  })()}
                  {result.execution_result.columns && result.execution_result.results && (
                    <ResultChart columns={result.execution_result.columns} results={result.execution_result.results} />
                  )}
                  {result.execution_result.results && result.execution_result.results.length > 0 && (
                    <div style={{ maxHeight: showAllRows ? '600px' : '300px', overflow: 'auto' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
                        <thead>
                          <tr style={{ background: '#fafafa', borderBottom: '1px solid #d9d9d9', position: 'sticky', top: 0, zIndex: 1 }}>
                            {result.execution_result.columns?.map((col, idx) => (
                              <th key={idx} style={{ padding: '8px', textAlign: 'left', border: '1px solid #d9d9d9', background: '#fafafa' }}>{col}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {(showAllRows ? result.execution_result.results : result.execution_result.results.slice(0, 10)).map((row, idx) => (
                            <tr key={idx} style={{ borderBottom: '1px solid #f0f0f0' }}>
                              {result.execution_result!.columns?.map((_, colIdx) => (
                                <td key={colIdx} style={{ padding: '8px', border: '1px solid #d9d9d9' }}>
                                  <ImageCell value={row[colIdx]} />
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {result.execution_result.results.length > 10 && (
                        <div style={{ textAlign: 'center', marginTop: 8 }}>
                          <Button type="link" onClick={() => setShowAllRows(!showAllRows)} style={{ color: '#006241' }}>
                            {showAllRows ? '접기' : `더보기 (총 ${result.execution_result.row_count}건)`}
                          </Button>
                        </div>
                      )}
                    </div>
                  )}
                </Space>
              )}
            </Card>
          )}
        </Space>
      )}
    </Space>
  );
};

export default Text2SQLTab;
